import asyncio
import os
import json
import gspread
import requests
import xml.etree.ElementTree as ET
import re
from google.oauth2.service_account import Credentials

# --- [설정] ---
SHEET_ID = '1s73BDNtCPe5mOs9EjBE5npEfcaNtYRyWJxRBUmJI-WA'

creds_json = json.loads(os.environ.get('GCP_SERVICE_ACCOUNT_KEY'))
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)
worksheet = sh.get_worksheet(0)

TEST_MODE = True
TEST_LIMIT = 3

SEIBRO_URL = "https://seibro.or.kr/IPORTAL/jsp/callServletService.jsp"
TASK = "ksd.safe.bip.cnts.bone.process.BondSecnDetailPTask"

SESSION = requests.Session()
SESSION.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'https://seibro.or.kr/websquare/control.jsp?w2xPath=/IPORTAL/user/bond/BIP_CNTS03005V.xml',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'X-Requested-With': 'XMLHttpRequest',
})

def seibro_call(action, isin):
    payload = f'<reqParam action="{action}" task="{TASK}"><ISIN value="{isin}"/><PAGE_ON_CNT value="100"/><PAGE_NUM value="1"/></reqParam>'
    try:
        r = SESSION.post(
            SEIBRO_URL,
            data={'reqParam': payload},
            timeout=10
        )
        r.raise_for_status()
        root = ET.fromstring(r.content)
        return root
    except Exception as e:
        print(f"  ⚠ 호출 실패 [{action}]: {e}")
        return None

def format_date(raw):
    if raw and len(raw) == 8 and raw.isdigit():
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    return raw or '-'

def extract_hosu(nm):
    m = re.search(r'제\s*(\d+)\s*회', nm or '')
    if m:
        return m.group(1)
    m = re.search(r'(\d+)회', nm or '')
    if m:
        return m.group(1)
    return '-'

def determine_bond_type(nm):
    nm = nm or ''
    if 'EB' in nm or '교환' in nm:
        return 'EB'
    if 'CB' in nm or '전환' in nm:
        return 'CB'
    if 'BW' in nm or '신주인수' in nm:
        return 'BW'
    return '-'

def get_mezzanine_data(isin):
    print(f"\n{'='*50}")
    print(f"  🔍 조회 중: {isin}")

    hosu      = '-'
    bond_type = '-'
    xrc_price = '0'
    issu_dt   = '-'

    # ── 1) 기본정보: 발행일 + 종목명(회차, 종류) ─────
    root = seibro_call('intPayInfoView', isin)
    if root is not None:
        issu_dt = format_date(root.findtext('.//ISSU_DT', ''))
        print(f"  📅 발행일: {issu_dt}")

    # 종목명은 검색창에 입력된 값(A열 종목명)으로 판단하기 어려우므로
    # SEIBRO 종목요약정보에서 KOR_SECN_NM 가져오기
    root2 = seibro_call('bondBasiInfoView', isin)
    if root2 is not None:
        secn_nm = (
            root2.findtext('.//KOR_SECN_NM', '') or
            root2.findtext('.//SECN_NM', '') or
            root2.findtext('.//BOND_NM', '')
        )
        print(f"  📌 종목명 응답: {secn_nm}")
        if secn_nm:
            hosu = extract_hosu(secn_nm)
            bond_type = determine_bond_type(secn_nm)
    else:
        print(f"  ⚠ bondBasiInfoView 응답 없음, 종목명 직접 파싱 시도")

    # bondBasiInfoView가 안 되면 A열 종목명 + 스프레드시트 데이터 활용
    # (main에서 corp_name 넘겨주는 방식으로 보완)

    # ── 2) 주식관련옵션: 행사가격 ────────────────────
    root = seibro_call('exerDetailListCnt', isin)
    if root is not None:
        item = root.find('.//result')
        if item is not None:
            xrc_price = item.findtext('XRC_PRICE', '0').replace(',', '')
            kor_nm = item.findtext('KOR_SECN_NM', '')
            print(f"  💰 행사가격: {xrc_price}, 대상주식: {kor_nm}")

    result = [hosu, bond_type, xrc_price, issu_dt, '-']
    print(f"  ✅ 최종 결과: {result}")
    return result

async def main():
    print("📋 스프레드시트 읽는 중...")
    all_values = worksheet.get_all_values()

    data_rows = [
        (i + 2, row)
        for i, row in enumerate(all_values[1:])
        if len(row) > 1 and row[1].strip().startswith('KR')
    ]

    if TEST_MODE:
        data_rows = data_rows[:TEST_LIMIT]
        print(f"🧪 테스트 모드: 상위 {TEST_LIMIT}개 종목만 실행\n")

    batch_updates = []
    start_row = data_rows[0][0] if data_rows else 2

    for sheet_row, row in data_rows:
        isin      = row[1].strip()
        corp_name = row[0].strip()  # A열 종목명 (보조용)

        result = get_mezzanine_data(isin)

        # bondBasiInfoView 실패 시 A열 종목명으로 보완
        if result[0] == '-' or result[1] == '-':
            print(f"  🔄 A열 종목명으로 보완: {corp_name}")
            if result[1] == '-':
                result[1] = determine_bond_type(corp_name)
            if result[0] == '-':
                result[0] = extract_hosu(corp_name)

        batch_updates.append(result)
        await asyncio.sleep(1.5)

    if batch_updates:
        end_row = start_row + len(batch_updates) - 1
        range_str = f"C{start_row}:G{end_row}"
        worksheet.update(range_str, batch_updates)
        print(f"\n🏁 완료! {len(batch_updates)}개 종목 → {range_str} 업데이트됨")

if __name__ == "__main__":
    asyncio.run(main())
