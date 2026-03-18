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

def seibro_call(action, isin, extra_params=""):
    """SEIBRO callServletService.jsp 호출."""
    payload = f'<reqParam action="{action}" task="{TASK}"><ISIN value="{isin}"/>{extra_params}</reqParam>'
    try:
        r = SESSION.post(
            SEIBRO_URL,
            data={'reqParam': payload},
            timeout=10
        )
        r.raise_for_status()

        # EUC-KR 디코딩 후 XML 선언부 제거
        try:
            decoded = r.content.decode('euc-kr', errors='replace')
        except Exception:
            decoded = r.content.decode('utf-8', errors='replace')

        cleaned = re.sub(r'<\?xml[^?]*\?>', '', decoded).strip()

        if '<!DOCTYPE' in cleaned or '<html' in cleaned.lower():
            print(f"  ❌ HTML 에러페이지 [{action}]")
            print(f"  📄 Raw: {cleaned[:200]}")
            return None

        root = ET.fromstring(cleaned.encode('utf-8'))
        return root

    except ET.ParseError as e:
        print(f"  ❌ XML 파싱 실패 [{action}]: {e}")
        print(f"  📄 Raw (앞 300자): {r.content[:300]}")
        return None
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

def get_mezzanine_data(isin, corp_name):
    print(f"\n{'='*50}")
    print(f"  🔍 조회 중: {isin} ({corp_name})")

    hosu      = '-'
    bond_type = '-'
    xrc_price = '0'
    issu_dt   = '-'

    # ── 1) issuInfoViewEL1: 종목명(회차/종류) + 발행일 ──
    root = seibro_call('issuInfoViewEL1', isin)
    if root is not None:
        # 전체 필드 출력 (디버깅용)
        print(f"  📋 issuInfoViewEL1 필드:")
        for el in root.iter():
            if el.text and el.text.strip():
                print(f"      {el.tag}: {el.text.strip()}")

        # 발행일
        issu_dt = format_date(
            root.findtext('.//ISSU_DT', '') or
            root.findtext('.//ISS_DT', '')
        )

        # 종목명
        secn_nm = (
            root.findtext('.//KOR_SECN_NM', '') or
            root.findtext('.//SECN_NM', '') or
            root.findtext('.//BOND_NM', '') or
            root.findtext('.//BOND_ISSU_NM', '')
        )
        print(f"  📌 종목명: {secn_nm} | 발행일: {issu_dt}")

        if secn_nm:
            hosu = extract_hosu(secn_nm)
            bond_type = determine_bond_type(secn_nm)

    # ── 2) intPayInfoView: 발행일 보완용 ────────────────
    if issu_dt == '-':
        root2 = seibro_call('intPayInfoView', isin)
        if root2 is not None:
            issu_dt = format_date(root2.findtext('.//ISSU_DT', ''))
            print(f"  📅 발행일(intPayInfoView): {issu_dt}")

    # issuInfoViewEL1 실패 시 A열 종목명으로 보완
    if bond_type == '-':
        bond_type = determine_bond_type(corp_name)
        print(f"  🔄 종류 보완(A열): {bond_type}")
    if hosu == '-':
        hosu = extract_hosu(corp_name)
        print(f"  🔄 회차 보완(A열): {hosu}")

    # ── 3) exerDetailListCnt: 행사가격 ─────────────────
    root = seibro_call('exerDetailListCnt', isin,
                       extra_params='<PAGE_ON_CNT value="100"/><PAGE_NUM value="1"/>')
    if root is not None:
        item = root.find('.//result')
        if item is not None:
            xrc_price = item.findtext('XRC_PRICE', '0').replace(',', '')
            print(f"  💰 행사가격: {xrc_price}")

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
        corp_name = row[0].strip()
        result = get_mezzanine_data(isin, corp_name)
        batch_updates.append(result)
        await asyncio.sleep(1.5)

    if batch_updates:
        end_row = start_row + len(batch_updates) - 1
        range_str = f"C{start_row}:G{end_row}"
        worksheet.update(range_str, batch_updates)
        print(f"\n🏁 완료! {len(batch_updates)}개 종목 → {range_str} 업데이트됨")

if __name__ == "__main__":
    asyncio.run(main())
