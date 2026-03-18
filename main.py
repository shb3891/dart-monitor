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

SEIBRO_URL = "https://seibro.or.kr/websquare/engine/proworks/callServletService.jsp"
TASK = "ksd.safe.bip.cnts.bone.process.BondSecnDetailPTask"

SESSION = requests.Session()
SESSION.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'ko-KR,ko;q=0.9',
})

def init_session():
    print("🔐 SEIBRO 세션 초기화 중...")
    try:
        SESSION.get(
            "https://seibro.or.kr/websquare/control.jsp?w2xPath=/IPORTAL/user/bond/BIP_CNTS03005V.xml",
            timeout=10
        )
        SESSION.headers.update({
            'Referer': 'https://seibro.or.kr/websquare/control.jsp?w2xPath=/IPORTAL/user/bond/BIP_CNTS03005V.xml',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'X-Requested-With': 'XMLHttpRequest',
            'Accept': 'application/xml, text/xml, */*',
        })
        print(f"✅ 세션 초기화 완료")
        return True
    except Exception as e:
        print(f"❌ 세션 초기화 실패: {e}")
        return False

def seibro_call(action, isin, extra_params=""):
    payload = f'<reqParam action="{action}" task="{TASK}"><ISIN value="{isin}"/>{extra_params}</reqParam>'
    try:
        r = SESSION.post(SEIBRO_URL, data={'reqParam': payload}, timeout=10)
        r.raise_for_status()

        try:
            decoded = r.content.decode('euc-kr', errors='replace')
        except Exception:
            decoded = r.content.decode('utf-8', errors='replace')

        cleaned = re.sub(r'<\?xml[^?]*\?>', '', decoded).strip()

        if '<!DOCTYPE' in cleaned or '<html' in cleaned.lower():
            print(f"  ❌ HTML 에러페이지 [{action}]")
            return None

        return ET.fromstring(cleaned.encode('utf-8'))

    except ET.ParseError as e:
        print(f"  ❌ XML 파싱 실패 [{action}]: {e}")
        return None
    except Exception as e:
        print(f"  ⚠ 호출 실패 [{action}]: {e}")
        return None

def get_attr(element, tag):
    """✅ attribute 방식 XML 파싱: <TAG value="..."/> → 값 반환"""
    el = element.find(f'.//{tag}')
    if el is not None:
        return el.get('value', '')
    return ''

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

def get_mezzanine_data(isin, corp_name):
    print(f"\n{'='*50}")
    print(f"  🔍 조회 중: {isin} ({corp_name})")

    hosu      = '-'
    bond_type = '-'
    xrc_price = '0'
    issu_dt   = '-'

    # ── 1) issuInfoViewEL1: 종목명 + 발행일 + 종류 ───
    root = seibro_call('issuInfoViewEL1', isin)
    if root is not None:
        secn_nm   = get_attr(root, 'KOR_SECN_NM')
        issu_dt   = format_date(get_attr(root, 'ISSU_DT'))
        bond_type = get_attr(root, 'PARTICUL_BOND_KIND')  # EB/CB/BW 직접!

        print(f"  📌 종목명: {secn_nm}")
        print(f"  📅 발행일: {issu_dt}")
        print(f"  🏷 종류: {bond_type}")

        hosu = extract_hosu(secn_nm)
        print(f"  🔢 회차: {hosu}")

    # 보완: PARTICUL_BOND_KIND 없을 경우 종목명으로 판단
    if not bond_type or bond_type == '-':
        nm = get_attr(root, 'KOR_SECN_NM') if root else corp_name
        if 'EB' in nm or '교환' in nm:
            bond_type = 'EB'
        elif 'CB' in nm or '전환' in nm:
            bond_type = 'CB'
        elif 'BW' in nm or '신주인수' in nm:
            bond_type = 'BW'

    # ── 2) exerDetailListCnt: 행사가격 ────────────────
    root2 = seibro_call('exerDetailListCnt', isin,
                        extra_params='<PAGE_ON_CNT value="100"/><PAGE_NUM value="1"/>')
    if root2 is not None:
        result_el = root2.find('.//result')
        if result_el is not None:
            xrc_price = get_attr(result_el, 'XRC_PRICE').replace(',', '')
            print(f"  💰 행사가격: {xrc_price}")

    result = [hosu, bond_type, xrc_price, issu_dt, '-']
    print(f"  ✅ 최종 결과: {result}")
    return result

async def main():
    if not init_session():
        print("❌ 세션 초기화 실패. 종료합니다.")
        return

    print("\n📋 스프레드시트 읽는 중...")
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
