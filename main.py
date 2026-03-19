import asyncio
import os
import json
import gspread
import requests
import xml.etree.ElementTree as ET
import re
from google.oauth2.service_account import Credentials

# --- [설정] ---
SEIBRO_KEY = 'e1e03a31bc0583fc0c853d4c41a0dc018dc4d2aa21c363c3d6b1b0b96e85221b'
SHEET_ID = '1s73BDNtCPe5mOs9EjBE5npEfcaNtYRyWJxRBUmJI-WA'

creds_json = json.loads(os.environ.get('GCP_SERVICE_ACCOUNT_KEY'))
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)
worksheet = sh.get_worksheet(0)

TEST_MODE = True
TEST_LIMIT = 3

BASE_URL = "http://seibro.or.kr/OpenPlatform/callOpenAPI.jsp"

def seibro_api(api_id, params_dict):
    params_str = ','.join([f"{k}:{v}" for k, v in params_dict.items()])
    try:
        r = requests.get(
            BASE_URL,
            params={'key': SEIBRO_KEY, 'apiId': api_id, 'params': params_str},
            timeout=10
        )
        r.raise_for_status()

        try:
            decoded = r.content.decode('euc-kr', errors='replace')
        except Exception:
            decoded = r.content.decode('utf-8', errors='replace')

        # ✅ 디버깅: 실제 응답 출력
        print(f"  📡 [{api_id}] 실제 응답:")
        print(decoded[:800])

        cleaned = re.sub(r'<\?xml[^?]*\?>', '', decoded).strip()
        if not cleaned:
            return None

        root = ET.fromstring(cleaned.encode('utf-8'))

        vector = root.find('.//vector')
        if vector is None:
            print(f"  ⚠ [{api_id}] vector 없음")
            return None

        result_count = vector.get('result', '0')
        if result_count == '0':
            print(f"  ⚠ [{api_id}] 결과 없음")
            return None

        return root

    except Exception as e:
        print(f"  ⚠ API 호출 실패 [{api_id}]: {e}")
        return None

def get_attr(element, tag):
    el = element.find(f'.//{tag}')
    if el is not None:
        return el.get('value', '')
    return ''

def format_date(raw):
    raw = str(raw).strip() if raw else ''
    if len(raw) == 8 and raw.isdigit():
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

def determine_bond_type(kind_tpcd, secn_nm):
    nm = secn_nm or ''
    if 'EB' in nm or '교환' in nm:
        return 'EB'
    if 'CB' in nm or '전환' in nm:
        return 'CB'
    if 'BW' in nm or '신주인수' in nm:
        return 'BW'
    code_map = {'1': 'CB', '2': 'EB', '3': 'BW'}
    return code_map.get(kind_tpcd, '-')

def get_mezzanine_data(isin):
    print(f"\n{'='*50}")
    print(f"  🔍 조회 중: {isin}")

    hosu           = '-'
    bond_type      = '-'
    xrc_price      = '0'
    issu_dt        = '-'
    right_start_dt = '-'

    # ── 1) getBondStatInfo: 종목명, 발행일, 종류 ──────
    root = seibro_api('getBondStatInfo', {'ISIN': isin})
    if root is not None:
        result_el = root.find('.//result')
        if result_el is not None:
            secn_nm   = get_attr(result_el, 'KOR_SECN_NM')
            issu_dt   = format_date(get_attr(result_el, 'ISSU_DT'))
            bond_kind = get_attr(result_el, 'PARTICUL_BOND_KIND_TPCD')
            bond_type = determine_bond_type(bond_kind, secn_nm)
            hosu      = extract_hosu(secn_nm)

            print(f"  📌 종목명: {secn_nm}")
            print(f"  📅 발행일: {issu_dt}")
            print(f"  🏷 종류: {bond_type}")
            print(f"  🔢 회차: {hosu}")

    # ── 2) getBondOptionXrcInfo: 권리청구시작일 ───────
    root2 = seibro_api('getBondOptionXrcInfo', {'ISIN': isin})
    if root2 is not None:
        items = root2.findall('.//result')
        earliest = None
        for item in items:
            option_cd = get_attr(item, 'OPTION_TPCD')
            xrc_begin = get_attr(item, 'XRC_BEGIN_DT')
            if option_cd != '9402' and xrc_begin and xrc_begin.strip():
                if earliest is None or xrc_begin < earliest:
                    earliest = xrc_begin
        if earliest:
            right_start_dt = format_date(earliest)
            print(f"  📅 권리청구시작일: {right_start_dt}")

    result = [hosu, bond_type, xrc_price, issu_dt, right_start_dt]
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
        isin = row[1].strip()
        result = get_mezzanine_data(isin)
        batch_updates.append(result)
        await asyncio.sleep(1.0)

    if batch_updates:
        end_row = start_row + len(batch_updates) - 1
        range_str = f"C{start_row}:G{end_row}"
        worksheet.update(range_str, batch_updates)
        print(f"\n🏁 완료! {len(batch_updates)}개 종목 → {range_str} 업데이트됨")

if __name__ == "__main__":
    asyncio.run(main())
