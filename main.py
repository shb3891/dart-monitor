import asyncio
import os
import json
import gspread
import requests
import xml.etree.ElementTree as ET
import re
from google.oauth2.service_account import Credentials

# --- [설정] ---
SERVICE_KEY = '040c722e03bd9f412852134b7984002f9aaab072aebb672ec28d0792cc996a34'
SHEET_ID = '1s73BDNtCPe5mOs9EjBE5npEfcaNtYRyWJxRBUmJI-WA'

creds_json = json.loads(os.environ.get('GCP_SERVICE_ACCOUNT_KEY'))
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)
worksheet = sh.get_worksheet(0)

BASE_URL = "http://api.seibro.or.kr/openapi/service/BondSvc"

def xml_get(isin, endpoint, extra_params=None):
    """SEIBRO API 호출 공통 함수. item 엘리먼트 반환, 없으면 None."""
    params = {'serviceKey': SERVICE_KEY, 'isin': isin}
    if extra_params:
        params.update(extra_params)
    try:
        r = requests.get(f"{BASE_URL}/{endpoint}", params=params, timeout=10)
        r.raise_for_status()
        root = ET.fromstring(r.content)

        result_code = root.findtext('.//resultCode', '')
        if result_code not in ('', '00', '000'):
            result_msg = root.findtext('.//resultMsg', '')
            print(f"  ⚠ API 에러 [{isin}] {endpoint}: {result_code} - {result_msg}")
            return None

        item = root.find('.//item')
        return item
    except requests.exceptions.Timeout:
        print(f"  ⏱ Timeout [{isin}] {endpoint}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"  🔥 Request 에러 [{isin}] {endpoint}: {e}")
        return None
    except ET.ParseError as e:
        print(f"  ❌ XML 파싱 에러 [{isin}] {endpoint}: {e}")
        return None

def determine_bond_type(bond_nm):
    """채권명으로 메자닌 종류 판단."""
    nm = bond_nm or ''
    if '전환' in nm or 'CB' in nm.upper():
        return 'CB'
    if '교환' in nm or 'EB' in nm.upper():
        return 'EB'
    if '신주인수' in nm or 'BW' in nm.upper():
        return 'BW'
    return 'CB'

def format_date(raw):
    """YYYYMMDD → YYYY-MM-DD 변환."""
    if raw and len(raw) == 8 and raw.isdigit():
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    return raw or '-'

def extract_hosu(bond_nm):
    """채권명에서 회차 추출."""
    m = re.search(r'제\s*(\d+)\s*회', bond_nm or '')
    if m:
        return m.group(1)
    m = re.search(r'(\d+)회', bond_nm or '')
    if m:
        return m.group(1)
    return '1'

def get_mezzanine_data(isin):
    """ISIN으로 메자닌 채권 정보를 SEIBRO에서 가져옵니다."""
    print(f"  🔍 조회 중: {isin}")

    item = xml_get(isin, 'getBondIssuInfo')
    if item is None:
        print(f"  ❌ 기본정보 없음: {isin}")
        return ['-', '-', '0', '-', '-']

    bond_nm  = item.findtext('bondIssuNm', '') or item.findtext('bondNm', '')
    issu_dt  = item.findtext('issuDt', '') or item.findtext('bondIssuDt', '')

    hosu      = extract_hosu(bond_nm)
    bond_type = determine_bond_type(bond_nm)
    issu_dt_fmt = format_date(issu_dt)

    exercise_price = '0'
    right_start_dt = '-'

    type_endpoint_map = {
        'CB': 'getBondConvInfo',
        'EB': 'getBondExchInfo',
        'BW': 'getBondWrantInfo',
    }

    detail_endpoint = type_endpoint_map.get(bond_type)
    if detail_endpoint:
        detail = xml_get(isin, detail_endpoint)

        if detail is not None:
            for field in ['convPrcNow', 'convPrice', 'exchPrc', 'wrantExrcPrc', 'issuConvPrice']:
                val = detail.findtext(field, '')
                if val and val.strip() not in ('', '0', '-'):
                    exercise_price = val.strip().replace(',', '')
                    break

            for field in ['convAplcStrtDt', 'exchAplcStrtDt', 'wrantExrcStrtDt', 'rightStrtDt']:
                val = detail.findtext(field, '')
                if val and val.strip() not in ('', '-'):
                    right_start_dt = format_date(val.strip())
                    break

    if exercise_price == '0':
        for field in ['issuConvPrice', 'convPrc', 'exchPrc']:
            val = item.findtext(field, '')
            if val and val.strip() not in ('', '0', '-'):
                exercise_price = val.strip().replace(',', '')
                break

    result = [hosu, bond_type, exercise_price, issu_dt_fmt, right_start_dt]
    print(f"  ✅ {bond_nm} → {result}")
    return result

async def main():
    print("📋 스프레드시트 읽는 중...")
    all_values = worksheet.get_all_values()

    data_rows = [
        (i + 2, row)
        for i, row in enumerate(all_values[1:])
        if len(row) > 1 and row[1].strip().startswith('KR')
    ]

    print(f"🚀 총 {len(data_rows)}개 종목 처리 시작\n")

    batch_updates = []
    start_row = data_rows[0][0] if data_rows else 2

    for sheet_row, row in data_rows:
        isin = row[1].strip()
        result = get_mezzanine_data(isin)
        batch_updates.append(result)
        await asyncio.sleep(1.2)

    if batch_updates:
        end_row = start_row + len(batch_updates) - 1
        range_str = f"C{start_row}:G{end_row}"
        worksheet.update(range_str, batch_updates)
        print(f"\n🏁 완료! {len(batch_updates)}개 종목 → {range_str} 업데이트됨")

if __name__ == "__main__":
    asyncio.run(main())
