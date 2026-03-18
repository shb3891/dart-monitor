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

TEST_MODE = True
TEST_LIMIT = 3

def xml_get(isin, endpoint, extra_params=None):
    params = {'serviceKey': SERVICE_KEY, 'isin': isin}
    if extra_params:
        params.update(extra_params)
    try:
        r = requests.get(f"{BASE_URL}/{endpoint}", params=params, timeout=10)
        r.raise_for_status()

        # ✅ 핵심 수정: 여러 방식으로 파싱 시도
        raw_bytes = r.content
        xml_str = None

        # 방법 1: euc-kr 디코딩 후 xml 선언부 제거하고 utf-8로 재인코딩
        try:
            decoded = raw_bytes.decode('euc-kr', errors='replace')
            # XML 선언부(<?xml ...?>) 제거 후 파싱 (인코딩 충돌 방지)
            cleaned = re.sub(r'<\?xml[^?]*\?>', '', decoded).strip()
            root = ET.fromstring(cleaned.encode('utf-8'))
        except ET.ParseError:
            # 방법 2: 바이트 그대로 파싱
            try:
                root = ET.fromstring(raw_bytes)
            except ET.ParseError:
                # 방법 3: utf-8 강제 디코딩
                try:
                    decoded = raw_bytes.decode('utf-8', errors='replace')
                    cleaned = re.sub(r'<\?xml[^?]*\?>', '', decoded).strip()
                    root = ET.fromstring(cleaned.encode('utf-8'))
                except ET.ParseError as e:
                    print(f"  ❌ XML 파싱 최종 실패 [{isin}] {endpoint}: {e}")
                    print(f"  📄 Raw (앞 300자): {raw_bytes[:300]}")
                    return None

        result_code = root.findtext('.//resultCode', '')
        if result_code not in ('', '00', '000'):
            result_msg = root.findtext('.//resultMsg', '')
            print(f"  ⚠ API 에러 [{isin}] {endpoint}: {result_code} - {result_msg}")
            return None

        item = root.find('.//item')

        # 디버깅: 필드 전체 출력
        if item is not None:
            print(f"  📋 [{endpoint}] 필드 목록:")
            for child in item:
                print(f"      {child.tag}: {child.text}")
        else:
            print(f"  ⚠ [{endpoint}] item 태그 없음. resultCode={result_code}")

        return item

    except requests.exceptions.Timeout:
        print(f"  ⏱ Timeout [{isin}] {endpoint}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"  🔥 Request 에러 [{isin}] {endpoint}: {e}")
        return None

def determine_bond_type(bond_nm):
    nm = bond_nm or ''
    if '전환' in nm:
        return 'CB'
    if '교환' in nm:
        return 'EB'
    if '신주인수' in nm:
        return 'BW'
    return 'CB'

def format_date(raw):
    if raw and len(raw) == 8 and raw.isdigit():
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    return raw or '-'

def extract_hosu(bond_nm):
    m = re.search(r'제\s*(\d+)\s*회', bond_nm or '')
    if m:
        return m.group(1)
    m = re.search(r'(\d+)회', bond_nm or '')
    if m:
        return m.group(1)
    return '1'

def get_mezzanine_data(isin):
    print(f"\n{'='*50}")
    print(f"  🔍 조회 중: {isin}")

    item = xml_get(isin, 'getBondIssuInfo')
    if item is None:
        print(f"  ❌ 기본정보 없음: {isin}")
        return ['-', '-', '0', '-', '-']

    bond_nm     = item.findtext('bondIssuNm', '') or item.findtext('bondNm', '')
    issu_dt     = item.findtext('issuDt', '') or item.findtext('bondIssuDt', '')

    hosu        = extract_hosu(bond_nm)
    bond_type   = determine_bond_type(bond_nm)
    issu_dt_fmt = format_date(issu_dt)

    print(f"  📌 채권명: {bond_nm} | 회차: {hosu} | 종류: {bond_type} | 발행일: {issu_dt_fmt}")

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
                    print(f"  💰 행사가액: {field} = {exercise_price}")
                    break

            for field in ['convAplcStrtDt', 'exchAplcStrtDt', 'wrantExrcStrtDt', 'rightStrtDt']:
                val = detail.findtext(field, '')
                if val and val.strip() not in ('', '-'):
                    right_start_dt = format_date(val.strip())
                    print(f"  📅 권리청구시작일: {field} = {right_start_dt}")
                    break

    result = [hosu, bond_type, exercise_price, issu_dt_fmt, right_start_dt]
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
        await asyncio.sleep(1.2)

    if batch_updates:
        end_row = start_row + len(batch_updates) - 1
        range_str = f"C{start_row}:G{end_row}"
        worksheet.update(range_str, batch_updates)
        print(f"\n🏁 완료! {len(batch_updates)}개 종목 → {range_str} 업데이트됨")

if __name__ == "__main__":
    asyncio.run(main())
