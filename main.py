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

TEST_MODE = False

BASE_URL = "https://seibro.or.kr/OpenPlatform/callOpenAPI.jsp"

def seibro_api(api_id, params_dict):
    params_str = ','.join([f"{k}:{v}" for k, v in params_dict.items()])
    full_url = f"{BASE_URL}?key={SEIBRO_KEY}&apiId={api_id}&params={params_str}"
    try:
        r = requests.get(full_url, timeout=10)
        r.raise_for_status()

        for encoding in ['utf-8', 'euc-kr']:
            try:
                decoded = r.content.decode(encoding, errors='strict')
                break
            except UnicodeDecodeError:
                continue
        else:
            decoded = r.content.decode('utf-8', errors='replace')

        cleaned = re.sub(r'<\?xml[^?]*\?>', '', decoded).strip()
        if not cleaned:
            return None

        root = ET.fromstring(cleaned.encode('utf-8'))

        error = root.find('.//error')
        if error is not None:
            code = error.find('code')
            if code is not None and code.get('value') not in ('000', '00', ''):
                return None

        vector = root.find('.//vector')
        if vector is None:
            return None

        if vector.get('result', '0') == '0':
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
    m = re.search(r'[가-힣a-zA-Z\s]+\s*(\d+)\s*(?:CB|EB|BW)', nm or '')
    if m:
        return m.group(1)
    m = re.search(r'(\d+)회', nm or '')
    if m:
        return m.group(1)
    return '-'

def extract_corp_name(nm):
    m = re.match(r'([가-힣a-zA-Z\s]+?)\s*\d+\s*(?:CB|EB|BW)', nm or '')
    if m:
        return m.group(1).strip()
    return nm.split('(')[0].strip() if nm else '-'

def determine_bond_type(secn_nm):
    nm = secn_nm or ''
    if 'EB' in nm or '교환' in nm:
        return 'EB'
    if 'CB' in nm or '전환' in nm:
        return 'CB'
    if 'BW' in nm or '신주인수' in nm:
        return 'BW'
    return '-'

def get_mezzanine_data(isin, existing_row):
    print(f"  🔍 {isin}", end=' ')

    root = seibro_api('getBondStatInfo', {'ISIN': isin})
    if root is not None:
        result_el = root.find('.//result')
        if result_el is not None:
            secn_nm   = get_attr(result_el, 'KOR_SECN_NM')
            issu_dt   = format_date(get_attr(result_el, 'ISSU_DT'))
            xpir_dt   = format_date(get_attr(result_el, 'XPIR_DT'))
            bond_type = determine_bond_type(secn_nm)
            hosu      = extract_hosu(secn_nm)
            corp_name = extract_corp_name(secn_nm)

            print(f"→ {corp_name} {hosu}회 {bond_type} {issu_dt} ~ {xpir_dt}")

            # 새 열 구조: C회차 D종류 E발행일 F만기일 G~ (행사가액은 나중에 I열)
            return {
                'corp_name': corp_name,
                'row': [
                    hosu,      # C: 회차
                    bond_type, # D: 종류
                    issu_dt,   # E: 발행일
                    xpir_dt,   # F: 만기일
                ]
            }

    # API 실패 시 기존값 유지
    print(f"→ ⚠ API 없음 (기존값 유지)")
    return {
        'corp_name': existing_row[0].strip() if len(existing_row) > 0 else '-',
        'row': [
            existing_row[2].strip() if len(existing_row) > 2 else '-',  # C: 회차
            existing_row[3].strip() if len(existing_row) > 3 else '-',  # D: 종류
            existing_row[4].strip() if len(existing_row) > 4 else '-',  # E: 발행일
            existing_row[5].strip() if len(existing_row) > 5 else '-',  # F: 만기일
        ]
    }

async def main():
    print("📋 스프레드시트 읽는 중...")
    all_values = worksheet.get_all_values()

    data_rows = [
        (i + 2, row)
        for i, row in enumerate(all_values[1:])
        if len(row) > 1 and row[1].strip().startswith('KR')
    ]

    if TEST_MODE:
        data_rows = data_rows[:3]
        print(f"🧪 테스트 모드: 상위 3개만\n")
    else:
        print(f"🚀 전체 {len(data_rows)}개 종목 실행\n")

    a_updates  = []
    cf_updates = []
    start_row  = data_rows[0][0] if data_rows else 2

    for sheet_row, row in data_rows:
        isin   = row[1].strip()
        result = get_mezzanine_data(isin, row)
        a_updates.append([result['corp_name']])
        cf_updates.append(result['row'])
        await asyncio.sleep(1.0)

    if cf_updates:
        end_row = start_row + len(cf_updates) - 1

        # A열: 종목명
        worksheet.update(
            range_name=f"A{start_row}:A{end_row}",
            values=a_updates
        )
        await asyncio.sleep(1.0)

        # C~F열: 회차, 종류, 발행일, 만기일
        worksheet.update(
            range_name=f"C{start_row}:F{end_row}",
            values=cf_updates
        )
        print(f"\n🏁 완료! {len(cf_updates)}개 종목 업데이트됨")

if __name__ == "__main__":
    asyncio.run(main())
