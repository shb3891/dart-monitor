import asyncio
import os
import json
import gspread
import requests
import zipfile
import xml.etree.ElementTree as ET
import re
import io
from google.oauth2.service_account import Credentials

# --- [설정] ---
DART_API_KEY = 'bfc4e4e445de4727ae0bcc27e80ba5cf0e3818e6'
SHEET_ID = '1s73BDNtCPe5mOs9EjBE5npEfcaNtYRyWJxRBUmJI-WA'

creds_json = json.loads(os.environ.get('GCP_SERVICE_ACCOUNT_KEY'))
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)
worksheet = sh.get_worksheet(0)

DART_URL = "https://opendart.fss.or.kr/api"

TEST_MODE = True
TEST_LIMIT = 3

# ── DART 회사 고유번호 전체 목록 로드 ──────────────────────────
def load_corp_codes():
    """DART 전체 회사 고유번호 ZIP 다운로드 후 파싱."""
    print("📥 DART 회사 고유번호 목록 다운로드 중...")
    r = requests.get(
        f"{DART_URL}/corpCode.xml",
        params={'crtfc_key': DART_API_KEY},
        timeout=30
    )
    z = zipfile.ZipFile(io.BytesIO(r.content))
    xml_data = z.read('CORPCODE.xml')
    root = ET.fromstring(xml_data)

    corp_map = {}  # 회사명 → corp_code
    for item in root.findall('.//list'):
        name = item.findtext('corp_name', '').strip()
        code = item.findtext('corp_code', '').strip()
        stock_code = item.findtext('stock_code', '').strip()
        if name and code:
            corp_map[name] = {'corp_code': code, 'stock_code': stock_code}

    print(f"✅ 총 {len(corp_map)}개 회사 로드 완료")
    return corp_map

# ── DART 공시 검색 ──────────────────────────────────────────────
def search_disclosure(corp_code, pblntf_detail_ty):
    """
    CB: 'C001' / EB: 'C002' / BW: 'C003'
    """
    r = requests.get(
        f"{DART_URL}/list.json",
        params={
            'crtfc_key': DART_API_KEY,
            'corp_code': corp_code,
            'pblntf_ty': 'B',           # 주요사항보고
            'pblntf_detail_ty': pblntf_detail_ty,
            'page_count': 5,
        },
        timeout=10
    )
    data = r.json()
    if data.get('status') == '000' and data.get('list'):
        return data['list']  # 최신 공시 목록
    return []

# ── 공시 원문에서 데이터 파싱 ───────────────────────────────────
def get_document_data(rcept_no):
    """공시 원문 ZIP에서 XML 파싱하여 전환가액, 발행일, 권리청구시작일 추출."""
    r = requests.get(
        f"{DART_URL}/document.xml",
        params={'crtfc_key': DART_API_KEY, 'rcept_no': rcept_no},
        timeout=15
    )
    try:
        z = zipfile.ZipFile(io.BytesIO(r.content))
        # XML 파일 찾기
        xml_files = [f for f in z.namelist() if f.endswith('.xml')]
        if not xml_files:
            return None
        xml_data = z.read(xml_files[0])
        return xml_data.decode('utf-8', errors='replace')
    except Exception as e:
        print(f"    ⚠ 원문 파싱 실패: {e}")
        return None

def parse_from_text(text, patterns):
    """정규식 패턴 리스트로 값 추출."""
    for pattern in patterns:
        m = re.search(pattern, text, re.DOTALL)
        if m:
            val = m.group(1).strip().replace(',', '').replace(' ', '')
            if val and val not in ('-', ''):
                return val
    return None

# ── 메인 데이터 추출 ────────────────────────────────────────────
def get_mezzanine_data(corp_name, corp_map):
    print(f"\n{'='*50}")
    print(f"  🔍 조회 중: {corp_name}")

    corp_info = corp_map.get(corp_name)
    if not corp_info:
        print(f"  ❌ DART에서 회사명 없음: {corp_name}")
        return ['-', '-', '0', '-', '-']

    corp_code = corp_info['corp_code']
    print(f"  📌 corp_code: {corp_code}")

    # CB → EB → BW 순서로 공시 검색
    type_map = {
        'CB': 'C001',
        'EB': 'C002',
        'BW': 'C003',
    }

    disclosures = []
    bond_type = '-'

    for btype, code in type_map.items():
        disclosures = search_disclosure(corp_code, code)
        if disclosures:
            bond_type = btype
            print(f"  ✅ {btype} 공시 {len(disclosures)}건 발견")
            break

    if not disclosures:
        print(f"  ❌ CB/EB/BW 공시 없음: {corp_name}")
        return ['-', '-', '0', '-', '-']

    # 가장 최신 공시
    latest = disclosures[0]
    rcept_no = latest.get('rcept_no', '')
    rcept_dt = latest.get('rcept_dt', '')  # 접수일 = 대략 발행일
    report_nm = latest.get('report_nm', '')

    print(f"  📄 공시: {report_nm} ({rcept_dt})")

    # 회차 추출
    hosu = '1'
    m = re.search(r'제\s*(\d+)\s*회', report_nm)
    if m:
        hosu = m.group(1)

    # 발행일 포맷
    issu_dt = '-'
    if rcept_dt and len(rcept_dt) == 8:
        issu_dt = f"{rcept_dt[:4]}-{rcept_dt[4:6]}-{rcept_dt[6:]}"

    exercise_price = '0'
    right_start_dt = '-'

    # 공시 원문 파싱
    doc_text = get_document_data(rcept_no)
    if doc_text:
        # 전환가액 패턴
        price_patterns = [
            r'전환가액[^\d]*?([\d,]+)\s*원',
            r'교환가액[^\d]*?([\d,]+)\s*원',
            r'행사가액[^\d]*?([\d,]+)\s*원',
            r'전환가격[^\d]*?([\d,]+)\s*원',
        ]
        val = parse_from_text(doc_text, price_patterns)
        if val:
            exercise_price = val
            print(f"  💰 행사가액: {exercise_price}")

        # 권리청구시작일 패턴
        date_patterns = [
            r'전환(?:청구)?(?:기간|시작)[^\d]*?(\d{4}[년\-\.]\s*\d{1,2}[월\-\.]\s*\d{1,2})',
            r'전환권\s*행사\s*기간[^\d]*?(\d{4}[년\-\.]\s*\d{1,2}[월\-\.]\s*\d{1,2})',
            r'교환청구기간[^\d]*?(\d{4}[년\-\.]\s*\d{1,2}[월\-\.]\s*\d{1,2})',
            r'행사기간[^\d]*?(\d{4}[년\-\.]\s*\d{1,2}[월\-\.]\s*\d{1,2})',
        ]
        val = parse_from_text(doc_text, date_patterns)
        if val:
            # 날짜 정규화
            val = re.sub(r'[년월\.\s]', '-', val).strip('-')
            val = re.sub(r'-+', '-', val)
            right_start_dt = val
            print(f"  📅 권리청구시작일: {right_start_dt}")

    result = [hosu, bond_type, exercise_price, issu_dt, right_start_dt]
    print(f"  ✅ 최종 결과: {result}")
    return result

# ── 메인 ────────────────────────────────────────────────────────
async def main():
    # DART 회사 목록 로드
    corp_map = load_corp_codes()

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
        corp_name = row[0].strip()  # A열: 종목명
        result = get_mezzanine_data(corp_name, corp_map)
        batch_updates.append(result)
        await asyncio.sleep(1.0)

    if batch_updates:
        end_row = start_row + len(batch_updates) - 1
        range_str = f"C{start_row}:G{end_row}"
        worksheet.update(range_str, batch_updates)
        print(f"\n🏁 완료! {len(batch_updates)}개 종목 → {range_str} 업데이트됨")

if __name__ == "__main__":
    asyncio.run(main())
