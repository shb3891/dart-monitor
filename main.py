import asyncio
import os
import json
import gspread
import requests
import xml.etree.ElementTree as ET
import re
from google.oauth2.service_account import Credentials

# ============================================================
# [설정]
# ============================================================
SEIBRO_KEY = os.environ.get('SEIBRO_KEY', 'e1e03a31bc0583fc0c853d4c41a0dc018dc4d2aa21c363c3d6b1b0b96e85221b')
SHEET_ID   = os.environ.get('SHEET_ID',   '1s73BDNtCPe5mOs9EjBE5npEfcaNtYRyWJxRBUmJI-WA')

# ============================================================
# [API 승인 플래그] 승인 나면 True로 변경
# ============================================================
API_BOND_APPROVED  = True   # ✅ 채권정보 (getBondStatInfo, getBondOptionXrcInfo, getIntPayInfo)
API_STOCK_APPROVED = False  # ⏳ 주식정보 (getXrcStkStatInfo, getXrcStkOptionXrcInfo)

# ============================================================
# [테스트 모드]
# ============================================================
TEST_MODE = False  # True면 상위 3개만 실행

# ============================================================
# [Google Sheets 연결]
# ============================================================
creds_json = json.loads(os.environ.get('GCP_SERVICE_ACCOUNT_KEY'))
scopes = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
creds     = Credentials.from_service_account_info(creds_json, scopes=scopes)
gc        = gspread.authorize(creds)
sh        = gc.open_by_key(SHEET_ID)
worksheet = sh.get_worksheet(0)

BASE_URL = "https://seibro.or.kr/OpenPlatform/callOpenAPI.jsp"


# ============================================================
# [공통 유틸]
# ============================================================
def seibro_api(api_id, params_dict):
    """SEIBRO OpenAPI 호출 공통 함수"""
    params_str = ','.join([f"{k}:{v}" for k, v in params_dict.items()])
    full_url   = f"{BASE_URL}?key={SEIBRO_KEY}&apiId={api_id}&params={params_str}"
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
    """XML 엘리먼트에서 value 속성 추출"""
    el = element.find(f'.//{tag}')
    if el is not None:
        return el.get('value', '')
    return ''


def format_date(raw):
    """YYYYMMDD → YYYY-MM-DD 변환"""
    raw = str(raw).strip() if raw else ''
    if len(raw) == 8 and raw.isdigit():
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    return raw or '-'


def extract_hosu(nm):
    """종목명에서 회차 추출"""
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
    """종목명에서 회사명 추출"""
    m = re.match(r'([가-힣a-zA-Z\s]+?)\s*\d+\s*(?:CB|EB|BW)', nm or '')
    if m:
        return m.group(1).strip()
    return nm.split('(')[0].strip() if nm else '-'


def determine_bond_type(secn_nm):
    """종목명에서 CB/EB/BW 구분"""
    nm = secn_nm or ''
    if 'EB' in nm or '교환' in nm:
        return 'EB'
    if 'CB' in nm or '전환' in nm:
        return 'CB'
    if 'BW' in nm or '신주인수권' in nm:
        return 'BW'
    return '-'


# ============================================================
# [파싱 함수들] API별로 분리
# ============================================================

def parse_bond_basic(isin):
    """
    getBondStatInfo → A(종목명), C(회차), D(종류), E(발행일), F(만기일), G(Coupon)
    ✅ 채권API 승인됨
    """
    root = seibro_api('getBondStatInfo', {'ISIN': isin})
    if root is None:
        return None

    result_el = root.find('.//result')
    if result_el is None:
        return None

    secn_nm     = get_attr(result_el, 'KOR_SECN_NM')
    issu_dt     = format_date(get_attr(result_el, 'ISSU_DT'))
    xpir_dt     = format_date(get_attr(result_el, 'XPIR_DT'))
    coupon_rate = get_attr(result_el, 'COUPON_RATE')
    bond_type   = determine_bond_type(secn_nm)
    hosu        = extract_hosu(secn_nm)
    corp_name   = extract_corp_name(secn_nm)

    # Coupon: 0이면 빈칸 처리
    coupon = coupon_rate if coupon_rate and coupon_rate != '0' else ''

    return {
        'corp_name': corp_name,
        'hosu':      hosu,
        'bond_type': bond_type,
        'issu_dt':   issu_dt,
        'xpir_dt':   xpir_dt,
        'coupon':    coupon,
    }


def parse_put_call(isin):
    """
    getBondOptionXrcInfo → M(PUT시작일), N(PUT종료일), O(PUT상환지급일), Q(CALL비율), R(CALL시작일), S(CALL종료일)
    OPTION_TPCD: 9401=CALL, 9402=PUT, 9403=CALL+PUT
    ✅ 채권API 승인됨 — 단, 조기상환 이력이 없는 종목은 빈값 반환
    """
    root = seibro_api('getBondOptionXrcInfo', {'ISIN': isin})

    result = {
        'put_begin':  '',
        'put_end':    '',
        'put_date':   '',
        'call_ratio': '',
        'call_begin': '',
        'call_end':   '',
    }

    if root is None:
        return result

    for result_el in root.findall('.//result'):
        option_tpcd = get_attr(result_el, 'OPTION_TPCD')
        xrc_begin   = format_date(get_attr(result_el, 'XRC_BEGIN_DT'))
        xrc_end     = format_date(get_attr(result_el, 'XRC_EXPRY_DT'))
        erly_red_dt = format_date(get_attr(result_el, 'ERLY_RED_DT'))
        xrc_ratio   = get_attr(result_el, 'XRC_RATIO')

        if option_tpcd in ('9402', '9403'):  # PUT 또는 CALL+PUT
            result['put_begin'] = result['put_begin'] or xrc_begin
            result['put_end']   = result['put_end']   or xrc_end
            result['put_date']  = result['put_date']  or erly_red_dt

        if option_tpcd in ('9401', '9403'):  # CALL 또는 CALL+PUT
            result['call_begin'] = result['call_begin'] or xrc_begin
            result['call_end']   = result['call_end']   or xrc_end
            result['call_ratio'] = result['call_ratio'] or xrc_ratio

    return result


def parse_exercise_info(isin):
    """
    getXrcStkStatInfo  → I(행사가액)
    getXrcStkOptionXrcInfo → K(권리청구시작일), L(권리청구종료일)
    ⏳ 주식API 승인 대기 중
    """
    result = {
        'xrc_price':      '',
        'xrc_begin':      '',
        'xrc_end':        '',
    }

    # --- 행사가액: getXrcStkStatInfo ---
    root = seibro_api('getXrcStkStatInfo', {'BOND_ISIN': isin})
    if root is not None:
        result_el = root.find('.//result')
        if result_el is not None:
            price = get_attr(result_el, 'XRC_PRICE')
            if price and price != '0':
                # 숫자에 콤마 포맷 적용
                try:
                    result['xrc_price'] = f"{int(float(price)):,}"
                except Exception:
                    result['xrc_price'] = price

    # --- 권리청구기간: getXrcStkOptionXrcInfo (가장 초기 행사시작일 / 가장 최근 행사종료일) ---
    root2 = seibro_api('getXrcStkOptionXrcInfo', {'BOND_ISIN': isin})
    if root2 is not None:
        begin_dates = []
        end_dates   = []
        for result_el in root2.findall('.//result'):
            b = get_attr(result_el, 'XRC_POSS_BEGIN_DT')
            e = get_attr(result_el, 'XRC_POSS_EXPRY_DT')
            if b:
                begin_dates.append(b)
            if e:
                end_dates.append(e)
        if begin_dates:
            result['xrc_begin'] = format_date(min(begin_dates))
        if end_dates:
            result['xrc_end'] = format_date(max(end_dates))

    return result


# ============================================================
# [메인 오케스트레이터] 종목 1개의 모든 데이터 수집
# ============================================================
def get_mezzanine_data(isin, existing_row):
    """
    ISIN 1개에 대해 모든 API 호출 후 시트 업데이트용 데이터 반환
    반환: {
        'corp_name': str,
        'basic_row': [회차, 종류, 발행일, 만기일],   → C~F
        'coupon':    str,                            → G
        'put_call':  dict,                           → M~N, O, Q~S
        'exercise':  dict,                           → I, K, L
    }
    """
    print(f"  🔍 {isin}", end=' ')

    # --- 기본정보 (✅ 항상 실행) ---
    basic = parse_bond_basic(isin)
    if basic:
        print(f"→ {basic['corp_name']} {basic['hosu']}회 {basic['bond_type']}", end=' ')
        corp_name = basic['corp_name']
        basic_row = [basic['hosu'], basic['bond_type'], basic['issu_dt'], basic['xpir_dt']]
        coupon    = basic['coupon']
    else:
        print(f"→ ⚠ getBondStatInfo 실패 (기존값 유지)", end=' ')
        corp_name = existing_row[0].strip() if len(existing_row) > 0 else '-'
        basic_row = [
            existing_row[2].strip() if len(existing_row) > 2 else '-',
            existing_row[3].strip() if len(existing_row) > 3 else '-',
            existing_row[4].strip() if len(existing_row) > 4 else '-',
            existing_row[5].strip() if len(existing_row) > 5 else '-',
        ]
        coupon = existing_row[6].strip() if len(existing_row) > 6 else ''

    # --- PUT/CALL (✅ 채권API 승인됨) ---
    put_call = parse_put_call(isin) if API_BOND_APPROVED else {
        'put_begin': '', 'put_end': '', 'put_date': '',
        'call_ratio': '', 'call_begin': '', 'call_end': '',
    }

    # --- 행사가액·권리청구기간 (⏳ 주식API 승인 대기) ---
    exercise = parse_exercise_info(isin) if API_STOCK_APPROVED else {
        'xrc_price': '', 'xrc_begin': '', 'xrc_end': '',
    }

    print()  # 줄바꿈
    return {
        'corp_name': corp_name,
        'basic_row': basic_row,
        'coupon':    coupon,
        'put_call':  put_call,
        'exercise':  exercise,
    }


# ============================================================
# [메인 실행]
# ============================================================
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

    if not data_rows:
        print("⚠ 데이터 없음. 종료.")
        return

    # 결과 수집
    results = []
    for sheet_row, row in data_rows:
        isin   = row[1].strip()
        result = get_mezzanine_data(isin, row)
        results.append((sheet_row, result))
        await asyncio.sleep(1.0)  # API rate limit

    # --------------------------------------------------------
    # 시트 업데이트 (컬럼별 배치)
    # --------------------------------------------------------
    print("\n📝 시트 업데이트 중...")

    # A열: 종목명
    a_updates = [[r['corp_name']] for _, r in results]
    worksheet.update(
        range_name=f"A{results[0][0]}:A{results[-1][0]}",
        values=a_updates
    )
    await asyncio.sleep(1.0)

    # C~F열: 회차, 종류, 발행일, 만기일
    cf_updates = [r['basic_row'] for _, r in results]
    worksheet.update(
        range_name=f"C{results[0][0]}:F{results[-1][0]}",
        values=cf_updates
    )
    await asyncio.sleep(1.0)

    # G열: Coupon
    g_updates = [[r['coupon']] for _, r in results]
    worksheet.update(
        range_name=f"G{results[0][0]}:G{results[-1][0]}",
        values=g_updates
    )
    await asyncio.sleep(1.0)

    # I열: 행사가액 (주식API 승인 후 활성화)
    if API_STOCK_APPROVED:
        i_updates = [[r['exercise']['xrc_price']] for _, r in results]
        worksheet.update(
            range_name=f"I{results[0][0]}:I{results[-1][0]}",
            values=i_updates
        )
        await asyncio.sleep(1.0)

    # K~L열: 권리청구 시작일, 종료일 (주식API 승인 후 활성화)
    if API_STOCK_APPROVED:
        kl_updates = [
            [r['exercise']['xrc_begin'], r['exercise']['xrc_end']]
            for _, r in results
        ]
        worksheet.update(
            range_name=f"K{results[0][0]}:L{results[-1][0]}",
            values=kl_updates
        )
        await asyncio.sleep(1.0)

    # M~O열: PUT 시작일, 종료일, 상환지급일
    if API_BOND_APPROVED:
        mo_updates = [
            [r['put_call']['put_begin'], r['put_call']['put_end'], r['put_call']['put_date']]
            for _, r in results
        ]
        worksheet.update(
            range_name=f"M{results[0][0]}:O{results[-1][0]}",
            values=mo_updates
        )
        await asyncio.sleep(1.0)

    # Q~S열: CALL 비율, 시작일, 종료일
    if API_BOND_APPROVED:
        qs_updates = [
            [r['put_call']['call_ratio'], r['put_call']['call_begin'], r['put_call']['call_end']]
            for _, r in results
        ]
        worksheet.update(
            range_name=f"Q{results[0][0]}:S{results[-1][0]}",
            values=qs_updates
        )
        await asyncio.sleep(1.0)

    print(f"\n🏁 완료! {len(results)}개 종목 업데이트됨")
    print(f"👉 https://docs.google.com/spreadsheets/d/{SHEET_ID}")
    print(f"\n📌 현재 업데이트 현황:")
    print(f"  ✅ A열  종목명")
    print(f"  ✅ C~F열 회차·종류·발행일·만기일")
    print(f"  ✅ G열  Coupon")
    print(f"  {'✅' if API_BOND_APPROVED else '⏳'} M~O열 PUT 정보")
    print(f"  {'✅' if API_BOND_APPROVED else '⏳'} Q~S열 CALL 정보")
    print(f"  {'✅' if API_STOCK_APPROVED else '⏳'} I열   행사가액")
    print(f"  {'✅' if API_STOCK_APPROVED else '⏳'} K~L열 권리청구기간")


if __name__ == "__main__":
    asyncio.run(main())
