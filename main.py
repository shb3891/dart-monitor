import asyncio
import os
import json
import gspread
import requests
import xml.etree.ElementTree as ET
import re
import zipfile
import io
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials

# ============================================================
# [설정]
# ============================================================
SEIBRO_KEY = os.environ.get('SEIBRO_KEY', 'e1e03a31bc0583fc0c853d4c41a0dc018dc4d2aa21c363c3d6b1b0b96e85221b')
SHEET_ID   = os.environ.get('SHEET_ID',   '1s73BDNtCPe5mOs9EjBE5npEfcaNtYRyWJxRBUmJI-WA')

DART_KEY = (
    os.environ.get('DART_API_KEY') or
    os.environ.get('DART_KEY') or
    os.environ.get('DART_API') or
    'bfc4e4e445de4727ae0bcc27e80ba5cf0e3818e6'
)

# ============================================================
# [API 승인 플래그]
# ============================================================
API_BOND_APPROVED    = True
API_STOCK_APPROVED   = True
API_DERIV_APPROVED   = True
API_CORP_APPROVED    = True
API_FOREIGN_APPROVED = True
API_DART_YTM         = True

# ============================================================
# [테스트 / 디버그 모드]
# ============================================================
TEST_MODE  = True   # 확인 후 False로 변경
DEBUG_MODE = False

DEBUG_ISINS = [
    "KR6177831E26",
    "KR6214271E32",
    "KR6222421DC0",
]

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

SEIBRO_BASE = "https://seibro.or.kr/OpenPlatform/callOpenAPI.jsp"
DART_BASE   = "https://opendart.fss.or.kr/api"

# ============================================================
# [DART 기업코드 맵 (전역 캐시)]
# stock_code(6자리) → corp_code 딕셔너리
# 최초 1회만 다운로드 후 재사용
# ============================================================
_DART_CORP_MAP = {}

def dart_load_corp_map():
    """
    DART 전체 기업코드 ZIP 다운로드 → stock_code:corp_code 딕셔너리 생성
    https://opendart.fss.or.kr/api/corpCode.xml
    """
    global _DART_CORP_MAP
    if _DART_CORP_MAP:
        return  # 이미 로드됨

    try:
        print("  📥 DART 기업코드 전체 다운로드 중...")
        url = f"{DART_BASE}/corpCode.xml"
        r = requests.get(url, params={'crtfc_key': DART_KEY}, timeout=30)
        print(f"  📥 응답코드: {r.status_code} / 크기: {len(r.content):,} bytes")

        z = zipfile.ZipFile(io.BytesIO(r.content))
        xml_data = z.read('CORPCODE.xml')
        root = ET.fromstring(xml_data)

        count = 0
        for item in root.findall('.//list'):
            corp_code  = item.findtext('corp_code', '').strip()
            stock_code = item.findtext('stock_code', '').strip()
            if stock_code and len(stock_code) == 6:
                _DART_CORP_MAP[stock_code] = corp_code
                count += 1

        print(f"  ✅ DART 기업코드 로드 완료: {count:,}개 (상장사)")
    except Exception as e:
        print(f"  ⚠ DART 기업코드 로드 실패: {e}")


def dart_get_corp_code(stock_code_6):
    """stock_code(6자리) → corp_code"""
    dart_load_corp_map()
    corp_code = _DART_CORP_MAP.get(stock_code_6, '')
    if corp_code:
        print(f"    📌 corp_code: {corp_code} (stock: {stock_code_6})")
    else:
        print(f"    ⚠ corp_code 없음 (stock: {stock_code_6})")
    return corp_code


# ============================================================
# [SEIBRO 공통 유틸]
# ============================================================
def seibro_api(api_id, params_dict):
    params_str = ','.join([f"{k}:{v}" for k, v in params_dict.items()])
    full_url   = f"{SEIBRO_BASE}?key={SEIBRO_KEY}&apiId={api_id}&params={params_str}"
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
        print(f"  ⚠ SEIBRO 호출 실패 [{api_id}]: {e}")
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


def fmt_number(val):
    if not val or val == '0':
        return ''
    try:
        return f"{int(float(val)):,}"
    except Exception:
        return val


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
    if 'BW' in nm or '신주인수권' in nm:
        return 'BW'
    return '-'


# ============================================================
# [DART API - 공시 검색 및 YTM 파싱]
# ============================================================
def dart_search_cb_disclosure(corp_code, issu_dt_str):
    """CB/BW/EB 발행결정 공시 검색 → rcept_no 반환"""
    try:
        issu_date = datetime.strptime(issu_dt_str, '%Y-%m-%d')
        bgn_de = (issu_date - timedelta(days=30)).strftime('%Y%m%d')
        end_de = (issu_date + timedelta(days=5)).strftime('%Y%m%d')

        url = f"{DART_BASE}/list.json"
        params = {
            'crtfc_key':  DART_KEY,
            'corp_code':  corp_code,
            'pblntf_ty':  'B',
            'bgn_de':     bgn_de,
            'end_de':     end_de,
            'page_count': 20,
        }
        r = requests.get(url, params=params, timeout=10)
        data = r.json()
        print(f"    🌐 DART list status: {data.get('status')} (기간: {bgn_de}~{end_de})")

        if data.get('status') not in ('000', '013'):
            print(f"    ⚠ 공시목록 조회 실패: {data.get('message', '')}")
            return None

        keywords = ['전환사채', '신주인수권', '교환사채']
        items = data.get('list', [])
        print(f"    📋 공시 {len(items)}건")
        for item in items:
            rpt = item.get('report_nm', '')
            if any(kw in rpt for kw in keywords):
                rcept_no = item.get('rcept_no')
                print(f"    📄 발견: {rpt} ({rcept_no})")
                return rcept_no

        print(f"    ℹ 해당 공시 없음")
    except Exception as e:
        print(f"    ⚠ 공시검색 실패: {e}")
    return None


def dart_parse_ytm(rcept_no):
    """공시 문서 ZIP 다운로드 → 만기이자율 파싱"""
    try:
        url = f"{DART_BASE}/document.xml"
        r = requests.get(url, params={'crtfc_key': DART_KEY, 'rcept_no': rcept_no}, timeout=30)
        print(f"    🌐 document API: {r.status_code} / {len(r.content):,} bytes")

        z = zipfile.ZipFile(io.BytesIO(r.content))

        for fname in z.namelist():
            if not (fname.endswith('.xml') or fname.endswith('.html') or fname.endswith('.htm')):
                continue

            raw = z.read(fname)
            text = None
            for enc in ['utf-8', 'euc-kr', 'cp949']:
                try:
                    text = raw.decode(enc)
                    break
                except Exception:
                    continue
            if not text:
                continue

            clean = re.sub(r'<[^>]+>', ' ', text)
            clean = re.sub(r'\s+', ' ', clean)

            idx = clean.find('만기이자율')
            if idx >= 0:
                print(f"    🔎 '만기이자율' 발견: ...{clean[max(0,idx-10):idx+50]}...")

            patterns = [
                r'만기이자율\s*[\(%\s]*([0-9]+(?:\.[0-9]+)?)',
                r'만기\s*이자율[^0-9]*([0-9]+(?:\.[0-9]+)?)',
                r'보장\s*수익률[^0-9]*([0-9]+(?:\.[0-9]+)?)',
            ]
            for pat in patterns:
                m = re.search(pat, clean)
                if m:
                    val = m.group(1)
                    print(f"    ✅ 만기이자율: {val}%")
                    return val

        print(f"    ℹ 만기이자율 미발견")
    except Exception as e:
        print(f"    ⚠ 문서 파싱 실패: {e}")
    return ''


def parse_dart_ytm_for_bond(isin, issu_dt_str, xrc_stk_isin):
    if not API_DART_YTM:
        return ''

    stock_code_6 = ''
    if xrc_stk_isin and len(xrc_stk_isin) >= 9:
        stock_code_6 = xrc_stk_isin[3:9]

    print(f"    📎 xrc_stk_isin={xrc_stk_isin} → stock_code_6={stock_code_6}")

    if not stock_code_6:
        print(f"    ⚠ stock_code 추출 실패")
        return ''

    corp_code = dart_get_corp_code(stock_code_6)
    if not corp_code:
        return ''

    rcept_no = dart_search_cb_disclosure(corp_code, issu_dt_str)
    if not rcept_no:
        return ''

    return dart_parse_ytm(rcept_no)


# ============================================================
# [디버깅용]
# ============================================================
def debug_raw_xml(isin):
    print(f"\n{'='*60}")
    print(f"🔬 디버깅 ISIN: {isin}")
    apis = [
        ('getXrcStkStatInfo',      {'BOND_ISIN': isin}),
        ('getBondOptionXrcInfo',   {'ISIN': isin}),
        ('getXrcStkOptionXrcInfo', {'BOND_ISIN': isin}),
    ]
    for api_id, params in apis:
        print(f"\n--- {api_id} ---")
        params_str = ','.join([f"{k}:{v}" for k, v in params.items()])
        full_url   = f"{SEIBRO_BASE}?key={SEIBRO_KEY}&apiId={api_id}&params={params_str}"
        try:
            r = requests.get(full_url, timeout=10)
            decoded = r.content.decode('utf-8', errors='replace')
            print(decoded[:3000])
        except Exception as e:
            print(f"  ⚠ 호출 실패: {e}")
    print(f"{'='*60}\n")


# ============================================================
# [SEIBRO 파싱 함수들]
# ============================================================
def parse_bond_basic(isin):
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
    coupon      = coupon_rate if coupon_rate and coupon_rate != '0' else ''

    return {
        'corp_name': corp_name,
        'hosu':      hosu,
        'bond_type': bond_type,
        'issu_dt':   issu_dt,
        'xpir_dt':   xpir_dt,
        'coupon':    coupon,
    }


def parse_put_call(isin):
    root = seibro_api('getBondOptionXrcInfo', {'ISIN': isin})
    result = {
        'put_begin': '', 'put_end': '', 'put_date': '',
        'call_ratio': '', 'call_begin': '', 'call_end': '',
    }
    if root is None:
        return result

    for result_el in root.findall('.//result'):
        option_tpcd = get_attr(result_el, 'OPTION_TPCD')
        xrc_begin   = format_date(get_attr(result_el, 'XRC_BEGIN_DT'))
        xrc_end     = format_date(get_attr(result_el, 'XRC_EXPRY_DT'))
        erly_red_dt = format_date(get_attr(result_el, 'ERLY_RED_DT'))
        xrc_ratio   = get_attr(result_el, 'XRC_RATIO')

        if option_tpcd in ('9402', '9403'):
            result['put_begin'] = result['put_begin'] or xrc_begin
            result['put_end']   = result['put_end']   or xrc_end
            result['put_date']  = result['put_date']  or erly_red_dt

        if option_tpcd in ('9401', '9403'):
            result['call_begin'] = result['call_begin'] or xrc_begin
            result['call_end']   = result['call_end']   or xrc_end
            result['call_ratio'] = result['call_ratio'] or xrc_ratio

    return result


def parse_exercise_info(isin):
    result = {
        'xrc_price': '', 'xrc_begin': '', 'xrc_end': '', 'xrc_stk_isin': '',
    }

    root = seibro_api('getXrcStkStatInfo', {'BOND_ISIN': isin})
    if root is not None:
        result_el = root.find('.//result')
        if result_el is not None:
            result['xrc_price']    = fmt_number(get_attr(result_el, 'XRC_PRICE'))
            result['xrc_stk_isin'] = get_attr(result_el, 'XRC_STK_ISIN')

    root2 = seibro_api('getXrcStkOptionXrcInfo', {'BOND_ISIN': isin})
    if root2 is not None:
        begin_dates, end_dates = [], []
        for result_el in root2.findall('.//result'):
            b = get_attr(result_el, 'XRC_POSS_BEGIN_DT')
            e = get_attr(result_el, 'XRC_POSS_EXPRY_DT')
            if b: begin_dates.append(b)
            if e: end_dates.append(e)
        if begin_dates:
            result['xrc_begin'] = format_date(min(begin_dates))
        if end_dates:
            result['xrc_end'] = format_date(max(end_dates))

    return result


# ============================================================
# [메인 오케스트레이터]
# ============================================================
def get_mezzanine_data(isin, existing_row):
    print(f"  🔍 {isin}", end=' ')

    basic = parse_bond_basic(isin)
    if basic:
        print(f"→ {basic['corp_name']} {basic['hosu']}회 {basic['bond_type']}")
        corp_name = basic['corp_name']
        basic_row = [basic['hosu'], basic['bond_type'], basic['issu_dt'], basic['xpir_dt']]
        coupon    = basic['coupon']
        issu_dt   = basic['issu_dt']
    else:
        print(f"→ ⚠ getBondStatInfo 실패 (기존값 유지)")
        corp_name = existing_row[0].strip() if len(existing_row) > 0 else '-'
        basic_row = [
            existing_row[2].strip() if len(existing_row) > 2 else '-',
            existing_row[3].strip() if len(existing_row) > 3 else '-',
            existing_row[4].strip() if len(existing_row) > 4 else '-',
            existing_row[5].strip() if len(existing_row) > 5 else '-',
        ]
        coupon  = existing_row[6].strip() if len(existing_row) > 6 else ''
        issu_dt = existing_row[4].strip() if len(existing_row) > 4 else ''

    put_call = parse_put_call(isin) if API_BOND_APPROVED else {
        'put_begin': '', 'put_end': '', 'put_date': '',
        'call_ratio': '', 'call_begin': '', 'call_end': '',
    }

    exercise = parse_exercise_info(isin) if API_STOCK_APPROVED else {
        'xrc_price': '', 'xrc_begin': '', 'xrc_end': '', 'xrc_stk_isin': '',
    }

    ytm = ''
    if API_DART_YTM and exercise.get('xrc_stk_isin') and issu_dt and issu_dt != '-':
        print(f"    🌐 DART YTM 조회 중... (발행일: {issu_dt})")
        ytm = parse_dart_ytm_for_bond(isin, issu_dt, exercise['xrc_stk_isin'])
    else:
        print(f"    ℹ DART YTM 스킵 (xrc_stk_isin={exercise.get('xrc_stk_isin')}, issu_dt={issu_dt})")

    return {
        'corp_name': corp_name,
        'basic_row': basic_row,
        'coupon':    coupon,
        'ytm':       ytm,
        'put_call':  put_call,
        'exercise':  exercise,
    }


# ============================================================
# [메인 실행]
# ============================================================
async def main():

    if DEBUG_MODE:
        print("🔬 디버그 모드 (시트 업데이트 안 함)\n")
        for isin in DEBUG_ISINS:
            debug_raw_xml(isin)
            await asyncio.sleep(1.0)
        print("✅ 디버그 완료.")
        return

    print(f"🔑 DART_KEY: {DART_KEY[:6] if DART_KEY else '없음'}...")

    # DART 기업코드 사전 로드 (1회)
    dart_load_corp_map()

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

    results = []
    for sheet_row, row in data_rows:
        isin   = row[1].strip()
        result = get_mezzanine_data(isin, row)
        results.append((sheet_row, result))
        await asyncio.sleep(1.5)

    print("\n📝 시트 업데이트 중...")
    first_row = results[0][0]
    last_row  = results[-1][0]

    worksheet.update([[r['corp_name']] for _, r in results], range_name=f"A{first_row}:A{last_row}")
    await asyncio.sleep(1.0)

    worksheet.update([r['basic_row'] for _, r in results], range_name=f"C{first_row}:F{last_row}")
    await asyncio.sleep(1.0)

    worksheet.update([[r['coupon']] for _, r in results], range_name=f"G{first_row}:G{last_row}")
    await asyncio.sleep(1.0)

    if API_DART_YTM:
        worksheet.update([[r['ytm']] for _, r in results], range_name=f"H{first_row}:H{last_row}")
        await asyncio.sleep(1.0)

    if API_STOCK_APPROVED:
        worksheet.update([[r['exercise']['xrc_price']] for _, r in results], range_name=f"I{first_row}:I{last_row}")
        await asyncio.sleep(1.0)

    if API_STOCK_APPROVED:
        worksheet.update(
            [[r['exercise']['xrc_begin'], r['exercise']['xrc_end']] for _, r in results],
            range_name=f"K{first_row}:L{last_row}"
        )
        await asyncio.sleep(1.0)

    if API_BOND_APPROVED:
        worksheet.update(
            [[r['put_call']['put_begin'], r['put_call']['put_end'], r['put_call']['put_date']] for _, r in results],
            range_name=f"M{first_row}:O{last_row}"
        )
        await asyncio.sleep(1.0)

    if API_BOND_APPROVED:
        worksheet.update(
            [[r['put_call']['call_ratio'], r['put_call']['call_begin'], r['put_call']['call_end']] for _, r in results],
            range_name=f"Q{first_row}:S{last_row}"
        )
        await asyncio.sleep(1.0)

    ytm_count = sum(1 for _, r in results if r.get('ytm'))
    print(f"\n🏁 완료! {len(results)}개 종목 업데이트됨")
    print(f"👉 https://docs.google.com/spreadsheets/d/{SHEET_ID}")
    print(f"\n📌 업데이트 현황:")
    print(f"  ✅ A열    종목명")
    print(f"  ✅ C~F열  회차·종류·발행일·만기일")
    print(f"  ✅ G열    Coupon")
    print(f"  ✅ H열    YTM ← DART 연동 ({ytm_count}/{len(results)}개 성공)")
    print(f"  ✅ I열    행사가액")
    print(f"  ✅ K~L열  권리청구기간")
    print(f"  ✅ M~O열  PUT 정보")
    print(f"  ✅ Q~S열  CALL 정보")
    print(f"  ⏳ J열    리픽싱플로어 (추후 DART 연동)")
    print(f"  ⏳ P열    YTP  (추후)")
    print(f"  ⏳ T열    YTC  (추후)")


if __name__ == "__main__":
    asyncio.run(main())
