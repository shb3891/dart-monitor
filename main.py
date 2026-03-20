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
API_DART             = True   # DART 전체 (YTM + 리픽싱 + PUT + CALL)

# ============================================================
# [테스트 / 디버그 모드]
# ============================================================
TEST_MODE  = True   # ← 5개만 실행
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
# [DART 기업코드 딕셔너리] 전역 캐시
# ============================================================
DART_CORP_DICT = {}

def load_dart_corp_codes():
    global DART_CORP_DICT
    try:
        print(f"  📥 DART 기업코드 전체 다운로드 중...")
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
                DART_CORP_DICT[stock_code] = corp_code
                count += 1

        print(f"  ✅ DART 기업코드 로드 완료: {count:,}개 (상장사)")
    except Exception as e:
        print(f"  ⚠ DART 기업코드 로드 실패: {e}")


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


def parse_korean_date(text):
    """한국어/숫자 날짜 → YYYY-MM-DD"""
    text = str(text).strip()
    # YYYY년 M월 D일
    m = re.search(r'(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일', text)
    if m:
        return f"{m.group(1)}-{m.group(2).zfill(2)}-{m.group(3).zfill(2)}"
    # YYYY.MM.DD
    m = re.match(r'(\d{4})\.(\d{1,2})\.(\d{1,2})', text)
    if m:
        return f"{m.group(1)}-{m.group(2).zfill(2)}-{m.group(3).zfill(2)}"
    # YYYY-MM-DD
    m = re.match(r'(\d{4})-(\d{2})-(\d{2})', text)
    if m:
        return text[:10]
    return ''


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
# [DART API 유틸]
# ============================================================
def dart_get_corp_code(stock_code_6):
    corp_code = DART_CORP_DICT.get(stock_code_6, '')
    if corp_code:
        print(f"    📌 corp_code: {corp_code} (stock: {stock_code_6})")
    else:
        print(f"    ⚠ corp_code 없음 (stock: {stock_code_6})")
    return corp_code


def dart_search_cb_disclosure(corp_code, issu_dt_str):
    """CB/BW/EB 발행결정 공시 검색 → rcept_no 반환"""
    try:
        issu_date = datetime.strptime(issu_dt_str, '%Y-%m-%d')
        bgn_de = (issu_date - timedelta(days=60)).strftime('%Y%m%d')
        end_de = (issu_date + timedelta(days=10)).strftime('%Y%m%d')

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
            print(f"    ⚠ DART 공시목록 조회 실패: {data.get('message', '')}")
            return None

        # ← 핵심 수정: 발행결정 공시만 인식 (취득결정, 행사자지정 등 제외)
        issue_keywords = [
            '전환사채권발행결정',
            '교환사채권발행결정',
            '신주인수권부사채권발행결정',
        ]

        items = data.get('list', [])
        print(f"    📋 공시 {len(items)}건")
        for item in items:
            rpt = item.get('report_nm', '')
            if any(kw in rpt for kw in issue_keywords):
                rcept_no = item.get('rcept_no')
                print(f"    📄 발견: {rpt} ({rcept_no})")
                return rcept_no

        print(f"    ℹ 발행결정 공시 없음")
    except Exception as e:
        print(f"    ⚠ DART 공시검색 실패: {e}")
    return None


def dart_parse_disclosure(rcept_no):
    """
    공시 문서 ZIP 다운로드 → YTM + 리픽싱플로어 + PUT + CALL 파싱
    """
    result = {
        'ytm':        '',
        'rfxg_floor': '',
        'put_begin':  '',
        'put_end':    '',
        'put_date':   '',
        'call_ratio': '',
        'call_begin': '',
        'call_end':   '',
    }
    try:
        url = f"{DART_BASE}/document.xml"
        r = requests.get(url, params={'crtfc_key': DART_KEY, 'rcept_no': rcept_no}, timeout=30)
        print(f"    🌐 document API: {r.status_code} / {len(r.content):,} bytes")

        z = zipfile.ZipFile(io.BytesIO(r.content))

        for fname in z.namelist():
            if not any(fname.endswith(ext) for ext in ['.xml', '.html', '.htm']):
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

            # HTML 태그 제거 + 공백 정리
            clean = re.sub(r'<[^>]+>', ' ', text)
            clean = re.sub(r'&nbsp;', ' ', clean)
            clean = re.sub(r'\s+', ' ', clean)

            # ── 1) YTM ─────────────────────────────────────────
            for pat in [
                r'만기이자율\s*[\(%]*\s*([0-9]+(?:\.[0-9]+)?)',
                r'만기\s*이자율[^0-9]*([0-9]+(?:\.[0-9]+)?)',
                r'보장\s*수익률[^0-9]*([0-9]+(?:\.[0-9]+)?)',
            ]:
                m = re.search(pat, clean)
                if m:
                    result['ytm'] = m.group(1)
                    print(f"    ✅ YTM: {result['ytm']}%")
                    break

            # ── 2) 리픽싱 플로어 ───────────────────────────────
            for pat in [
                r'최저\s*조정\s*가액[^0-9\n]{0,10}([0-9,]+)',
                r'최저\s*전환\s*가액[^0-9\n]{0,10}([0-9,]+)',
                r'최저\s*행사\s*가액[^0-9\n]{0,10}([0-9,]+)',
                r'리픽싱.*?하한[^0-9\n]{0,10}([0-9,]+)',
            ]:
                m = re.search(pat, clean)
                if m:
                    val = m.group(1).replace(',', '')
                    try:
                        if int(val) > 100:  # 가액(원)이므로 100원 초과인 경우만
                            result['rfxg_floor'] = f"{int(val):,}"
                            print(f"    ✅ 리픽싱플로어: {result['rfxg_floor']}")
                            break
                    except Exception:
                        pass

            # ── 3) PUT - 조기상환청구기간 ──────────────────────
            put_idx = -1
            for put_kw in ['조기상환 청구기간', '조기상환청구기간', 'PUT 옵션', 'Put옵션']:
                idx = clean.find(put_kw)
                if idx >= 0:
                    put_idx = idx
                    break

            if put_idx >= 0:
                put_section = clean[put_idx: put_idx + 3000]
                dates = re.findall(r'\d{4}-\d{2}-\d{2}', put_section)
                # 날짜가 3의 배수로 (FROM, TO, 상환기일) 반복
                n = len(dates) // 3
                if n >= 1:
                    froms  = [dates[i * 3]     for i in range(n)]
                    tos    = [dates[i * 3 + 1] for i in range(n)]
                    redeem = [dates[i * 3 + 2] for i in range(n)]
                    result['put_begin'] = min(froms)
                    result['put_end']   = max(tos)
                    result['put_date']  = redeem[0]
                    print(f"    ✅ PUT: {result['put_begin']} ~ {result['put_end']}, 상환일: {result['put_date']}")

            # ── 4) CALL - 매도청구권 ───────────────────────────
            call_idx = -1
            for call_kw in ['매도청구권', '매도 청구권', 'Call Option', 'CALL Option', '콜옵션', '콜 옵션']:
                m_idx = re.search(re.escape(call_kw), clean, re.IGNORECASE)
                if m_idx:
                    call_idx = m_idx.start()
                    break

            if call_idx >= 0:
                call_section = clean[max(0, call_idx - 200): call_idx + 2000]

                # 날짜 범위: "YYYY년 M월 D일부터 ~ YYYY년 M월 D일까지"
                begin_m = re.search(r'(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)부터', call_section)
                end_m   = re.search(r'(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)까지', call_section)

                if begin_m:
                    result['call_begin'] = parse_korean_date(begin_m.group(1))
                if end_m:
                    result['call_end'] = parse_korean_date(end_m.group(1))

                # CALL 비율
                for ratio_pat in [
                    r'Call\s*Option\s*([0-9]+(?:\.[0-9]+)?)%',
                    r'콜\s*옵션\s*([0-9]+(?:\.[0-9]+)?)%',
                    r'([0-9]+(?:\.[0-9]+)?)%를?\s*총\s*한도',
                    r'전자등록총액\s*([0-9]+(?:\.[0-9]+)?)%',
                    r'발행총액\s*([0-9]+(?:\.[0-9]+)?)%',
                    r'취득규모.*?([0-9]+(?:\.[0-9]+)?)%\)',
                ]:
                    ratio_m = re.search(ratio_pat, call_section, re.IGNORECASE)
                    if ratio_m:
                        result['call_ratio'] = ratio_m.group(1)
                        break

                if result['call_begin'] or result['call_ratio']:
                    print(f"    ✅ CALL: {result['call_begin']} ~ {result['call_end']}, 비율: {result['call_ratio']}%")

    except Exception as e:
        print(f"    ⚠ DART 문서 파싱 실패: {e}")

    return result


def parse_dart_for_bond(isin, issu_dt_str, xrc_stk_isin):
    """DART에서 YTM, 리픽싱플로어, PUT, CALL 전부 파싱"""
    empty = {
        'ytm': '', 'rfxg_floor': '',
        'put_begin': '', 'put_end': '', 'put_date': '',
        'call_ratio': '', 'call_begin': '', 'call_end': '',
    }

    if not API_DART:
        return empty

    stock_code_6 = ''
    if xrc_stk_isin and len(xrc_stk_isin) >= 9:
        stock_code_6 = xrc_stk_isin[3:9]

    print(f"    📎 xrc_stk_isin={xrc_stk_isin} → stock_code_6={stock_code_6}")

    if not stock_code_6:
        print(f"    ⚠ 주식 단축코드 추출 실패 → 중단")
        return empty

    corp_code = dart_get_corp_code(stock_code_6)
    if not corp_code:
        return empty

    rcept_no = dart_search_cb_disclosure(corp_code, issu_dt_str)
    if not rcept_no:
        return empty

    return dart_parse_disclosure(rcept_no)


# ============================================================
# [디버깅용] raw XML 출력 함수
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

    # ← 수정: 0도 표시 (빈값/None만 빈칸 처리)
    coupon = coupon_rate if coupon_rate else ''

    return {
        'corp_name': corp_name,
        'hosu':      hosu,
        'bond_type': bond_type,
        'issu_dt':   issu_dt,
        'xpir_dt':   xpir_dt,
        'coupon':    coupon,
    }


def parse_put_call_seibro(isin):
    """SEIBRO PUT/CALL (이력 기반 - 보완용)"""
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
        'xrc_price':    '',
        'xrc_begin':    '',
        'xrc_end':      '',
        'xrc_stk_isin': '',
    }

    root = seibro_api('getXrcStkStatInfo', {'BOND_ISIN': isin})
    if root is not None:
        result_el = root.find('.//result')
        if result_el is not None:
            result['xrc_price']    = fmt_number(get_attr(result_el, 'XRC_PRICE'))
            result['xrc_stk_isin'] = get_attr(result_el, 'XRC_STK_ISIN')

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
# [메인 오케스트레이터]
# ============================================================
def get_mezzanine_data(isin, existing_row):
    print(f"  🔍 {isin}", end=' ')

    # ── SEIBRO 기본정보 ──
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

    # ── SEIBRO 행사가액·권리청구기간 ──
    exercise = parse_exercise_info(isin) if API_STOCK_APPROVED else {
        'xrc_price': '', 'xrc_begin': '', 'xrc_end': '', 'xrc_stk_isin': '',
    }

    # ── SEIBRO PUT/CALL (이력 기반 보완용) ──
    seibro_put_call = parse_put_call_seibro(isin) if API_BOND_APPROVED else {
        'put_begin': '', 'put_end': '', 'put_date': '',
        'call_ratio': '', 'call_begin': '', 'call_end': '',
    }

    # ── DART (YTM + 리픽싱 + PUT + CALL) ──
    dart_data = {
        'ytm': '', 'rfxg_floor': '',
        'put_begin': '', 'put_end': '', 'put_date': '',
        'call_ratio': '', 'call_begin': '', 'call_end': '',
    }
    if API_DART and exercise.get('xrc_stk_isin') and issu_dt and issu_dt != '-':
        print(f"    🌐 DART 조회 중... (발행일: {issu_dt})")
        dart_data = parse_dart_for_bond(isin, issu_dt, exercise['xrc_stk_isin'])
    else:
        print(f"    ℹ DART 스킵 (xrc_stk_isin={exercise.get('xrc_stk_isin')}, issu_dt={issu_dt})")

    # ── DART 우선, SEIBRO 보완 ──
    final_put_call = {
        'put_begin':  dart_data['put_begin']  or seibro_put_call['put_begin'],
        'put_end':    dart_data['put_end']    or seibro_put_call['put_end'],
        'put_date':   dart_data['put_date']   or seibro_put_call['put_date'],
        'call_ratio': dart_data['call_ratio'] or seibro_put_call['call_ratio'],
        'call_begin': dart_data['call_begin'] or seibro_put_call['call_begin'],
        'call_end':   dart_data['call_end']   or seibro_put_call['call_end'],
    }

    return {
        'corp_name':  corp_name,
        'basic_row':  basic_row,
        'coupon':     coupon,
        'ytm':        dart_data['ytm'],
        'rfxg_floor': dart_data['rfxg_floor'],
        'put_call':   final_put_call,
        'exercise':   exercise,
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

    print(f"🔑 DART_KEY 로드 확인: {DART_KEY[:6] if DART_KEY else '없음'}...")

    if API_DART:
        load_dart_corp_codes()

    print("📋 스프레드시트 읽는 중...")
    all_values = worksheet.get_all_values()

    data_rows = [
        (i + 2, row)
        for i, row in enumerate(all_values[1:])
        if len(row) > 1 and row[1].strip().startswith('KR')
    ]

    if TEST_MODE:
        data_rows = data_rows[:5]
        print(f"🧪 테스트 모드: 상위 5개만\n")
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

    # A열: 종목명
    worksheet.update(
        [[r['corp_name']] for _, r in results],
        range_name=f"A{first_row}:A{last_row}"
    )
    await asyncio.sleep(1.0)

    # C~F열: 회차, 종류, 발행일, 만기일
    worksheet.update(
        [r['basic_row'] for _, r in results],
        range_name=f"C{first_row}:F{last_row}"
    )
    await asyncio.sleep(1.0)

    # G열: Coupon (0 포함)
    worksheet.update(
        [[r['coupon']] for _, r in results],
        range_name=f"G{first_row}:G{last_row}"
    )
    await asyncio.sleep(1.0)

    # H열: YTM (DART)
    worksheet.update(
        [[r['ytm']] for _, r in results],
        range_name=f"H{first_row}:H{last_row}"
    )
    await asyncio.sleep(1.0)

    # I열: 행사가액 (SEIBRO)
    if API_STOCK_APPROVED:
        worksheet.update(
            [[r['exercise']['xrc_price']] for _, r in results],
            range_name=f"I{first_row}:I{last_row}"
        )
        await asyncio.sleep(1.0)

    # J열: 리픽싱플로어 (DART) ← 신규
    worksheet.update(
        [[r['rfxg_floor']] for _, r in results],
        range_name=f"J{first_row}:J{last_row}"
    )
    await asyncio.sleep(1.0)

    # K~L열: 권리청구 시작일, 종료일 (SEIBRO)
    if API_STOCK_APPROVED:
        worksheet.update(
            [[r['exercise']['xrc_begin'], r['exercise']['xrc_end']] for _, r in results],
            range_name=f"K{first_row}:L{last_row}"
        )
        await asyncio.sleep(1.0)

    # M~O열: PUT 시작일, 종료일, 상환지급일 (DART 우선)
    worksheet.update(
        [[r['put_call']['put_begin'], r['put_call']['put_end'], r['put_call']['put_date']] for _, r in results],
        range_name=f"M{first_row}:O{last_row}"
    )
    await asyncio.sleep(1.0)

    # Q~S열: CALL 비율, 시작일, 종료일 (DART 우선)
    worksheet.update(
        [[r['put_call']['call_ratio'], r['put_call']['call_begin'], r['put_call']['call_end']] for _, r in results],
        range_name=f"Q{first_row}:S{last_row}"
    )
    await asyncio.sleep(1.0)

    ytm_count   = sum(1 for _, r in results if r.get('ytm'))
    rfxg_count  = sum(1 for _, r in results if r.get('rfxg_floor'))
    put_count   = sum(1 for _, r in results if r['put_call']['put_begin'])
    call_count  = sum(1 for _, r in results if r['put_call']['call_begin'] or r['put_call']['call_ratio'])

    print(f"\n🏁 완료! {len(results)}개 종목 업데이트됨")
    print(f"👉 https://docs.google.com/spreadsheets/d/{SHEET_ID}")
    print(f"\n📌 업데이트 현황:")
    print(f"  ✅ A열    종목명")
    print(f"  ✅ C~F열  회차·종류·발행일·만기일")
    print(f"  ✅ G열    Coupon (0 포함)")
    print(f"  ✅ H열    YTM ← DART ({ytm_count}/{len(results)}개)")
    print(f"  ✅ I열    행사가액")
    print(f"  ✅ J열    리픽싱플로어 ← DART ({rfxg_count}/{len(results)}개)")
    print(f"  ✅ K~L열  권리청구기간")
    print(f"  ✅ M~O열  PUT 정보 ← DART 우선 ({put_count}/{len(results)}개)")
    print(f"  ✅ Q~S열  CALL 정보 ← DART 우선 ({call_count}/{len(results)}개)")
    print(f"  ⏳ P열    YTP  (추후)")
    print(f"  ⏳ T열    YTC  (추후)")


if __name__ == "__main__":
    asyncio.run(main())
