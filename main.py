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

TODAY = datetime.now().strftime('%Y-%m-%d')

# ============================================================
# [API 승인 플래그]
# ============================================================
API_BOND_APPROVED = True
API_STOCK_APPROVED = True
API_DART = True

# ============================================================
# [테스트 / 디버그 모드]
# ============================================================
TEST_MODE  = True   # 5개 테스트
DEBUG_MODE = False

DEBUG_ISINS = ["KR6177831E26", "KR6214271E32", "KR6222421DC0"]

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
# [DART 기업코드 딕셔너리]
# ============================================================
DART_CORP_DICT = {}

def load_dart_corp_codes():
    global DART_CORP_DICT
    try:
        print("  📥 DART 기업코드 전체 다운로드 중...")
        r = requests.get(f"{DART_BASE}/corpCode.xml", params={'crtfc_key': DART_KEY}, timeout=30)
        print(f"  📥 응답코드: {r.status_code} / 크기: {len(r.content):,} bytes")
        z = zipfile.ZipFile(io.BytesIO(r.content))
        root = ET.fromstring(z.read('CORPCODE.xml'))
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
# [공통 유틸]
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
    m = re.search(r'(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일', text)
    if m:
        return f"{m.group(1)}-{m.group(2).zfill(2)}-{m.group(3).zfill(2)}"
    m = re.match(r'(\d{4})\.(\d{1,2})\.(\d{1,2})', text)
    if m:
        return f"{m.group(1)}-{m.group(2).zfill(2)}-{m.group(3).zfill(2)}"
    m = re.match(r'(\d{4})-(\d{2})-(\d{2})', text)
    if m:
        return text[:10]
    return ''


def find_next_upcoming_row(rows):
    """
    rows: list of (from_dt, to_dt, pay_dt) YYYY-MM-DD
    오늘 이후 가장 가까운 차 반환. 모두 지났으면 마지막 반환.
    """
    for from_dt, to_dt, pay_dt in rows:
        if from_dt >= TODAY or (from_dt <= TODAY <= to_dt):
            return from_dt, to_dt, pay_dt
    if rows:
        return rows[-1]
    return '', '', ''


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
# [DART 파싱]
# ============================================================
def dart_get_corp_code(stock_code_6):
    corp_code = DART_CORP_DICT.get(stock_code_6, '')
    if corp_code:
        print(f"    📌 corp_code: {corp_code} (stock: {stock_code_6})")
    else:
        print(f"    ⚠ corp_code 없음 (stock: {stock_code_6})")
    return corp_code


def dart_search_cb_disclosure(corp_code, issu_dt_str):
    try:
        issu_date = datetime.strptime(issu_dt_str, '%Y-%m-%d')
        bgn_de = (issu_date - timedelta(days=60)).strftime('%Y%m%d')
        end_de = (issu_date + timedelta(days=10)).strftime('%Y%m%d')
        params = {
            'crtfc_key': DART_KEY, 'corp_code': corp_code,
            'pblntf_ty': 'B', 'bgn_de': bgn_de,
            'end_de': end_de, 'page_count': 20,
        }
        r = requests.get(f"{DART_BASE}/list.json", params=params, timeout=10)
        data = r.json()
        print(f"    🌐 DART list: {data.get('status')} ({bgn_de}~{end_de})")
        if data.get('status') not in ('000', '013'):
            return None
        issue_kws = ['전환사채권발행결정', '교환사채권발행결정', '신주인수권부사채권발행결정']
        items = data.get('list', [])
        print(f"    📋 공시 {len(items)}건")
        for item in items:
            rpt = item.get('report_nm', '')
            if any(kw in rpt for kw in issue_kws):
                rcept_no = item.get('rcept_no')
                print(f"    📄 발견: {rpt} ({rcept_no})")
                return rcept_no
        print("    ℹ 발행결정 공시 없음")
    except Exception as e:
        print(f"    ⚠ DART 공시검색 실패: {e}")
    return None


def dart_parse_disclosure(rcept_no, xrc_price=''):
    """
    공시 원문에서 YTM / 리픽싱플로어 / 전환청구기간 / PUT / CALL / YTC 파싱
    xrc_price: 리픽싱 없음(-) 판정 시 fallback 행사가액
    """
    result = {
        'ytm':            '',
        'rfxg_floor':     '',
        'xrc_begin_dart': '',
        'xrc_end_dart':   '',
        'put_begin':      '',
        'put_end':        '',
        'put_date':       '',
        'call_ratio':     '',
        'call_begin':     '',
        'call_end':       '',
        'ytc':            '',
    }

    try:
        r = requests.get(
            f"{DART_BASE}/document.xml",
            params={'crtfc_key': DART_KEY, 'rcept_no': rcept_no},
            timeout=30
        )
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

            # ── 1) YTM ──────────────────────────────────────────────
            for pat in [
                r'만기이자율[^0-9\n]*([0-9]+(?:\.[0-9]+)?)',
                r'만기\s*이자율[^0-9\n]*([0-9]+(?:\.[0-9]+)?)',
            ]:
                m = re.search(pat, clean)
                if m:
                    try:
                        val = float(m.group(1))
                        if val < 100:   # 연이율은 100% 미만 (연도 2025 등 오탐 방지)
                            result['ytm'] = m.group(1)
                            print(f"    ✅ YTM: {val}%")
                            break
                    except Exception:
                        pass

            # ── 2) 리픽싱플로어 ──────────────────────────────────────
            # '최저 조정가액 (원) -' → 리픽싱 없음 → 행사가액과 동일
            rfxg_m = re.search(
                r'최저\s*(?:조정|전환|행사)\s*가액[^0-9\n]*?(-|–|[0-9][0-9,]+)',
                clean
            )
            if rfxg_m:
                val = rfxg_m.group(1).strip()
                if val in ('-', '–'):
                    result['rfxg_floor'] = xrc_price
                    print(f"    ✅ 리픽싱플로어: 없음(-)→ 행사가액={xrc_price}")
                else:
                    val_n = val.replace(',', '')
                    try:
                        n = int(float(val_n))
                        if n > 100:
                            result['rfxg_floor'] = f"{n:,}"
                            print(f"    ✅ 리픽싱플로어: {result['rfxg_floor']}")
                    except Exception:
                        pass
            else:
                # 섹션 자체 없으면 행사가액과 동일 처리
                if xrc_price:
                    result['rfxg_floor'] = xrc_price
                    print(f"    ✅ 리픽싱플로어: 미기재→ 행사가액={xrc_price}")

            # ── 3) 전환청구기간 (K/L열) ──────────────────────────────
            for kw in ['전환청구기간', '교환청구기간', '행사청구기간', '권리행사기간', '전환권 행사']:
                idx = clean.find(kw)
                if idx < 0:
                    continue
                section = clean[idx: idx + 500]
                m_b = re.search(r'시작일[^0-9\n]*(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)', section)
                m_e = re.search(r'종료일[^0-9\n]*(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)', section)
                if not m_b:
                    m_b = re.search(r'시작일[^0-9\n]*(\d{4}-\d{2}-\d{2})', section)
                if not m_e:
                    m_e = re.search(r'종료일[^0-9\n]*(\d{4}-\d{2}-\d{2})', section)
                if m_b:
                    result['xrc_begin_dart'] = parse_korean_date(m_b.group(1))
                    print(f"    ✅ 전환청구시작: {result['xrc_begin_dart']}")
                if m_e:
                    result['xrc_end_dart'] = parse_korean_date(m_e.group(1))
                    print(f"    ✅ 전환청구종료: {result['xrc_end_dart']}")
                if result['xrc_begin_dart'] or result['xrc_end_dart']:
                    break

            # ── 4) PUT - 조기상환청구기간 ────────────────────────────
            put_idx = -1
            for kw in ['조기상환 청구기간', '조기상환청구기간', '[조기상환청구권', '조기상환청구권(Put']:
                idx = clean.find(kw)
                if idx >= 0:
                    put_idx = idx
                    break

            if put_idx >= 0:
                put_section = clean[put_idx: put_idx + 5000]
                dates = re.findall(r'\d{4}-\d{2}-\d{2}', put_section)
                rows = [
                    (dates[i*3], dates[i*3+1], dates[i*3+2])
                    for i in range(len(dates) // 3)
                ]
                if rows:
                    f, t, p = find_next_upcoming_row(rows)
                    result['put_begin'] = f
                    result['put_end']   = t
                    result['put_date']  = p
                    print(f"    ✅ PUT: {f}~{t}, 상환: {p}")
            else:
                result['put_begin'] = '-'
                result['put_end']   = '-'
                result['put_date']  = '-'
                print("    ℹ PUT 없음(-)")

            # ── 5) CALL - 매도청구권 ─────────────────────────────────
            call_idx = -1
            for kw in [
                '매도청구권(Call Option)', '매도청구권(call option)',
                '[매도청구권', '매도청구권에 관한', '매도 청구권',
                '콜옵션', '콜 옵션',
            ]:
                m_idx = re.search(re.escape(kw), clean, re.IGNORECASE)
                if m_idx:
                    call_idx = m_idx.start()
                    break

            if call_idx >= 0:
                call_section = clean[call_idx: call_idx + 5000]

                # YTC: 연 단리 X%
                ytc_m = re.search(r'연\s*단리\s*([0-9]+(?:\.[0-9]+)?)\s*%', call_section)
                if ytc_m:
                    result['ytc'] = ytc_m.group(1)
                    print(f"    ✅ YTC: {result['ytc']}%")

                # CALL 비율
                for ratio_pat in [
                    r'([0-9]+(?:\.[0-9]+)?)%를?\s*총\s*한도',
                    r'\(Call\s*Option\s*([0-9]+(?:\.[0-9]+)?)\s*%\)',
                    r'Call\s*Option\s*([0-9]+(?:\.[0-9]+)?)\s*%',
                    r'전자등록총액\s*([0-9]+(?:\.[0-9]+)?)\s*%',
                    r'발행총액[^0-9]*([0-9]+(?:\.[0-9]+)?)\s*%',
                ]:
                    ratio_m = re.search(ratio_pat, call_section, re.IGNORECASE)
                    if ratio_m:
                        try:
                            val = float(ratio_m.group(1))
                            if val <= 100:
                                result['call_ratio'] = ratio_m.group(1)
                                print(f"    ✅ CALL비율: {result['call_ratio']}%")
                                break
                        except Exception:
                            pass

                # CALL 날짜 테이블 (FROM, TO, 매매일) - 3개씩 묶기
                call_dates = re.findall(r'\d{4}-\d{2}-\d{2}', call_section)
                call_rows = [
                    (call_dates[i*3], call_dates[i*3+1], call_dates[i*3+2])
                    for i in range(len(call_dates) // 3)
                ]
                if call_rows:
                    f, t, _ = find_next_upcoming_row(call_rows)
                    result['call_begin'] = f
                    result['call_end']   = t
                    print(f"    ✅ CALL일정: {f}~{t}")
                else:
                    # 한국어 날짜 형식 시도
                    b_m = re.search(r'(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)부터', call_section)
                    e_m = re.search(r'(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)까지', call_section)
                    if b_m:
                        result['call_begin'] = parse_korean_date(b_m.group(1))
                    if e_m:
                        result['call_end'] = parse_korean_date(e_m.group(1))
                    if result['call_begin']:
                        print(f"    ✅ CALL일정(한국어): {result['call_begin']}~{result['call_end']}")
            else:
                result['call_begin'] = '-'
                result['call_end']   = '-'
                result['call_ratio'] = '-'
                print("    ℹ CALL 없음(-)")

    except Exception as e:
        print(f"    ⚠ DART 문서 파싱 실패: {e}")

    return result


def parse_dart_for_bond(isin, issu_dt_str, xrc_stk_isin, xrc_price=''):
    empty = {
        'ytm': '', 'rfxg_floor': '',
        'xrc_begin_dart': '', 'xrc_end_dart': '',
        'put_begin': '', 'put_end': '', 'put_date': '',
        'call_ratio': '', 'call_begin': '', 'call_end': '',
        'ytc': '',
    }
    if not API_DART:
        return empty

    stock_code_6 = xrc_stk_isin[3:9] if xrc_stk_isin and len(xrc_stk_isin) >= 9 else ''
    print(f"    📎 xrc_stk_isin={xrc_stk_isin} → stock_code_6={stock_code_6}")
    if not stock_code_6:
        return empty

    corp_code = dart_get_corp_code(stock_code_6)
    if not corp_code:
        return empty

    rcept_no = dart_search_cb_disclosure(corp_code, issu_dt_str)
    if not rcept_no:
        return empty

    return dart_parse_disclosure(rcept_no, xrc_price=xrc_price)


# ============================================================
# [SEIBRO 파싱]
# ============================================================
def parse_bond_basic(isin):
    root = seibro_api('getBondStatInfo', {'ISIN': isin})
    if root is None:
        return None
    el = root.find('.//result')
    if el is None:
        return None
    secn_nm = get_attr(el, 'KOR_SECN_NM')
    return {
        'corp_name': extract_corp_name(secn_nm),
        'hosu':      extract_hosu(secn_nm),
        'bond_type': determine_bond_type(secn_nm),
        'issu_dt':   format_date(get_attr(el, 'ISSU_DT')),
        'xpir_dt':   format_date(get_attr(el, 'XPIR_DT')),
        'coupon':    get_attr(el, 'COUPON_RATE'),   # 0 포함, 빈값만 제외
    }


def parse_put_call_seibro(isin):
    """SEIBRO PUT/CALL (이력 기반 - DART 없을 때 보완용)"""
    root = seibro_api('getBondOptionXrcInfo', {'ISIN': isin})
    r = {'put_begin': '', 'put_end': '', 'put_date': '',
         'call_ratio': '', 'call_begin': '', 'call_end': ''}
    if root is None:
        return r
    for el in root.findall('.//result'):
        tpcd  = get_attr(el, 'OPTION_TPCD')
        begin = format_date(get_attr(el, 'XRC_BEGIN_DT'))
        end   = format_date(get_attr(el, 'XRC_EXPRY_DT'))
        pay   = format_date(get_attr(el, 'ERLY_RED_DT'))
        ratio = get_attr(el, 'XRC_RATIO')
        if tpcd in ('9402', '9403'):
            r['put_begin'] = r['put_begin'] or begin
            r['put_end']   = r['put_end']   or end
            r['put_date']  = r['put_date']  or pay
        if tpcd in ('9401', '9403'):
            r['call_begin'] = r['call_begin'] or begin
            r['call_end']   = r['call_end']   or end
            r['call_ratio'] = r['call_ratio'] or ratio
    return r


def parse_exercise_info(isin):
    r = {'xrc_price': '', 'xrc_begin': '', 'xrc_end': '', 'xrc_stk_isin': ''}
    root = seibro_api('getXrcStkStatInfo', {'BOND_ISIN': isin})
    if root is not None:
        el = root.find('.//result')
        if el is not None:
            r['xrc_price']    = fmt_number(get_attr(el, 'XRC_PRICE'))
            r['xrc_stk_isin'] = get_attr(el, 'XRC_STK_ISIN')
    root2 = seibro_api('getXrcStkOptionXrcInfo', {'BOND_ISIN': isin})
    if root2 is not None:
        begins, ends = [], []
        for el in root2.findall('.//result'):
            b = get_attr(el, 'XRC_POSS_BEGIN_DT')
            e = get_attr(el, 'XRC_POSS_EXPRY_DT')
            if b: begins.append(b)
            if e: ends.append(e)
        if begins: r['xrc_begin'] = format_date(min(begins))
        if ends:   r['xrc_end']   = format_date(max(ends))
    return r


# ============================================================
# [디버깅용]
# ============================================================
def debug_raw_xml(isin):
    print(f"\n{'='*60}\n🔬 디버깅 ISIN: {isin}")
    for api_id, params in [
        ('getXrcStkStatInfo', {'BOND_ISIN': isin}),
        ('getBondOptionXrcInfo', {'ISIN': isin}),
        ('getXrcStkOptionXrcInfo', {'BOND_ISIN': isin}),
    ]:
        print(f"\n--- {api_id} ---")
        params_str = ','.join([f"{k}:{v}" for k, v in params.items()])
        try:
            r = requests.get(
                f"{SEIBRO_BASE}?key={SEIBRO_KEY}&apiId={api_id}&params={params_str}",
                timeout=10
            )
            print(r.content.decode('utf-8', errors='replace')[:3000])
        except Exception as e:
            print(f"  ⚠ {e}")
    print(f"{'='*60}\n")


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
        print("→ ⚠ getBondStatInfo 실패 (기존값 유지)")
        corp_name = existing_row[0].strip() if existing_row else '-'
        basic_row = [
            existing_row[2].strip() if len(existing_row) > 2 else '-',
            existing_row[3].strip() if len(existing_row) > 3 else '-',
            existing_row[4].strip() if len(existing_row) > 4 else '-',
            existing_row[5].strip() if len(existing_row) > 5 else '-',
        ]
        coupon  = existing_row[6].strip() if len(existing_row) > 6 else ''
        issu_dt = existing_row[4].strip() if len(existing_row) > 4 else ''

    # ── SEIBRO 행사가액 / 권리청구기간 ──
    exercise = parse_exercise_info(isin) if API_STOCK_APPROVED else {
        'xrc_price': '', 'xrc_begin': '', 'xrc_end': '', 'xrc_stk_isin': '',
    }

    # ── SEIBRO PUT/CALL (보완용) ──
    seibro_pc = parse_put_call_seibro(isin) if API_BOND_APPROVED else {
        'put_begin': '', 'put_end': '', 'put_date': '',
        'call_ratio': '', 'call_begin': '', 'call_end': '',
    }

    # ── DART (YTM / 리픽싱 / 전환청구기간 / PUT / CALL / YTC) ──
    dart_data = {
        'ytm': '', 'rfxg_floor': '',
        'xrc_begin_dart': '', 'xrc_end_dart': '',
        'put_begin': '', 'put_end': '', 'put_date': '',
        'call_ratio': '', 'call_begin': '', 'call_end': '',
        'ytc': '',
    }
    if API_DART and exercise.get('xrc_stk_isin') and issu_dt and issu_dt != '-':
        print(f"    🌐 DART 조회 중... (발행일: {issu_dt})")
        dart_data = parse_dart_for_bond(
            isin, issu_dt, exercise['xrc_stk_isin'],
            xrc_price=exercise.get('xrc_price', '')
        )
    else:
        print(f"    ℹ DART 스킵 (xrc_stk_isin={exercise.get('xrc_stk_isin')}, issu_dt={issu_dt})")

    # K/L열: DART 우선, SEIBRO 보완
    final_xrc_begin = dart_data.get('xrc_begin_dart') or exercise['xrc_begin']
    final_xrc_end   = dart_data.get('xrc_end_dart')   or exercise['xrc_end']

    # M/N/O열: DART '-' 이면 그대로, 아니면 DART 우선 + SEIBRO 보완
    if dart_data['put_begin'] == '-':
        final_put = {'put_begin': '-', 'put_end': '-', 'put_date': '-'}
    else:
        final_put = {
            'put_begin': dart_data['put_begin'] or seibro_pc['put_begin'],
            'put_end':   dart_data['put_end']   or seibro_pc['put_end'],
            'put_date':  dart_data['put_date']  or seibro_pc['put_date'],
        }

    # Q/R/S열: DART '-' 이면 그대로
    if dart_data['call_ratio'] == '-':
        final_call = {'call_ratio': '-', 'call_begin': '-', 'call_end': '-'}
    else:
        final_call = {
            'call_ratio': dart_data['call_ratio'] or seibro_pc['call_ratio'],
            'call_begin': dart_data['call_begin'] or seibro_pc['call_begin'],
            'call_end':   dart_data['call_end']   or seibro_pc['call_end'],
        }

    return {
        'corp_name':  corp_name,
        'basic_row':  basic_row,
        'coupon':     coupon,
        'ytm':        dart_data['ytm'],
        'xrc_price':  exercise.get('xrc_price', ''),
        'rfxg_floor': dart_data['rfxg_floor'],
        'xrc_begin':  final_xrc_begin,
        'xrc_end':    final_xrc_end,
        'put':        final_put,
        'call':       final_call,
        'ytc':        dart_data.get('ytc', ''),
    }


# ============================================================
# [메인 실행]
# ============================================================
async def main():

    if DEBUG_MODE:
        print("🔬 디버그 모드\n")
        for isin in DEBUG_ISINS:
            debug_raw_xml(isin)
            await asyncio.sleep(1.0)
        return

    print(f"🔑 DART_KEY: {DART_KEY[:6]}...")
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
        print(f"🧪 테스트 모드: 상위 5개\n")
    else:
        print(f"🚀 전체 {len(data_rows)}개 종목\n")

    if not data_rows:
        print("⚠ 데이터 없음.")
        return

    results = []
    for sheet_row, row in data_rows:
        result = get_mezzanine_data(row[1].strip(), row)
        results.append((sheet_row, result))
        await asyncio.sleep(1.5)

    print("\n📝 시트 업데이트 중...")
    fr = results[0][0]
    lr = results[-1][0]

    def upd(rng, vals):
        worksheet.update(vals, range_name=rng)

    upd(f"A{fr}:A{lr}", [[r['corp_name']] for _, r in results])
    await asyncio.sleep(1.0)
    upd(f"C{fr}:F{lr}", [r['basic_row'] for _, r in results])
    await asyncio.sleep(1.0)
    upd(f"G{fr}:G{lr}", [[r['coupon']] for _, r in results])
    await asyncio.sleep(1.0)
    upd(f"H{fr}:H{lr}", [[r['ytm']] for _, r in results])
    await asyncio.sleep(1.0)
    upd(f"I{fr}:I{lr}", [[r['xrc_price']] for _, r in results])
    await asyncio.sleep(1.0)
    upd(f"J{fr}:J{lr}", [[r['rfxg_floor']] for _, r in results])
    await asyncio.sleep(1.0)
    upd(f"K{fr}:L{lr}", [[r['xrc_begin'], r['xrc_end']] for _, r in results])
    await asyncio.sleep(1.0)
    upd(f"M{fr}:O{lr}", [[r['put']['put_begin'], r['put']['put_end'], r['put']['put_date']] for _, r in results])
    await asyncio.sleep(1.0)
    upd(f"Q{fr}:S{lr}", [[r['call']['call_ratio'], r['call']['call_begin'], r['call']['call_end']] for _, r in results])
    await asyncio.sleep(1.0)
    upd(f"T{fr}:T{lr}", [[r['ytc']] for _, r in results])
    await asyncio.sleep(1.0)

    ytm_c  = sum(1 for _, r in results if r['ytm'])
    rfxg_c = sum(1 for _, r in results if r['rfxg_floor'])
    put_c  = sum(1 for _, r in results if r['put']['put_begin'] not in ('', '-'))
    call_c = sum(1 for _, r in results if r['call']['call_ratio'] not in ('', '-'))
    ytc_c  = sum(1 for _, r in results if r['ytc'])

    print(f"\n🏁 완료! {len(results)}개 종목 업데이트됨")
    print(f"👉 https://docs.google.com/spreadsheets/d/{SHEET_ID}")
    print(f"\n📌 업데이트 현황:")
    print(f"  ✅ A열    종목명")
    print(f"  ✅ C~F열  회차·종류·발행일·만기일")
    print(f"  ✅ G열    Coupon (0 포함)")
    print(f"  ✅ H열    YTM ← DART ({ytm_c}/{len(results)}개)")
    print(f"  ✅ I열    행사가액")
    print(f"  ✅ J열    리픽싱플로어 ← DART ({rfxg_c}/{len(results)}개, 없으면 행사가액)")
    print(f"  ✅ K~L열  전환청구기간 ← DART 우선 + SEIBRO 보완")
    print(f"  ✅ M~O열  PUT ← DART 다음차수 ({put_c}/{len(results)}개, 없으면 -)")
    print(f"  ✅ Q~S열  CALL ← DART 다음차수 ({call_c}/{len(results)}개, 없으면 -)")
    print(f"  ✅ T열    YTC ← DART ({ytc_c}/{len(results)}개)")
    print(f"  ⏳ P열    YTP (추후)")


if __name__ == "__main__":
    asyncio.run(main())
