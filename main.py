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
API_BOND_APPROVED  = True
API_STOCK_APPROVED = True
API_DART           = True

# ============================================================
# [테스트 / 디버그 모드]
# ============================================================
TEST_MODE   = False
TEST_COUNT  = 5
DEBUG_MODE  = False
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
DART_NAME_DICT = {}  # 기업명 → corp_code (전체, 비상장 포함)

def load_dart_corp_codes():
    global DART_CORP_DICT, DART_NAME_DICT
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
            corp_name  = item.findtext('corp_name', '').strip()
            if stock_code and len(stock_code) == 6:
                DART_CORP_DICT[stock_code] = corp_code
                count += 1
            if corp_name and corp_code:
                DART_NAME_DICT[corp_name] = corp_code
        print(f"  ✅ DART 기업코드 로드 완료: {count:,}개 (상장사), 이름사전 {len(DART_NAME_DICT):,}개")
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
    valid_rows = [(f, t, p) for f, t, p in rows if f <= t]
    if not valid_rows:
        return '', '', ''
    for from_dt, to_dt, pay_dt in valid_rows:
        if from_dt >= TODAY or (from_dt <= TODAY <= to_dt):
            return from_dt, to_dt, pay_dt
    return valid_rows[-1]


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
    """
    발행일 기준 -90일 ~ +30일로 범위 검색.
    [기재정정] 우선 수용, [첨부정정] 스킵, [첨부추가] 수용.
    B타입 실패 시 전체타입 재검색.
    반환값: (기재정정_rcept_no, 원본_rcept_no) 튜플
    """
    try:
        issu_date = datetime.strptime(issu_dt_str, '%Y-%m-%d')
        bgn_de = (issu_date - timedelta(days=90)).strftime('%Y%m%d')
        end_de = (issu_date + timedelta(days=30)).strftime('%Y%m%d')

        issue_kws = [
            '전환사채권발행결정', '교환사채권발행결정', '신주인수권부사채권발행결정',
        ]
        correction_kws = [
            '[기재정정]전환사채권발행결정', '[기재정정]교환사채권발행결정',
            '[기재정정]신주인수권부사채권발행결정',
        ]

        for pblntf_ty in ['B', '']:
            params = {
                'crtfc_key': DART_KEY, 'corp_code': corp_code,
                'bgn_de': bgn_de, 'end_de': end_de, 'page_count': 40,
            }
            if pblntf_ty:
                params['pblntf_ty'] = pblntf_ty

            r = requests.get(f"{DART_BASE}/list.json", params=params, timeout=10)
            data = r.json()
            label = "B타입" if pblntf_ty else "전체타입"
            print(f"    🌐 DART list({label}): {data.get('status')} ({bgn_de}~{end_de})")

            if data.get('status') not in ('000', '013'):
                continue

            items = data.get('list', [])
            print(f"    📋 공시 {len(items)}건 ({label})")

            correction_no = ''
            original_no   = ''

            for item in items:
                rpt = item.get('report_nm', '')
                rcept_no = item.get('rcept_no', '')

                # [첨부정정]은 본문 없는 껍데기 → 스킵
                if '[첨부정정]' in rpt:
                    print(f"    ⏭ 첨부정정 스킵: {rpt}")
                    continue

                # [첨부추가]는 수용 (한진 케이스)
                # [기재정정] 우선 저장
                if any(kw in rpt for kw in correction_kws):
                    if not correction_no:
                        correction_no = rcept_no
                        print(f"    📄 기재정정 발견: {rpt} ({rcept_no})")

                # 원본 발행결정 저장
                elif any(kw in rpt for kw in issue_kws):
                    if not original_no:
                        original_no = rcept_no
                        print(f"    📄 원본 발견: {rpt} ({rcept_no})")

                # [첨부추가] 케이스 - 원본으로 취급
                elif '[첨부추가]' in rpt and any(kw in rpt for kw in [
                    '전환사채권발행결정', '교환사채권발행결정', '신주인수권부사채권발행결정'
                ]):
                    if not original_no:
                        original_no = rcept_no
                        print(f"    📄 첨부추가 발견: {rpt} ({rcept_no})")

            if correction_no or original_no:
                return correction_no, original_no

        print("    ℹ 발행결정 공시 없음")
    except Exception as e:
        print(f"    ⚠ DART 공시검색 실패: {e}")
    return '', ''


def _parse_document_text(rcept_no):
    """공시 본문을 받아 clean text로 반환"""
    try:
        r = requests.get(
            f"{DART_BASE}/document.xml",
            params={'crtfc_key': DART_KEY, 'rcept_no': rcept_no},
            timeout=30
        )
        z = zipfile.ZipFile(io.BytesIO(r.content))
        for fname in z.namelist():
            if not any(fname.endswith(ext) for ext in ['.xml', '.html', '.htm']):
                continue
            raw = z.read(fname)
            for enc in ['utf-8', 'euc-kr', 'cp949']:
                try:
                    text = raw.decode(enc)
                    break
                except Exception:
                    continue
            else:
                continue
            clean = re.sub(r'<[^>]+>', ' ', text)
            clean = re.sub(r'&nbsp;', ' ', clean)
            clean = re.sub(r'\s+', ' ', clean)
            return clean
    except Exception as e:
        print(f"    ⚠ 문서 로드 실패 ({rcept_no}): {e}")
    return ''


def dart_parse_disclosure(correction_rcept_no, original_rcept_no, xrc_price=''):
    """
    기재정정 공시와 원본 공시를 모두 파싱.
    - 전환청구 시작일: 원본 공시에서 우선 파싱
    - 전환청구 종료일: 기재정정 공시에서 우선 파싱 (변경된 값)
    - 나머지: 기재정정 → 원본 순서로 파싱
    """
    result = {
        'ytm': '', 'rfxg_floor': '',
        'xrc_begin_dart': '', 'xrc_end_dart': '',
        'put_begin': '', 'put_end': '', 'put_date': '',
        'call_ratio': '', 'call_begin': '', 'call_end': '',
        'ytc': '',
    }

    # 텍스트 로드 (기재정정 + 원본)
    correction_text = _parse_document_text(correction_rcept_no) if correction_rcept_no else ''
    original_text   = _parse_document_text(original_rcept_no)   if original_rcept_no   else ''

    print(f"    🌐 document API: 기재정정={bool(correction_text)}, 원본={bool(original_text)}")

    # 파싱은 원본 우선 → 기재정정으로 보완하는 텍스트 리스트
    # (전환청구 시작일은 원본에 있고, 종료일은 기재정정에서 바뀔 수 있음)
    texts_primary   = [t for t in [original_text, correction_text] if t]   # 원본 우선
    texts_correction = [t for t in [correction_text, original_text] if t]  # 기재정정 우선

    def parse_from_texts(texts, parse_fn):
        for text in texts:
            val = parse_fn(text)
            if val:
                return val
        return ''

    # ── YTM ──
    def _ytm(clean):
        for pat in [
            r'만기이자율[^0-9\n]*([0-9]+(?:\.[0-9]+)?)',
            r'만기\s*이자율[^0-9\n]*([0-9]+(?:\.[0-9]+)?)',
            r'만기보장수익률[^0-9\n]*([0-9]+(?:\.[0-9]+)?)',
            r'만기\s*보장\s*수익률[^0-9\n]*([0-9]+(?:\.[0-9]+)?)',
            r'만기\s*상환\s*이율[^0-9\n]*([0-9]+(?:\.[0-9]+)?)',
            r'표면이자율[^0-9\n]*([0-9]+(?:\.[0-9]+)?)',
            r'권면이자율[^0-9\n]*([0-9]+(?:\.[0-9]+)?)',
        ]:
            m = re.search(pat, clean)
            if m:
                try:
                    val = float(m.group(1))
                    if val < 100:
                        return m.group(1)
                except Exception:
                    pass
        return ''

    result['ytm'] = parse_from_texts(texts_primary, _ytm)
    if result['ytm']:
        print(f"    ✅ YTM: {result['ytm']}%")

    # ── 리픽싱플로어 ──
    def _rfxg(clean):
        m = re.search(r'최저\s*(?:조정|전환|행사)\s*가액[^0-9\n]*?(-|–|[0-9][0-9,]+)', clean)
        if m:
            val = m.group(1).strip()
            if val in ('-', '–'):
                return xrc_price
            val_n = val.replace(',', '')
            try:
                n = int(float(val_n))
                if n > 100:
                    return f"{n:,}"
            except Exception:
                pass
        return ''

    result['rfxg_floor'] = parse_from_texts(texts_primary, _rfxg)
    if not result['rfxg_floor'] and xrc_price:
        result['rfxg_floor'] = xrc_price
        print(f"    ✅ 리픽싱플로어: 미기재→행사가액={xrc_price}")
    elif result['rfxg_floor']:
        print(f"    ✅ 리픽싱플로어: {result['rfxg_floor']}")

    # ── 전환청구기간 ──
    # 시작일: 원본 우선, 종료일: 기재정정 우선
    def _xrc_dates(clean):
        begin, end = '', ''
        for kw in ['전환청구기간', '교환청구기간', '행사청구기간', '권리행사기간', '전환권 행사', '전환권행사']:
            idx = clean.find(kw)
            if idx < 0:
                continue
            section = clean[idx: idx + 800]

            m_b = (
                re.search(r'시작일[^0-9\n]*(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)', section) or
                re.search(r'시작일[^0-9\n]*(\d{4}-\d{2}-\d{2})', section) or
                re.search(r'시작일[^0-9\n]*(\d{4}\.\d{1,2}\.\d{1,2})', section) or
                re.search(r'(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)\s*부터', section) or
                re.search(r'부터[^0-9\n]*(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)', section)
            )
            m_e = (
                re.search(r'종료일[^0-9\n]*(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)', section) or
                re.search(r'종료일[^0-9\n]*(\d{4}-\d{2}-\d{2})', section) or
                re.search(r'종료일[^0-9\n]*(\d{4}\.\d{1,2}\.\d{1,2})', section) or
                re.search(r'(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)\s*까지', section) or
                re.search(r'까지[^0-9\n]*(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)', section)
            )

            # ~ 범위 패턴
            if not m_b and not m_e:
                range_m = re.search(
                    r'(\d{4}년\s*\d{1,2}월\s*\d{1,2}일|\d{4}-\d{2}-\d{2}|\d{4}\.\d{2}\.\d{2})'
                    r'\s*[~∼]\s*'
                    r'(\d{4}년\s*\d{1,2}월\s*\d{1,2}일|\d{4}-\d{2}-\d{2}|\d{4}\.\d{2}\.\d{2})',
                    section
                )
                if range_m:
                    begin = parse_korean_date(range_m.group(1))
                    end   = parse_korean_date(range_m.group(2))

            if m_b:
                begin = parse_korean_date(m_b.group(1))
            if m_e:
                end = parse_korean_date(m_e.group(1))

            if begin or end:
                break
        return begin, end

    # 시작일: 원본 우선
    for text in texts_primary:
        b, e = _xrc_dates(text)
        if b:
            result['xrc_begin_dart'] = b
            break
    # 종료일: 기재정정 우선 (정정된 값 반영)
    for text in texts_correction:
        b, e = _xrc_dates(text)
        if e:
            result['xrc_end_dart'] = e
            break

    if result['xrc_begin_dart']:
        print(f"    ✅ 전환청구시작: {result['xrc_begin_dart']}")
    if result['xrc_end_dart']:
        print(f"    ✅ 전환청구종료: {result['xrc_end_dart']}")

    # ── PUT ──
    def _put(clean):
        put_idx = -1
        for kw in ['조기상환 청구기간', '조기상환청구기간', '[조기상환청구권', '조기상환청구권(Put']:
            idx = clean.find(kw)
            if idx >= 0:
                put_idx = idx
                break
        if put_idx >= 0:
            put_section = clean[put_idx: put_idx + 5000]
            dates = re.findall(r'\d{4}-\d{2}-\d{2}', put_section)
            rows = [(dates[i*3], dates[i*3+1], dates[i*3+2]) for i in range(len(dates) // 3)]
            if rows:
                return find_next_upcoming_row(rows)
        return None

    for text in texts_primary:
        put_result = _put(text)
        if put_result is not None:
            f, t, p = put_result
            result['put_begin'] = f
            result['put_end']   = t
            result['put_date']  = p
            print(f"    ✅ PUT: {f}~{t}, 상환: {p}")
            break
    else:
        result['put_begin'] = '-'
        result['put_end']   = '-'
        result['put_date']  = '-'
        print("    ℹ PUT 없음(-)")

    # ── CALL ──
    def _call(clean):
        call_idx = -1
        for kw in [
            '매도청구권(Call Option)', '매도청구권(call option)',
            '[매도청구권', '매도청구권에 관한', '매도 청구권', '콜옵션', '콜 옵션',
        ]:
            m_idx = re.search(re.escape(kw), clean, re.IGNORECASE)
            if m_idx:
                call_idx = m_idx.start()
                break
        if call_idx < 0:
            return None

        call_section = clean[call_idx: call_idx + 5000]
        ytc, ratio, begin, end = '', '', '', ''

        ytc_m = re.search(r'연\s*단리\s*([0-9]+(?:\.[0-9]+)?)\s*%', call_section)
        if ytc_m:
            ytc = ytc_m.group(1)

        for ratio_pat in [
            r'([0-9]+(?:\.[0-9]+)?)%를?\s*총\s*한도',
            r'\(Call\s*Option\s*([0-9]+(?:\.[0-9]+)?)\s*%\)',
            r'Call\s*Option\s*([0-9]+(?:\.[0-9]+)?)\s*%',
            r'전자등록총액\s*([0-9]+(?:\.[0-9]+)?)\s*%',
            r'발행총액[^0-9]*([0-9]+(?:\.[0-9]+)?)\s*%',
            r'사채\s*원금의\s*([0-9]+(?:\.[0-9]+)?)\s*%',
            r'잔액의\s*([0-9]+(?:\.[0-9]+)?)\s*%',
            r'([0-9]+(?:\.[0-9]+)?)\s*%\s*이내',
            r'한도\s*[:：]\s*([0-9]+(?:\.[0-9]+)?)\s*%',
            r'총액의\s*([0-9]+(?:\.[0-9]+)?)\s*%',
            r'권면총액의\s*([0-9]+(?:\.[0-9]+)?)\s*%',
        ]:
            ratio_m = re.search(ratio_pat, call_section, re.IGNORECASE)
            if ratio_m:
                try:
                    val = float(ratio_m.group(1))
                    if val <= 100:
                        ratio = ratio_m.group(1)
                        break
                except Exception:
                    pass

        call_dates = re.findall(r'\d{4}-\d{2}-\d{2}', call_section)
        call_rows = [
            (call_dates[i*3], call_dates[i*3+1], call_dates[i*3+2])
            for i in range(len(call_dates) // 3)
        ]
        if call_rows:
            f, t, _ = find_next_upcoming_row(call_rows)
            begin, end = f, t
        else:
            b_m = re.search(r'(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)부터', call_section)
            e_m = re.search(r'(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)까지', call_section)
            if not b_m:
                range_m = re.search(
                    r'(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)\s*~\s*(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)',
                    call_section
                )
                if range_m:
                    begin = parse_korean_date(range_m.group(1))
                    end   = parse_korean_date(range_m.group(2))
            if b_m:
                begin = parse_korean_date(b_m.group(1))
            if e_m:
                end = parse_korean_date(e_m.group(1))

        return ytc, ratio, begin, end

    call_found = False
    for text in texts_primary:
        call_result = _call(text)
        if call_result is not None:
            ytc, ratio, begin, end = call_result
            result['ytc']        = ytc
            result['call_ratio'] = ratio
            result['call_begin'] = begin
            result['call_end']   = end
            if ytc:   print(f"    ✅ YTC: {ytc}%")
            if ratio: print(f"    ✅ CALL비율: {ratio}%")
            if begin: print(f"    ✅ CALL일정: {begin}~{end}")
            call_found = True
            break

    if not call_found:
        result['call_begin'] = '-'
        result['call_end']   = '-'
        result['call_ratio'] = '-'
        print("    ℹ CALL 없음(-)")

    return result


# ── EB 발행사 corp_code 조회 ──
def get_issuer_corp_code(corp_name):
    """
    EB의 경우 교환 대상 주식코드가 아닌 발행사 기업명으로 corp_code 검색.
    DART_NAME_DICT(로컬 사전) 우선, 실패 시 API 검색.
    """
    # 정확 매칭
    for name_try in [corp_name, corp_name.replace('(주)', '').strip(), corp_name.replace('주식회사', '').strip()]:
        if name_try in DART_NAME_DICT:
            corp_code = DART_NAME_DICT[name_try]
            print(f"    📌 발행사 corp_code(이름사전): {corp_code} ({name_try})")
            return corp_code

    # 부분 매칭
    matches = [(name, code) for name, code in DART_NAME_DICT.items()
               if corp_name in name or name in corp_name]
    if len(matches) == 1:
        print(f"    📌 발행사 corp_code(부분매칭): {matches[0][1]} ({matches[0][0]})")
        return matches[0][1]
    elif len(matches) > 1:
        best = min(matches, key=lambda x: len(x[0]))
        print(f"    📌 발행사 corp_code(최단매칭): {best[1]} ({best[0]})")
        return best[1]

    print(f"    ⚠ 발행사 corp_code 탐색 실패: {corp_name}")
    return ''


def extract_stock_code_from_isin(isin, corp_name=''):
    """xrc_stk_isin 없을 때 기업명으로 corp_code 검색 (CB/BW용)"""
    if not corp_name or corp_name == '-':
        return ''
    return get_issuer_corp_code(corp_name)


def parse_dart_for_bond(isin, issu_dt_str, xrc_stk_isin, xrc_price='', corp_name='', bond_type=''):
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
    print(f"    📎 xrc_stk_isin={xrc_stk_isin} → stock_code_6={stock_code_6} / bond_type={bond_type}")

    corp_code = ''

    # ── EB: 교환대상 주식코드가 아닌 발행사 corp_code로 검색 ──
    if bond_type == 'EB':
        print(f"    🔄 EB → 발행사 기업명으로 corp_code 검색: {corp_name}")
        corp_code = get_issuer_corp_code(corp_name)
    else:
        # CB/BW: 기존 방식 (xrc_stk_isin → stock_code → corp_code)
        if stock_code_6:
            corp_code = dart_get_corp_code(stock_code_6)
        if not corp_code:
            print(f"    🔄 CB/BW fallback: 기업명으로 corp_code 검색: {corp_name}")
            corp_code = extract_stock_code_from_isin(isin, corp_name=corp_name)

    if not corp_code:
        print(f"    ⚠ corp_code 확보 실패 → DART 스킵")
        return empty

    correction_no, original_no = dart_search_cb_disclosure(corp_code, issu_dt_str)
    if not correction_no and not original_no:
        return empty

    return dart_parse_disclosure(correction_no, original_no, xrc_price=xrc_price)


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
        'coupon':    get_attr(el, 'COUPON_RATE'),
    }


def parse_put_call_seibro(isin):
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
        ('getXrcStkStatInfo',      {'BOND_ISIN': isin}),
        ('getBondOptionXrcInfo',   {'ISIN': isin}),
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

    basic = parse_bond_basic(isin)
    if basic:
        print(f"→ {basic['corp_name']} {basic['hosu']}회 {basic['bond_type']}")
        corp_name = basic['corp_name']
        bond_type = basic['bond_type']
        basic_row = [basic['hosu'], basic['bond_type'], basic['issu_dt'], basic['xpir_dt']]
        coupon    = basic['coupon']
        issu_dt   = basic['issu_dt']
    else:
        print("→ ⚠ getBondStatInfo 실패 (기존값 유지)")
        corp_name = existing_row[0].strip() if existing_row else '-'
        bond_type = existing_row[3].strip() if len(existing_row) > 3 else '-'
        basic_row = [
            existing_row[2].strip() if len(existing_row) > 2 else '-',
            existing_row[3].strip() if len(existing_row) > 3 else '-',
            existing_row[4].strip() if len(existing_row) > 4 else '-',
            existing_row[5].strip() if len(existing_row) > 5 else '-',
        ]
        coupon  = existing_row[6].strip() if len(existing_row) > 6 else ''
        issu_dt = existing_row[4].strip() if len(existing_row) > 4 else ''

    exercise = parse_exercise_info(isin) if API_STOCK_APPROVED else {
        'xrc_price': '', 'xrc_begin': '', 'xrc_end': '', 'xrc_stk_isin': '',
    }

    seibro_pc = parse_put_call_seibro(isin) if API_BOND_APPROVED else {
        'put_begin': '', 'put_end': '', 'put_date': '',
        'call_ratio': '', 'call_begin': '', 'call_end': '',
    }

    dart_data = {
        'ytm': '', 'rfxg_floor': '',
        'xrc_begin_dart': '', 'xrc_end_dart': '',
        'put_begin': '', 'put_end': '', 'put_date': '',
        'call_ratio': '', 'call_begin': '', 'call_end': '',
        'ytc': '',
    }

    if API_DART and issu_dt and issu_dt != '-':
        print(f"    🌐 DART 조회 중... (발행일: {issu_dt})")
        dart_data = parse_dart_for_bond(
            isin, issu_dt,
            exercise.get('xrc_stk_isin', ''),
            xrc_price=exercise.get('xrc_price', ''),
            corp_name=corp_name,
            bond_type=bond_type,
        )
    else:
        print(f"    ℹ DART 스킵 (issu_dt={issu_dt})")

    final_xrc_begin = dart_data.get('xrc_begin_dart') or exercise['xrc_begin']
    final_xrc_end   = dart_data.get('xrc_end_dart')   or exercise['xrc_end']

    if dart_data['put_begin'] == '-':
        final_put = {'put_begin': '-', 'put_end': '-', 'put_date': '-'}
    else:
        final_put = {
            'put_begin': dart_data['put_begin'] or seibro_pc['put_begin'],
            'put_end':   dart_data['put_end']   or seibro_pc['put_end'],
            'put_date':  dart_data['put_date']  or seibro_pc['put_date'],
        }

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
        data_rows = data_rows[:TEST_COUNT]
        print(f"🧪 테스트 모드: 상위 {TEST_COUNT}개\n")
    else:
        print(f"🚀 전체 {len(data_rows)}개 종목 실행\n")

    if not data_rows:
        print("⚠ 데이터 없음.")
        return

    results = []
    for sheet_row, row in data_rows:
        result = get_mezzanine_data(row[1].strip(), row)
        results.append((sheet_row, result))
        await asyncio.sleep(1.5)

    # ── 시트 업데이트 ──
    print("\n📝 시트 업데이트 중...")
    fr = results[0][0]
    lr = results[-1][0]

    def upd(rng, vals):
        worksheet.update(vals, range_name=rng)

    upd(f"A{fr}:A{lr}",  [[r['corp_name']]  for _, r in results])
    await asyncio.sleep(1.0)
    upd(f"C{fr}:F{lr}",  [r['basic_row']    for _, r in results])
    await asyncio.sleep(1.0)
    upd(f"G{fr}:G{lr}",  [[r['coupon']]     for _, r in results])
    await asyncio.sleep(1.0)
    upd(f"H{fr}:H{lr}",  [[r['ytm']]        for _, r in results])
    await asyncio.sleep(1.0)
    upd(f"I{fr}:I{lr}",  [[r['xrc_price']]  for _, r in results])
    await asyncio.sleep(1.0)
    upd(f"J{fr}:J{lr}",  [[r['rfxg_floor']] for _, r in results])
    await asyncio.sleep(1.0)
    upd(f"K{fr}:L{lr}",  [[r['xrc_begin'],  r['xrc_end']]  for _, r in results])
    await asyncio.sleep(1.0)
    upd(f"M{fr}:O{lr}",  [[r['put']['put_begin'], r['put']['put_end'], r['put']['put_date']] for _, r in results])
    await asyncio.sleep(1.0)
    upd(f"Q{fr}:S{lr}",  [[r['call']['call_ratio'], r['call']['call_begin'], r['call']['call_end']] for _, r in results])
    await asyncio.sleep(1.0)
    upd(f"T{fr}:T{lr}",  [[r['ytc']]        for _, r in results])
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
    print(f"  ✅ G열    Coupon")
    print(f"  ✅ H열    YTM       ← DART ({ytm_c}/{len(results)}개)")
    print(f"  ✅ I열    행사가액")
    print(f"  ✅ J열    리픽싱플로어 ← DART ({rfxg_c}/{len(results)}개)")
    print(f"  ✅ K~L열  전환청구기간 ← DART 우선 + SEIBRO 보완")
    print(f"  ✅ M~O열  PUT       ← DART ({put_c}/{len(results)}개, 없으면 -)")
    print(f"  ✅ Q~S열  CALL      ← DART ({call_c}/{len(results)}개, 없으면 -)")
    print(f"  ✅ T열    YTC       ← DART ({ytc_c}/{len(results)}개)")
    print(f"  ⏳ P열    YTP (추후)")

    await update_stock_code_sheet(data_rows, results)


# ============================================================
# [주식코드 시트 업데이트]
# ============================================================
async def update_stock_code_sheet(data_rows, results):
    print("\n📊 주식코드 시트 업데이트 중...")

    STOCK_SHEET_NAME = '주식코드'
    try:
        ws_stock = sh.worksheet(STOCK_SHEET_NAME)
    except Exception:
        ws_stock = sh.add_worksheet(title=STOCK_SHEET_NAME, rows=200, cols=6)
        print(f"  ✅ '{STOCK_SHEET_NAME}' 시트 새로 생성")

    headers = ['종목명', '채권ISIN', '종류', '발행사 주식코드', '교환대상 주식코드']
    ws_stock.update([headers], range_name='A1:E1')
    ws_stock.format('A1:E1', {
        'textFormat': {'bold': True},
        'backgroundColor': {'red': 0.2, 'green': 0.5, 'blue': 0.3},
        'horizontalAlignment': 'CENTER',
    })
    await asyncio.sleep(1.0)

    rows = []
    for (sheet_row, row), (_, result) in zip(data_rows, results):
        isin      = row[1].strip()
        bond_type = result['basic_row'][1] if result['basic_row'] else row[3].strip()
        name      = result['corp_name']

        xrc_stk_isin = ''
        stock_code   = ''
        try:
            ex = parse_exercise_info(isin)
            xrc_stk_isin = ex.get('xrc_stk_isin', '')
            if xrc_stk_isin and len(xrc_stk_isin) >= 9:
                stock_code = xrc_stk_isin[3:9]
        except Exception:
            pass

        if bond_type == 'EB':
            issuer_code = ''
            target_code = stock_code
        else:
            issuer_code = stock_code
            target_code = stock_code

        rows.append([name, isin, bond_type, issuer_code, target_code])
        await asyncio.sleep(0.3)

    if rows:
        ws_stock.batch_clear([f'A2:E{len(rows)+10}'])
        await asyncio.sleep(1.0)
        ws_stock.update(rows, range_name=f'A2:E{len(rows)+1}')
        await asyncio.sleep(1.0)

    print(f"  ✅ 주식코드 시트 업데이트 완료: {len(rows)}개")


if __name__ == "__main__":
    asyncio.run(main())
