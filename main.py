"""
dart-monitor main.py (v2.0)
- 기존 기능: 시트1 자동 갱신 (113개 종목)
- NEW: 새 풋콜스케줄 시트 자동 갱신 (6종 이벤트)
- NEW: 자본변동 감지 (액면병합/분할/무상증자)
- NEW: 신규 메자닌 발행 자동 감지
"""

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
from dateutil.relativedelta import relativedelta
from google.oauth2.service_account import Credentials


# ============================================================
# [설정]
# ============================================================
SEIBRO_KEY = os.environ.get('SEIBRO_KEY', '')
SHEET_ID = os.environ.get('SHEET_ID', '1s73BDNtCPe5mOs9EjBE5npEfcaNtYRyWJxRBUmJI-WA')
DART_KEY = (
    os.environ.get('DART_API_KEY') or
    os.environ.get('DART_KEY') or
    os.environ.get('DART_API') or
    ''
)

# 환경변수 누락 시 즉시 실패 (보안)
if not SEIBRO_KEY:
    raise RuntimeError("SEIBRO_KEY 환경변수가 설정되지 않았습니다")
if not DART_KEY:
    raise RuntimeError("DART_API_KEY 환경변수가 설정되지 않았습니다")

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
TEST_MODE = False
TEST_COUNT = 5
DEBUG_MODE = False
DEBUG_ISINS = ["KR6177831E26", "KR6214271E32", "KR6222421DC0"]

# ============================================================
# [시트 이름 상수]
# ============================================================
SHEET_PORTFOLIO = 0  # 첫 번째 시트 (포트폴리오)
SHEET_SCHEDULE = '풋콜스케줄'  # 새 풋콜스케줄 시트
SHEET_STOCK_CODE = '주식코드'
SHEET_STOCK_MATCH = '주식코드매칭'
SHEET_ALIAS = '별칭사전'
SHEET_CAPITAL_ACTION = '자본변동이력'  # 신규: 자본변동 기록

# ============================================================
# [Google Sheets 연결]
# ============================================================
creds_json = json.loads(os.environ.get('GCP_SERVICE_ACCOUNT_KEY'))
scopes = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)
worksheet = sh.get_worksheet(SHEET_PORTFOLIO)

SEIBRO_BASE = "https://seibro.or.kr/OpenPlatform/callOpenAPI.jsp"
DART_BASE = "https://opendart.fss.or.kr/api"

# ============================================================
# [DART 기업코드 딕셔너리]
# ============================================================
DART_CORP_DICT = {}        # stock_code → corp_code
DART_CORP_NAME_DICT = {}   # stock_code → corp_name
DART_NAME_DICT = {}        # name → corp_code


def load_dart_corp_codes():
    """DART 전체 기업코드 다운로드 + 캐싱"""
    global DART_CORP_DICT, DART_CORP_NAME_DICT, DART_NAME_DICT
    try:
        print("  📥 DART 기업코드 전체 다운로드 중...")
        r = requests.get(f"{DART_BASE}/corpCode.xml",
                         params={'crtfc_key': DART_KEY}, timeout=30)
        print(f"  📥 응답코드: {r.status_code} / 크기: {len(r.content):,} bytes")
        z = zipfile.ZipFile(io.BytesIO(r.content))
        root = ET.fromstring(z.read('CORPCODE.xml'))
        count = 0
        for item in root.findall('.//list'):
            corp_code = item.findtext('corp_code', '').strip()
            stock_code = item.findtext('stock_code', '').strip()
            corp_name = item.findtext('corp_name', '').strip()
            if stock_code and len(stock_code) == 6:
                DART_CORP_DICT[stock_code] = corp_code
                DART_CORP_NAME_DICT[stock_code] = corp_name
                count += 1
            if corp_name and corp_code:
                DART_NAME_DICT[corp_name] = corp_code
        print(f"  ✅ DART 기업코드 로드 완료: {count:,}개 (상장사), 이름사전 {len(DART_NAME_DICT):,}개")
    except Exception as e:
        print(f"  ⚠ DART 기업코드 로드 실패: {e}")


# ============================================================
# [ISIN → 발행사 주식코드 변환]
# ============================================================
def isin_to_issuer_stock_code(isin):
    """채권 ISIN에서 발행사 주식코드 추출 (마지막 자리 0으로)"""
    if not isin or len(isin) < 9 or not isin.startswith('KR6'):
        return ''
    bond_code = isin[3:9]
    if not bond_code.isdigit():
        return ''
    stock_code = bond_code[:5] + '0'
    return stock_code


def stock_isin_to_code(stock_isin):
    """주식 ISIN(KR7xxxxxxxxxx)에서 6자리 주식코드 추출"""
    if not stock_isin or len(stock_isin) < 9:
        return ''
    code = stock_isin[3:9]
    return code if code.isdigit() else ''


# ============================================================
# [공통 유틸]
# ============================================================
def seibro_api(api_id, params_dict):
    params_str = ','.join([f"{k}:{v}" for k, v in params_dict.items()])
    full_url = f"{SEIBRO_BASE}?key={SEIBRO_KEY}&apiId={api_id}&params={params_str}"
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
# [NEW: 리픽싱 일정 계산 유틸]
# ============================================================
def calculate_refix_schedule(issue_date_str, maturity_date_str, cycle_months):
    """발행일 + 리픽싱 주기로 리픽싱 일정 자동 계산"""
    if not (issue_date_str and maturity_date_str and cycle_months):
        return []
    try:
        issue_dt = datetime.strptime(issue_date_str, '%Y-%m-%d')
        maturity_dt = datetime.strptime(maturity_date_str, '%Y-%m-%d')
    except Exception:
        return []
    
    schedules = []
    chasu = 1
    current = issue_dt + relativedelta(months=cycle_months)
    while current < maturity_dt:
        schedules.append({
            'chasu': chasu,
            'date': current.strftime('%Y-%m-%d'),
        })
        chasu += 1
        current += relativedelta(months=cycle_months)
        if chasu > 50:  # 안전장치
            break
    return schedules


def parse_refix_cycle_from_dart(dart_text):
    """DART 본문에서 리픽싱 주기(개월) 추출"""
    if not dart_text:
        return None
    patterns = [
        r'매\s*(\d+)\s*개월[이가 ]{0,3}되는\s*날[을 ]{1,2}(?:전환|행사)?가(?:격|액)\s*조정',
        r'본\s*사채\s*발행일\s*로?부터\s*매\s*(\d+)\s*개월',
        r'발행일\s*로?부터\s*매\s*(\d+)\s*개월[이가 ]{0,3}되는\s*날',
        r'(\d+)\s*개월\s*마다\s*(?:전환|행사)?가(?:격|액)\s*조정',
    ]
    for p in patterns:
        m = re.search(p, dart_text[:50000])
        if m:
            return int(m.group(1))
    return None


# ============================================================
# [NEW: 자본변동 감지]
# ============================================================
def detect_capital_actions(corp_code, days_back=30):
    """
    DART에서 자본변동 공시 감지
    감지 대상:
    - 주식병합
    - 액면병합
    - 주식분할
    - 무상증자
    - 유상증자 (저가발행)
    - 감자
    - 주식배당
    
    Returns: list of dict {date, type, report_name, rcept_no, link}
    """
    if not corp_code:
        return []
    
    actions = []
    
    # 자본변동 키워드 (DART 보고서명 기준)
    capital_keywords = {
        '주식병합': '주식병합',
        '액면병합': '액면병합',
        '주식분할': '주식분할',
        '액면분할': '액면분할',
        '무상증자': '무상증자',
        '유상증자': '유상증자',
        '무상감자': '감자',
        '유상감자': '감자',
        '주식배당': '주식배당',
        '액면가변경': '액면가변경',
    }
    
    try:
        bgn_de = (datetime.now() - timedelta(days=days_back)).strftime('%Y%m%d')
        end_de = datetime.now().strftime('%Y%m%d')
        
        r = requests.get(
            f"{DART_BASE}/list.json",
            params={
                'crtfc_key': DART_KEY,
                'corp_code': corp_code,
                'bgn_de': bgn_de,
                'end_de': end_de,
                'page_count': 40,
            },
            timeout=10
        )
        data = r.json()
        if data.get('status') not in ('000', '013'):
            return []
        
        for item in data.get('list', []):
            rpt = item.get('report_nm', '')
            # 첨부정정 등 스킵
            if '[첨부정정]' in rpt or '[첨부추가]' in rpt:
                continue
            
            # 키워드 매칭
            matched_type = None
            for kw, action_type in capital_keywords.items():
                if kw in rpt:
                    matched_type = action_type
                    break
            if not matched_type:
                continue
            
            rcept_no = item.get('rcept_no', '')
            rcept_dt = item.get('rcept_dt', '')
            fmt_dt = f"{rcept_dt[:4]}-{rcept_dt[4:6]}-{rcept_dt[6:]}" if len(rcept_dt) == 8 else rcept_dt
            
            actions.append({
                'date': fmt_dt,
                'type': matched_type,
                'report_name': rpt,
                'rcept_no': rcept_no,
                'link': f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}",
            })
        
    except Exception as e:
        print(f"  ⚠ 자본변동 감지 실패 (corp_code={corp_code}): {e}")
    
    return actions


def detect_all_capital_actions(holdings_with_corp_code, days_back=30):
    """
    전체 보유 종목의 자본변동 일괄 감지
    
    Args:
        holdings_with_corp_code: [(stock_name, corp_code, isin), ...]
        days_back: 며칠 전부터 검색
    
    Returns:
        {stock_name: [actions]} dict
    """
    print(f"\n🔍 자본변동 감지 시작 ({days_back}일 전부터)...")
    
    # corp_code 중복 제거
    unique_corps = {}
    for name, corp_code, isin in holdings_with_corp_code:
        if corp_code and corp_code not in unique_corps:
            unique_corps[corp_code] = (name, isin)
    
    all_actions = {}
    detected_count = 0
    
    import time
    for corp_code, (name, isin) in unique_corps.items():
        actions = detect_capital_actions(corp_code, days_back)
        if actions:
            all_actions[name] = actions
            detected_count += len(actions)
            for a in actions:
                print(f"  📢 {name}: {a['type']} ({a['date']}) - {a['report_name']}")
        time.sleep(0.3)  # DART API rate limit
    
    print(f"  → 총 {detected_count}건 감지 (대상 {len(unique_corps)}개 발행사)")
    return all_actions


# ============================================================
# [DART 파싱]
# ============================================================
def get_dart_info_by_stock_code(stock_code):
    """주식코드 → (corp_code, corp_name)"""
    corp_code = DART_CORP_DICT.get(stock_code, '')
    corp_name = DART_CORP_NAME_DICT.get(stock_code, '')
    return corp_code, corp_name


def dart_search_cb_disclosure(corp_code, issu_dt_str):
    """발행일 기준 -90일 ~ +30일 검색"""
    try:
        issu_date = datetime.strptime(issu_dt_str, '%Y-%m-%d')
        bgn_de = (issu_date - timedelta(days=90)).strftime('%Y%m%d')
        end_de = (issu_date + timedelta(days=30)).strftime('%Y%m%d')

        issue_kws = [
            '전환사채권발행결정', '교환사채권발행결정', '신주인수권부사채권발행결정',
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
            print(f"  🌐 DART list({label}): {data.get('status')} ({bgn_de}~{end_de})")
            if data.get('status') not in ('000', '013'):
                continue
            items = data.get('list', [])
            print(f"  📋 공시 {len(items)}건 ({label})")

            correction_no = ''
            original_no = ''
            for item in items:
                rpt = item.get('report_nm', '')
                rcept_no = item.get('rcept_no', '')
                if '[첨부정정]' in rpt:
                    continue
                if '[기재정정]' in rpt and any(kw in rpt for kw in issue_kws):
                    if not correction_no:
                        correction_no = rcept_no
                        print(f"  📄 기재정정: {rpt} ({rcept_no})")
                elif '[기재정정]' not in rpt and '[첨부추가]' not in rpt and any(kw in rpt for kw in issue_kws):
                    if not original_no:
                        original_no = rcept_no
                        print(f"  📄 원본: {rpt} ({rcept_no})")
                elif '[첨부추가]' in rpt and any(kw in rpt for kw in issue_kws):
                    if not original_no:
                        original_no = rcept_no
                        print(f"  📄 첨부추가: {rpt} ({rcept_no})")
            if correction_no or original_no:
                return correction_no, original_no
        print("  ℹ 발행결정 공시 없음")
    except Exception as e:
        print(f"  ⚠ DART 공시검색 실패: {e}")
    return '', ''


def _parse_document_text(rcept_no):
    """DART 공시 문서 본문 파싱"""
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
        print(f"  ⚠ 문서 로드 실패 ({rcept_no}): {e}")
    return ''


def dart_parse_disclosure(correction_rcept_no, original_rcept_no, xrc_price=''):
    """
    DART 공시문서에서 6종 정보 추출
    - YTM (만기이자율)
    - 리픽싱플로어
    - 전환청구 시작/종료
    - PUT 시작/종료/지급
    - CALL 비율/시작/종료
    - YTC
    - NEW: 리픽싱 주기 (개월)
    """
    result = {
        'ytm': '', 'rfxg_floor': '',
        'xrc_begin_dart': '', 'xrc_end_dart': '',
        'put_begin': '', 'put_end': '', 'put_date': '',
        'call_ratio': '', 'call_begin': '', 'call_end': '',
        'ytc': '',
        'refix_cycle_months': None,  # NEW
        'put_schedule': [],  # NEW: 모든 PUT 차수
        'call_schedule': [], # NEW: 모든 CALL 차수
    }

    correction_text = _parse_document_text(correction_rcept_no) if correction_rcept_no else ''
    original_text = _parse_document_text(original_rcept_no) if original_rcept_no else ''
    print(f"  🌐 document API: 기재정정={bool(correction_text)}, 원본={bool(original_text)}")

    texts_primary = [t for t in [original_text, correction_text] if t]
    texts_correction = [t for t in [correction_text, original_text] if t]

    def parse_from_texts(texts, parse_fn):
        for text in texts:
            val = parse_fn(text)
            if val:
                return val
        return ''

    # YTM
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
        print(f"  ✅ YTM: {result['ytm']}%")

    # 리픽싱플로어
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
        print(f"  ✅ 리픽싱플로어: 미기재→행사가액={xrc_price}")
    elif result['rfxg_floor']:
        print(f"  ✅ 리픽싱플로어: {result['rfxg_floor']}")

    # NEW: 리픽싱 주기 추출
    for text in texts_primary:
        cycle = parse_refix_cycle_from_dart(text)
        if cycle:
            result['refix_cycle_months'] = cycle
            print(f"  ✅ 리픽싱 주기: 매 {cycle}개월")
            break

    # 전환청구기간
    def _xrc_dates(clean):
        begin, end = '', ''
        for kw in [
            '전환청구기간', '교환청구기간', '행사청구기간',
            '권리행사기간', '전환권 행사', '전환권행사',
            '권리행사기 간',  # OCR 띄어쓰기 변형
        ]:
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
            if not m_b and not m_e:
                range_m = re.search(
                    r'(\d{4}년\s*\d{1,2}월\s*\d{1,2}일|\d{4}-\d{2}-\d{2}|\d{4}\.\d{2}\.\d{2})'
                    r'\s*[~∼]\s*'
                    r'(\d{4}년\s*\d{1,2}월\s*\d{1,2}일|\d{4}-\d{2}-\d{2}|\d{4}\.\d{2}\.\d{2})',
                    section
                )
                if range_m:
                    begin = parse_korean_date(range_m.group(1))
                    end = parse_korean_date(range_m.group(2))
            if m_b:
                begin = parse_korean_date(m_b.group(1))
            if m_e:
                end = parse_korean_date(m_e.group(1))
            if begin or end:
                break
        return begin, end

    for text in texts_primary:
        b, e = _xrc_dates(text)
        if b:
            result['xrc_begin_dart'] = b
            break
    for text in texts_correction:
        b, e = _xrc_dates(text)
        if e:
            result['xrc_end_dart'] = e
            break
    if result['xrc_begin_dart']:
        print(f"  ✅ 전환청구시작: {result['xrc_begin_dart']}")
    if result['xrc_end_dart']:
        print(f"  ✅ 전환청구종료: {result['xrc_end_dart']}")

    # PUT (전체 스케줄 추출)
    def _put_all(clean):
        """모든 PUT 차수 추출"""
        put_idx = -1
        for kw in ['조기상환 청구기간', '조기상환청구기간', '[조기상환청구권', '조기상환청구권(Put']:
            idx = clean.find(kw)
            if idx >= 0:
                put_idx = idx
                break
        if put_idx < 0:
            return []
        
        put_section = clean[put_idx: put_idx + 5000]
        dates = re.findall(r'\d{4}-\d{2}-\d{2}', put_section)
        rates = re.findall(r'(\d{2,3}\.\d{2,6})\s*%', put_section)
        
        schedule = []
        for i in range(len(dates) // 3):
            schedule.append({
                'chasu': i + 1,
                'from_date': dates[i*3],
                'to_date': dates[i*3+1],
                'pay_date': dates[i*3+2],
                'rate': rates[i] if i < len(rates) else '',
            })
        return schedule

    for text in texts_primary:
        put_schedule = _put_all(text)
        if put_schedule:
            result['put_schedule'] = put_schedule
            # 현재 PUT (가장 가까운 미래 또는 진행 중)
            today = datetime.now().strftime('%Y-%m-%d')
            for p in put_schedule:
                if p['to_date'] >= today:
                    result['put_begin'] = p['from_date']
                    result['put_end'] = p['to_date']
                    result['put_date'] = p['pay_date']
                    break
            else:
                # 모든 PUT이 과거 (만기에 가까운 경우)
                if put_schedule:
                    last = put_schedule[-1]
                    result['put_begin'] = last['from_date']
                    result['put_end'] = last['to_date']
                    result['put_date'] = last['pay_date']
            print(f"  ✅ PUT 스케줄: {len(put_schedule)}개 차수")
            break
    
    if not result['put_schedule']:
        result['put_begin'] = '-'
        result['put_end'] = '-'
        result['put_date'] = '-'
        print("  ℹ PUT 없음(-)")

    # CALL (전체 스케줄 추출)
    def _call_all(clean):
        """모든 CALL 차수 추출"""
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
            return [], '', ''
        
        call_section = clean[call_idx: call_idx + 5000]
        
        # YTC
        ytc = ''
        ytc_m = re.search(r'연\s*단리\s*([0-9]+(?:\.[0-9]+)?)\s*%', call_section)
        if ytc_m:
            ytc = ytc_m.group(1)
        
        # CALL 비율
        ratio = ''
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
        
        # CALL 스케줄
        dates = re.findall(r'\d{4}-\d{2}-\d{2}', call_section)
        rates = re.findall(r'(\d{2,3}\.\d{2,6})\s*%', call_section)
        
        schedule = []
        for i in range(len(dates) // 3):
            schedule.append({
                'chasu': i + 1,
                'from_date': dates[i*3],
                'to_date': dates[i*3+1],
                'pay_date': dates[i*3+2],
                'rate': rates[i] if i < len(rates) else '',
            })
        
        return schedule, ytc, ratio

    for text in texts_primary:
        call_schedule, ytc, ratio = _call_all(text)
        if call_schedule:
            result['call_schedule'] = call_schedule
            result['ytc'] = ytc
            result['call_ratio'] = ratio
            # 현재 CALL
            today = datetime.now().strftime('%Y-%m-%d')
            for c in call_schedule:
                if c['to_date'] >= today:
                    result['call_begin'] = c['from_date']
                    result['call_end'] = c['to_date']
                    break
            if ytc: print(f"  ✅ YTC: {ytc}%")
            if ratio: print(f"  ✅ CALL비율: {ratio}%")
            print(f"  ✅ CALL 스케줄: {len(call_schedule)}개 차수")
            break
    
    if not result['call_schedule']:
        result['call_begin'] = '-'
        result['call_end'] = '-'
        result['call_ratio'] = '-'
        print("  ℹ CALL 없음(-)")

    return result


def parse_dart_for_bond(issuer_corp_code, issu_dt_str, xrc_price=''):
    """DART에서 채권 발행공시 파싱"""
    empty = {
        'ytm': '', 'rfxg_floor': '',
        'xrc_begin_dart': '', 'xrc_end_dart': '',
        'put_begin': '', 'put_end': '', 'put_date': '',
        'call_ratio': '', 'call_begin': '', 'call_end': '',
        'ytc': '',
        'refix_cycle_months': None,
        'put_schedule': [], 'call_schedule': [],
    }
    if not API_DART or not issuer_corp_code:
        return empty
    correction_no, original_no = dart_search_cb_disclosure(issuer_corp_code, issu_dt_str)
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
        'secn_nm': secn_nm,
        'hosu': extract_hosu(secn_nm),
        'bond_type': determine_bond_type(secn_nm),
        'issu_dt': format_date(get_attr(el, 'ISSU_DT')),
        'xpir_dt': format_date(get_attr(el, 'XPIR_DT')),
        'coupon': get_attr(el, 'COUPON_RATE'),
    }


def parse_put_call_seibro(isin):
    """SEIBRO에서 PUT/CALL 정보 (보완용)"""
    root = seibro_api('getBondOptionXrcInfo', {'ISIN': isin})
    r = {'put_begin': '', 'put_end': '', 'put_date': '',
         'call_ratio': '', 'call_begin': '', 'call_end': ''}
    if root is None:
        return r
    for el in root.findall('.//result'):
        tpcd = get_attr(el, 'OPTION_TPCD')
        begin = format_date(get_attr(el, 'XRC_BEGIN_DT'))
        end = format_date(get_attr(el, 'XRC_EXPRY_DT'))
        pay = format_date(get_attr(el, 'ERLY_RED_DT'))
        ratio = get_attr(el, 'XRC_RATIO')
        if tpcd in ('9402', '9403'):
            r['put_begin'] = r['put_begin'] or begin
            r['put_end'] = r['put_end'] or end
            r['put_date'] = r['put_date'] or pay
        if tpcd in ('9401', '9403'):
            r['call_begin'] = r['call_begin'] or begin
            r['call_end'] = r['call_end'] or end
            r['call_ratio'] = r['call_ratio'] or ratio
    return r


def parse_exercise_info(isin):
    """SEIBRO에서 행사가/전환청구기간/교환대상주식 정보"""
    r = {
        'xrc_price': '', 'xrc_begin': '', 'xrc_end': '',
        'xrc_stk_isin': '', 'xrc_stk_name': '',
    }
    root = seibro_api('getXrcStkStatInfo', {'BOND_ISIN': isin})
    if root is not None:
        el = root.find('.//result')
        if el is not None:
            r['xrc_price'] = fmt_number(get_attr(el, 'XRC_PRICE'))
            r['xrc_stk_isin'] = get_attr(el, 'XRC_STK_ISIN')
            r['xrc_stk_name'] = get_attr(el, 'STK_SECN_NM')

    root2 = seibro_api('getXrcStkOptionXrcInfo', {'BOND_ISIN': isin})
    if root2 is not None:
        begins, ends = [], []
        for el in root2.findall('.//result'):
            b = get_attr(el, 'XRC_POSS_BEGIN_DT')
            e = get_attr(el, 'XRC_POSS_EXPRY_DT')
            if b: begins.append(b)
            if e: ends.append(e)
        if begins: r['xrc_begin'] = format_date(min(begins))
        if ends: r['xrc_end'] = format_date(max(ends))
    return r


# ============================================================
# [메인 오케스트레이터 - 종목당 처리]
# ============================================================
def get_mezzanine_data(isin, existing_row):
    """ISIN 1개에 대한 모든 데이터 수집"""
    print(f"  🔍 {isin}", end=' ')

    basic = parse_bond_basic(isin)
    issuer_stock_code = isin_to_issuer_stock_code(isin)
    issuer_corp_code = ''
    issuer_corp_name = ''
    if issuer_stock_code:
        issuer_corp_code, issuer_corp_name = get_dart_info_by_stock_code(issuer_stock_code)
        if issuer_corp_code:
            print(f" [발행사: {issuer_corp_name} / 주식코드: {issuer_stock_code}]")
        else:
            print(f" [발행사 stock_code={issuer_stock_code}, DART 매칭 실패]")
    else:
        print(" [발행사 stock_code 추출 실패]")

    if basic:
        bond_type = basic['bond_type']
        if issuer_corp_name:
            corp_name = issuer_corp_name
        else:
            m = re.match(r'([가-힣a-zA-Z\s]+?)\s*\d+\s*(?:CB|EB|BW)', basic['secn_nm'])
            corp_name = m.group(1).strip() if m else (basic['secn_nm'].split('(')[0].strip())
        basic_row = [basic['hosu'], basic['bond_type'], basic['issu_dt'], basic['xpir_dt']]
        coupon = basic['coupon']
        issu_dt = basic['issu_dt']
        print(f"   → {corp_name} {basic['hosu']}회 {basic['bond_type']}")
    else:
        print("   → ⚠ getBondStatInfo 실패 (기존값 유지)")
        corp_name = issuer_corp_name or (existing_row[0].strip() if existing_row else '-')
        bond_type = existing_row[3].strip() if len(existing_row) > 3 else '-'
        basic_row = [
            existing_row[2].strip() if len(existing_row) > 2 else '-',
            existing_row[3].strip() if len(existing_row) > 3 else '-',
            existing_row[4].strip() if len(existing_row) > 4 else '-',
            existing_row[5].strip() if len(existing_row) > 5 else '-',
        ]
        coupon = existing_row[6].strip() if len(existing_row) > 6 else ''
        issu_dt = existing_row[4].strip() if len(existing_row) > 4 else ''

    exercise = parse_exercise_info(isin) if API_STOCK_APPROVED else {
        'xrc_price': '', 'xrc_begin': '', 'xrc_end': '',
        'xrc_stk_isin': '', 'xrc_stk_name': '',
    }

    target_stock_code = ''
    target_corp_name = ''
    if bond_type == 'EB' and exercise.get('xrc_stk_isin'):
        target_stock_code = stock_isin_to_code(exercise['xrc_stk_isin'])
        if target_stock_code:
            _, target_corp_name = get_dart_info_by_stock_code(target_stock_code)
        if not target_corp_name:
            target_corp_name = exercise.get('xrc_stk_name', '')
        print(f"  🎯 교환대상: {target_corp_name} ({target_stock_code})")

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
        'refix_cycle_months': None,
        'put_schedule': [], 'call_schedule': [],
    }
    if API_DART and issu_dt and issu_dt != '-' and issuer_corp_code:
        print(f"  🌐 DART 조회 중... (발행일: {issu_dt}, corp_code: {issuer_corp_code})")
        dart_data = parse_dart_for_bond(
            issuer_corp_code, issu_dt,
            xrc_price=exercise.get('xrc_price', ''),
        )
    else:
        print(f"  ℹ DART 스킵 (issu_dt={issu_dt}, corp_code={issuer_corp_code})")

    final_xrc_begin = dart_data.get('xrc_begin_dart') or exercise['xrc_begin']
    final_xrc_end = dart_data.get('xrc_end_dart') or exercise['xrc_end']

    if dart_data['put_begin'] == '-':
        final_put = {'put_begin': '-', 'put_end': '-', 'put_date': '-'}
    else:
        final_put = {
            'put_begin': dart_data['put_begin'] or seibro_pc['put_begin'],
            'put_end': dart_data['put_end'] or seibro_pc['put_end'],
            'put_date': dart_data['put_date'] or seibro_pc['put_date'],
        }

    if dart_data['call_ratio'] == '-':
        final_call = {'call_ratio': '-', 'call_begin': '-', 'call_end': '-'}
    else:
        final_call = {
            'call_ratio': dart_data['call_ratio'] or seibro_pc['call_ratio'],
            'call_begin': dart_data['call_begin'] or seibro_pc['call_begin'],
            'call_end': dart_data['call_end'] or seibro_pc['call_end'],
        }

    return {
        'isin': isin,
        'corp_name': corp_name,
        'basic_row': basic_row,
        'coupon': coupon,
        'ytm': dart_data['ytm'],
        'xrc_price': exercise.get('xrc_price', ''),
        'rfxg_floor': dart_data['rfxg_floor'],
        'xrc_begin': final_xrc_begin,
        'xrc_end': final_xrc_end,
        'put': final_put,
        'call': final_call,
        'ytc': dart_data.get('ytc', ''),
        'issuer_stock_code': issuer_stock_code,
        'issuer_corp_code': issuer_corp_code,
        'target_corp_name': target_corp_name,
        'target_stock_code': target_stock_code,
        # NEW: 전체 스케줄
        'refix_cycle_months': dart_data.get('refix_cycle_months'),
        'put_schedule': dart_data.get('put_schedule', []),
        'call_schedule': dart_data.get('call_schedule', []),
        # 메타
        'issu_dt': issu_dt,
        'xpir_dt': basic_row[3] if len(basic_row) > 3 else '',
    }


# ============================================================
# [NEW: 풋콜스케줄 시트 빌드]
# ============================================================
def build_schedule_rows(data, capital_actions=None):
    """
    한 종목의 데이터를 풋콜스케줄 시트 행들로 변환
    
    이벤트 6종:
    - 전환청구시작
    - 전환청구종료
    - 만기
    - 리픽싱 (1차, 2차...)
    - 풋옵션 (1차, 2차...)
    - 콜옵션 (1차, 2차...)
    
    Returns: list of rows [종목코드, 종목명, 이벤트유형, 차수, 시작일, 종료일, 지급일, 비율/금리, 비고]
    """
    rows = []
    isin = data['isin']
    name = data['corp_name']
    
    # 자본변동 비고 생성
    capital_note = ''
    if capital_actions:
        capital_note = ' / '.join([f"{a['type']}({a['date']})" for a in capital_actions])
    
    # 1) 전환청구시작
    if data['xrc_begin'] and data['xrc_begin'] != '-':
        rows.append([isin, name, '전환청구시작', '', 
                     data['xrc_begin'], '', '', '', capital_note])
    
    # 2) 전환청구종료
    if data['xrc_end'] and data['xrc_end'] != '-':
        rows.append([isin, name, '전환청구종료', '',
                     data['xrc_end'], '', '', '', capital_note])
    
    # 3) 만기
    if data['xpir_dt'] and data['xpir_dt'] != '-':
        rows.append([isin, name, '만기', '',
                     data['xpir_dt'], '', '', '', capital_note])
    
    # 4) 리픽싱 (계산)
    if data.get('refix_cycle_months') and data['issu_dt'] and data['xpir_dt']:
        floor_str = f"{data['rfxg_floor']}" if data['rfxg_floor'] else ''
        refix_schedule = calculate_refix_schedule(
            data['issu_dt'], data['xpir_dt'], data['refix_cycle_months']
        )
        for refix in refix_schedule:
            rows.append([isin, name, '리픽싱', f"{refix['chasu']}차",
                         refix['date'], '', '', floor_str, capital_note])
    
    # 5) 풋옵션 (스케줄에서)
    for put in data.get('put_schedule', []):
        rate_str = f"{put['rate']}%" if put.get('rate') else ''
        rows.append([isin, name, '풋옵션', f"{put['chasu']}차",
                     put.get('from_date', '') or '',
                     put.get('to_date', '') or '',
                     put.get('pay_date', '') or '',
                     rate_str, capital_note])
    
    # 6) 콜옵션 (스케줄에서)
    for call in data.get('call_schedule', []):
        rate_str = f"{call['rate']}%" if call.get('rate') else ''
        rows.append([isin, name, '콜옵션', f"{call['chasu']}차",
                     call.get('from_date', '') or '',
                     call.get('to_date', '') or '',
                     call.get('pay_date', '') or '',
                     rate_str, capital_note])
    
    return rows


def update_schedule_sheet(all_schedule_rows):
    """새 풋콜스케줄 시트에 일괄 쓰기"""
    print(f"\n📝 풋콜스케줄 시트 업데이트 ({len(all_schedule_rows)}행)...")
    
    try:
        ws_schedule = sh.worksheet(SHEET_SCHEDULE)
    except gspread.exceptions.WorksheetNotFound:
        # 신규 생성
        ws_schedule = sh.add_worksheet(
            title=SHEET_SCHEDULE, 
            rows=max(3000, len(all_schedule_rows) + 100), 
            cols=10
        )
        print(f"  ✅ '{SHEET_SCHEDULE}' 시트 새로 생성")
    
    # 헤더 (한 번만 쓰기)
    headers = ['종목코드', '종목명', '이벤트유형', '차수', '시작일', '종료일', '지급일', '비율/금리', '비고']
    ws_schedule.update([headers], range_name='A1:I1')
    ws_schedule.format('A1:I1', {
        'textFormat': {'bold': True, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}},
        'backgroundColor': {'red': 0.27, 'green': 0.45, 'blue': 0.77},
        'horizontalAlignment': 'CENTER',
    })
    
    import time
    time.sleep(1.5)
    
    # 기존 데이터 클리어 (A2부터)
    last_row = max(3000, len(all_schedule_rows) + 10)
    ws_schedule.batch_clear([f'A2:I{last_row}'])
    time.sleep(1.5)
    
    # 일괄 업데이트
    if all_schedule_rows:
        ws_schedule.update(all_schedule_rows, range_name=f'A2:I{len(all_schedule_rows)+1}')
        time.sleep(1.0)
    
    # 이벤트유형별 색상 (별도 batch_format)
    print(f"  🎨 색상 적용 중...")
    event_colors = {
        '전환청구시작': {'red': 0.85, 'green': 0.88, 'blue': 0.95},  # 연파랑
        '전환청구종료': {'red': 0.85, 'green': 0.88, 'blue': 0.95},
        '만기':       {'red': 0.95, 'green': 0.80, 'blue': 0.80},  # 빨강
        '리픽싱':     {'red': 1.00, 'green': 0.95, 'blue': 0.80},  # 노랑
        '풋옵션':     {'red': 0.85, 'green': 0.93, 'blue': 0.83},  # 초록
        '콜옵션':     {'red': 0.90, 'green': 0.81, 'blue': 0.95},  # 보라
    }
    
    # 행별로 색상 그룹화
    from collections import defaultdict
    color_groups = defaultdict(list)
    for i, row in enumerate(all_schedule_rows, start=2):
        event_type = row[2]
        if event_type in event_colors:
            color_groups[event_type].append(i)
    
    # batch_format (이벤트유형당 하나의 format)
    formats = []
    for event_type, row_indices in color_groups.items():
        # 연속된 행 그룹화하여 range 생성
        # (간단히 행마다 개별 적용)
        for idx in row_indices:
            formats.append({
                'range': f'A{idx}:I{idx}',
                'format': {'backgroundColor': event_colors[event_type]}
            })
    
    # batch_format은 한 번에 너무 많으면 느리니까 200개씩 분할
    BATCH_SIZE = 200
    for i in range(0, len(formats), BATCH_SIZE):
        batch = formats[i:i+BATCH_SIZE]
        try:
            ws_schedule.batch_format(batch)
            time.sleep(0.5)
        except Exception as e:
            print(f"  ⚠ batch_format 실패 (skip): {e}")
            break
    
    # 첫 행 고정
    try:
        ws_schedule.freeze(rows=1)
    except Exception:
        pass
    
    print(f"  ✅ 풋콜스케줄 시트 업데이트 완료")


# ============================================================
# [NEW: 자본변동 시트 업데이트]
# ============================================================
def update_capital_action_sheet(all_actions):
    """자본변동이력 시트 업데이트
    
    Args:
        all_actions: {stock_name: [action, ...]} dict
    """
    print(f"\n📊 자본변동이력 시트 업데이트...")
    
    try:
        ws_ca = sh.worksheet(SHEET_CAPITAL_ACTION)
    except gspread.exceptions.WorksheetNotFound:
        ws_ca = sh.add_worksheet(title=SHEET_CAPITAL_ACTION, rows=500, cols=6)
        print(f"  ✅ '{SHEET_CAPITAL_ACTION}' 시트 새로 생성")
    
    headers = ['감지일자', '종목명', '자본변동유형', '공시일자', '보고서명', 'DART링크']
    ws_ca.update([headers], range_name='A1:F1')
    ws_ca.format('A1:F1', {
        'textFormat': {'bold': True, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}},
        'backgroundColor': {'red': 0.6, 'green': 0.2, 'blue': 0.2},
        'horizontalAlignment': 'CENTER',
    })
    
    import time
    time.sleep(1.0)
    
    # 신규 행 추가 (기존 + 새로 감지된 것)
    today = datetime.now().strftime('%Y-%m-%d')
    new_rows = []
    for stock_name, actions in all_actions.items():
        for a in actions:
            new_rows.append([
                today, stock_name, a['type'], a['date'],
                a['report_name'], a['link']
            ])
    
    if not new_rows:
        print("  ℹ 신규 자본변동 없음")
        return
    
    # 기존 데이터 + 신규
    existing = ws_ca.get_all_values()
    existing_data = existing[1:] if len(existing) > 1 else []
    
    # 중복 제거 (rcept_link 기준)
    existing_links = set(row[5] for row in existing_data if len(row) > 5)
    deduped_new = [r for r in new_rows if r[5] not in existing_links]
    
    if not deduped_new:
        print(f"  ℹ 모두 중복 (기존 {len(existing_data)}개 기록)")
        return
    
    all_rows = existing_data + deduped_new
    # 날짜 역순 정렬
    all_rows.sort(key=lambda r: r[3] if len(r) > 3 else '', reverse=True)
    
    # 업데이트
    last_row = len(all_rows) + 1
    ws_ca.batch_clear([f'A2:F500'])
    time.sleep(1.0)
    ws_ca.update(all_rows, range_name=f'A2:F{last_row}')
    time.sleep(1.0)
    
    print(f"  ✅ {len(deduped_new)}건 신규 추가 (전체 {len(all_rows)}건)")


# ============================================================
# [메인 실행]
# ============================================================
async def main():
    print(f"🔑 DART_KEY: {DART_KEY[:6]}...")
    if API_DART:
        load_dart_corp_codes()

    print("📋 포트폴리오 시트 읽는 중...")
    all_values = worksheet.get_all_values()

    print("📋 헤더 업데이트 중 (U/V/W 컬럼 추가)...")
    worksheet.update(
        [['발행사 주식코드', '교환대상 회사명', '교환대상 주식코드']],
        range_name='U1:W1'
    )
    worksheet.format('U1:W1', {
        'textFormat': {'bold': True},
        'backgroundColor': {'red': 0.2, 'green': 0.5, 'blue': 0.3},
        'horizontalAlignment': 'CENTER',
    })
    await asyncio.sleep(1.0)

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

    # === 종목별 데이터 수집 ===
    results = []
    for sheet_row, row in data_rows:
        result = get_mezzanine_data(row[1].strip(), row)
        results.append((sheet_row, result))
        await asyncio.sleep(1.5)

    # === 포트폴리오 시트(시트1) 업데이트 ===
    print("\n📝 포트폴리오 시트 업데이트 중...")
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
    upd(f"U{fr}:W{lr}", [[r['issuer_stock_code'], r['target_corp_name'], r['target_stock_code']] for _, r in results])
    await asyncio.sleep(1.0)

    # === NEW: 자본변동 감지 ===
    print("\n🔍 자본변동 감지 중...")
    holdings_for_ca = [
        (r['corp_name'], r['issuer_corp_code'], r['isin'])
        for _, r in results
    ]
    capital_actions = detect_all_capital_actions(holdings_for_ca, days_back=30)
    
    # 자본변동이력 시트 업데이트
    if capital_actions:
        update_capital_action_sheet(capital_actions)

    # === NEW: 풋콜스케줄 시트 업데이트 ===
    print("\n📅 풋콜스케줄 시트 빌드 중...")
    all_schedule_rows = []
    for _, r in results:
        # 해당 종목의 자본변동 가져오기
        stock_actions = capital_actions.get(r['corp_name'], [])
        rows = build_schedule_rows(r, stock_actions)
        all_schedule_rows.extend(rows)
    
    print(f"  → 총 {len(all_schedule_rows)}개 이벤트 행 생성")
    update_schedule_sheet(all_schedule_rows)

    # === 통계 ===
    ytm_c = sum(1 for _, r in results if r['ytm'])
    rfxg_c = sum(1 for _, r in results if r['rfxg_floor'])
    put_c = sum(1 for _, r in results if r['put']['put_begin'] not in ('', '-'))
    call_c = sum(1 for _, r in results if r['call']['call_ratio'] not in ('', '-'))
    ytc_c = sum(1 for _, r in results if r['ytc'])
    issuer_c = sum(1 for _, r in results if r['issuer_stock_code'])
    target_c = sum(1 for _, r in results if r['target_corp_name'])
    refix_c = sum(1 for _, r in results if r.get('refix_cycle_months'))
    
    # 이벤트별 통계
    from collections import Counter
    event_counts = Counter(row[2] for row in all_schedule_rows)
    ca_total = sum(len(actions) for actions in capital_actions.values())

    print(f"\n🏁 완료! {len(results)}개 종목 업데이트됨")
    print(f"👉 https://docs.google.com/spreadsheets/d/{SHEET_ID}")
    print(f"\n📌 포트폴리오 시트:")
    print(f"  ✅ A열 종목명 ← DART 등록명")
    print(f"  ✅ C~F열 회차·종류·발행일·만기일")
    print(f"  ✅ G열 Coupon")
    print(f"  ✅ H열 YTM ← DART ({ytm_c}/{len(results)}개)")
    print(f"  ✅ I열 행사가액")
    print(f"  ✅ J열 리픽싱플로어 ← DART ({rfxg_c}/{len(results)}개)")
    print(f"  ✅ K~L열 전환청구기간 ← DART 우선 + SEIBRO 보완")
    print(f"  ✅ M~O열 PUT ← DART ({put_c}/{len(results)}개, 없으면 -)")
    print(f"  ✅ Q~S열 CALL ← DART ({call_c}/{len(results)}개, 없으면 -)")
    print(f"  ✅ T열 YTC ← DART ({ytc_c}/{len(results)}개)")
    print(f"  ✅ U열 발행사 주식코드 ({issuer_c}/{len(results)}개)")
    print(f"  ✅ V열 교환대상 회사명 ({target_c}/{len(results)}개, EB만)")
    print(f"  ✅ W열 교환대상 주식코드")
    
    print(f"\n📅 풋콜스케줄 시트 (NEW):")
    print(f"  ✅ 총 {len(all_schedule_rows)}개 이벤트")
    for evt, cnt in event_counts.most_common():
        print(f"     - {evt}: {cnt}건")
    print(f"  ✅ 리픽싱 자동 계산: {refix_c}/{len(results)}개 종목")
    
    print(f"\n🔔 자본변동 감지: {ca_total}건 ({len(capital_actions)}개 종목)")

    await update_stock_code_sheet(data_rows, results)


# ============================================================
# [주식코드 시트 업데이트 - 기존 유지]
# ============================================================
async def update_stock_code_sheet(data_rows, results):
    print("\n📊 주식코드 시트 업데이트 중...")
    STOCK_SHEET_NAME = SHEET_STOCK_CODE
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
        isin = row[1].strip()
        bond_type = result['basic_row'][1] if result['basic_row'] else row[3].strip()
        name = result['corp_name']
        issuer_code = result.get('issuer_stock_code', '')
        target_code = result.get('target_stock_code', '') or issuer_code
        rows.append([name, isin, bond_type, issuer_code, target_code])

    if rows:
        ws_stock.batch_clear([f'A2:E{len(rows)+10}'])
        await asyncio.sleep(1.0)
        ws_stock.update(rows, range_name=f'A2:E{len(rows)+1}')
        await asyncio.sleep(1.0)

    print(f"  ✅ 주식코드 시트 업데이트 완료: {len(rows)}개")


# ============================================================
# [엔트리포인트]
# ============================================================
if __name__ == "__main__":
    asyncio.run(main())
