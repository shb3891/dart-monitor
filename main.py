"""
dart-monitor main.py (v2.4)
─────────────────────────────────────────────────────────────
v2.4 변경점 (v2.2 → v2.4)
 1) [네트워크] DART 기업코드 다운로드 재시도(backoff) + 로컬 캐시(corp_code_cache.json)
    → GitHub Actions 러너 IP 차단/일시 장애 시에도 캐시로 동작 지속
 2) [네트워크] SEIBRO/requests 공통 재시도(backoff) 래퍼
 3) [네트워크] 전면 실패 감지(SEIBRO/DART 응답률 0%) → 시트 쓰기 전면 스킵
    → 오염된 값이 정상 데이터를 덮어쓰는 사고 방지 (안전장치)
 4) [파싱] _call_all 전면 강화:
    - 서술문 전용(표 없음): "12개월이 되는 날(2027-04-30)~18개월" 시작/종료 추출
    - 상세가 뒷 섹션("기타 투자판단")에 위치 → 마지막 상세 앵커부터 스캔
    - 2자리 연도 표('26.07.17), 2날짜+가액 표, 소수점 비율(20.57%), 괄호변형
 5) [정합성] X/Y/Z 수동 override 로직 완전 폐기
    → 원문 파싱값(Q/R/S)이 항상 정답. X/Y/Z는 더 이상 읽지 않음.
    → 원문에 CALL이 실제로 없는 종목만 별도 처리(에르코스: is_fallback flag)
─────────────────────────────────────────────────────────────
"""

import asyncio
import os
import json
import time
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

if not SEIBRO_KEY:
    raise RuntimeError("SEIBRO_KEY 환경변수가 설정되지 않았습니다")
if not DART_KEY:
    raise RuntimeError("DART_API_KEY 환경변수가 설정되지 않았습니다")

TODAY = datetime.now().strftime('%Y-%m-%d')

# DART 기업코드 로컬 캐시 파일 (레포에 커밋되어 재사용됨)
CORP_CODE_CACHE = os.environ.get('CORP_CODE_CACHE', 'corp_code_cache.json')

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

# 네트워크 재시도 설정
RETRY_MAX = 3            # 총 시도 횟수
RETRY_BACKOFF = 2.0     # 지수 백오프 기준(초): 2, 4, 8...

# ============================================================
# [시트 이름 상수]
# ============================================================
SHEET_PORTFOLIO = 0
SHEET_SCHEDULE = '풋콜스케줄'
SHEET_STOCK_CODE = '주식코드'
SHEET_STOCK_MATCH = '주식코드매칭'
SHEET_ALIAS = '별칭사전'
SHEET_CAPITAL_ACTION = '자본변동이력'

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
# [네트워크 상태 추적 - 전면 실패 감지용]
# ============================================================
NET_STATS = {'seibro_ok': 0, 'seibro_fail': 0, 'dart_ok': 0, 'dart_fail': 0}


def http_get_retry(url, params=None, timeout=15, kind='generic'):
    """공통 재시도 래퍼 (지수 백오프). 성공 시 Response, 실패 시 None."""
    last_err = None
    for attempt in range(1, RETRY_MAX + 1):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            if kind == 'seibro':
                NET_STATS['seibro_ok'] += 1
            elif kind == 'dart':
                NET_STATS['dart_ok'] += 1
            return r
        except Exception as e:
            last_err = e
            if attempt < RETRY_MAX:
                wait = RETRY_BACKOFF ** attempt
                time.sleep(wait)
    if kind == 'seibro':
        NET_STATS['seibro_fail'] += 1
    elif kind == 'dart':
        NET_STATS['dart_fail'] += 1
    print(f"  ⚠ HTTP 실패({kind}, {RETRY_MAX}회 재시도 후): {last_err}")
    return None


# ============================================================
# [DART 기업코드 딕셔너리]
# ============================================================
DART_CORP_DICT = {}
DART_CORP_NAME_DICT = {}
DART_NAME_DICT = {}


def _save_corp_cache():
    try:
        with open(CORP_CODE_CACHE, 'w', encoding='utf-8') as f:
            json.dump({
                'corp': DART_CORP_DICT,
                'corp_name': DART_CORP_NAME_DICT,
                'name': DART_NAME_DICT,
                'saved_at': TODAY,
            }, f, ensure_ascii=False)
        print(f"  💾 DART 기업코드 캐시 저장: {CORP_CODE_CACHE}")
    except Exception as e:
        print(f"  ⚠ 캐시 저장 실패: {e}")


def _load_corp_cache():
    """로컬 캐시에서 기업코드 로드. 성공 시 True."""
    global DART_CORP_DICT, DART_CORP_NAME_DICT, DART_NAME_DICT
    try:
        if not os.path.exists(CORP_CODE_CACHE):
            return False
        with open(CORP_CODE_CACHE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        DART_CORP_DICT = data.get('corp', {})
        DART_CORP_NAME_DICT = data.get('corp_name', {})
        DART_NAME_DICT = data.get('name', {})
        if DART_CORP_DICT:
            print(f"  ♻ DART 기업코드 캐시 사용: {len(DART_CORP_DICT):,}개 "
                  f"(저장일: {data.get('saved_at', '?')})")
            return True
    except Exception as e:
        print(f"  ⚠ 캐시 로드 실패: {e}")
    return False


def load_dart_corp_codes():
    """DART 전체 기업코드 다운로드 + 캐싱. 실패 시 로컬 캐시로 폴백."""
    global DART_CORP_DICT, DART_CORP_NAME_DICT, DART_NAME_DICT
    print("  📥 DART 기업코드 전체 다운로드 중...")
    r = http_get_retry(f"{DART_BASE}/corpCode.xml",
                       params={'crtfc_key': DART_KEY}, timeout=30, kind='dart')
    if r is None:
        print("  ⚠ DART 기업코드 다운로드 실패 → 로컬 캐시 폴백 시도")
        if _load_corp_cache():
            return
        print("  ❌ 캐시도 없음 → DART 파싱 불가 상태로 진행")
        return
    try:
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
        print(f"  ✅ DART 기업코드 로드 완료: {count:,}개 (상장사), "
              f"이름사전 {len(DART_NAME_DICT):,}개")
        _save_corp_cache()
    except Exception as e:
        print(f"  ⚠ DART 기업코드 파싱 실패: {e} → 캐시 폴백")
        _load_corp_cache()


# ============================================================
# [ISIN → 발행사 주식코드 변환]
# ============================================================
def isin_to_issuer_stock_code(isin):
    if not isin or len(isin) < 9 or not isin.startswith('KR6'):
        return ''
    bond_code = isin[3:9]
    if not bond_code.isdigit():
        return ''
    return bond_code[:5] + '0'


def stock_isin_to_code(stock_isin):
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
    r = http_get_retry(full_url, timeout=10, kind='seibro')
    if r is None:
        print(f"  ⚠ SEIBRO 호출 실패 [{api_id}]")
        return None
    try:
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
        print(f"  ⚠ SEIBRO 파싱 실패 [{api_id}]: {e}")
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


def _kr_to_iso(s):
    m = re.match(r'(\d{4})\s*년?\s*(\d{1,2})\s*월?\s*(\d{1,2})\s*일?', s)
    return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}" if m else None


def _yy_to_iso(s):
    """'26.07.17 → 2026-07-17 (2자리 연도)"""
    m = re.match(r"'?(\d{2})\.(\d{1,2})\.(\d{1,2})", s.strip())
    if not m:
        return None
    return f"{2000 + int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
# ============================================================
# [리픽싱 일정 계산 유틸]
# ============================================================
def calculate_refix_schedule(issue_date_str, maturity_date_str, cycle_months):
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
        schedules.append({'chasu': chasu, 'date': current.strftime('%Y-%m-%d')})
        chasu += 1
        current += relativedelta(months=cycle_months)
        if chasu > 50:
            break
    return schedules


def parse_refix_cycle_from_dart(dart_text):
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
# [자본변동 감지]
# ============================================================
def detect_capital_actions(corp_code, days_back=30):
    if not corp_code:
        return []
    actions = []
    capital_keywords = {
        '주식병합': '주식병합', '액면병합': '액면병합', '주식분할': '주식분할',
        '액면분할': '액면분할', '무상증자': '무상증자', '유상증자': '유상증자',
        '무상감자': '감자', '유상감자': '감자', '주식배당': '주식배당', '액면가변경': '액면가변경',
    }
    try:
        bgn_de = (datetime.now() - timedelta(days=days_back)).strftime('%Y%m%d')
        end_de = datetime.now().strftime('%Y%m%d')
        r = http_get_retry(f"{DART_BASE}/list.json", params={
            'crtfc_key': DART_KEY, 'corp_code': corp_code,
            'bgn_de': bgn_de, 'end_de': end_de, 'page_count': 40,
        }, timeout=10, kind='dart')
        if r is None:
            return []
        data = r.json()
        if data.get('status') not in ('000', '013'):
            return []
        for item in data.get('list', []):
            rpt = item.get('report_nm', '')
            if '[첨부정정]' in rpt or '[첨부추가]' in rpt:
                continue
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
                'date': fmt_dt, 'type': matched_type, 'report_name': rpt,
                'rcept_no': rcept_no,
                'link': f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}",
            })
    except Exception as e:
        print(f"  ⚠ 자본변동 감지 실패 (corp_code={corp_code}): {e}")
    return actions


def detect_all_capital_actions(holdings_with_corp_code, days_back=30):
    print(f"\n🔍 자본변동 감지 시작 ({days_back}일 전부터)...")
    unique_corps = {}
    for name, corp_code, isin in holdings_with_corp_code:
        if corp_code and corp_code not in unique_corps:
            unique_corps[corp_code] = (name, isin)
    all_actions = {}
    detected_count = 0
    for corp_code, (name, isin) in unique_corps.items():
        actions = detect_capital_actions(corp_code, days_back)
        if actions:
            all_actions[name] = actions
            detected_count += len(actions)
            for a in actions:
                print(f"  📢 {name}: {a['type']} ({a['date']}) - {a['report_name']}")
        time.sleep(0.3)
    print(f"  → 총 {detected_count}건 감지 (대상 {len(unique_corps)}개 발행사)")
    return all_actions


# ============================================================
# [DART 파싱]
# ============================================================
def get_dart_info_by_stock_code(stock_code):
    return DART_CORP_DICT.get(stock_code, ''), DART_CORP_NAME_DICT.get(stock_code, '')


def dart_search_cb_disclosure(corp_code, issu_dt_str):
    try:
        issu_date = datetime.strptime(issu_dt_str, '%Y-%m-%d')
        bgn_de = (issu_date - timedelta(days=90)).strftime('%Y%m%d')
        end_de = (issu_date + timedelta(days=30)).strftime('%Y%m%d')
        issue_kws = ['전환사채권발행결정', '교환사채권발행결정', '신주인수권부사채권발행결정']
        for pblntf_ty in ['B', '']:
            params = {'crtfc_key': DART_KEY, 'corp_code': corp_code,
                      'bgn_de': bgn_de, 'end_de': end_de, 'page_count': 40}
            if pblntf_ty:
                params['pblntf_ty'] = pblntf_ty
            r = http_get_retry(f"{DART_BASE}/list.json", params=params, timeout=10, kind='dart')
            if r is None:
                continue
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
    r = http_get_retry(f"{DART_BASE}/document.xml",
                       params={'crtfc_key': DART_KEY, 'rcept_no': rcept_no},
                       timeout=30, kind='dart')
    if r is None:
        return ''
    try:
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


# ============================================================
# [v2.4 강화된 CALL 파싱]
# ============================================================
def _call_all(clean):
    """
    v2.4 CALL 파싱. 반환: (schedule, ytc, ratio, is_fallback)
    - 표: ISO3날짜 / 2자리연도3날짜 / 한국어3날짜 / 2날짜+가액(통지·지급기일) 대응
    - 비율: 소수점/괄호변형/한도표현, 여러 값이면 최솟값(사용자 방침)
    """
    text = re.sub(r'([가-힣])\n([가-힣])', r'\1\2', clean)
    if '매도청구권' not in text and 'Call Option' not in text \
            and 'Call option' not in text and 'CALL OPTION' not in text:
        return [], '', '', False

    # --- 상세 앵커 수집 ---
    detail_anchors = [
        r'매도청구권\s*\(?\s*Call\s*[Oo]ption\s*\)?\s*(?:에\s*관한|\])',
        r'\[\s*매도청구권',
        r'매도청구권\s*통지\s*기간',
        r'매도청구\s*통지기일',
        r'구분\s+매도청구권?\s*통지',
        r'\d\.\s*매도청구권\s*[:：]',
    ]
    anchor_pos = []
    for pat in detail_anchors:
        anchor_pos += [m.start() for m in re.finditer(pat, text, re.IGNORECASE)]
    if not anchor_pos:
        for m in re.finditer(r'매도청구권|Call\s*[Oo]ption', text, re.IGNORECASE):
            tail = text[m.start():m.start() + 1500]
            rm = re.search(r'\d\s*차\s+[\'\d]', tail)
            pm = re.search(r'조기상환\s*청구', tail)
            if rm and (pm is None or rm.start() < pm.start()):
                anchor_pos.append(m.start())

    # --- 표 파싱 ---
    schedule = []
    end_markers = [
        r'조기상환\s*청구권', r'조기상환\s*청구기간', r'전환에\s*관한\s*사항',
        r'주식으로의\s*전환청구', r'합병', r'청약일', r'제\s*6\s*조', r'제\s*7\s*조',
        r'\(3\)\s*대금지급', r'\(4\)\s*매도청구권\s*행사\s*범위', r'사채권자의\s*본\s*사채\s*의무보유',
    ]
    emr = [re.compile(p) for p in end_markers]
    windows = []
    for pos in sorted(set(anchor_pos)):
        seg = text[pos:pos + 3500]
        cut = len(seg)
        for rgx in emr:
            mm = rgx.search(seg, 30)
            if mm and mm.start() < cut:
                cut = mm.start()
        windows.append(seg[:cut])
    region = '\n'.join(windows) if windows else ''

    def _dedup_add(rows):
        seen = set()
        for chasu, frm, to, pay, rate in rows:
            if chasu in seen:
                continue
            seen.add(chasu)
            schedule.append({'chasu': chasu, 'from_date': frm, 'to_date': to,
                             'pay_date': pay, 'rate': rate or ''})

    if region:
        rows = re.findall(
            r'(\d{1,2})\s*차\s+(\d{4}-\d{2}-\d{2})\s+(\d{4}-\d{2}-\d{2})\s+(\d{4}-\d{2}-\d{2})'
            r'(?:\s+(?:전자등록금액의\s*)?(\d{1,3}(?:\.\d{1,6})?)\s*%?)?', region)
        if rows:
            _dedup_add([(int(r[0]), r[1], r[2], r[3], r[4]) for r in rows])
        if not schedule:
            rows = re.findall(
                r"(\d{1,2})\s*차\s+('?\d{2}\.\d{1,2}\.\d{1,2})\s+('?\d{2}\.\d{1,2}\.\d{1,2})\s+"
                r"('?\d{2}\.\d{1,2}\.\d{1,2})(?:\s+(\d{1,3}(?:\.\d{1,6})?)\s*%?)?", region)
            if rows:
                _dedup_add([(int(r[0]), _yy_to_iso(r[1]), _yy_to_iso(r[2]),
                             _yy_to_iso(r[3]), r[4]) for r in rows])
        if not schedule:
            rows = re.findall(
                r'(\d{1,2})\s*차\s+(\d{4}\s*년\s*\d{1,2}\s*월\s*\d{1,2}\s*일)\s+'
                r'(\d{4}\s*년\s*\d{1,2}\s*월\s*\d{1,2}\s*일)\s+'
                r'(\d{4}\s*년\s*\d{1,2}\s*월\s*\d{1,2}\s*일)', region)
            if rows:
                _dedup_add([(int(r[0]), _kr_to_iso(r[1]), _kr_to_iso(r[2]),
                             _kr_to_iso(r[3]), '') for r in rows])
        if not schedule:  # 2날짜+가액 ISO
            rows = re.findall(
                r'(\d{1,2})\s*차\s+(\d{4}-\d{2}-\d{2})\s+(\d{4}-\d{2}-\d{2})'
                r'\s+(\d{1,3}(?:\.\d{1,6})?)\s*%', region)
            if rows:
                _dedup_add([(int(r[0]), r[1], r[2], r[2], r[3]) for r in rows])
        if not schedule:  # 2날짜+가액 2자리연도
            rows = re.findall(
                r"(\d{1,2})\s*차\s+('?\d{2}\.\d{1,2}\.\d{1,2})\s+('?\d{2}\.\d{1,2}\.\d{1,2})"
                r"\s+(\d{1,3}(?:\.\d{1,6})?)\s*%", region)
            if rows:
                _dedup_add([(int(r[0]), _yy_to_iso(r[1]), _yy_to_iso(r[2]),
                             _yy_to_iso(r[2]), r[3]) for r in rows])

    schedule.sort(key=lambda x: x['chasu'])

    # --- 비율 ---
    positions = [m.start() for m in re.finditer(r'매도청구권|Call\s*[Oo]ption|CALL\s*OPTION', text)]
    ctx = ''
    if positions:
        ctx = text[positions[0]:positions[0] + 20000]
        if len(positions) > 10:
            for pos in positions[-3:]:
                if pos > positions[0] + 20000:
                    ctx += '\n' + text[pos:pos + 4000]

    candidate_ratios = []
    strong = [
        r'\(\s*Call\s*Option\s*(\d+(?:\.\d+)?)\s*%\s*\)',
        r'전자등록총액\s*\)?\s*의\s*(\d+(?:\.\d+)?)\s*%',
        r'발행가액\s*총액\s*\(?\s*전자등록총액\s*\)?\s*의\s*(\d+(?:\.\d+)?)\s*%',
        r'발행가액의\s*(\d+(?:\.\d+)?)\s*%\s*를?\s*(?:총\s*한도|초과|이내|한도)',
        r'인수금액의\s*(\d+(?:\.\d+)?)\s*%',
        r'권면금액\s*합계액의\s*(\d+(?:\.\d+)?)\s*%',
        r'콜옵션\s*(\d+(?:\.\d+)?)\s*%',
    ]
    for m in re.finditer(strong[0], text, re.IGNORECASE):
        try:
            v = float(m.group(1))
            if 0 < v <= 100:
                candidate_ratios.append(v)
        except Exception:
            pass
    if ctx:
        for p in strong[1:]:
            for m in re.finditer(p, ctx):
                try:
                    v = float(m.group(1))
                    if 0 < v <= 100:
                        candidate_ratios.append(v)
                except Exception:
                    pass

    ratio = ''
    is_fallback = False
    if candidate_ratios:
        best = min(candidate_ratios)  # 사용자 방침: 보수적(최솟값)
        ratio = str(int(best)) if best == int(best) else str(best)

    if not ratio and ctx and ('전자등록금액의 전부' in ctx or '전부 또는 일부' in ctx):
        explicit = re.search(
            r'(?:취득\s*규모|Call\s*Option|전자등록총액|권면총액|사채권|발행가액|인수금액).{0,50}?\d+(?:\.\d+)?\s*%',
            ctx, re.IGNORECASE)
        if not explicit:
            ratio = '100'
            is_fallback = True

    # --- YTC ---
    ytc = ''
    m = re.search(r'(?:매매대금|매도청구권|콜옵션)[^조]{0,200}?(?:연복리|복리\s*연|연\s*복리)\s*(\d+(?:\.\d+)?)', text)
    if m:
        ytc = m.group(1)

    return schedule, ytc, ratio, is_fallback


def _call_dates_from_narrative(clean):
    """표 없을 때 서술문에서 CALL 시작/종료 (상세 앵커 우선)"""
    text = re.sub(r'([가-힣])\n([가-힣])', r'\1\2', clean)
    idxs = [m.start() for m in re.finditer(
        r'\[\s*매도청구권|매도청구권\s*\(?\s*Call\s*[Oo]ption\s*\)?\s*에\s*관한|\d\.\s*매도청구권\s*[:：]',
        text)]
    if not idxs:
        idxs = [m.start() for m in re.finditer(r'매도청구권', text)]
    if not idxs:
        return '', ''
    idx = idxs[-1]
    section = text[idx:idx + 2000]
    for marker in ['조기상환청구권', '10. 합병', '11. 청약일', '(3) 대금지급', '(4) 매도청구권']:
        mi = section.find(marker, 50)
        if 0 < mi:
            section = section[:mi]
            break
    dates = []
    for m in re.finditer(
            r'(\d+)\s*(?:개월|년)이?\s*(?:경과한|되는)\s*날?(?:인)?(?:의\s*전일)?\s*'
            r'\(?\s*(\d{4})\s*년?\s*(\d{1,2})\s*월?\s*(\d{1,2})\s*일?\s*\)?', section):
        try:
            dates.append((m.start(), f"{int(m.group(2)):04d}-{int(m.group(3)):02d}-{int(m.group(4)):02d}"))
        except Exception:
            pass
    for m in re.finditer(r'까지\s*\(?\s*(\d{4})\s*년?\s*(\d{1,2})\s*월?\s*(\d{1,2})\s*일?', section):
        try:
            dates.append((m.start(), f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"))
        except Exception:
            pass
    dates.sort(key=lambda x: x[0])
    uniq = []
    for _, d in dates:
        if d not in uniq:
            uniq.append(d)
    if len(uniq) >= 2:
        return uniq[0], uniq[1]
    elif len(uniq) == 1:
        return uniq[0], ''
    return '', ''
def dart_parse_disclosure(correction_rcept_no, original_rcept_no, xrc_price=''):
    result = {
        'ytm': '', 'rfxg_floor': '', 'xrc_begin_dart': '', 'xrc_end_dart': '',
        'put_begin': '', 'put_end': '', 'put_date': '',
        'call_ratio': '', 'call_begin': '', 'call_end': '', 'call_fallback': False,
        'ytc': '', 'refix_cycle_months': None, 'put_schedule': [], 'call_schedule': [],
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
                    if float(m.group(1)) < 100:
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
            try:
                n = int(float(val.replace(',', '')))
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

    for text in texts_primary:
        cycle = parse_refix_cycle_from_dart(text)
        if cycle:
            result['refix_cycle_months'] = cycle
            print(f"  ✅ 리픽싱 주기: 매 {cycle}개월")
            break

    # 전환청구기간
    def _xrc_dates(clean):
        begin, end = '', ''
        for kw in ['전환청구기간', '교환청구기간', '행사청구기간', '권리행사기간',
                   '전환권 행사', '전환권행사', '권리행사기 간']:
            idx = clean.find(kw)
            if idx < 0:
                continue
            section = clean[idx: idx + 800]
            m_b = (re.search(r'시작일[^0-9\n]*(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)', section) or
                   re.search(r'시작일[^0-9\n]*(\d{4}-\d{2}-\d{2})', section) or
                   re.search(r'시작일[^0-9\n]*(\d{4}\.\d{1,2}\.\d{1,2})', section) or
                   re.search(r'(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)\s*부터', section) or
                   re.search(r'부터[^0-9\n]*(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)', section))
            m_e = (re.search(r'종료일[^0-9\n]*(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)', section) or
                   re.search(r'종료일[^0-9\n]*(\d{4}-\d{2}-\d{2})', section) or
                   re.search(r'종료일[^0-9\n]*(\d{4}\.\d{1,2}\.\d{1,2})', section) or
                   re.search(r'(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)\s*까지', section) or
                   re.search(r'까지[^0-9\n]*(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)', section))
            if not m_b and not m_e:
                range_m = re.search(
                    r'(\d{4}년\s*\d{1,2}월\s*\d{1,2}일|\d{4}-\d{2}-\d{2}|\d{4}\.\d{2}\.\d{2})'
                    r'\s*[~∼]\s*'
                    r'(\d{4}년\s*\d{1,2}월\s*\d{1,2}일|\d{4}-\d{2}-\d{2}|\d{4}\.\d{2}\.\d{2})', section)
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

    # PUT (차수 기반)
    def _put_all(clean):
        put_idx = -1
        for kw in ['조기상환 청구기간', '조기상환청구기간', '[조기상환청구권', '조기상환청구권(Put']:
            idx = clean.find(kw)
            if idx >= 0:
                put_idx = idx
                break
        if put_idx < 0:
            return []
        end_markers = ['매도청구권(Call', '매도청구권(call', '[매도청구권', '매도 청구권',
                       '매수청구권', 'Call Option', 'call option', '주식으로의 전환청구',
                       '전환에 관한 사항', '가) 사채의 종류', '나) 사채의 종류',
                       '기타 투자판단', '기타 특약사항', '기타사항', '제6조', '제7조']
        put_end = put_idx + 8000
        for marker in end_markers:
            m_idx = clean.find(marker, put_idx + 100)
            if m_idx > 0 and m_idx < put_end:
                put_end = m_idx
        put_section = clean[put_idx: put_end]
        pattern = re.compile(
            r'(\d{1,2})\s*차\s+(\d{4}-\d{2}-\d{2})\s+(\d{4}-\d{2}-\d{2})\s+(\d{4}-\d{2}-\d{2})'
            r'(?:\s+(\d{2,3}\.\d{1,6}))?')
        matches = pattern.findall(put_section)
        schedule = []
        seen_chasu = set()
        for m in matches:
            chasu = int(m[0])
            if chasu in seen_chasu:
                continue
            seen_chasu.add(chasu)
            schedule.append({'chasu': chasu, 'from_date': m[1], 'to_date': m[2],
                             'pay_date': m[3], 'rate': m[4] if m[4] else ''})
        if not schedule:
            dates = re.findall(r'\d{4}-\d{2}-\d{2}', put_section)
            rates = re.findall(r'(\d{2,3}\.\d{2,6})\s*%', put_section)
            for i in range(len(dates) // 3):
                schedule.append({'chasu': i + 1, 'from_date': dates[i*3],
                                 'to_date': dates[i*3+1], 'pay_date': dates[i*3+2],
                                 'rate': rates[i] if i < len(rates) else ''})
        schedule.sort(key=lambda x: x['chasu'])
        return schedule

    for text in texts_primary:
        put_schedule = _put_all(text)
        if put_schedule:
            result['put_schedule'] = put_schedule
            today = datetime.now().strftime('%Y-%m-%d')
            for p in put_schedule:
                if p['to_date'] >= today:
                    result['put_begin'] = p['from_date']
                    result['put_end'] = p['to_date']
                    result['put_date'] = p['pay_date']
                    break
            else:
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

    # CALL (v2.4 강화 파싱)
    for text in texts_primary:
        call_schedule, ytc, ratio, is_fb = _call_all(text)
        if call_schedule:
            result['call_schedule'] = call_schedule
            result['ytc'] = ytc
            result['call_ratio'] = ratio
            result['call_fallback'] = is_fb
            if len(call_schedule) >= 2:
                result['call_begin'] = call_schedule[0]['from_date']
                result['call_end'] = call_schedule[-1]['from_date']
            elif len(call_schedule) == 1:
                result['call_begin'] = call_schedule[0]['from_date']
                result['call_end'] = call_schedule[0]['from_date']
            if ytc:
                print(f"  ✅ YTC: {ytc}%")
            if ratio:
                print(f"  ✅ CALL비율: {ratio}%{'  ⚠️ (fallback-확인필요)' if is_fb else ''}")
            print(f"  ✅ CALL 스케줄: {len(call_schedule)}개 차수")
            break
        if ratio and not result['call_ratio']:
            result['call_ratio'] = ratio
            result['call_fallback'] = is_fb
        if ytc and not result['ytc']:
            result['ytc'] = ytc

    # 표 없고 비율만 있으면 서술문에서 날짜
    if not result['call_schedule'] and result['call_ratio']:
        for text in texts_primary:
            cb, ce = _call_dates_from_narrative(text)
            if cb:
                result['call_begin'] = cb
                result['call_end'] = ce
                break

    if not result['call_schedule'] and not result['call_ratio']:
        result['call_begin'] = '-'
        result['call_end'] = '-'
        result['call_ratio'] = '-'
        print("  ℹ CALL 없음(-)")
    elif not result['call_schedule']:
        print(f"  ✅ 서술문 CALL: 비율={result['call_ratio']}%, "
              f"시작={result['call_begin']}, 종료={result['call_end']}")

    return result


def parse_dart_for_bond(issuer_corp_code, issu_dt_str, xrc_price=''):
    empty = {
        'ytm': '', 'rfxg_floor': '', 'xrc_begin_dart': '', 'xrc_end_dart': '',
        'put_begin': '', 'put_end': '', 'put_date': '',
        'call_ratio': '', 'call_begin': '', 'call_end': '', 'call_fallback': False,
        'ytc': '', 'refix_cycle_months': None, 'put_schedule': [], 'call_schedule': [],
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
        'secn_nm': secn_nm, 'hosu': extract_hosu(secn_nm),
        'bond_type': determine_bond_type(secn_nm),
        'issu_dt': format_date(get_attr(el, 'ISSU_DT')),
        'xpir_dt': format_date(get_attr(el, 'XPIR_DT')),
        'coupon': get_attr(el, 'COUPON_RATE'),
    }


def parse_put_call_seibro(isin):
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
    r = {'xrc_price': '', 'xrc_begin': '', 'xrc_end': '', 'xrc_stk_isin': '', 'xrc_stk_name': ''}
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
            if b:
                begins.append(b)
            if e:
                ends.append(e)
        if begins:
            r['xrc_begin'] = format_date(min(begins))
        if ends:
            r['xrc_end'] = format_date(max(ends))
    return r


# ============================================================
# [오케스트레이터]
# ============================================================
def get_mezzanine_data(isin, existing_row):
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
        'xrc_price': '', 'xrc_begin': '', 'xrc_end': '', 'xrc_stk_isin': '', 'xrc_stk_name': ''}

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
        'call_ratio': '', 'call_begin': '', 'call_end': ''}

    dart_data = {
        'ytm': '', 'rfxg_floor': '', 'xrc_begin_dart': '', 'xrc_end_dart': '',
        'put_begin': '', 'put_end': '', 'put_date': '',
        'call_ratio': '', 'call_begin': '', 'call_end': '', 'call_fallback': False,
        'ytc': '', 'refix_cycle_months': None, 'put_schedule': [], 'call_schedule': [],
    }
    if API_DART and issu_dt and issu_dt != '-' and issuer_corp_code:
        print(f"  🌐 DART 조회 중... (발행일: {issu_dt}, corp_code: {issuer_corp_code})")
        dart_data = parse_dart_for_bond(issuer_corp_code, issu_dt, xrc_price=exercise.get('xrc_price', ''))
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
        final_call = {'call_ratio': '-', 'call_begin': '-', 'call_end': '-', 'fallback': False}
    else:
        final_call = {
            'call_ratio': dart_data['call_ratio'] or seibro_pc['call_ratio'],
            'call_begin': dart_data['call_begin'] or seibro_pc['call_begin'],
            'call_end': dart_data['call_end'] or seibro_pc['call_end'],
            'fallback': dart_data.get('call_fallback', False),
        }

    return {
        'isin': isin, 'corp_name': corp_name, 'basic_row': basic_row, 'coupon': coupon,
        'ytm': dart_data['ytm'], 'xrc_price': exercise.get('xrc_price', ''),
        'rfxg_floor': dart_data['rfxg_floor'], 'xrc_begin': final_xrc_begin, 'xrc_end': final_xrc_end,
        'put': final_put, 'call': final_call, 'ytc': dart_data.get('ytc', ''),
        'issuer_stock_code': issuer_stock_code, 'issuer_corp_code': issuer_corp_code,
        'target_corp_name': target_corp_name, 'target_stock_code': target_stock_code,
        'refix_cycle_months': dart_data.get('refix_cycle_months'),
        'put_schedule': dart_data.get('put_schedule', []),
        'call_schedule': dart_data.get('call_schedule', []),
        'issu_dt': issu_dt, 'xpir_dt': basic_row[3] if len(basic_row) > 3 else '',
    }


# ============================================================
# [풋콜스케줄 시트 빌드]
# ============================================================
def build_schedule_rows(data, capital_actions=None):
    rows = []
    isin = data['isin']
    name = data['corp_name']
    capital_note = ''
    if capital_actions:
        capital_note = ' / '.join([f"{a['type']}({a['date']})" for a in capital_actions])
    if data['xrc_begin'] and data['xrc_begin'] != '-':
        rows.append([isin, name, '전환청구시작', '', data['xrc_begin'], '', '', '', capital_note])
    if data['xrc_end'] and data['xrc_end'] != '-':
        rows.append([isin, name, '전환청구종료', '', data['xrc_end'], '', '', '', capital_note])
    if data['xpir_dt'] and data['xpir_dt'] != '-':
        rows.append([isin, name, '만기', '', data['xpir_dt'], '', '', '', capital_note])
    if data.get('refix_cycle_months') and data['issu_dt'] and data['xpir_dt']:
        floor_str = f"{data['rfxg_floor']}" if data['rfxg_floor'] else ''
        for refix in calculate_refix_schedule(data['issu_dt'], data['xpir_dt'], data['refix_cycle_months']):
            rows.append([isin, name, '리픽싱', f"{refix['chasu']}차", refix['date'], '', '', floor_str, capital_note])
    for put in data.get('put_schedule', []):
        rate_str = f"{put['rate']}%" if put.get('rate') else ''
        rows.append([isin, name, '풋옵션', f"{put['chasu']}차",
                     put.get('from_date', '') or '', put.get('to_date', '') or '',
                     put.get('pay_date', '') or '', rate_str, capital_note])
    for call in data.get('call_schedule', []):
        rate_str = f"{call['rate']}%" if call.get('rate') else ''
        rows.append([isin, name, '콜옵션', f"{call['chasu']}차",
                     call.get('from_date', '') or '', call.get('to_date', '') or '',
                     call.get('pay_date', '') or '', rate_str, capital_note])
    return rows


def update_schedule_sheet(all_schedule_rows):
    print(f"\n📝 풋콜스케줄 시트 업데이트 ({len(all_schedule_rows)}행)...")
    try:
        ws_schedule = sh.worksheet(SHEET_SCHEDULE)
    except gspread.exceptions.WorksheetNotFound:
        ws_schedule = sh.add_worksheet(title=SHEET_SCHEDULE,
                                       rows=max(3000, len(all_schedule_rows) + 100), cols=10)
        print(f"  ✅ '{SHEET_SCHEDULE}' 시트 새로 생성")
    headers = ['종목코드', '종목명', '이벤트유형', '차수', '시작일', '종료일', '지급일', '비율/금리', '비고']
    ws_schedule.update([headers], range_name='A1:I1')
    ws_schedule.format('A1:I1', {
        'textFormat': {'bold': True, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}},
        'backgroundColor': {'red': 0.27, 'green': 0.45, 'blue': 0.77},
        'horizontalAlignment': 'CENTER'})
    time.sleep(1.5)
    try:
        current_row_count = ws_schedule.row_count
    except Exception:
        current_row_count = 5000
    clear_last_row = max(current_row_count, len(all_schedule_rows) + 100)
    ws_schedule.batch_clear([f'A2:I{clear_last_row}'])
    print(f"  🧹 A2:I{clear_last_row} 클리어")
    time.sleep(1.5)
    if all_schedule_rows:
        ws_schedule.update(all_schedule_rows, range_name=f'A2:I{len(all_schedule_rows)+1}')
        time.sleep(1.0)
    print("  🎨 색상 적용 중...")
    event_colors = {
        '전환청구시작': {'red': 0.85, 'green': 0.88, 'blue': 0.95},
        '전환청구종료': {'red': 0.85, 'green': 0.88, 'blue': 0.95},
        '만기': {'red': 0.95, 'green': 0.80, 'blue': 0.80},
        '리픽싱': {'red': 1.00, 'green': 0.95, 'blue': 0.80},
        '풋옵션': {'red': 0.85, 'green': 0.93, 'blue': 0.83},
        '콜옵션': {'red': 0.90, 'green': 0.81, 'blue': 0.95},
    }
    from collections import defaultdict
    color_groups = defaultdict(list)
    for i, row in enumerate(all_schedule_rows, start=2):
        if row[2] in event_colors:
            color_groups[row[2]].append(i)
    formats = []
    for event_type, row_indices in color_groups.items():
        for idx in row_indices:
            formats.append({'range': f'A{idx}:I{idx}',
                            'format': {'backgroundColor': event_colors[event_type]}})
    BATCH_SIZE = 200
    for i in range(0, len(formats), BATCH_SIZE):
        try:
            ws_schedule.batch_format(formats[i:i+BATCH_SIZE])
            time.sleep(0.5)
        except Exception as e:
            print(f"  ⚠ batch_format 실패 (skip): {e}")
            break
    try:
        ws_schedule.freeze(rows=1)
    except Exception:
        pass
    print("  ✅ 풋콜스케줄 시트 업데이트 완료")


def update_capital_action_sheet(all_actions):
    print("\n📊 자본변동이력 시트 업데이트...")
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
        'horizontalAlignment': 'CENTER'})
    time.sleep(1.0)
    today = datetime.now().strftime('%Y-%m-%d')
    new_rows = []
    for stock_name, actions in all_actions.items():
        for a in actions:
            new_rows.append([today, stock_name, a['type'], a['date'], a['report_name'], a['link']])
    if not new_rows:
        print("  ℹ 신규 자본변동 없음")
        return
    existing = ws_ca.get_all_values()
    existing_data = existing[1:] if len(existing) > 1 else []
    existing_links = set(row[5] for row in existing_data if len(row) > 5)
    deduped_new = [r for r in new_rows if r[5] not in existing_links]
    if not deduped_new:
        print(f"  ℹ 모두 중복 (기존 {len(existing_data)}개 기록)")
        return
    all_rows = existing_data + deduped_new
    all_rows.sort(key=lambda r: r[3] if len(r) > 3 else '', reverse=True)
    ws_ca.batch_clear(['A2:F500'])
    time.sleep(1.0)
    ws_ca.update(all_rows, range_name=f'A2:F{len(all_rows)+1}')
    time.sleep(1.0)
    print(f"  ✅ {len(deduped_new)}건 신규 추가 (전체 {len(all_rows)}건)")


# ============================================================
# [메인 실행]
# ============================================================
def _valid_manual_call_ratio(raw):
    """X열(CALL 비율 수동) 값 검증.
    안전 규칙: 0 초과 100 이하의 숫자만 유효한 CALL 비율로 인정.
      - 빈칸/'-'  → None (수동값 없음, 원문 파싱값 사용)
      - 음수, 100 초과, 숫자 아님 → None (오염 의심, 무시하고 원문값 사용)
      - 0         → 유효 (콜옵션 비율 0%. 남성/영흥 EB처럼 콜 없는 경우 명시)
    과거 X/Y/Z에 보유내역 평가금액/수익률(-17.16, 202.26 등)이
    오염 복사됐던 사고를 이 검증으로 차단한다.
    반환: (문자열 비율, True) 또는 (None, False)
    """
    if raw is None:
        return None, False
    s = str(raw).strip().replace('%', '').replace(',', '')
    if s == '' or s == '-':
        return None, False
    try:
        v = float(s)
    except ValueError:
        return None, False
    if v < 0 or v > 100:
        return None, False
    # 정수면 정수로, 소수면 그대로 (20.57 같은 값 보존)
    out = str(int(v)) if v == int(v) else s
    return out, True


async def main():
    print(f"🔑 DART_KEY: {DART_KEY[:6]}...")
    if API_DART:
        load_dart_corp_codes()

    print("📋 포트폴리오 시트 읽는 중...")
    all_values = worksheet.get_all_values()

    print("📋 헤더 업데이트 중 (U/V/W)...")
    worksheet.update([['발행사 주식코드', '교환대상 회사명', '교환대상 주식코드']], range_name='U1:W1')
    worksheet.format('U1:W1', {
        'textFormat': {'bold': True},
        'backgroundColor': {'red': 0.2, 'green': 0.5, 'blue': 0.3},
        'horizontalAlignment': 'CENTER'})
    await asyncio.sleep(1.0)

    # v2.5: X열만 "CALL 비율(수동)"로 사용. Y/Z(구 시작일·종료일 수동)는 폐기 → 헤더/데이터 비움.
    print("📋 헤더 정리 중 (X=수동 CALL 비율 / Y·Z 폐기)...")
    worksheet.update([['CALL 비율(수동)', '', '']], range_name='X1:Z1')
    await asyncio.sleep(1.0)

    # ⚠️ v2.4: X/Y/Z 수동 override 로직 완전 폐기.
    #   원문 파싱값(Q/R/S)이 항상 정답. X/Y/Z는 더 이상 읽지 않는다.
    #   (과거 X/Y/Z에 보유내역 평가금액/수익률이 오염 복사돼 Q/R/S를 망가뜨렸던 문제 제거)

    data_rows = [
        (i + 2, row) for i, row in enumerate(all_values[1:])
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
        # v2.5: X열(인덱스 23) 수동 CALL 비율 검증 후 부착 (Y/Z는 무시)
        x_raw = row[23] if len(row) > 23 else ''
        manual_ratio, has_manual = _valid_manual_call_ratio(x_raw)
        result['_manual_call_ratio'] = manual_ratio
        result['_has_manual_call'] = has_manual
        results.append((sheet_row, result))
        await asyncio.sleep(1.5)

    # ============================================================
    # [v2.4 안전장치] 네트워크 전면 실패 감지 → 시트 쓰기 스킵
    #   SEIBRO/DART 응답이 사실상 0이면, 오염 방지 위해 시트를 건드리지 않음
    # ============================================================
    seibro_total = NET_STATS['seibro_ok'] + NET_STATS['seibro_fail']
    dart_total = NET_STATS['dart_ok'] + NET_STATS['dart_fail']
    seibro_rate = (NET_STATS['seibro_ok'] / seibro_total) if seibro_total else 0
    dart_rate = (NET_STATS['dart_ok'] / dart_total) if dart_total else 0
    print(f"\n📡 네트워크 상태: SEIBRO {NET_STATS['seibro_ok']}/{seibro_total} "
          f"({seibro_rate:.0%}), DART {NET_STATS['dart_ok']}/{dart_total} ({dart_rate:.0%})")

    have_corp = bool(DART_CORP_DICT)
    if seibro_rate == 0 and (dart_rate == 0 or not have_corp):
        print("❌ [안전중단] SEIBRO/DART 응답률 0% — 네트워크 전면 실패로 판단.")
        print("   시트 쓰기를 건너뜁니다 (기존 정상 데이터 보호).")
        print("   원인: GitHub Actions 러너 IP 차단 가능성 높음.")
        print("   → 로컬/국내서버 실행 또는 self-hosted runner 권장.")
        return

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
    # v2.5: X열 수동 CALL 비율이 유효하면 원문 파싱 비율을 대체 (R/S 날짜는 원문 유지)
    #   수동값이 적용된 종목은 fallback(노란색) 대상에서도 제외
    manual_applied = []
    for sr, r in results:
        if r.get('_has_manual_call'):
            mr = r['_manual_call_ratio']
            orig = r['call']['call_ratio']
            r['call']['call_ratio'] = mr
            r['call']['fallback'] = False  # 사람이 확정한 값이므로 경고 해제
            manual_applied.append((sr, orig, mr))

    upd(f"Q{fr}:S{lr}", [[r['call']['call_ratio'], r['call']['call_begin'], r['call']['call_end']] for _, r in results])
    await asyncio.sleep(1.0)

    if manual_applied:
        print(f"\n✍️  X열 수동 CALL 비율 적용 {len(manual_applied)}개 (원문값 대체):")
        for sr, orig, mr in manual_applied:
            print(f"   행 {sr}: 원문 {orig or '-'} → 수동 {mr}%")

    # CALL 비율 시각화: fallback(원문에 % 없이 추정)만 노란색
    fallback_rows = [sr for sr, r in results if r['call'].get('fallback')]
    try:
        worksheet.format(f'Q{fr}:Q{lr}', {'backgroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0}})
        await asyncio.sleep(0.5)
    except Exception:
        pass
    if fallback_rows:
        print(f"\n⚠️ Fallback CALL 비율 종목 {len(fallback_rows)}개 (Q열 노란색, 수동 확인 필요):")
        for sr in fallback_rows:
            try:
                worksheet.format(f'Q{sr}', {'backgroundColor': {'red': 1.0, 'green': 0.95, 'blue': 0.5}})
                print(f"   행 {sr}")
            except Exception:
                pass
        await asyncio.sleep(0.5)

    upd(f"T{fr}:T{lr}", [[r['ytc']] for _, r in results])
    await asyncio.sleep(1.0)
    upd(f"U{fr}:W{lr}", [[r['issuer_stock_code'], r['target_corp_name'], r['target_stock_code']] for _, r in results])
    await asyncio.sleep(1.0)

    # v2.5: 최근업데이트(AA열)를 오늘 날짜로 갱신 (main.py 실행 시각 기록)
    #   update_holdings.py도 같은 AA열을 갱신하므로, 둘 중 나중에 실행된 쪽 날짜가 남는다.
    upd(f"AA{fr}:AA{lr}", [[TODAY] for _ in results])
    await asyncio.sleep(1.0)

    print("\n🔍 자본변동 감지 중...")
    holdings_for_ca = [(r['corp_name'], r['issuer_corp_code'], r['isin']) for _, r in results]
    capital_actions = detect_all_capital_actions(holdings_for_ca, days_back=30)
    if capital_actions:
        update_capital_action_sheet(capital_actions)

    print("\n📅 풋콜스케줄 시트 빌드 중...")
    all_schedule_rows = []
    for _, r in results:
        stock_actions = capital_actions.get(r['corp_name'], [])
        all_schedule_rows.extend(build_schedule_rows(r, stock_actions))
    print(f"  → 총 {len(all_schedule_rows)}개 이벤트 행 생성")
    update_schedule_sheet(all_schedule_rows)

    ytm_c = sum(1 for _, r in results if r['ytm'])
    rfxg_c = sum(1 for _, r in results if r['rfxg_floor'])
    put_c = sum(1 for _, r in results if r['put']['put_begin'] not in ('', '-'))
    call_c = sum(1 for _, r in results if r['call']['call_ratio'] not in ('', '-'))
    ytc_c = sum(1 for _, r in results if r['ytc'])
    issuer_c = sum(1 for _, r in results if r['issuer_stock_code'])
    target_c = sum(1 for _, r in results if r['target_corp_name'])
    refix_c = sum(1 for _, r in results if r.get('refix_cycle_months'))
    from collections import Counter
    event_counts = Counter(row[2] for row in all_schedule_rows)
    ca_total = sum(len(a) for a in capital_actions.values())

    print(f"\n🏁 완료! {len(results)}개 종목 업데이트됨")
    print(f"👉 https://docs.google.com/spreadsheets/d/{SHEET_ID}")
    print(f"  ✅ H열 YTM ({ytm_c}), J열 리픽싱({rfxg_c}), M~O PUT({put_c}), "
          f"Q~S CALL({call_c}), T YTC({ytc_c})")
    print(f"  ✅ U 발행사코드({issuer_c}), V 교환대상({target_c})")
    print(f"\n📅 풋콜스케줄: {len(all_schedule_rows)}개 이벤트")
    for evt, cnt in event_counts.most_common():
        print(f"     - {evt}: {cnt}건")
    print(f"  ✅ 리픽싱 자동계산: {refix_c}/{len(results)}개")
    print(f"\n🔔 자본변동 감지: {ca_total}건 ({len(capital_actions)}개 종목)")

    await update_stock_code_sheet(data_rows, results)


async def update_stock_code_sheet(data_rows, results):
    print("\n📊 주식코드 시트 업데이트 중...")
    try:
        ws_stock = sh.worksheet(SHEET_STOCK_CODE)
    except Exception:
        ws_stock = sh.add_worksheet(title=SHEET_STOCK_CODE, rows=200, cols=6)
        print(f"  ✅ '{SHEET_STOCK_CODE}' 시트 새로 생성")
    headers = ['종목명', '채권ISIN', '종류', '발행사 주식코드', '교환대상 주식코드']
    ws_stock.update([headers], range_name='A1:E1')
    ws_stock.format('A1:E1', {
        'textFormat': {'bold': True},
        'backgroundColor': {'red': 0.2, 'green': 0.5, 'blue': 0.3},
        'horizontalAlignment': 'CENTER'})
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


if __name__ == "__main__":
    asyncio.run(main())
