import os
import json
import re
import time
import zipfile
import io
import requests
import xml.etree.ElementTree as ET
import gspread
from datetime import datetime
from google.oauth2.service_account import Credentials

# ============================================================
# [설정]
# ============================================================
DART_KEY = (
    os.environ.get('DART_API_KEY') or
    os.environ.get('DART_KEY') or
    'bfc4e4e445de4727ae0bcc27e80ba5cf0e3818e6'
)
SHEET_ID = os.environ.get('SHEET_ID', '1s73BDNtCPe5mOs9EjBE5npEfcaNtYRyWJxRBUmJI-WA')
DART_BASE = "https://opendart.fss.or.kr/api"

# ── 확인할 사업연도 ──────────────────────────────────────────
# 2025 사업연도: 12월 결산법인 기준 제출 마감 2026년 3월말
# 지금(2026년 3월) 한창 제출 중 → 미제출도 정상일 수 있음
TARGET_YEARS = [2025]

# ── 테스트 모드 ──────────────────────────────────────────────
TEST_MODE  = False   # True: 상위 10개만, False: 전체
TEST_COUNT = 10

# ============================================================
# [Google Sheets 연결]
# ============================================================
creds_json = json.loads(os.environ.get('GCP_SERVICE_ACCOUNT_KEY'))
scopes = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
]
creds     = Credentials.from_service_account_info(creds_json, scopes=scopes)
gc        = gspread.authorize(creds)
sh        = gc.open_by_key(SHEET_ID)
ws_master = sh.get_worksheet(0)   # 시트1 (마스터)

# ============================================================
# [감사보고서 시트 준비]
# ============================================================
AUDIT_SHEET_NAME = '감사보고서'

def get_or_create_audit_sheet():
    """감사보고서 시트가 없으면 생성, 있으면 기존 시트 반환"""
    try:
        ws = sh.worksheet(AUDIT_SHEET_NAME)
        print(f"  📋 기존 '{AUDIT_SHEET_NAME}' 시트 사용")
        return ws
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=AUDIT_SHEET_NAME, rows=200, cols=11)
        print(f"  ✅ '{AUDIT_SHEET_NAME}' 시트 새로 생성")
        return ws

def write_header(ws):
    headers = [
        '종목명', '예탁원 종목코드', '종류', '사업연도',
        '감사보고서 제출', '감사의견', '제출일자', '감사인', 'DART 링크', '비고',
    ]
    ws.update([headers], range_name='A1:J1')
    ws.format('A1:J1', {
        'textFormat': {'bold': True},
        'backgroundColor': {'red': 0.2, 'green': 0.4, 'blue': 0.8},
        'horizontalAlignment': 'CENTER',
    })
    # 마지막 업데이트 시각 기록 (K1)
    ws.update([[f"최종 업데이트: {datetime.now().strftime('%Y-%m-%d %H:%M')}"]], range_name='K1')
    time.sleep(0.5)

# ============================================================
# [DART 기업코드 로드]
# ============================================================
DART_CORP_DICT = {}   # stock_code_6 → corp_code

def load_dart_corp_codes():
    global DART_CORP_DICT
    try:
        print("  📥 DART 기업코드 다운로드 중...")
        r = requests.get(f"{DART_BASE}/corpCode.xml", params={'crtfc_key': DART_KEY}, timeout=30)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        root = ET.fromstring(z.read('CORPCODE.xml'))
        for item in root.findall('.//list'):
            corp_code  = item.findtext('corp_code', '').strip()
            stock_code = item.findtext('stock_code', '').strip()
            if stock_code and len(stock_code) == 6:
                DART_CORP_DICT[stock_code] = corp_code
        print(f"  ✅ {len(DART_CORP_DICT):,}개 상장사 로드")
    except Exception as e:
        print(f"  ⚠ 기업코드 로드 실패: {e}")

# ============================================================
# [감사보고서 공시 검색]
# ============================================================
def search_audit_report(corp_code, biz_year):
    """
    DART에서 해당 법인의 감사보고서 공시 검색.
    2025 사업연도: 12월 결산법인 기준 2026-01-01 ~ 2026-06-30 검색
    """
    bgn_de = f"{biz_year + 1}0101"
    end_de = f"{biz_year + 1}0630"

    audit_kws = ['감사보고서', '내부회계관리제도']

    # ── 1차: pblntf_ty='A'(정기공시) 전체 검색 ──
    try:
        params = {
            'crtfc_key': DART_KEY, 'corp_code': corp_code,
            'bgn_de': bgn_de, 'end_de': end_de,
            'page_count': 40, 'pblntf_ty': 'A',
        }
        r = requests.get(f"{DART_BASE}/list.json", params=params, timeout=10)
        data = r.json()
        if data.get('status') in ('000', '013'):
            for item in (data.get('list') or []):
                rpt = item.get('report_nm', '')
                if '[첨부정정]' in rpt or '[첨부추가]' in rpt:
                    continue
                if any(kw in rpt for kw in audit_kws):
                    return item.get('rcept_no'), rpt, item.get('rcept_dt', '')
    except Exception as e:
        print(f"    ⚠ A타입 검색 오류: {e}")

    # ── 2차: 타입 무관 전체 검색 ──
    try:
        params = {
            'crtfc_key': DART_KEY, 'corp_code': corp_code,
            'bgn_de': bgn_de, 'end_de': end_de, 'page_count': 40,
        }
        r = requests.get(f"{DART_BASE}/list.json", params=params, timeout=10)
        data = r.json()
        if data.get('status') in ('000', '013'):
            for item in (data.get('list') or []):
                rpt = item.get('report_nm', '')
                if '[첨부정정]' in rpt or '[첨부추가]' in rpt:
                    continue
                if '감사보고서' in rpt:
                    return item.get('rcept_no'), rpt, item.get('rcept_dt', '')
    except Exception as e:
        print(f"    ⚠ 전체타입 검색 오류: {e}")

    return None, None, None

# ============================================================
# [감사보고서 파싱: 감사의견 + 감사인]
# ============================================================
def parse_audit_opinion(rcept_no):
    """
    DART document.xml에서 감사의견 텍스트 추출.
    반환: (opinion, auditor)
      opinion: '적정' | '한정' | '부적정' | '의견거절' | '파악불가'
      auditor: 감사인 회계법인명
    """
    opinion = '파악불가'
    auditor = ''
    try:
        r = requests.get(
            f"{DART_BASE}/document.xml",
            params={'crtfc_key': DART_KEY, 'rcept_no': rcept_no},
            timeout=30,
        )
        if r.status_code != 200:
            return opinion, auditor
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
            clean = re.sub(r'<[^>]+>', ' ', text)
            clean = re.sub(r'&nbsp;|&amp;|&lt;|&gt;', ' ', clean)
            clean = re.sub(r'\s+', ' ', clean)

            # ── 감사의견 추출 ──
            # 우선순위: 명시적 문구 → 키워드 검색
            opinion_found = False
            for pattern in [
                r'감사의견\s*[:：]\s*(적정|한정|부적정|의견\s*거절)',
                r'(적정|한정|부적정|의견거절)\s*의견',
                r'감사인의\s*의견\s*[:：]?\s*(적정|한정|부적정|의견\s*거절)',
                r'우리의\s*의견으로는[^.。]{0,80}(적정|한정|부적정|의견거절)',
                r'내부회계관리제도에\s*대한\s*감사인의\s*의견\s*[:：]?\s*(적정)',
            ]:
                m = re.search(pattern, clean)
                if m:
                    raw_op = m.group(1).replace(' ', '')
                    opinion_map = {'적정': '적정', '한정': '한정', '부적정': '부적정', '의견거절': '의견거절'}
                    opinion = opinion_map.get(raw_op, raw_op)
                    opinion_found = True
                    break

            if not opinion_found:
                # 단어 등장 횟수로 판단
                for kw in ['부적정', '의견거절', '한정', '적정']:
                    if kw in clean:
                        opinion = kw
                        break

            # ── 감사인 추출 ──
            for aud_pat in [
                r'(삼일|삼정|한영|안진|대주|세일|신한|대성|한울|이촌|신우|진일|다산|율촌|천지인|가현|하나|도원|청아|삼화|광교|현대|백제|중앙|영화|한국|청솔|혜인|동인|정인|우리|광장|태성|삼덕|거산|광현|강남|삼도|삼성|안경|경일|새빛|정우|승인|고려|인덕|선일|선진|대일|서울|부산|대구|광주|경기|인천|수원|평택|천안|춘천|원주|청주|강릉|구미|포항|울산|창원|전주|군산|제주)\s*회계법인',
                r'([가-힣]+\s*회계법인)',
            ]:
                m = re.search(aud_pat, clean)
                if m:
                    auditor = m.group(0).strip()
                    break

            if opinion != '파악불가' or auditor:
                break   # 첫 번째 유효 파일에서 파싱 완료

    except Exception as e:
        print(f"    ⚠ 문서 파싱 실패: {e}")
    return opinion, auditor

# ============================================================
# [메인: 보유 종목 감사보고서 체크]
# ============================================================
def check_audit_reports():
    print("📋 시트1 읽는 중...")
    all_values = ws_master.get_all_values()

    # B열: ISIN, A열: 종목명, D열: 종류 (인덱스 0-based)
    holdings = [
        {'name': row[0].strip(), 'isin': row[1].strip(), 'bond_type': row[3].strip()}
        for row in all_values[1:]
        if len(row) > 1 and row[1].strip().startswith('KR')
           and row[0].strip() not in ('-', '')
    ]

    if TEST_MODE:
        holdings = holdings[:TEST_COUNT]
        print(f"🧪 테스트 모드: {TEST_COUNT}개\n")
    else:
        print(f"🚀 전체 {len(holdings)}개 종목 확인\n")

    # ── corp_code 매핑 (xrc_stk_isin 없이 ISIN에서 직접 시도) ──
    # 메자닌 채권 ISIN → 발행사 주식코드를 직접 알기 어려움
    # → SEIBRO getXrcStkStatInfo 없이 DART company.json으로 기업명 검색
    results = []
    # 오늘 날짜 기준 제출 마감 전인지 판단 (12월 결산 기준 3월 31일)
    today = datetime.now()
    deadline = datetime(today.year, 3, 31) if today.month <= 3 else datetime(today.year + 1, 3, 31)
    before_deadline = today <= deadline

    for h in holdings:
        name      = h['name']
        isin      = h['isin']
        bond_type = h['bond_type']
        print(f"  🔍 {name} ({isin})")

        for biz_year in TARGET_YEARS:
            # ── corp_code 획득: 기업명으로 DART 검색 ──
            corp_code = get_corp_code_by_name(name)
            if not corp_code:
                print(f"    ⚠ corp_code 없음 → 스킵")
                results.append({
                    'name': name, 'isin': isin, 'bond_type': bond_type,
                    'biz_year': biz_year,
                    'submitted': '확인불가', 'opinion': '-',
                    'rcept_dt': '-', 'auditor': '-', 'link': '-',
                })
                continue

            # ── 감사보고서 검색 ──
            rcept_no, rpt_nm, rcept_dt = search_audit_report(corp_code, biz_year)

            if rcept_no:
                fmt_dt = f"{rcept_dt[:4]}-{rcept_dt[4:6]}-{rcept_dt[6:]}" if len(rcept_dt) == 8 else rcept_dt
                print(f"    📄 발견: {rpt_nm} ({fmt_dt})")
                opinion, auditor = parse_audit_opinion(rcept_no)
                icon = {'적정': '✅', '한정': '⚠️', '부적정': '❌', '의견거절': '❌'}.get(opinion, '❓')
                print(f"    {icon} 감사의견: {opinion} | 감사인: {auditor}")
                results.append({
                    'name': name, 'isin': isin, 'bond_type': bond_type,
                    'biz_year': biz_year,
                    'submitted': '제출', 'opinion': opinion,
                    'rcept_dt': fmt_dt, 'auditor': auditor,
                    'link': f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}",
                    'note': '',
                })
            else:
                note = f"마감 전 미제출 ({deadline.strftime('%Y-%m-%d')} 마감)" if before_deadline else "마감 후 미제출"
                print(f"    {'⏳' if before_deadline else '❌'} 감사보고서 없음 → {note}")
                results.append({
                    'name': name, 'isin': isin, 'bond_type': bond_type,
                    'biz_year': biz_year,
                    'submitted': '미제출', 'opinion': '-',
                    'rcept_dt': '-', 'auditor': '-', 'link': '-',
                    'note': note,
                })
            time.sleep(1.2)

    return results


def get_corp_code_by_name(corp_name):
    """기업명으로 DART corp_code 검색. 상장사 우선."""
    # 1) 이미 로드된 딕셔너리에서 이름 매칭 시도 (불가 - 딕셔너리는 stock_code 기반)
    # 2) DART company.json API 사용
    try:
        r = requests.get(
            f"{DART_BASE}/company.json",
            params={'crtfc_key': DART_KEY, 'corp_name': corp_name},
            timeout=10,
        )
        data = r.json()
        if data.get('status') != '000':
            return ''
        items = data.get('list', [])
        # 상장사(stock_code 있는 것) 우선
        listed   = [x for x in items if x.get('stock_code', '').strip()]
        unlisted = [x for x in items if not x.get('stock_code', '').strip()]
        for item in (listed + unlisted):
            code = item.get('corp_code', '')
            if code:
                return code
    except Exception:
        pass
    return ''

# ============================================================
# [시트 기록]
# ============================================================
def write_results_to_sheet(ws, results):
    if not results:
        print("  ⚠ 기록할 데이터 없음")
        return

    rows = []
    for r in results:
        rows.append([
            r['name'], r['isin'], r['bond_type'], str(r['biz_year']),
            r['submitted'], r['opinion'], r['rcept_dt'], r['auditor'],
            r['link'], r.get('note', ''),
        ])

    write_header(ws)
    time.sleep(0.5)

    if rows:
        ws.update(rows, range_name=f'A2:J{len(rows)+1}')
        time.sleep(1.0)

    # ── 감사의견 컬럼 행 전체 색상 ──
    for i, r in enumerate(results, start=2):
        op = r['opinion']
        sub = r['submitted']
        if op == '적정':
            color = {'red': 0.85, 'green': 0.95, 'blue': 0.85}   # 연초록
        elif op == '한정':
            color = {'red': 1.0,  'green': 0.95, 'blue': 0.7}    # 연노랑
        elif op in ('부적정', '의견거절'):
            color = {'red': 1.0,  'green': 0.8,  'blue': 0.8}    # 연빨강
        elif sub == '미제출' and '마감 전' in r.get('note', ''):
            color = {'red': 0.97, 'green': 0.97, 'blue': 0.97}   # 연회색 (마감 전 정상)
        elif sub == '미제출':
            color = {'red': 1.0,  'green': 0.88, 'blue': 0.8}    # 연주황 (마감 후 미제출 = 주의)
        elif sub == '확인불가':
            color = {'red': 0.95, 'green': 0.9,  'blue': 1.0}    # 연보라
        else:
            continue
        ws.format(f'A{i}:J{i}', {'backgroundColor': color})
        time.sleep(0.15)

    print(f"\n  ✅ {len(rows)}행 기록 완료")

# ============================================================
# [요약 출력]
# ============================================================
def print_summary(results):
    total     = len(results)
    submitted = sum(1 for r in results if r['submitted'] == '제출')
    opinions  = {}
    for r in results:
        op = r['opinion']
        opinions[op] = opinions.get(op, 0) + 1

    print(f"\n{'='*50}")
    print(f"📊 감사보고서 확인 결과 ({TARGET_YEARS}년 사업연도)")
    print(f"{'='*50}")
    print(f"  전체 종목: {total}개")
    print(f"  제출 확인: {submitted}개 / 미확인: {total - submitted}개")
    print(f"\n  감사의견 분포:")
    for op, cnt in sorted(opinions.items()):
        icon = {'적정': '✅', '한정': '⚠️', '부적정': '❌', '의견거절': '❌', '-': '❓', '파악불가': '❓', '확인불가': '⬜', '미제출/미확인': '❌'}.get(op, '  ')
        print(f"    {icon} {op}: {cnt}개")

    # ── 주의 필요 종목 ──
    warn = [r for r in results if r['opinion'] in ('한정', '부적정', '의견거절', '미제출/미확인', '확인불가')]
    if warn:
        print(f"\n  ⚠ 주의 필요 종목 ({len(warn)}개):")
        for r in warn:
            print(f"    - {r['name']} ({r['isin']}): {r['submitted']} / {r['opinion']}")
    else:
        print(f"\n  🎉 전 종목 적정의견 확인!")

# ============================================================
# [실행]
# ============================================================
if __name__ == '__main__':
    print(f"🔑 DART_KEY: {DART_KEY[:6]}...")
    load_dart_corp_codes()

    ws_audit = get_or_create_audit_sheet()
    results  = check_audit_reports()

    print("\n📝 시트 기록 중...")
    write_results_to_sheet(ws_audit, results)
    print_summary(results)

    print(f"\n👉 https://docs.google.com/spreadsheets/d/{SHEET_ID}")
