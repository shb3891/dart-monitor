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
SEIBRO_KEY  = os.environ.get('SEIBRO_KEY', 'e1e03a31bc0583fc0c853d4c41a0dc018dc4d2aa21c363c3d6b1b0b96e85221b')
DART_KEY    = (
    os.environ.get('DART_API_KEY') or
    os.environ.get('DART_KEY') or
    'bfc4e4e445de4727ae0bcc27e80ba5cf0e3818e6'
)
SHEET_ID    = os.environ.get('SHEET_ID', '1s73BDNtCPe5mOs9EjBE5npEfcaNtYRyWJxRBUmJI-WA')
DART_BASE   = "https://opendart.fss.or.kr/api"
SEIBRO_BASE = "https://seibro.or.kr/OpenPlatform/callOpenAPI.jsp"

TARGET_YEARS = [2025]
TEST_MODE    = False
TEST_COUNT   = 10

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
ws_master = sh.get_worksheet(0)

AUDIT_SHEET_NAME = '감사보고서'

# ============================================================
# [DART 기업코드 딕셔너리] stock_code_6 → corp_code
# ============================================================
DART_CORP_DICT = {}

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
# [SEIBRO: 채권 ISIN → xrc_stk_isin]
# main.py와 동일 방식. 결과를 캐시해서 재사용.
# ============================================================
_xrc_cache = {}   # bond_isin → xrc_stk_isin

def get_xrc_stk_isin_cached(bond_isin):
    if bond_isin in _xrc_cache:
        return _xrc_cache[bond_isin]
    val = ''
    try:
        params_str = f"BOND_ISIN:{bond_isin}"
        url = f"{SEIBRO_BASE}?key={SEIBRO_KEY}&apiId=getXrcStkStatInfo&params={params_str}"
        r = requests.get(url, timeout=10)
        for enc in ['utf-8', 'euc-kr']:
            try:
                decoded = r.content.decode(enc, errors='strict')
                break
            except UnicodeDecodeError:
                continue
        else:
            decoded = r.content.decode('utf-8', errors='replace')
        cleaned = re.sub(r'<\?xml[^?]*\?>', '', decoded).strip()
        if cleaned:
            root = ET.fromstring(cleaned.encode('utf-8'))
            vector = root.find('.//vector')
            if vector is not None and vector.get('result', '0') != '0':
                el = root.find('.//result')
                if el is not None:
                    xrc = el.find('.//XRC_STK_ISIN')
                    if xrc is not None:
                        val = xrc.get('value', '')
    except Exception:
        pass
    _xrc_cache[bond_isin] = val
    return val

# ============================================================
# [corp_code 조회: SEIBRO → DART_CORP_DICT (main.py 동일 방식)]
# ============================================================
def get_corp_code_for_audit(bond_isin):
    """
    채권 ISIN → corp_code
    SEIBRO로 xrc_stk_isin 조회 → stock_code_6 → DART_CORP_DICT
    EB의 경우 교환 대상 주식 발행사가 아닌 사채 발행사가 필요하므로
    stock_code 매칭 실패 시 DART_CORP_DICT 전체에서 ISIN 앞 6자리로도 시도
    """
    xrc = get_xrc_stk_isin_cached(bond_isin)
    if xrc and len(xrc) >= 9:
        stock_code_6 = xrc[3:9]
        corp_code = DART_CORP_DICT.get(stock_code_6, '')
        if corp_code:
            return corp_code

    # Fallback: 채권 ISIN 자체의 4~9번째 자리로 시도 (일부 CB/BW 케이스)
    candidate = bond_isin[3:9] if len(bond_isin) >= 9 else ''
    if candidate:
        corp_code = DART_CORP_DICT.get(candidate, '')
        if corp_code:
            return corp_code

    return ''

# ============================================================
# [감사보고서 시트 준비]
# ============================================================
def get_or_create_audit_sheet():
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
    ws.update([[f"최종 업데이트: {datetime.now().strftime('%Y-%m-%d %H:%M')}"]], range_name='K1')
    time.sleep(1.0)

# ============================================================
# [감사보고서 공시 검색]
# ============================================================
def search_audit_report(corp_code, biz_year):
    bgn_de = f"{biz_year + 1}0101"
    end_de = f"{biz_year + 1}0630"
    audit_kws = ['감사보고서', '내부회계관리제도']

    for pblntf_ty in ['A', None]:
        try:
            params = {
                'crtfc_key': DART_KEY, 'corp_code': corp_code,
                'bgn_de': bgn_de, 'end_de': end_de, 'page_count': 40,
            }
            if pblntf_ty:
                params['pblntf_ty'] = pblntf_ty
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
            print(f"    ⚠ 공시검색 오류: {e}")
    return None, None, None

# ============================================================
# [감사의견 + 감사인 파싱]
# ============================================================
def parse_audit_opinion(rcept_no):
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

            for pattern in [
                r'감사의견\s*[:：]\s*(적정|한정|부적정|의견\s*거절)',
                r'(적정|한정|부적정|의견거절)\s*의견',
                r'감사인의\s*의견\s*[:：]?\s*(적정|한정|부적정|의견\s*거절)',
                r'우리의\s*의견으로는[^.。]{0,80}(적정|한정|부적정|의견거절)',
            ]:
                m = re.search(pattern, clean)
                if m:
                    opinion = m.group(1).replace(' ', '')
                    break
            if opinion == '파악불가':
                for kw in ['부적정', '의견거절', '한정', '적정']:
                    if kw in clean:
                        opinion = kw
                        break

            m = re.search(r'([가-힣]+\s*회계법인)', clean)
            if m:
                auditor = m.group(0).strip()

            if opinion != '파악불가' or auditor:
                break
    except Exception as e:
        print(f"    ⚠ 문서 파싱 실패: {e}")
    return opinion, auditor

# ============================================================
# [메인: 보유 종목 감사보고서 체크]
# ============================================================
def check_audit_reports():
    print("📋 시트1 읽는 중...")
    all_values = ws_master.get_all_values()

    holdings = [
        {'name': row[0].strip(), 'isin': row[1].strip(), 'bond_type': row[3].strip()}
        for row in all_values[1:]
        if len(row) > 1 and row[1].strip().startswith('KR')
           and row[0].strip() not in ('-', '')
    ]

    # ── 중복 제거: corp_code 기준 ──
    # SEIBRO는 여기서 1번만 호출하고 캐시에 저장 → 본 처리에서 재사용
    print("  🔄 corp_code 사전 조회 중 (SEIBRO + DART_CORP_DICT)...")
    seen_corp_codes = set()
    deduped = []
    for h in holdings:
        corp_code = get_corp_code_for_audit(h['isin'])
        dedup_key = corp_code if corp_code else f"unknown_{h['isin']}"
        if dedup_key not in seen_corp_codes:
            seen_corp_codes.add(dedup_key)
            h['_corp_code'] = corp_code
            deduped.append(h)
        else:
            print(f"  ⏭ 중복 법인 스킵: {h['name']} ({h['isin']})")
        time.sleep(0.8)   # SEIBRO 과호출 방지 (충분한 간격)
    holdings = deduped

    if TEST_MODE:
        holdings = holdings[:TEST_COUNT]
        print(f"🧪 테스트 모드: {TEST_COUNT}개\n")
    else:
        print(f"🚀 전체 {len(holdings)}개 종목 확인\n")

    today = datetime.now()
    deadline = datetime(2026, 3, 31)
    before_deadline = today <= deadline

    results = []
    for h in holdings:
        name      = h['name']
        isin      = h['isin']
        bond_type = h['bond_type']
        corp_code = h.get('_corp_code', '')

        print(f"  🔍 {name} ({isin})", end=' ')

        if not corp_code:
            print(f"→ ⚠ corp_code 없음")
            results.append({
                'name': name, 'isin': isin, 'bond_type': bond_type,
                'biz_year': TARGET_YEARS[0],
                'submitted': '확인불가', 'opinion': '-',
                'rcept_dt': '-', 'auditor': '-', 'link': '-',
                'note': 'corp_code 조회 실패',
            })
            continue

        print(f"→ corp_code: {corp_code}")

        for biz_year in TARGET_YEARS:
            rcept_no, rpt_nm, rcept_dt = search_audit_report(corp_code, biz_year)
            if rcept_no:
                fmt_dt = f"{rcept_dt[:4]}-{rcept_dt[4:6]}-{rcept_dt[6:]}" if len(rcept_dt) == 8 else rcept_dt
                print(f"    📄 {rpt_nm} ({fmt_dt})")
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
                icon = '⏳' if before_deadline else '❌'
                print(f"    {icon} 감사보고서 미제출 ({biz_year}년)")
                results.append({
                    'name': name, 'isin': isin, 'bond_type': bond_type,
                    'biz_year': biz_year,
                    'submitted': '미제출', 'opinion': '-',
                    'rcept_dt': '-', 'auditor': '-', 'link': '-', 'note': note,
                })
        time.sleep(1.2)

    return results

# ============================================================
# [시트 기록]
# ============================================================
def write_results_to_sheet(ws, results):
    if not results:
        print("  ⚠ 기록할 데이터 없음")
        return

    rows = [[
        r['name'], r['isin'], r['bond_type'], str(r['biz_year']),
        r['submitted'], r['opinion'], r['rcept_dt'], r['auditor'],
        r['link'], r.get('note', ''),
    ] for r in results]

    write_header(ws)
    time.sleep(2.0)

    ws.batch_clear(['A2:J300'])
    time.sleep(1.5)

    ws.update(rows, range_name=f'A2:J{len(rows)+1}')
    time.sleep(2.0)

    # 색상 batch 처리
    color_map = {
        '적정':     {'red': 0.85, 'green': 0.95, 'blue': 0.85},
        '한정':     {'red': 1.0,  'green': 0.95, 'blue': 0.7},
        '부적정':   {'red': 1.0,  'green': 0.8,  'blue': 0.8},
        '의견거절': {'red': 1.0,  'green': 0.8,  'blue': 0.8},
        '미제출':   {'red': 0.97, 'green': 0.97, 'blue': 0.97},
        '확인불가': {'red': 0.95, 'green': 0.9,  'blue': 1.0},
    }
    from collections import defaultdict
    color_rows = defaultdict(list)
    for i, r in enumerate(results, start=2):
        key = r['opinion'] if r['opinion'] in color_map else r['submitted']
        if key in color_map:
            color_rows[key].append(i)

    formats = [
        {'range': f'A{idx}:J{idx}', 'format': {'backgroundColor': color_map[key]}}
        for key, idxs in color_rows.items()
        for idx in idxs
    ]
    if formats:
        ws.batch_format(formats)
        time.sleep(1.0)

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
    print(f"  제출 확인: {submitted}개 / 미제출·미확인: {total - submitted}개")
    print(f"\n  감사의견 분포:")
    icons = {'적정': '✅', '한정': '⚠️', '부적정': '❌', '의견거절': '❌', '-': '⏳', '파악불가': '❓'}
    for op, cnt in sorted(opinions.items()):
        print(f"    {icons.get(op,'  ')} {op}: {cnt}개")

    warn = [r for r in results if r['opinion'] in ('한정', '부적정', '의견거절')
            or (r['submitted'] == '미제출' and '마감 후' in r.get('note', ''))]
    if warn:
        print(f"\n  ⚠ 주의 필요 종목 ({len(warn)}개):")
        for r in warn:
            print(f"    - {r['name']} ({r['isin']}): {r['submitted']} / {r['opinion']}")
    else:
        print(f"\n  🎉 현재까지 이상 없음!")

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
