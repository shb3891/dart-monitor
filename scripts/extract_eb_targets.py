"""
EB 교환대상 자동 추출 스크립트 (GitHub Actions 실행용)

작업:
1. 27개 EB 종목에 대해 SEIBRO getXrcStkStatInfo API 호출
2. 각 EB의 교환대상 주식 ISIN과 종목명 추출
3. 주식 ISIN → 주식코드 변환
4. DART corpCode.xml에서 주식코드 → corp_name 매칭
5. 결과를 스프레드시트 'EB교환대상' 시트에 저장 + 콘솔 출력

사용법:
- dart-monitor 레포에 이 파일 추가 (scripts/extract_eb_targets.py)
- GitHub Actions에서 수동 실행 (workflow_dispatch)
- 결과는 Actions 로그와 스프레드시트 둘 다에서 확인 가능
"""

import os
import json
import re
import zipfile
import io
import xml.etree.ElementTree as ET
import requests
import time
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# [설정]
# ============================================================
SEIBRO_KEY = os.environ.get('SEIBRO_KEY', 'e1e03a31bc0583fc0c853d4c41a0dc018dc4d2aa21c363c3d6b1b0b96e85221b')
DART_KEY   = os.environ.get('DART_API_KEY', 'bfc4e4e445de4727ae0bcc27e80ba5cf0e3818e6')
SHEET_ID   = os.environ.get('SHEET_ID',   '1s73BDNtCPe5mOs9EjBE5npEfcaNtYRyWJxRBUmJI-WA')

SEIBRO_BASE = "https://seibro.or.kr/OpenPlatform/callOpenAPI.jsp"
DART_BASE   = "https://opendart.fss.or.kr/api"


# ============================================================
# [EB 27개 목록]
# ============================================================
EB_LIST = [
    ('KR6001081G15', '만호제강1EB'),
    ('KR6004271FC8', '남성33EB'),
    ('KR6004911FC9', '조광페인트6EB'),
    ('KR6007591F81', '동방아그로23EB'),
    ('KR6008771E74', '호텔신라75EB'),
    ('KR6012161FC1', '영흥4EB'),
    ('KR6012802FC0', '대창7EB'),
    ('KR6014831E62', '유니드1EB'),
    ('KR60156S1G12', '다솔1EB'),
    ('KR6034811FC5', '해성산업2EB'),
    ('KR6039831FB0', '오로라월드1EB'),
    ('KR6049071FA3', '인탑스1EB'),
    ('KR6060721F91', '케이에이치바텍4EB'),
    ('KR6070961F65', '모나용평1EB'),
    ('KR6079371FC6', '제우스3EB'),
    ('KR6102121E80', '어보브반도체2EB'),
    ('KR6102122E89', '어보브반도체3EB'),
    ('KR6137401FC1', '피엔티6EB'),
    ('KR6151861F64', '케이지에코솔루션3EB'),
    ('KR6251271FB0', '넷마블3EB'),
    ('KR6255441FC3', '야스1EB'),
    ('KR6272291FC1', '이녹스첨단소재3EB'),
    ('KR6285131FA8', 'SK케미칼1EB'),
    ('KR6332571G52', 'PS일렉트로닉스5EB'),
    ('KR6332575G58', 'PS일렉트로닉스9EB'),
    ('KR6362321FC7', '청담글로벌10EB'),
    ('KR6397521FA5', '탑코2EB'),
]


# ============================================================
# [DART 기업코드 로드]
# ============================================================
DART_CORP_DICT = {}
DART_CORP_NAME_DICT = {}

def load_dart_corp_codes():
    print("📥 DART 기업코드 다운로드 중...")
    r = requests.get(f"{DART_BASE}/corpCode.xml", params={'crtfc_key': DART_KEY}, timeout=30)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    root = ET.fromstring(z.read('CORPCODE.xml'))
    for item in root.findall('.//list'):
        corp_code = item.findtext('corp_code', '').strip()
        stock_code = item.findtext('stock_code', '').strip()
        corp_name = item.findtext('corp_name', '').strip()
        if stock_code and len(stock_code) == 6:
            DART_CORP_DICT[stock_code] = corp_code
            DART_CORP_NAME_DICT[stock_code] = corp_name
    print(f"   ✅ 상장사 {len(DART_CORP_DICT):,}개\n")


# ============================================================
# [SEIBRO API 호출]
# ============================================================
def seibro_api(api_id, params_dict):
    params_str = ','.join([f"{k}:{v}" for k, v in params_dict.items()])
    full_url = f"{SEIBRO_BASE}?key={SEIBRO_KEY}&apiId={api_id}&params={params_str}"
    try:
        r = requests.get(full_url, timeout=10)
        r.raise_for_status()
        for enc in ['utf-8', 'euc-kr']:
            try:
                decoded = r.content.decode(enc, errors='strict')
                break
            except UnicodeDecodeError:
                continue
        else:
            decoded = r.content.decode('utf-8', errors='replace')
        cleaned = re.sub(r'<\?xml[^?]*\?>', '', decoded).strip()
        if not cleaned:
            return None
        root = ET.fromstring(cleaned.encode('utf-8'))
        vector = root.find('.//vector')
        if vector is None or vector.get('result', '0') == '0':
            return None
        return root
    except Exception as e:
        print(f"  ⚠ SEIBRO 호출 실패: {e}")
        return None


def get_attr(element, tag):
    el = element.find(f'.//{tag}')
    if el is not None:
        return el.get('value', '')
    return ''


def stock_isin_to_code(stock_isin):
    """주식 ISIN(KR7...) → 6자리 주식코드"""
    if not stock_isin or len(stock_isin) < 9:
        return ''
    code = stock_isin[3:9]
    return code if code.isdigit() else ''


# ============================================================
# [EB 1개 처리]
# ============================================================
def extract_eb_target(isin, bond_name):
    """EB 1개에서 교환대상 정보 추출"""
    print(f"\n🔍 {isin}  {bond_name}")
    
    root = seibro_api('getXrcStkStatInfo', {'BOND_ISIN': isin})
    
    result = {
        'isin': isin,
        'bond_name': bond_name,
        'xrc_stk_isin': '',
        'xrc_stk_name': '',
        'xrc_stk_code': '',
        'dart_corp_name': '',
        'status': '',
        'note': '',
    }
    
    if root is None:
        result['status'] = '❌ SEIBRO 실패'
        result['note'] = 'getXrcStkStatInfo 조회 결과 없음 (비상장 발행사 가능)'
        print(f"   ❌ SEIBRO 조회 실패")
        return result
    
    el = root.find('.//result')
    if el is None:
        result['status'] = '❌ 결과 없음'
        result['note'] = 'API는 응답했으나 result 노드 없음'
        return result
    
    xrc_stk_isin = get_attr(el, 'XRC_STK_ISIN')
    xrc_stk_name = get_attr(el, 'STK_SECN_NM')
    
    result['xrc_stk_isin'] = xrc_stk_isin
    result['xrc_stk_name'] = xrc_stk_name
    
    if not xrc_stk_isin:
        result['status'] = '⚠️ 교환대상 ISIN 없음'
        return result
    
    # 주식 ISIN → 주식코드
    stock_code = stock_isin_to_code(xrc_stk_isin)
    result['xrc_stk_code'] = stock_code
    
    # DART corp_name 매칭
    if stock_code:
        dart_name = DART_CORP_NAME_DICT.get(stock_code, '')
        result['dart_corp_name'] = dart_name
        
        if dart_name:
            if dart_name == xrc_stk_name or dart_name in xrc_stk_name or xrc_stk_name in dart_name:
                result['status'] = '✅ 완전매칭'
            else:
                result['status'] = '⚠️ 이름 차이'
                result['note'] = f'SEIBRO({xrc_stk_name}) vs DART({dart_name})'
        else:
            result['status'] = '⚠️ DART 매칭 실패'
            result['note'] = f'주식코드({stock_code})는 추출했으나 DART 상장사에 없음'
    else:
        result['status'] = '❌ 주식코드 변환 실패'
    
    print(f"   교환대상: {xrc_stk_name} (ISIN: {xrc_stk_isin})")
    print(f"   주식코드: {stock_code}")
    print(f"   DART명:   {result['dart_corp_name']}")
    print(f"   상태:     {result['status']}")
    
    return result


# ============================================================
# [Google Sheets 저장]
# ============================================================
def save_to_sheet(results):
    print("\n📝 스프레드시트 저장 중...")
    
    creds_json = json.loads(os.environ.get('GCP_SERVICE_ACCOUNT_KEY'))
    creds = Credentials.from_service_account_info(creds_json, scopes=[
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ])
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)
    
    SHEET_NAME = 'EB교환대상'
    try:
        ws = sh.worksheet(SHEET_NAME)
        ws.clear()
        print(f"  ♻️ '{SHEET_NAME}' 시트 초기화")
    except Exception:
        ws = sh.add_worksheet(title=SHEET_NAME, rows=50, cols=10)
        print(f"  ✅ '{SHEET_NAME}' 시트 생성")
    
    headers = ['채권ISIN', '채권명', '교환대상_주식ISIN', '교환대상_종목명(SEIBRO)',
               '교환대상_주식코드', '교환대상_종목명(DART)', '매칭상태', '비고']
    ws.update([headers], range_name='A1:H1')
    ws.format('A1:H1', {
        'textFormat': {'bold': True},
        'backgroundColor': {'red': 0.12, 'green': 0.3, 'blue': 0.47},
        'horizontalAlignment': 'CENTER',
    })
    
    rows = []
    for r in results:
        rows.append([
            r['isin'], r['bond_name'],
            r['xrc_stk_isin'], r['xrc_stk_name'],
            r['xrc_stk_code'], r['dart_corp_name'],
            r['status'], r['note'],
        ])
    
    if rows:
        ws.update(rows, range_name=f'A2:H{len(rows)+1}')
    
    print(f"  ✅ {len(rows)}개 저장 완료")
    print(f"  👉 https://docs.google.com/spreadsheets/d/{SHEET_ID}")


# ============================================================
# [메인]
# ============================================================
def main():
    print(f"🚀 EB 교환대상 추출 시작 ({len(EB_LIST)}개 종목)")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    load_dart_corp_codes()
    
    results = []
    for isin, bond_name in EB_LIST:
        result = extract_eb_target(isin, bond_name)
        results.append(result)
        time.sleep(1.0)  # SEIBRO rate limit
    
    # 통계
    print("\n" + "="*70)
    print("📊 결과 통계")
    print("="*70)
    
    status_counts = {}
    for r in results:
        s = r['status']
        status_counts[s] = status_counts.get(s, 0) + 1
    
    for status, count in sorted(status_counts.items(), key=lambda x: -x[1]):
        print(f"  {status}: {count}개")
    
    # 매칭 실패 종목 상세
    print("\n⚠️ 수동확인 필요 종목:")
    for r in results:
        if '✅' not in r['status']:
            print(f"  - {r['isin']} {r['bond_name']}: {r['status']}")
            if r['note']:
                print(f"    └ {r['note']}")
    
    # 스프레드시트 저장
    save_to_sheet(results)
    
    print("\n🏁 완료!")


if __name__ == '__main__':
    main()
