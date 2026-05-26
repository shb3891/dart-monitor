"""
setup_matching_sheet.py — "주식코드매칭" + "별칭사전" 시트 초기 생성

실행: GitHub Actions에서 수동 1회
- "주식코드매칭" 시트: 헤더와 서식만 생성
- "별칭사전" 시트: 헤더 + 검증된 별칭 14개 초기 등록
"""

import os
import json
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials


SHEET_ID = os.environ.get('SHEET_ID', '1s73BDNtCPe5mOs9EjBE5npEfcaNtYRyWJxRBUmJI-WA')


# ============================================================
# [초기 등록 별칭]
# ============================================================
INITIAL_ALIASES = [
    # (예탁원 표기, DART 표기, 등록방법, 비고)
    ('케이씨그린홀딩스',       'KC그린홀딩스',       '수동',      'CB/BW 발행사'),
    ('케이비아이메탈',         'KBI메탈',            '수동',      'CB/BW 발행사'),
    ('케이에이치바텍',         'KH바텍',             '수동',      'EB 발행사'),
    ('케이지에코솔루션',       'KG에코솔루션',       '수동',      'EB 발행사'),
    ('오로라월드',             '오로라',             '수동',      'EB 발행사'),
    ('디와이피',               'DYP',                '수동',      'CB 발행사'),
    ('엘앤케이바이오메드',     '엘앤케이바이오',     '수동',      'CB 발행사'),
    ('에스씨엘사이언스',       'SCL사이언스',        '수동',      'CB 발행사'),
    ('티에스인베스트먼트',     'TS인베스트먼트',     '수동',      'CB 발행사'),
    ('넥스트엔터테인먼트월드', 'NEW',                '수동',      'CB 발행사'),
    ('에르코스 농업회사법인',  '에르코스',           '수동',      'CB 발행사 (다솔1EB 교환대상도 동일)'),
    ('에스케이바이오사이언스', 'SK바이오사이언스',   '수동',      'SK케미칼1EB 교환대상'),
    ('네온테크',               '지아이에스',         '수동',      'CB 발행사 (회사명 변경)'),
]


# ============================================================
# [Google Sheets 연결]
# ============================================================
creds_json = json.loads(os.environ.get('GCP_SERVICE_ACCOUNT_KEY'))
creds = Credentials.from_service_account_info(creds_json, scopes=[
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
])
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)


# ============================================================
# [주식코드매칭 시트 생성]
# ============================================================
def setup_matching_sheet():
    sheet_name = '주식코드매칭'
    
    try:
        ws = sh.worksheet(sheet_name)
        print(f"  ℹ '{sheet_name}' 시트 이미 존재 — 헤더만 갱신")
    except Exception:
        ws = sh.add_worksheet(title=sheet_name, rows=200, cols=15)
        print(f"  ✅ '{sheet_name}' 시트 새로 생성")
    
    headers = [
        '채권ISIN',           # A
        '채권명',             # B
        '종류',               # C
        '콜상태',             # D
        '발행사주식코드',     # E
        '공시대상종목명',     # F
        '공시대상주식코드',   # G
        'DARTcorp_code',      # H
        '매칭상태',           # I
        '매칭방법',           # J
        '최초등록일',         # K
        '최근검증일',         # L
        '메모',               # M
    ]
    
    ws.update([headers], range_name='A1:M1')
    ws.format('A1:M1', {
        'textFormat': {'bold': True, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}},
        'backgroundColor': {'red': 0.12, 'green': 0.3, 'blue': 0.47},
        'horizontalAlignment': 'CENTER',
        'verticalAlignment': 'MIDDLE',
    })
    
    # 컬럼 너비 조정
    ws.update_dimension_properties = None  # placeholder; gspread 직접 API 호출 필요
    
    # 헤더 행 고정
    ws.freeze(rows=1)
    
    print(f"     A: 채권ISIN")
    print(f"     B: 채권명")
    print(f"     C: 종류 (CB/EB/BW)")
    print(f"     D: 콜상태")
    print(f"     E: 발행사주식코드 (ISIN 자동변환)")
    print(f"     F: 공시대상종목명 ★ (봇 알람 키)")
    print(f"     G: 공시대상주식코드 ★ (DART 공시검색 키)")
    print(f"     H: DART corp_code")
    print(f"     I: 매칭상태 (AUTO/ALIAS/MANUAL/FAILED)")
    print(f"     J: 매칭방법")
    print(f"     K: 최초등록일")
    print(f"     L: 최근검증일")
    print(f"     M: 메모")


# ============================================================
# [별칭사전 시트 생성 + 초기 데이터]
# ============================================================
def setup_alias_sheet():
    sheet_name = '별칭사전'
    
    try:
        ws = sh.worksheet(sheet_name)
        print(f"  ℹ '{sheet_name}' 시트 이미 존재 — 헤더 갱신 후 데이터 추가")
        # 기존 데이터 백업용으로 그대로 두고 헤더만 갱신
    except Exception:
        ws = sh.add_worksheet(title=sheet_name, rows=100, cols=4)
        print(f"  ✅ '{sheet_name}' 시트 새로 생성")
    
    headers = ['원본표기(예탁원/SEIBRO)', 'DART표기', '등록방법', '비고']
    ws.update([headers], range_name='A1:D1')
    ws.format('A1:D1', {
        'textFormat': {'bold': True, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}},
        'backgroundColor': {'red': 0.4, 'green': 0.5, 'blue': 0.6},
        'horizontalAlignment': 'CENTER',
    })
    ws.freeze(rows=1)
    
    # 기존 데이터 확인
    existing = ws.get_all_values()
    existing_keys = set()
    for row in existing[1:]:
        if row and row[0].strip():
            existing_keys.add(row[0].strip())
    
    # 새로 추가할 별칭만 필터링
    new_rows = []
    skipped = 0
    for orig, dart, method, note in INITIAL_ALIASES:
        if orig in existing_keys:
            skipped += 1
            continue
        new_rows.append([orig, dart, method, note])
    
    if new_rows:
        # 기존 행 수 다음부터 추가
        start_row = len(existing) + 1
        if start_row < 2:
            start_row = 2
        end_row = start_row + len(new_rows) - 1
        ws.update(new_rows, range_name=f'A{start_row}:D{end_row}')
        print(f"  ✅ 별칭 {len(new_rows)}개 추가 (기존 {skipped}개 스킵)")
    else:
        print(f"  ℹ 추가할 새 별칭 없음 (기존 {skipped}개 모두 등록됨)")
    
    print(f"\n  📋 등록된 별칭 목록:")
    for orig, dart, _, note in INITIAL_ALIASES:
        marker = '↻' if orig in existing_keys else '+'
        print(f"     {marker} {orig} → {dart}  ({note})")


# ============================================================
# [메인]
# ============================================================
def main():
    print(f"🚀 시트 초기화 시작 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})\n")
    
    print("📋 [1/2] 별칭사전 시트 셋업...")
    setup_alias_sheet()
    
    print("\n📋 [2/2] 주식코드매칭 시트 셋업...")
    setup_matching_sheet()
    
    print(f"\n🏁 완료!")
    print(f"👉 https://docs.google.com/spreadsheets/d/{SHEET_ID}")
    print(f"\n📌 다음 단계:")
    print(f"   1. 메인시트(시트1)의 B열 ISIN 값을 주식코드매칭 시트 A열로 복사")
    print(f"   2. 'Bulk Match' 워크플로 실행 → 자동매칭 시작")


if __name__ == '__main__':
    main()
