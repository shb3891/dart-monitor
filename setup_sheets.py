import os
import json
import gspread
from google.oauth2.service_account import Credentials

# --- [설정] ---
SHEET_ID = '1s73BDNtCPe5mOs9EjBE5npEfcaNtYRyWJxRBUmJI-WA'

creds_json = json.loads(os.environ.get('GCP_SERVICE_ACCOUNT_KEY'))
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)

def col_num_to_letter(n):
    result = ''
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        result = chr(65 + remainder) + result
    return result

# ── 시트1 헤더 정리 ───────────────────────────────────
def fix_master_sheet():
    ws = sh.get_worksheet(0)

    # 헤더 업데이트
    headers = [
        '종목명', '예탁원 종목코드', '회차', '종류',
        '발행일', '만기일', 'Coupon', 'YTM',
        '행사가액', '리픽싱 플로어',
        '권리청구 시작일', '권리청구 종료일',
        'PUT 시작일', 'PUT 종료일', 'PUT 상환지급일', 'YTP',
        'CALL 비율', 'CALL 시작일', 'CALL 종료일', 'YTC'
    ]
    ws.update('A1', [headers])

    # 기존 데이터 읽기
    all_data = ws.get_all_values()
    data_rows = [r for r in all_data[1:] if len(r) > 1 and r[1].strip().startswith('KR')]

    # 기존 데이터를 새 구조에 맞게 재배치
    # 기존: A종목명 B ISIN C회차 D종류 E행사가액 F발행일 G만기일
    # 새로: A종목명 B ISIN C회차 D종류 E발행일 F만기일 G~ (행사가액은 I열로)
    new_data = []
    for r in data_rows:
        row = [
            r[0] if len(r) > 0 else '',   # A: 종목명
            r[1] if len(r) > 1 else '',   # B: ISIN
            r[2] if len(r) > 2 else '',   # C: 회차
            r[3] if len(r) > 3 else '',   # D: 종류
            r[5] if len(r) > 5 else '',   # E: 발행일 (기존 F)
            r[6] if len(r) > 6 else '',   # F: 만기일 (기존 G)
            '',                            # G: Coupon
            '',                            # H: YTM
            r[4] if len(r) > 4 else '',   # I: 행사가액 (기존 E)
            '',                            # J: 리픽싱 플로어
            '',                            # K: 권리청구 시작일
            '',                            # L: 권리청구 종료일
            '',                            # M: PUT 시작일
            '',                            # N: PUT 종료일
            '',                            # O: PUT 상환지급일
            '',                            # P: YTP
            '',                            # Q: CALL 비율
            '',                            # R: CALL 시작일
            '',                            # S: CALL 종료일
            '',                            # T: YTC
        ]
        new_data.append(row)

    if new_data:
        ws.update('A2', new_data)

    # 헤더 서식
    requests = []
    requests.append({'repeatCell': {
        'range': {'sheetId': ws.id, 'startRowIndex': 0, 'endRowIndex': 1,
                  'startColumnIndex': 0, 'endColumnIndex': len(headers)},
        'cell': {'userEnteredFormat': {
            'backgroundColor': {'red': 0.2, 'green': 0.4, 'blue': 0.7},
            'textFormat': {'foregroundColor': {'red': 1, 'green': 1, 'blue': 1},
                           'bold': True, 'fontSize': 10},
            'horizontalAlignment': 'CENTER',
            'verticalAlignment': 'MIDDLE',
        }},
        'fields': 'userEnteredFormat'
    }})

    # 1행 고정
    requests.append({'updateSheetProperties': {
        'properties': {'sheetId': ws.id, 'gridProperties': {'frozenRowCount': 1}},
        'fields': 'gridProperties.frozenRowCount'
    }})

    # 열 너비
    col_widths = [100, 130, 50, 60, 90, 90, 70, 70, 80, 80, 100, 100, 100, 100, 110, 70, 70, 100, 100, 70]
    for i, w in enumerate(col_widths):
        requests.append({'updateDimensionProperties': {
            'range': {'sheetId': ws.id, 'dimension': 'COLUMNS', 'startIndex': i, 'endIndex': i+1},
            'properties': {'pixelSize': w}, 'fields': 'pixelSize'
        }})

    sh.batch_update({'requests': requests})
    print(f"✅ 시트1 헤더 정리 완료 ({len(new_data)}개 종목)")
    return ws, data_rows

# ── 시트2: 가로형 ─────────────────────────────────────
def create_horizontal_sheet(master_ws):
    try:
        sh.del_worksheet(sh.worksheet('📊 가로형'))
    except:
        pass

    ws = sh.add_worksheet(title='📊 가로형', rows=200, cols=30)
    master_name = master_ws.title

    headers_row1 = [
        '기본정보', '', '', '',
        '발행/만기', '',
        '수익률', '', '',
        'PUT (조기상환)', '', '', '',
        'CALL (매도청구권)', '', '', '',
        '수기입력', '', '',
    ]

    headers_row2 = [
        '종목명', 'ISIN', '회차', '종류',
        '발행일', '만기일',
        'Coupon', 'YTM', '행사가액',
        'PUT 시작일', 'PUT 종료일', 'PUT 상환지급일', 'YTP',
        'CALL 비율', 'CALL 시작일', 'CALL 종료일', 'YTC',
        '신용등급', '보유고객 (펀드명)', '비고',
    ]

    # 시트1 데이터 개수 파악
    all_data = master_ws.get_all_values()
    data_count = len([r for r in all_data[1:] if len(r) > 1 and r[1].strip().startswith('KR')])
    start_row = 3

    # 수식으로 시트1 참조
    ref_cols = {
        0: 'A',   # 종목명
        1: 'B',   # ISIN
        2: 'C',   # 회차
        3: 'D',   # 종류
        4: 'E',   # 발행일
        5: 'F',   # 만기일
        6: 'G',   # Coupon
        7: 'H',   # YTM
        8: 'I',   # 행사가액
        9: 'M',   # PUT 시작일
        10: 'N',  # PUT 종료일
        11: 'O',  # PUT 상환지급일
        12: 'P',  # YTP
        13: 'Q',  # CALL 비율
        14: 'R',  # CALL 시작일
        15: 'S',  # CALL 종료일
        16: 'T',  # YTC
    }

    formula_rows = []
    for i in range(data_count):
        master_row = i + 2  # 시트1은 2행부터 데이터
        row = []
        for col_idx in range(20):
            if col_idx in ref_cols:
                row.append(f"='{master_name}'!{ref_cols[col_idx]}{master_row}")
            else:
                row.append('')  # 수기입력 칸
        formula_rows.append(row)

    ws.update('A1', [headers_row1])
    ws.update('A2', [headers_row2])
    if formula_rows:
        ws.update('A3', formula_rows)

    total_rows = data_count + 2
    total_cols = len(headers_row2)
    requests = []

    # 1행 병합
    merge_ranges = [(0,3),(4,5),(6,8),(9,12),(13,16),(17,19)]
    for sc, ec in merge_ranges:
        if sc != ec:
            requests.append({'mergeCells': {
                'range': {'sheetId': ws.id, 'startRowIndex': 0, 'endRowIndex': 1,
                          'startColumnIndex': sc, 'endColumnIndex': ec+1},
                'mergeType': 'MERGE_ALL'
            }})

    # 헤더 색상
    group_colors = [
        ((0,3),  {'red':0.2,'green':0.4,'blue':0.7}),
        ((4,5),  {'red':0.2,'green':0.6,'blue':0.4}),
        ((6,8),  {'red':0.5,'green':0.3,'blue':0.7}),
        ((9,12), {'red':0.2,'green':0.5,'blue':0.8}),
        ((13,16),{'red':0.6,'green':0.4,'blue':0.2}),
        ((17,19),{'red':0.5,'green':0.5,'blue':0.5}),
    ]
    for (sc, ec), color in group_colors:
        for row_idx in range(2):
            requests.append({'repeatCell': {
                'range': {'sheetId': ws.id, 'startRowIndex': row_idx, 'endRowIndex': row_idx+1,
                          'startColumnIndex': sc, 'endColumnIndex': ec+1},
                'cell': {'userEnteredFormat': {
                    'backgroundColor': color,
                    'textFormat': {'foregroundColor': {'red':1,'green':1,'blue':1},
                                   'bold': True, 'fontSize': 10},
                    'horizontalAlignment': 'CENTER',
                    'verticalAlignment': 'MIDDLE',
                    'wrapStrategy': 'WRAP'
                }},
                'fields': 'userEnteredFormat'
            }})

    # 수기입력 열 노란색
    requests.append({'repeatCell': {
        'range': {'sheetId': ws.id, 'startRowIndex': 2, 'endRowIndex': total_rows,
                  'startColumnIndex': 17, 'endColumnIndex': 20},
        'cell': {'userEnteredFormat': {'backgroundColor': {'red':1.0,'green':1.0,'blue':0.8}}},
        'fields': 'userEnteredFormat.backgroundColor'
    }})

    # 행 높이
    requests.append({'updateDimensionProperties': {
        'range': {'sheetId': ws.id, 'dimension': 'ROWS', 'startIndex': 0, 'endIndex': 2},
        'properties': {'pixelSize': 45}, 'fields': 'pixelSize'
    }})

    # 열 너비
    col_widths = [100,130,50,60,90,90,70,70,80,100,100,110,70,70,100,100,70,70,160,160]
    for i, w in enumerate(col_widths):
        requests.append({'updateDimensionProperties': {
            'range': {'sheetId': ws.id, 'dimension': 'COLUMNS', 'startIndex': i, 'endIndex': i+1},
            'properties': {'pixelSize': w}, 'fields': 'pixelSize'
        }})

    # 테두리
    requests.append({'updateBorders': {
        'range': {'sheetId': ws.id, 'startRowIndex': 0, 'endRowIndex': total_rows,
                  'startColumnIndex': 0, 'endColumnIndex': total_cols},
        'innerHorizontal': {'style':'SOLID','width':1,'color':{'red':0.8,'green':0.8,'blue':0.8}},
        'innerVertical':   {'style':'SOLID','width':1,'color':{'red':0.8,'green':0.8,'blue':0.8}},
        'top':    {'style':'SOLID','width':2,'color':{'red':0.3,'green':0.3,'blue':0.3}},
        'bottom': {'style':'SOLID','width':1,'color':{'red':0.3,'green':0.3,'blue':0.3}},
        'left':   {'style':'SOLID','width':2,'color':{'red':0.3,'green':0.3,'blue':0.3}},
        'right':  {'style':'SOLID','width':2,'color':{'red':0.3,'green':0.3,'blue':0.3}},
    }})

    # 2행 고정
    requests.append({'updateSheetProperties': {
        'properties': {'sheetId': ws.id, 'gridProperties': {'frozenRowCount': 2}},
        'fields': 'gridProperties.frozenRowCount'
    }})

    sh.batch_update({'requests': requests})
    print("✅ 가로형 시트 생성 완료")

# ── 시트3: 세로형 ─────────────────────────────────────
def create_vertical_sheet(master_ws):
    try:
        sh.del_worksheet(sh.worksheet('📋 세로형'))
    except:
        pass

    ws = sh.add_worksheet(title='📋 세로형', rows=40, cols=200)
    master_name = master_ws.title

    row_labels = [
        '기본정보',
        '종목명',
        'ISIN',
        '회차',
        '종류',
        '신용등급',
        '보유고객 (펀드명)',
        '',
        '발행 / 만기',
        '발행일 / 만기일',
        'Coupon',
        'YTM',
        '',
        '행사가액',
        '행사가액',
        '리픽싱 플로어',
        '',
        '권리청구기간',
        '시작일',
        '종료일',
        '',
        '조기상환 (PUT)',
        'PUT 시작일',
        'PUT 종료일',
        'PUT 상환지급일',
        'YTP',
        '',
        '매도청구권 (CALL)',
        'CALL 비율',
        'CALL 시작일',
        'CALL 종료일',
        'YTC',
        '',
        '주간사 / 소싱 / 실무자',
        '특이사항 / 보유비율',
    ]

    # 시트1 참조 매핑 (행 인덱스 → 시트1 열)
    ref_map = {
        1:  'A',   # 종목명
        2:  'B',   # ISIN
        3:  'C',   # 회차
        4:  'D',   # 종류
        9:  'E',   # 발행일
        10: 'G',   # Coupon
        11: 'H',   # YTM
        14: 'I',   # 행사가액
        15: 'J',   # 리픽싱 플로어
        18: 'K',   # 권리청구 시작일
        19: 'L',   # 권리청구 종료일
        22: 'M',   # PUT 시작일
        23: 'N',   # PUT 종료일
        24: 'O',   # PUT 상환지급일
        25: 'P',   # YTP
        28: 'Q',   # CALL 비율
        29: 'R',   # CALL 시작일
        30: 'S',   # CALL 종료일
        31: 'T',   # YTC
    }

    # A열 항목명
    ws.update('A1', [[label] for label in row_labels])

    # 시트1 데이터 개수
    all_data = master_ws.get_all_values()
    data_rows = [r for r in all_data[1:] if len(r) > 1 and r[1].strip().startswith('KR')]

    # 전체 데이터를 행×열 형태로 구성
    all_col_data = [[''] * len(data_rows) for _ in range(len(row_labels))]

    for col_idx in range(len(data_rows)):
        master_row = col_idx + 2
        for row_idx in range(len(row_labels)):
            if row_idx in ref_map:
                all_col_data[row_idx][col_idx] = f"='{master_name}'!{ref_map[row_idx]}{master_row}"
            # 발행일/만기일 합쳐서 표시
            if row_idx == 9:
                all_col_data[row_idx][col_idx] = f"='{master_name}'!E{master_row}&\" / \"&'{master_name}'!F{master_row}"

    end_col = col_num_to_letter(len(data_rows) + 1)
    ws.update(f'B1:{end_col}{len(row_labels)}', all_col_data)

    total_cols = len(data_rows) + 1
    requests = []

    # 열 너비
    requests.append({'updateDimensionProperties': {
        'range': {'sheetId': ws.id, 'dimension': 'COLUMNS', 'startIndex': 0, 'endIndex': 1},
        'properties': {'pixelSize': 160}, 'fields': 'pixelSize'
    }})
    requests.append({'updateDimensionProperties': {
        'range': {'sheetId': ws.id, 'dimension': 'COLUMNS', 'startIndex': 1, 'endIndex': total_cols},
        'properties': {'pixelSize': 190}, 'fields': 'pixelSize'
    }})

    # 섹션 헤더 색상
    section_rows = [0, 8, 13, 17, 21, 27]
    section_colors = [
        {'red':0.2,'green':0.4,'blue':0.7},
        {'red':0.2,'green':0.6,'blue':0.4},
        {'red':0.8,'green':0.2,'blue':0.2},
        {'red':0.9,'green':0.5,'blue':0.1},
        {'red':0.2,'green':0.5,'blue':0.8},
        {'red':0.6,'green':0.4,'blue':0.2},
    ]
    for row_idx, color in zip(section_rows, section_colors):
        requests.append({'repeatCell': {
            'range': {'sheetId': ws.id, 'startRowIndex': row_idx, 'endRowIndex': row_idx+1,
                      'startColumnIndex': 0, 'endColumnIndex': total_cols},
            'cell': {'userEnteredFormat': {
                'backgroundColor': color,
                'textFormat': {'foregroundColor': {'red':1,'green':1,'blue':1}, 'bold': True},
                'horizontalAlignment': 'CENTER'
            }},
            'fields': 'userEnteredFormat'
        }})

    # 수기입력 행 노란색
    for row_idx in [5, 6, 33, 34]:
        requests.append({'repeatCell': {
            'range': {'sheetId': ws.id, 'startRowIndex': row_idx, 'endRowIndex': row_idx+1,
                      'startColumnIndex': 1, 'endColumnIndex': total_cols},
            'cell': {'userEnteredFormat': {'backgroundColor': {'red':1.0,'green':1.0,'blue':0.8}}},
            'fields': 'userEnteredFormat.backgroundColor'
        }})

    # A열 고정
    requests.append({'updateSheetProperties': {
        'properties': {'sheetId': ws.id, 'gridProperties': {'frozenColumnCount': 1}},
        'fields': 'gridProperties.frozenColumnCount'
    }})

    # 테두리
    requests.append({'updateBorders': {
        'range': {'sheetId': ws.id, 'startRowIndex': 0, 'endRowIndex': len(row_labels),
                  'startColumnIndex': 0, 'endColumnIndex': total_cols},
        'innerHorizontal': {'style':'SOLID','width':1,'color':{'red':0.8,'green':0.8,'blue':0.8}},
        'innerVertical':   {'style':'SOLID','width':1,'color':{'red':0.8,'green':0.8,'blue':0.8}},
        'top':    {'style':'SOLID','width':2,'color':{'red':0.3,'green':0.3,'blue':0.3}},
        'bottom': {'style':'SOLID','width':1,'color':{'red':0.3,'green':0.3,'blue':0.3}},
        'left':   {'style':'SOLID','width':2,'color':{'red':0.3,'green':0.3,'blue':0.3}},
        'right':  {'style':'SOLID','width':2,'color':{'red':0.3,'green':0.3,'blue':0.3}},
    }})

    sh.batch_update({'requests': requests})
    print("✅ 세로형 시트 생성 완료")


# 실행
print("🔧 시트 정리 시작...")
master_ws, _ = fix_master_sheet()
create_horizontal_sheet(master_ws)
create_vertical_sheet(master_ws)
print("\n🏁 완료!")
print(f"👉 https://docs.google.com/spreadsheets/d/{SHEET_ID}")
