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

# 기존 시트1 데이터 읽기
ws_original = sh.get_worksheet(0)
all_data = ws_original.get_all_values()
data_rows = [r for r in all_data[1:] if len(r) > 1 and r[1].strip().startswith('KR')]
print(f"📋 총 {len(data_rows)}개 종목 확인")

def col_num_to_letter(n):
    """열 번호(1부터)를 엑셀 열 문자로 변환. 예: 1→A, 26→Z, 27→AA"""
    result = ''
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        result = chr(65 + remainder) + result
    return result

# ── 시트2: 가로형 ─────────────────────────────────────
def create_horizontal_sheet():
    try:
        sh.del_worksheet(sh.worksheet('📊 가로형'))
    except:
        pass

    ws = sh.add_worksheet(title='📊 가로형', rows=200, cols=50)

    headers_row1 = [
        '기본정보', '', '', '',
        '발행/만기', '',
        '수익률', '',
        '권리청구기간', '',
        '행사가액', '',
        '조기상환 (PUT)', '', '', '',
        '매도청구권 (CALL)', '', '', '',
        '수기입력', '', '',
    ]

    headers_row2 = [
        '종목명', 'ISIN', '회차', '종류',
        '발행일', '만기일',
        'Coupon', 'YTM',
        '권리청구 시작일', '권리청구 종료일',
        '행사가액', '리픽싱 플로어',
        'PUT 시작일', 'PUT 종료일', 'PUT 상환지급일', 'YTP',
        'CALL 비율', 'CALL 시작일', 'CALL 종료일', 'YTC',
        '신용등급', '보유고객 (펀드명)', '비고',
    ]

    data_values = []
    for r in data_rows:
        row = [
            r[0] if len(r) > 0 else '',
            r[1] if len(r) > 1 else '',
            r[2] if len(r) > 2 else '',
            r[3] if len(r) > 3 else '',
            r[5] if len(r) > 5 else '',
            '',  # 만기일
            '',  # Coupon
            '',  # YTM
            '',  # 권리청구 시작일
            '',  # 권리청구 종료일
            r[4] if len(r) > 4 else '',
            '',  # 리픽싱 플로어
            '',  # PUT 시작일
            '',  # PUT 종료일
            '',  # PUT 상환지급일
            '',  # YTP
            '',  # CALL 비율
            '',  # CALL 시작일
            '',  # CALL 종료일
            '',  # YTC
            '',  # 신용등급 (수기)
            '',  # 보유고객 (수기)
            '',  # 비고 (수기)
        ]
        data_values.append(row)

    ws.update('A1', [headers_row1])
    ws.update('A2', [headers_row2])
    ws.update('A3', data_values)

    total_rows = len(data_values) + 2
    total_cols = len(headers_row2)

    requests = []

    # 1행 병합
    merge_ranges = [(0,3),(4,5),(6,7),(8,9),(10,11),(12,15),(16,19),(20,22)]
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
        ((6,7),  {'red':0.5,'green':0.3,'blue':0.7}),
        ((8,9),  {'red':0.9,'green':0.5,'blue':0.1}),
        ((10,11),{'red':0.8,'green':0.2,'blue':0.2}),
        ((12,15),{'red':0.2,'green':0.5,'blue':0.8}),
        ((16,19),{'red':0.6,'green':0.4,'blue':0.2}),
        ((20,22),{'red':0.5,'green':0.5,'blue':0.5}),
    ]
    for (sc, ec), color in group_colors:
        for row_idx in range(2):
            requests.append({'repeatCell': {
                'range': {'sheetId': ws.id, 'startRowIndex': row_idx, 'endRowIndex': row_idx+1,
                          'startColumnIndex': sc, 'endColumnIndex': ec+1},
                'cell': {'userEnteredFormat': {
                    'backgroundColor': color,
                    'textFormat': {'foregroundColor': {'red':1,'green':1,'blue':1}, 'bold': True, 'fontSize': 10},
                    'horizontalAlignment': 'CENTER',
                    'verticalAlignment': 'MIDDLE',
                    'wrapStrategy': 'WRAP'
                }},
                'fields': 'userEnteredFormat'
            }})

    # 수기입력 열 노란색
    requests.append({'repeatCell': {
        'range': {'sheetId': ws.id, 'startRowIndex': 2, 'endRowIndex': total_rows,
                  'startColumnIndex': 20, 'endColumnIndex': 23},
        'cell': {'userEnteredFormat': {'backgroundColor': {'red':1.0,'green':1.0,'blue':0.8}}},
        'fields': 'userEnteredFormat.backgroundColor'
    }})

    # 행 높이
    requests.append({'updateDimensionProperties': {
        'range': {'sheetId': ws.id, 'dimension': 'ROWS', 'startIndex': 0, 'endIndex': 2},
        'properties': {'pixelSize': 45}, 'fields': 'pixelSize'
    }})

    # 열 너비
    col_widths = [100,130,50,60,90,90,70,70,100,100,80,80,100,100,110,70,70,100,100,70,70,160,160]
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
def create_vertical_sheet():
    try:
        sh.del_worksheet(sh.worksheet('📋 세로형'))
    except:
        pass

    ws = sh.add_worksheet(title='📋 세로형', rows=40, cols=200)

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
        '권리청구기간',
        '시작일',
        '종료일',
        '',
        '행사가액',
        '행사가액',
        '리픽싱 플로어',
        'Refixing',
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

    # A열 항목명 입력
    ws.update('A1', [[label] for label in row_labels])

    # ✅ 종목별 데이터를 한번에 모아서 업데이트 (열 문자 버그 수정)
    all_col_data = [[''] * len(data_rows) for _ in range(len(row_labels))]

    for col_idx, r in enumerate(data_rows):
        values = [
            '',
            r[0] if len(r) > 0 else '',
            r[1] if len(r) > 1 else '',
            r[2] if len(r) > 2 else '',
            r[3] if len(r) > 3 else '',
            '',  # 신용등급 수기
            '',  # 보유고객 수기
            '',
            '',
            r[5] if len(r) > 5 else '',
            '',  # Coupon
            '',  # YTM
            '',
            '',
            '',  # 권리청구 시작일
            '',  # 권리청구 종료일
            '',
            '',
            r[4] if len(r) > 4 else '',
            '',  # 리픽싱 플로어
            '',  # Refixing
            '',
            '',
            '',  # PUT 시작일
            '',  # PUT 종료일
            '',  # PUT 상환지급일
            '',  # YTP
            '',
            '',
            '',  # CALL 비율
            '',  # CALL 시작일
            '',  # CALL 종료일
            '',  # YTC
            '',
            '',  # 주간사 수기
            '',  # 특이사항 수기
        ]
        for row_idx, val in enumerate(values):
            all_col_data[row_idx][col_idx] = val

    # B열부터 데이터 일괄 업데이트
    end_col = col_num_to_letter(len(data_rows) + 1)
    ws.update(f'B1:{end_col}{len(row_labels)}', all_col_data)

    total_cols = len(data_rows) + 1
    requests = []

    # A열 너비
    requests.append({'updateDimensionProperties': {
        'range': {'sheetId': ws.id, 'dimension': 'COLUMNS', 'startIndex': 0, 'endIndex': 1},
        'properties': {'pixelSize': 160}, 'fields': 'pixelSize'
    }})

    # 데이터 열 너비
    requests.append({'updateDimensionProperties': {
        'range': {'sheetId': ws.id, 'dimension': 'COLUMNS', 'startIndex': 1, 'endIndex': total_cols},
        'properties': {'pixelSize': 190}, 'fields': 'pixelSize'
    }})

    # 섹션 헤더 색상
    section_rows = [0, 8, 13, 17, 22, 28]
    section_colors = [
        {'red':0.2,'green':0.4,'blue':0.7},
        {'red':0.2,'green':0.6,'blue':0.4},
        {'red':0.9,'green':0.5,'blue':0.1},
        {'red':0.8,'green':0.2,'blue':0.2},
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
    for row_idx in [5, 6, 34, 35]:
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


create_horizontal_sheet()
create_vertical_sheet()
print("\n🏁 완료! 스프레드시트 확인해주세요.")
print(f"👉 https://docs.google.com/spreadsheets/d/{SHEET_ID}")
