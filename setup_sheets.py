import os
import json
import gspread
from google.oauth2.service_account import Credentials

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

def ref(sheet_name, col, row):
    """올바른 시트 참조 수식 생성"""
    return f"='{sheet_name}'!{col}{row}"

def fix_master_sheet():
    ws = sh.get_worksheet(0)
    headers = [
        '종목명', '예탁원 종목코드', '회차', '종류',
        '발행일', '만기일', 'Coupon', 'YTM',
        '행사가액', '리픽싱 플로어',
        '권리청구 시작일', '권리청구 종료일',
        'PUT 시작일', 'PUT 종료일', 'PUT 상환지급일', 'YTP',
        'CALL 비율', 'CALL 시작일', 'CALL 종료일', 'YTC'
    ]
    ws.update('A1', [headers])

    all_data = ws.get_all_values()
    data_rows = [r for r in all_data[1:] if len(r) > 1 and r[1].strip().startswith('KR')]

    new_data = []
    for r in data_rows:
        row = [
            r[0] if len(r) > 0 else '',
            r[1] if len(r) > 1 else '',
            r[2] if len(r) > 2 else '',
            r[3] if len(r) > 3 else '',
            r[4] if len(r) > 4 else '',
            r[5] if len(r) > 5 else '',
            '', '', '', '', '', '', '', '', '', '', '', '', '', '',
        ]
        new_data.append(row)

    if new_data:
        ws.update('A2', new_data)

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
    requests.append({'updateSheetProperties': {
        'properties': {'sheetId': ws.id, 'gridProperties': {'frozenRowCount': 1}},
        'fields': 'gridProperties.frozenRowCount'
    }})
    col_widths = [100,130,50,60,90,90,70,70,80,80,100,100,100,100,110,70,70,100,100,70]
    for i, w in enumerate(col_widths):
        requests.append({'updateDimensionProperties': {
            'range': {'sheetId': ws.id, 'dimension': 'COLUMNS', 'startIndex': i, 'endIndex': i+1},
            'properties': {'pixelSize': w}, 'fields': 'pixelSize'
        }})
    sh.batch_update({'requests': requests})
    print(f"✅ 시트1 헤더 정리 완료 ({len(new_data)}개 종목)")
    return ws

def create_horizontal_sheet(master_ws):
    try:
        sh.del_worksheet(sh.worksheet('📊 가로형'))
    except:
        pass

    ws = sh.add_worksheet(title='📊 가로형', rows=200, cols=30)
    mn = master_ws.title

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

    all_data = master_ws.get_all_values()
    data_count = len([r for r in all_data[1:] if len(r) > 1 and r[1].strip().startswith('KR')])

    ref_cols = {
        0:'A', 1:'B', 2:'C', 3:'D',
        4:'E', 5:'F',
        6:'G', 7:'H', 8:'I',
        9:'M', 10:'N', 11:'O', 12:'P',
        13:'Q', 14:'R', 15:'S', 16:'T',
    }

    formula_rows = []
    for i in range(data_count):
        master_row = i + 2
        row = []
        for col_idx in range(20):
            if col_idx in ref_cols:
                row.append(ref(mn, ref_cols[col_idx], master_row))
            else:
                row.append('')
        formula_rows.append(row)

    ws.update('A1', [headers_row1])
    ws.update('A2', [headers_row2])
    if formula_rows:
        ws.update('A3', formula_rows)

    total_rows = data_count + 2
    total_cols = len(headers_row2)
    requests = []

    merge_ranges = [(0,3),(4,5),(6,8),(9,12),(13,16),(17,19)]
    for sc, ec in merge_ranges:
        if sc != ec:
            requests.append({'mergeCells': {
                'range': {'sheetId': ws.id, 'startRowIndex': 0, 'endRowIndex': 1,
                          'startColumnIndex': sc, 'endColumnIndex': ec+1},
                'mergeType': 'MERGE_ALL'
            }})

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

    requests.append({'repeatCell': {
        'range': {'sheetId': ws.id, 'startRowIndex': 2, 'endRowIndex': total_rows,
                  'startColumnIndex': 17, 'endColumnIndex': 20},
        'cell': {'userEnteredFormat': {'backgroundColor': {'red':1.0,'green':1.0,'blue':0.8}}},
        'fields': 'userEnteredFormat.backgroundColor'
    }})
    requests.append({'updateDimensionProperties': {
        'range': {'sheetId': ws.id, 'dimension': 'ROWS', 'startIndex': 0, 'endIndex': 2},
        'properties': {'pixelSize': 45}, 'fields': 'pixelSize'
    }})
    col_widths = [100,130,50,60,90,90,70,70,80,100,100,110,70,70,100,100,70,70,160,160]
    for i, w in enumerate(col_widths):
        requests.append({'updateDimensionProperties': {
            'range': {'sheetId': ws.id, 'dimension': 'COLUMNS', 'startIndex': i, 'endIndex': i+1},
            'properties': {'pixelSize': w}, 'fields': 'pixelSize'
        }})
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
    requests.append({'updateSheetProperties': {
        'properties': {'sheetId': ws.id, 'gridProperties': {'frozenRowCount': 2}},
        'fields': 'gridProperties.frozenRowCount'
    }})
    sh.batch_update({'requests': requests})
    print("✅ 가로형 시트 생성 완료")

def create_vertical_sheet(master_ws):
    try:
        sh.del_worksheet(sh.worksheet('📋 세로형'))
    except:
        pass

    ws = sh.add_worksheet(title='📋 세로형', rows=40, cols=200)
    mn = master_ws.title

    row_labels = [
        '기본정보', '종목명', 'ISIN', '회차', '종류',
        '신용등급', '보유고객 (펀드명)', '',
        '발행 / 만기', '발행일 / 만기일', 'Coupon', 'YTM', '',
        '행사가액', '행사가액', '리픽싱 플로어', '',
        '권리청구기간', '시작일', '종료일', '',
        '조기상환 (PUT)', 'PUT 시작일', 'PUT 종료일', 'PUT 상환지급일', 'YTP', '',
        '매도청구권 (CALL)', 'CALL 비율', 'CALL 시작일', 'CALL 종료일', 'YTC', '',
        '주간사 / 소싱 / 실무자', '특이사항 / 보유비율',
    ]

    ref_map = {
        1:'A', 2:'B', 3:'C', 4:'D',
        10:'G', 11:'H',
        14:'I', 15:'J',
        18:'K', 19:'L',
        22:'M', 23:'N', 24:'O', 25:'P',
        28:'Q', 29:'R', 30:'S', 31:'T',
    }

    ws.update('A1', [[label] for label in row_labels])

    all_data = master_ws.get_all_values()
    data_rows = [r for r in all_data[1:] if len(r) > 1 and r[1].strip().startswith('KR')]

    all_col_data = [[''] * len(data_rows) for _ in range(len(row_labels))]

    for col_idx in range(len(data_rows)):
        master_row = col_idx + 2
        for row_idx in range(len(row_labels)):
            if row_idx == 9:
                # 발행일 / 만기일 합치기
                all_col_data[row_idx][col_idx] = f"='{mn}'!E{master_row}&\" / \"&'{mn}'!F{master_row}"
            elif row_idx in ref_map:
                all_col_data[row_idx][col_idx] = ref(mn, ref_map[row_idx], master_row)

    end_col = col_num_to_letter(len(data_rows) + 1)
    ws.update(f'B1:{end_col}{len(row_labels)}', all_col_data)

    total_cols = len(data_rows) + 1
    requests = []

    requests.append({'updateDimensionProperties': {
        'range': {'sheetId': ws.id, 'dimension': 'COLUMNS', 'startIndex': 0, 'endIndex': 1},
        'properties': {'pixelSize': 160}, 'fields': 'pixelSize'
    }})
    requests.append({'updateDimensionProperties': {
        'range': {'sheetId': ws.id, 'dimension': 'COLUMNS', 'startIndex': 1, 'endIndex': total_cols},
        'properties': {'pixelSize': 190}, 'fields': 'pixelSize'
    }})

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

    for row_idx in [5, 6, 33, 34]:
        requests.append({'repeatCell': {
            'range': {'sheetId': ws.id, 'startRowIndex': row_idx, 'endRowIndex': row_idx+1,
                      'startColumnIndex': 1, 'endColumnIndex': total_cols},
            'cell': {'userEnteredFormat': {'backgroundColor': {'red':1.0,'green':1.0,'blue':0.8}}},
            'fields': 'userEnteredFormat.backgroundColor'
        }})

    requests.append({'updateSheetProperties': {
        'properties': {'sheetId': ws.id, 'gridProperties': {'frozenColumnCount': 1}},
        'fields': 'gridProperties.frozenColumnCount'
    }})
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


print("🔧 시트 정리 시작...")
master_ws = fix_master_sheet()
create_horizontal_sheet(master_ws)
create_vertical_sheet(master_ws)
print("\n🏁 완료!")
print(f"👉 https://docs.google.com/spreadsheets/d/{SHEET_ID}")
