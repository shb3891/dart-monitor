import dart_fss as dart
import pandas as pd
import asyncio
import os
import json
import gspread
from google.oauth2.service_account import Credentials
from telegram import Bot
from datetime import datetime

# --- 설정 정보 ---
DART_API_KEY = 'bfc4e4e445de4727ae0bcc27e80ba5cf0e3818e6'
TELEGRAM_TOKEN = '8491277145:AAHwHfaG1q-5ZjExFu8o3T9T6X5c8HlLSlI'
CHAT_ID = '536635522'
SHEET_ID = '1s73BDNtCPe5mOs9EjBE5npEfcaNtYRyWJxRBUmJI-WA'

# 깃허브 Secrets에서 가져온 JSON 열쇠 로드
creds_json = json.loads(os.environ.get('GCP_SERVICE_ACCOUNT_KEY'))
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)
worksheet = sh.get_worksheet(0) # 첫 번째 시트

dart.set_api_key(api_key=DART_API_KEY)
corp_list = dart.get_corp_list()

async def update_sheet_with_mezzanine(stock_name, report):
    """공시 본문을 파싱하여 시트의 해당 종목 행을 업데이트합니다."""
    try:
        # 공시 본문에서 상세 데이터 추출 (예시: 전환가액 조정 공시 위주)
        # 실제로는 각 공시 타입별로 표(Table)를 읽는 복잡한 로직이 들어가야 함
        # 여기서는 과장님이 요청하신 항목들을 업데이트하는 구조만 잡습니다.
        
        cells = worksheet.find(stock_name)
        if not cells: return
        row = cells.row
        
        # B열: 현재전환가, D열: 공시날짜, E열: 링크 업데이트
        worksheet.update_cell(row, 4, report.rcept_dt) # D열: 공시날짜
        worksheet.update_cell(row, 5, f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={report.rcept_no}") # E열: 링크
        
        # [참고] 전환가액, 조기상환일 등 상세 파싱 로직은 dart-fss의 r.extract_tables() 기능을 활용합니다.
    except Exception as e:
        print(f"시트 업데이트 에러: {e}")

async def main():
    bot = Bot(token=TELEGRAM_TOKEN)
    data = worksheet.get_all_records()
    stocks = [row['종목명'] for row in data if row.get('종목명')]

    for stock in stocks:
        target = corp_list.find_by_corp_name(stock, exactly=True)
        if not target: continue
        
        reports = target[0].search_filings(bgn_de=datetime.now().strftime('%Y%m%d'))
        if not reports: continue
        
        for r in reports:
            # 1. 텔레그램 알림 발송
            msg = f"🔔 [공시 포착] {stock}\n📄 {r.report_nm}\n🔗 https://dart.fss.or.kr/dsaf001/main.do?rcpNo={r.rcept_no}"
            await bot.send_message(chat_id=CHAT_ID, text=msg)
            
            # 2. 시트 자동 업데이트 실행
            await update_sheet_with_mezzanine(stock, r)
            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
