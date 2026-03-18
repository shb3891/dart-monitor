import dart_fss as dart
import pandas as pd
import asyncio
import os
import json
import gspread
import re
from google.oauth2.service_account import Credentials
from telegram import Bot
from datetime import datetime

# --- 설정 정보 ---
DART_API_KEY = 'bfc4e4e445de4727ae0bcc27e80ba5cf0e3818e6'
TELEGRAM_TOKEN = '8491277145:AAHwHfaG1q-5ZjExFu8o3T9T6X5c8HlLSlI'
CHAT_ID = '536635522'
SHEET_ID = '1s73BDNtCPe5mOs9EjBE5npEfcaNtYRyWJxRBUmJI-WA'

# 구글 시트 연결 설정
creds_json = json.loads(os.environ.get('GCP_SERVICE_ACCOUNT_KEY'))
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)
worksheet = sh.get_worksheet(0)

dart.set_api_key(api_key=DART_API_KEY)
corp_list = dart.get_corp_list()

def extract_mezzanine_info(report):
    """공시 본문에서 행사가액 등 상세 정보를 추출합니다."""
    info = {"행사가액": "", "청구시작": "", "청구종료": ""}
    try:
        tables = report.extract_tables()
        for table in tables:
            df = table.to_df()
            text = df.to_string()
            if "전환가액" in text or "행사가액" in text:
                match = re.search(r'([\d,]+)\s*원', text)
                if match: info["행사가액"] = match.group(1)
        return info
    except:
        return info

async def update_sheet(stock_name, report, row_idx):
    """시트에 공시 날짜와 링크를 업데이트합니다."""
    try:
        m_info = extract_mezzanine_info(report)
        worksheet.update_cell(row_idx, 4, report.rcept_dt)
        worksheet.update_cell(row_idx, 5, f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={report.rcept_no}")
        if m_info["행사가액"]:
            worksheet.update_cell(row_idx, 6, m_info["행사가액"])
        print(f"✅ {stock_name} 시트 업데이트 완료")
    except Exception as e:
        print(f"❌ {stock_name} 업데이트 실패: {e}")

async def main():
    bot = Bot(token=TELEGRAM_TOKEN)
    all_values = worksheet.get_all_values()
    if not all_values: return
    
    header = all_values[0]
    rows = all_values[1:]
    
    try:
        name_col_idx = header.index("종목명")
        link_col_idx = 4 # E열
    except ValueError:
        print("❌ '종목명' 컬럼을 찾을 수 없습니다.")
        return

    print(f"🔍 실시간 감시 시작: {len(rows)}개 종목")

    for i, row in enumerate(rows):
        stock = row[name_col_idx]
        if not stock: continue
        
        existing_link = row[link_col_idx] if len(row) > link_col_idx else ""
        target = corp_list.find_by_corp_name(stock, exactly=True)
        if not target: continue
        
        try:
            today_str = datetime.now().strftime('%Y%m%d')
            reports = target[0].search_filings(bgn_de=today_str)
        except:
            continue
        
        if not reports: continue
        
        for r in reports:
            new_link = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={r.rcept_no}"
            if new_link == existing_link:
                continue
            
            msg = f"🔔 [새 공시] {stock}\n📄 {r.report_nm}\n🔗 {new_link}"
            await bot.send_message(chat_id=CHAT_ID, text=msg)
            await update_sheet(stock, r, i + 2)
            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
