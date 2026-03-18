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

# --- 설정 정보 (기존 시트 ID 확인 완료!) ---
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
worksheet = sh.get_worksheet(0)

dart.set_api_key(api_key=DART_API_KEY)
corp_list = dart.get_corp_list()

def extract_mezzanine_info(report):
    """공시 본문에서 메자닌 상세 정보를 추출합니다."""
    info = {"행사가액": "", "청구시작": "", "청구종료": ""}
    try:
        tables = report.extract_tables()
        for table in tables:
            df = table.to_df()
            text = df.to_string()
            if "전환가액" in text or "행사가액" in text:
                match = re.search(r'([\d,]+)\s*원', text)
                if match: info["행사가액"] = match.group(1)
            if "청구기간" in text:
                dates = re.findall(r'\d{4}년\s*\d{2}월\s*\d{2}일', text)
                if len(dates) >= 2:
                    info["청구시작"], info["청구종료"] = dates[0], dates[1]
        return info
    except:
        return info

async def update_sheet(stock_name, report):
    """시트의 해당 종목 행에 공시 정보를 업데이트합니다."""
    try:
        # 시트에서 종목명이 있는 셀 찾기
        cells = worksheet.find(stock_name)
        if not cells: return
        row = cells.row
        
        m_info = extract_mezzanine_info(report)
        
        # D열: 날짜, E열: 링크, F열: 행사가액 순으로 업데이트
        worksheet.update_cell(row, 4, report.rcept_dt) # D열 (Index 4)
        worksheet.update_cell(row, 5, f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={report.rcept_no}") # E열 (Index 5)
        if m_info["행사가액"]:
            worksheet.update_cell(row, 6, m_info["행사가액"]) # F열 (Index 6)
            
        print(f"✅ {stock_name} 업데이트 완료!")
    except Exception as e:
        print(f"❌ 시트 업데이트 중 오류 발생 ({stock_name}): {e}")

async def main():
    bot = Bot(token=TELEGRAM_TOKEN)
    # 시트의 모든 데이터를 읽어와 '종목명' 리스트 추출
    data = worksheet.get_all_records()
    stocks = [row['종목명'] for row in data if row.get('종목명')]

    print(f"🔍 조회 시작 종목 수: {len(stocks)}개")

    for stock in stocks:
        target = corp_list.find_by_corp_name(stock, exactly=True)
        if not target: 
            print(f"⚠️ {stock} 종목을 DART에서 찾을 수 없습니다.")
            continue
        
        try:
            # [테스트용] 3월 15일부터 현재까지의 공시를 검색
            reports = target[0].search_filings(bgn_de='20260315')
        except Exception:
            continue
        
        if not reports: continue
        
        print(f"📦 {stock} 종목의 공시를 {len(reports)}건 찾았습니다.")
        
        for r in reports:
            # 1. 텔레그램 발송
            msg = f"🔔 [공시 포착] {stock}\n📄 {r.report_nm}\n🔗 https://dart.fss.or.kr/dsaf001/main.do?rcpNo={r.rcept_no}"
            await bot.send_message(chat_id=CHAT_ID, text=msg)
            
            # 2. 시트 업데이트
            await update_sheet(stock, r)
            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
