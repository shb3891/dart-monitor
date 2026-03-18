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

# 구글 시트 연결
creds_json = json.loads(os.environ.get('GCP_SERVICE_ACCOUNT_KEY'))
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)
worksheet = sh.get_worksheet(0)

dart.set_api_key(api_key=DART_API_KEY)
corp_list = dart.get_corp_list()

def parse_mezzanine_by_isin(report, isin_code):
    """공시 본문에서 ISIN 코드를 찾아 해당 회차의 모든 정보를 추출합니다."""
    info = {"회차": "", "종류": "", "행사가액": "", "청구시작": "", "청구종료": "", "조기상환": ""}
    try:
        tables = report.extract_tables()
        for table in tables:
            df = table.to_df()
            text = df.to_string()
            
            # 내가 찾는 ISIN 코드가 이 표에 있는지 확인
            if isin_code in text:
                # 1. 종류 및 회차 (제목 등에서 추출)
                if "전환사채" in text: info["종류"] = "CB"
                elif "신주인수권" in text: info["종류"] = "BW"
                elif "교환사채" in text: info["종류"] = "EB"
                
                round_match = re.search(r'제\s*(\d+)\s*회', text)
                if round_match: info["회차"] = round_match.group(1)
                
                # 2. 행사가액
                price_match = re.search(r'([\d,]+)\s*원', text)
                if price_match: info["행사가액"] = price_match.group(1)
                
                # 3. 날짜들 (청구기간, 조기상환 등)
                dates = re.findall(r'\d{4}\s*[\.\-년]\s*\d{2}\s*[\.\-월]\s*\d{2}', text)
                if len(dates) >= 2:
                    info["청구시작"], info["청구종료"] = dates[0], dates[1]
        return info
    except:
        return info

async def main():
    bot = Bot(token=TELEGRAM_TOKEN)
    all_values = worksheet.get_all_values()
    if not all_values: return
    
    rows = all_values[1:] # 데이터 행
    print(f"🔍 총 {len(rows)}개 종목 분석 시작 (ISIN 기반)")

    for i, row in enumerate(rows):
        stock_name = row[0] # A열: 종목명
        isin_code = row[1]  # B열: 예탁원 코드
        if not isin_code: continue
        
        target = corp_list.find_by_corp_name(stock_name.split()[0], exactly=False)
        if not target: continue
        
        # 해당 종목의 최근 '발행결정' 또는 '사업보고서' 위주로 검색
        try:
            reports = target[0].search_filings(p_kind='A') # 정기공시 위주로 먼저 탐색
            if not reports:
                reports = target[0].search_filings(bgn_de='20240101') # 없으면 최근 1년 공시 탐색
        except: continue

        for r in reports:
            data = parse_mezzanine_by_isin(r, isin_code)
            if data["회차"] or data["행사가액"]: # 데이터를 찾았다면 시트 업데이트
                row_idx = i + 2
                if data["회차"]: worksheet.update_cell(row_idx, 3, data["회차"]) # C열
                if data["종류"]: worksheet.update_cell(row_idx, 4, data["종류"]) # D열
                if data["행사가액"]: worksheet.update_cell(row_idx, 5, data["행사가액"]) # E열
                if data["청구시작"]: worksheet.update_cell(row_idx, 6, data["청구시작"]) # F열
                
                print(f"✅ {stock_name} ({isin_code}) 업데이트 완료!")
                break # 찾았으면 다음 종목으로
        
        await asyncio.sleep(1) # 과부하 방지

if __name__ == "__main__":
    asyncio.run(main())
