import dart_fss as dart
import pandas as pd
import asyncio
import os
import json
import gspread
import re
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta

# --- 설정 정보 (알람 기능 제외) ---
DART_API_KEY = 'bfc4e4e445de4727ae0bcc27e80ba5cf0e3818e6'
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
    """공시 본문에서 ISIN 코드와 일치하는 표의 데이터를 정밀 추출합니다."""
    info = {"회차": "", "종류": "", "행사가액": "", "청구시작": "", "청구종료": ""}
    try:
        tables = report.extract_tables()
        for table in tables:
            df = table.to_df()
            table_text = df.to_string()
            
            # ISIN 코드가 포함된 표인지 확인
            if isin_code in table_text:
                # 종류 판별 (CB, BW, EB)
                if "전환사채" in table_text: info["종류"] = "CB"
                elif "신주인수권" in table_text: info["종류"] = "BW"
                elif "교환사채" in table_text: info["종류"] = "EB"
                
                # 회차 추출 (제N회차)
                round_match = re.search(r'제\s*(\d+)\s*회', table_text)
                if round_match: info["회차"] = round_match.group(1)
                
                # 행사가액 추출
                price_match = re.search(r'([\d,]+)\s*원', table_text)
                if price_match: info["행사가액"] = price_match.group(1)
                
                # 청구기간 추출 (날짜 형식 2개)
                dates = re.findall(r'\d{4}\s*[\.\-년]\s*\d{2}\s*[\.\-월]\s*\d{2}', table_text)
                if len(dates) >= 2:
                    info["청구시작"], info["청구종료"] = dates[0], dates[1]
                
                return info
        return None
    except:
        return None

async def main():
    all_values = worksheet.get_all_values()
    if not all_values: return
    
    rows = all_values[1:] # 데이터 시작
    # 과거 3년치 데이터를 위해 검색 시작일을 1,100일 전으로 설정
    bgn_de = (datetime.now() - timedelta(days=1100)).strftime('%Y%m%d')

    print(f"🚀 [3개년 데이터 수집 모드] {len(rows)}개 종목 분석 시작...")
    print(f"📅 검색 범위: {bgn_de} ~ 현재")

    for i, row in enumerate(rows):
        full_name = row[0]
        stock_name = full_name.split()[0] if full_name else "" # 종목명만 추출
        isin_code = row[1] # B열: 예탁원 종목코드
        
        if not isin_code or not stock_name: continue
        
        # 1. DART 종목 검색
        target = corp_list.find_by_corp_name(stock_name, exactly=False)
        if not target: 
            print(f"❓ 종목 찾기 실패: {stock_name}")
            continue
        
        # 2. 공시 검색 (주요사항보고서, 정기공시, 발행공시 전체 뒤지기)
        try:
            reports = target[0].search_filings(bgn_de=bgn_de, p_kind=['A', 'B', 'I'])
            if not reports: continue
        except: continue

        # 3. 데이터 파싱 및 시트 업데이트
        for r in reports:
            found_data = parse_mezzanine_by_isin(r, isin_code)
            if found_data:
                row_idx = i + 2 # 헤더 제외 실제 행 번호
                
                # C(3), D(4), E(5), F(6)열 순차 업데이트
                if found_data["회차"]: worksheet.update_cell(row_idx, 3, found_data["회차"])
                if found_data["종류"]: worksheet.update_cell(row_idx, 4, found_data["종류"])
                if found_data["행사가액"]: worksheet.update_cell(row_idx, 5, found_data["행사가액"])
                if found_data["청구시작"]: worksheet.update_cell(row_idx, 6, found_data["청구시작"])
                
                print(f"✅ {full_name} ({isin_code}) 업데이트 완료")
                break 
        
        # DART API 과부하 방지 및 속도 조절
        await asyncio.sleep(1.2)

if __name__ == "__main__":
    asyncio.run(main())
