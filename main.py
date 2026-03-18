import dart_fss as dart
import pandas as pd
import asyncio
import os
import json
import gspread
import re
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta

# --- 설정 정보 ---
DART_API_KEY = 'bfc4e4e445de4727ae0bcc27e80ba5cf0e3818e6'
SHEET_ID = '1s73BDNtCPe5mOs9EjBE5npEfcaNtYRyWJxRBUmJI-WA'

creds_json = json.loads(os.environ.get('GCP_SERVICE_ACCOUNT_KEY'))
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)
worksheet = sh.get_worksheet(0)

dart.set_api_key(api_key=DART_API_KEY)
corp_list = dart.get_corp_list()

def parse_mezzanine_by_isin(report, isin_code):
    """공시 본문에서 ISIN 코드를 포함한 모든 텍스트를 정밀 탐색합니다."""
    info = {"회차": "", "종류": "", "행사가액": "", "청구시작": "", "청구종료": ""}
    # 공백 제거 및 대문자 변환으로 매칭 확률 업
    target_isin = isin_code.strip().upper()
    
    try:
        tables = report.extract_tables()
        for table in tables:
            df = table.to_df()
            # 모든 셀의 내용을 합쳐서 검색
            table_text = df.to_string().replace(" ", "").upper()
            
            if target_isin in table_text:
                print(f"🎯 매칭 성공! 공시명: {report.report_nm}")
                # 종류 추출
                if "전환사채" in table_text or "CB" in table_text: info["종류"] = "CB"
                elif "신주인수권" in table_text or "BW" in table_text: info["종류"] = "BW"
                elif "교환사채" in table_text or "EB" in table_text: info["종류"] = "EB"
                
                # 회차 추출
                round_match = re.search(r'제?(\d+)회', table_text)
                if round_match: info["회차"] = round_match.group(1)
                
                # 행사가액 (원 단위 앞의 숫자들)
                price_match = re.search(r'([\d,]+)원', table_text)
                if price_match: info["행사가액"] = price_match.group(1)
                
                # 날짜 추출 (가장 먼저 나오는 날짜 2개를 청구기간으로 간주)
                dates = re.findall(r'20\d{2}[\.\-\/]\d{2}[\.\-\/]\d{2}', table_text)
                if len(dates) >= 2:
                    info["청구시작"], info["청구종료"] = dates[0], dates[1]
                
                return info
        return None
    except:
        return None

async def main():
    all_values = worksheet.get_all_values()
    if not all_values: return
    
    rows = all_values[1:]
    bgn_de = (datetime.now() - timedelta(days=1100)).strftime('%Y%m%d')

    print(f"🚀 [디버깅 모드] {len(rows)}개 종목 분석 시작...")

    for i, row in enumerate(rows):
        full_name = row[0]
        # 종목명에서 괄호나 숫자 제거하고 순수 이름만 추출
        stock_name = re.sub(r'[()\d\s\w]*$', '', full_name).strip()
        if not stock_name: stock_name = full_name.split()[0]
        
        isin_code = row[1]
        if not isin_code: continue
        
        print(f"🔍 {stock_name} ({isin_code}) 검색 중...")
        
        target = corp_list.find_by_corp_name(stock_name, exactly=False)
        if not target: continue
        
        try:
            # 주요사항보고서(B)와 발행공시(I)를 우선순위로 검색
            reports = target[0].search_filings(bgn_de=bgn_de, p_kind=['B', 'I', 'A'])
            if not reports: continue
        except: continue

        for r in reports:
            found_data = parse_mezzanine_by_isin(r, isin_code)
            if found_data:
                row_idx = i + 2
                # 일괄 업데이트 대신 하나씩 확인하며 입력
                if found_data["회차"]: worksheet.update_cell(row_idx, 3, found_data["회차"])
                if found_data["종류"]: worksheet.update_cell(row_idx, 4, found_data["종류"])
                if found_data["행사가액"]: worksheet.update_cell(row_idx, 5, found_data["행사가액"])
                if found_data["청구시작"]: worksheet.update_cell(row_idx, 6, found_data["청구시작"])
                print(f"✅ {stock_name} 입력 완료!")
                break
        
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
