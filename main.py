import dart_fss as dart
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

def parse_mezzanine_details(report, target_isin):
    """공시 본문에서 ISIN과 일치하는 행의 데이터를 정밀 추출합니다."""
    try:
        tables = report.extract_tables()
        for table in tables:
            df = table.to_df()
            raw_text = df.to_string().replace(" ", "").upper()
            
            if target_isin in raw_text:
                res = {"회차": "", "종류": "", "가격": "", "날짜": ""}
                
                if any(k in raw_text for k in ["전환사채", "CB"]): res["종류"] = "CB"
                elif any(k in raw_text for k in ["신주인수권", "BW"]): res["종류"] = "BW"
                elif any(k in raw_text for k in ["교환사채", "EB"]): res["종류"] = "EB"
                
                round_match = re.search(r'제?(\d+)회', raw_text)
                if round_match: res["회차"] = round_match.group(1)
                
                price_match = re.search(r'([\d,]+)원', raw_text)
                if price_match: res["가격"] = price_match.group(1)
                
                dates = re.findall(r'20\d{2}[\.\-\/]\d{2}[\.\-\/]\d{2}', raw_text)
                if dates: res["날짜"] = sorted(dates)[0]
                
                return res
        return None
    except:
        return None

async def main():
    all_values = worksheet.get_all_values()
    if not all_values: return
    
    rows = all_values[1:]
    bgn_de = (datetime.now() - timedelta(days=1100)).strftime('%Y%m%d')

    print(f"🚀 [에러 수정 모드] {len(rows)}개 종목 분석을 재시작합니다.")

    # 종목 리스트를 가져오는 가장 안정적인 방법 사용
    corp_list = dart.get_corp_list()

    for i, row in enumerate(rows):
        stock_name = row[0].strip()
        isin_code = row[1].strip().upper()
        
        if not isin_code or not stock_name: continue
        
        print(f"🔍 [{i+1}/{len(rows)}] {stock_name} ({isin_code}) 검색 중...")
        
        try:
            # 1. 종목 찾기
            corp = corp_list.find_by_corp_name(stock_name, exactly=True)
            if not corp:
                corp = corp_list.find_by_corp_name(stock_name, exactly=False)
            
            if not corp:
                print(f"   - 종목을 찾을 수 없습니다.")
                continue

            # 2. 해당 종목의 공시 검색 (발행공시-I, 주요사항-B 위주)
            reports = corp[0].search_filings(bgn_de=bgn_de, p_kind=['B', 'I', 'A'])
            
            if not reports:
                print(f"   - 관련 공시가 없습니다.")
                continue

            success = False
            for r in reports:
                data = parse_mezzanine_details(r, isin_code)
                if data:
                    row_idx = i + 2
                    if data["회차"]: worksheet.update_cell(row_idx, 3, data["회차"])
                    if data["종류"]: worksheet.update_cell(row_idx, 4, data["종류"])
                    if data["가격"]: worksheet.update_cell(row_idx, 5, data["가격"])
                    if data["날짜"]: worksheet.update_cell(row_idx, 6, data["날짜"])
                    print(f"   ✅ [성공] {stock_name} 데이터 입력 완료!")
                    success = True
                    break
            
            if not success:
                print(f"   - 본문 내 ISIN 매칭 실패")
                
        except Exception as e:
            print(f"   - 에러 발생: {e}")
            
        await asyncio.sleep(1.5)

if __name__ == "__main__":
    asyncio.run(main())
