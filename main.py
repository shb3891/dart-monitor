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
            # 데이터프레임의 모든 내용을 문자열로 변환
            raw_text = df.to_string().replace(" ", "").upper()
            
            if target_isin in raw_text:
                res = {"회차": "", "종류": "", "가격": "", "날짜": ""}
                
                # 종류 판별
                if "전환사채" in raw_text or "CB" in raw_text: res["종류"] = "CB"
                elif "신주인수권" in raw_text or "BW" in raw_text: res["종류"] = "BW"
                elif "교환사채" in raw_text or "EB" in raw_text: res["종류"] = "EB"
                
                # 회차 (숫자만 추출)
                round_match = re.search(r'제?\s*(\d+)\s*회', raw_text)
                if round_match: res["회차"] = round_match.group(1)
                
                # 가액 (숫자+원)
                price_match = re.search(r'([\d,]+)원', raw_text)
                if price_match: res["가격"] = price_match.group(1)
                
                # 청구시작일 (가장 빠른 날짜)
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
    # 확실한 발행 정보를 위해 3년치 검색
    bgn_de = (datetime.now() - timedelta(days=1100)).strftime('%Y%m%d')

    print(f"🚀 [정밀 분석 모드] {len(rows)}개 종목 분석을 시작합니다.")

    for i, row in enumerate(rows):
        stock_name = row[0].strip()
        isin_code = row[1].strip().upper() # B열 예탁원 코드
        
        if not isin_code or not stock_name: continue
        
        print(f"🔍 [{i+1}/{len(rows)}] {stock_name} ({isin_code}) 본문 파싱 중...")
        
        try:
            # 주요사항보고서(발행결정)를 최우선으로 검색
            reports = dart.search_filings(corp_name=stock_name, bgn_de=bgn_de, p_kind='B')
            # 주요사항에 없으면 정기공시(사업보고서 등) 검색
            if not reports:
                reports = dart.search_filings(corp_name=stock_name, bgn_de=bgn_de, p_kind='A')
            
            if not reports:
                print(f"   - 관련 공시를 찾지 못했습니다.")
                continue

            success = False
            for r in reports:
                data = parse_mezzanine_details(r, isin_code)
                if data:
                    row_idx = i + 2
                    # 찾은 데이터 시트에 기록
                    if data["회차"]: worksheet.update_cell(row_idx, 3, data["회차"])
                    if data["종류"]: worksheet.update_cell(row_idx, 4, data["종류"])
                    if data["가격"]: worksheet.update_cell(row_idx, 5, data["가격"])
                    if data["날짜"]: worksheet.update_cell(row_idx, 6, data["날짜"])
                    print(f"   ✅ 데이터 입력 완료!")
                    success = True
                    break
            
            if not success:
                print(f"   - ISIN 매칭 실패")
                
        except Exception as e:
            print(f"   - 에러 발생: {e}")
            
        await asyncio.sleep(1.5) # DART 서버 부하 방지

if __name__ == "__main__":
    asyncio.run(main())
