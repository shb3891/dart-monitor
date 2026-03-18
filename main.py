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
    """공시 본문의 모든 표에서 ISIN 코드를 찾아 상세 정보를 추출합니다."""
    try:
        tables = report.extract_tables()
        for table in tables:
            df = table.to_df()
            # 표의 모든 내용을 공백 없이 합침
            table_content = df.to_string().replace(" ", "").upper()
            
            if target_isin in table_content:
                res = {"회차": "", "종류": "", "가격": "", "날짜": ""}
                
                # 1. 종류 추출
                if any(x in table_content for x in ["전환사채", "CB"]): res["종류"] = "CB"
                elif any(x in table_content for x in ["신주인수권", "BW"]): res["종류"] = "BW"
                elif any(x in table_content for x in ["교환사채", "EB"]): res["종류"] = "EB"
                
                # 2. 회차 추출 (제N회)
                round_match = re.search(r'제?\s*(\d+)\s*회', table_content)
                if round_match: res["회차"] = round_match.group(1)
                
                # 3. 행사가액 (숫자+원)
                price_match = re.search(r'([\d,]+)원', table_content)
                if price_match: res["가격"] = price_match.group(1)
                
                # 4. 청구일자 (20XX.XX.XX 형태 중 첫 번째)
                dates = re.findall(r'20\d{2}[\.\-\/]\d{2}[\.\-\/]\d{2}', table_content)
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

    print(f"🚀 [최종 수정 모드] {len(rows)}개 종목 분석 시작 (검색 인자 수정 완료)")

    # 가장 안정적인 종목 리스트 로드
    corp_list = dart.get_corp_list()

    for i, row in enumerate(rows):
        stock_name = row[0].strip()
        isin_code = row[1].strip().upper()
        
        if not isin_code or not stock_name: continue
        
        print(f"🔍 [{i+1}/{len(rows)}] {stock_name} ({isin_code}) 분석 시도...")
        
        try:
            # 1. 종목 객체 찾기
            corp = corp_list.find_by_corp_name(stock_name, exactly=True)
            if not corp:
                corp = corp_list.find_by_corp_name(stock_name, exactly=False)
            
            if not corp:
                print(f"   - 종목 검색 실패")
                continue

            # 2. 공시 검색 (에러 원인인 p_kind 인자 제거 및 표준 검색 수행)
            # 주요사항보고서(B), 발행공시(I), 정기공시(A) 순차 검색
            reports = corp[0].search_filings(bgn_de=bgn_de)
            
            if not reports:
                print(f"   - 관련 공시 없음")
                continue

            success = False
            for r in reports:
                # 3. 본문 파싱
                data = parse_mezzanine_details(r, isin_code)
                if data:
                    row_idx = i + 2
                    # 찾은 데이터 즉시 시트 업데이트
                    if data["회차"]: worksheet.update_cell(row_idx, 3, data["회차"])
                    if data["종류"]: worksheet.update_cell(row_idx, 4, data["종류"])
                    if data["가격"]: worksheet.update_cell(row_idx, 5, data["가격"])
                    if data["날짜"]: worksheet.update_cell(row_idx, 6, data["날짜"])
                    print(f"   ✅ [성공] 데이터 입력 완료")
                    success = True
                    break
            
            if not success:
                print(f"   - ISIN 매칭 실패")
                
        except Exception as e:
            print(f"   - 처리 중 오류: {e}")
            
        await asyncio.sleep(1.5)

if __name__ == "__main__":
    asyncio.run(main())
