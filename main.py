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
corp_list = dart.get_corp_list()

def find_mezzanine_info(report):
    """공시 본문을 분석하여 회차, 종류, 행사가액을 추출합니다."""
    try:
        # 공시 제목에서 회차와 종류 1차 추출 (가장 정확함)
        # 예: 주요사항보고서(전환사채권발행결정)
        title = report.report_nm
        m_type = "CB" if "전환사채" in title else ("BW" if "신주인수권" in title else ("EB" if "교환사채" in title else ""))
        
        tables = report.extract_tables()
        for table in tables:
            df = table.to_df()
            text = df.to_string().replace(" ", "")
            
            # 본문 내 '제 N회' 검색
            round_match = re.search(r'제?\s*(\d+)\s*회', text)
            # 행사가액 검색
            price_match = re.search(r'([\d,]{4,})원', text)
            # 권리청구 시작일 검색
            date_match = re.findall(r'20\d{2}[\.\-\/]\d{2}[\.\-\/]\d{2}', text)
            
            if round_match or price_match:
                return {
                    "회차": round_match.group(1) if round_match else "1",
                    "종류": m_type if m_type else "CB",
                    "가격": price_match.group(1) if price_match else "",
                    "날짜": sorted(date_match)[0] if date_match else ""
                }
        return None
    except:
        return None

async def main():
    all_values = worksheet.get_all_values()
    if not all_values: return
    
    rows = all_values[1:]
    # 최근 3년 공시 대상
    bgn_de = (datetime.now() - timedelta(days=1100)).strftime('%Y%m%d')

    print(f"🚀 [DART 전용 모드] {len(rows)}개 종목 분석 시작")

    for i, row in enumerate(rows):
        stock_name = row[0].strip() # A열 종목명
        if not stock_name: continue
        
        print(f"🔍 [{i+1}/{len(rows)}] {stock_name} 공시 분석 중...")
        
        try:
            corp = corp_list.find_by_corp_name(stock_name, exactly=True)
            if not corp:
                corp = corp_list.find_by_corp_name(stock_name, exactly=False)
            
            if not corp: continue

            # '발행결정' 관련 공시만 필터링해서 검색 (이게 핵심입니다)
            reports = corp[0].search_filings(bgn_de=bgn_de, p_kind='B')
            
            success = False
            for r in reports:
                if "발행결정" in r.report_nm:
                    data = find_mezzanine_info(r)
                    if data:
                        row_idx = i + 2
                        # 데이터 일괄 업데이트 (C, D, E, F열)
                        worksheet.update(f"C{row_idx}:F{row_idx}", [[data["회차"], data["종류"], data["가격"], data["날짜"]]])
                        print(f"   ✅ {stock_name} 입력 성공! (제{data['회차']}회 {data['종류']})")
                        success = True
                        break
            
            if not success:
                print(f"   - 발행공시를 찾지 못했습니다.")

        except Exception as e:
            if "429" in str(e):
                print("⚠️ 시트 제한 발생! 10초 대기...")
                await asyncio.sleep(10)
            else:
                print(f"   - 에러: {e}")
            
        await asyncio.sleep(2) # 안전한 속도 유지

if __name__ == "__main__":
    asyncio.run(main())
