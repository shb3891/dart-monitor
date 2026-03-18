import asyncio
import os
import json
import gspread
import requests
import xml.etree.ElementTree as ET
import re
import time
from google.oauth2.service_account import Credentials

# --- [설정] ---
SERVICE_KEY = '040c722e03bd9f412852134b7984002f9aaab072aebb672ec28d0792cc996a34'
SHEET_ID = '1s73BDNtCPe5mOs9EjBE5npEfcaNtYRyWJxRBUmJI-WA'

creds_json = json.loads(os.environ.get('GCP_SERVICE_ACCOUNT_KEY'))
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)
worksheet = sh.get_worksheet(0)

def get_debug_info(isin):
    """API 응답 내용을 로그에 상세히 찍어 원인을 파악합니다."""
    # 가장 기본이 되는 '채권기본정보조회' 엔드포인트
    url = "http://api.seibro.or.kr/openapi/service/BondSvc/getBondIssuInfo"
    params = {'serviceKey': SERVICE_KEY, 'isin': isin}
    
    try:
        r = requests.get(url, params=params, timeout=5)
        print(f"📡 [{isin}] 응답 상태: {r.status_code}")
        
        # 만약 데이터가 없다고 나오면, ISIN 코드 끝자리를 하나 떼고 시도해봅니다. (우회책)
        if "<item>" not in r.text and len(isin) == 12:
            short_isin = isin[:-1]
            print(f"⚠️  데이터 없음. 끝자리 제외 시도: {short_isin}")
            params['isin'] = short_isin
            r = requests.get(url, params=params, timeout=5)

        if "<item>" in r.text:
            print(f"✅ 데이터 발견! 추출을 시작합니다.")
            root = ET.fromstring(r.text)
            item = root.find('.//item')
            nm = item.findtext('bondIssuNm', '-')
            # 발행일, 종류 등 파싱
            return [
                re.search(r'(\d+)회', nm).group(1) if re.search(r'(\d+)회', nm) else "1",
                "CB" if "전환" in nm else ("EB" if "교환" in nm else "BW"),
                item.findtext('issuConvPrice') or "0",
                item.findtext('issuDt') or "-",
                "-" # 권리청구일은 다른 API에서 가져와야 하지만 일단 패스
            ]
        else:
            # API가 에러 메시지를 뱉는지 확인
            print(f"❌ API 응답 내용: {r.text[:100]}...")
            
    except Exception as e:
        print(f"🔥 에러 발생: {e}")
    
    return ["-", "-", "0", "-", "-"]

async def main():
    all_values = worksheet.get_all_values()
    rows = all_values[1:6] # 🚀 너무 많으니까 상위 5개만 먼저 테스트해봅시다!
    
    print(f"🧪 [디버깅 모드] 상위 {len(rows)}개 종목만 테스트합니다.")
    
    results = []
    for i, row in enumerate(rows):
        isin = row[1].strip()
        data = get_debug_info(isin)
        results.append(data)
        await asyncio.sleep(1)

    # 시트 상단에 테스트 결과 업데이트
    if results:
        worksheet.update(f"C2:G{len(results)+1}", results)
        print("🏁 테스트 결과가 시트에 반영되었습니다.")

if __name__ == "__main__":
    asyncio.run(main())
