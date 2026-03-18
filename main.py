import asyncio
import os
import json
import gspread
import requests
import xml.etree.ElementTree as ET
import re
from google.oauth2.service_account import Credentials

# --- [설정] 송 과장님 인증 정보 ---
SERVICE_KEY = '040c722e03bd9f412852134b7984002f9aaab072aebb672ec28d0792cc996a34'
SHEET_ID = '1s73BDNtCPe5mOs9EjBE5npEfcaNtYRyWJxRBUmJI-WA'

creds_json = json.loads(os.environ.get('GCP_SERVICE_ACCOUNT_KEY'))
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)
worksheet = sh.get_worksheet(0)

def get_seibro_data(isin):
    """SEibro API에서 데이터를 긁어오되, 로그를 상세히 남깁니다."""
    # 채권기본정보조회 엔드포인트
    url = "http://api.seibro.or.kr/openapi/service/BondSvc/getBondIssuInfo"
    params = {'serviceKey': SERVICE_KEY, 'isin': isin}
    
    try:
        res = requests.get(url, params=params, timeout=7)
        if res.status_code == 200:
            # 인증키 미활성화 시 'SERVICE KEY IS NOT REGISTERED' 등이 포함됨
            if "<item>" not in res.text:
                if "SERVICE KEY" in res.text:
                    return ["키 활성화 대기", "-", "0", "-"]
                return ["데이터 없음", "-", "0", "-"]
            
            root = ET.fromstring(res.text)
            item = root.find('.//item')
            if item is not None:
                bond_nm = item.findtext('bondIssuNm', '')
                r_match = re.search(r'(\d+)회', bond_nm)
                r_val = r_match.group(1) if r_match else "1"
                b_type = "CB" if "전환" in bond_nm else ("EB" if "교환" in bond_nm else "BW")
                price = item.findtext('issuConvPrice', '0')
                dt = item.findtext('issuDt', '')
                return [r_val, b_type, price, dt]
    except Exception as e:
        print(f"      ⚠️ {isin} 상세 에러: {e}")
    return ["조회 실패", "-", "0", "-"]

async def main():
    all_values = worksheet.get_all_values()
    if not all_values: return
    
    data_rows = all_values[1:]
    payload = []

    print(f"🚀 [최종 마스터] 총 {len(data_rows)}건 업데이트 시작...")

    for i, row in enumerate(data_rows):
        isin = row[1].strip() if len(row) > 1 else ""
        if not isin.startswith('KR'):
            payload.append(["", "", "", ""])
            continue
            
        print(f"🔍 [{i+1}/{len(data_rows)}] {isin} 조회 중...")
        result = get_seibro_data(isin)
        payload.append(result)
        
        # 429 에러 방지 및 안정적인 조회를 위해 0.6초 대기
        await asyncio.sleep(0.6)

    # 💎 마지막에 일괄 업데이트 (이게 가장 안전합니다)
    if payload:
        worksheet.update(f"C2:F{len(payload)+1}", payload)
        print(f"✅ 시트 동기화 완료! (범위: C2:F{len(payload)+1})")

if __name__ == "__main__":
    asyncio.run(main())
