import asyncio
import os
import json
import gspread
import requests
import xml.etree.ElementTree as ET
import re
from google.oauth2.service_account import Credentials

# --- [설정] 과장님 정보 고정 ---
SERVICE_KEY = '040c722e03bd9f412852134b7984002f9aaab072aebb672ec28d0792cc996a34'
SHEET_ID = '1s73BDNtCPe5mOs9EjBE5npEfcaNtYRyWJxRBUmJI-WA'

creds_json = json.loads(os.environ.get('GCP_SERVICE_ACCOUNT_KEY'))
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)
worksheet = sh.get_worksheet(0)

def get_seibro_smart(isin):
    """실패 사유를 더 정확히 리턴합니다."""
    url = "http://api.seibro.or.kr/openapi/service/BondSvc/getBondIssuInfo"
    params = {'serviceKey': SERVICE_KEY, 'isin': isin}
    
    try:
        res = requests.get(url, params=params, timeout=7)
        if res.status_code == 200:
            # 1. 인증키 자체가 아직 활성화 안 된 경우
            if "SERVICE KEY IS NOT REGISTERED" in res.text:
                return ["인증키 대기", "-", "0", "-"]
            
            # 2. 데이터가 없는 경우
            if "<item>" not in res.text:
                return ["정보 없음", "-", "0", "-"]
            
            root = ET.fromstring(res.text)
            item = root.find('.//item')
            if item is not None:
                nm = item.findtext('bondIssuNm', '')
                r_match = re.search(r'(\d+)회차?', nm)
                return [
                    r_match.group(1) if r_match else "1",
                    "CB" if "전환" in nm else ("EB" if "교환" in nm else "BW"),
                    item.findtext('issuConvPrice', '0'), # 행사가액
                    item.findtext('issuDt', '') # 발행일
                ]
    except: pass
    return ["조회 지연", "-", "0", "-"]

async def main():
    all_values = worksheet.get_all_values()
    if not all_values: return
    
    rows = all_values[1:]
    payload = []

    print(f"🚀 [최종 보정] {len(rows)}건 처리 시작...")

    for i, row in enumerate(rows):
        isin = row[1].strip() if len(row) > 1 else ""
        if not isin.startswith('KR'):
            payload.append(["", "", "", ""])
            continue
            
        print(f"🔍 [{i+1}/{len(rows)}] {isin} 조회 중...")
        payload.append(get_seibro_smart(isin))
        await asyncio.sleep(0.6) # API 부하 방지용

    if payload:
        # C2부터 F열까지 일괄 업데이트
        worksheet.update(f"C2:F{len(payload)+1}", payload)
        print("✅ 보정 업데이트 완료!")

if __name__ == "__main__":
    asyncio.run(main())
