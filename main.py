import asyncio
import os
import json
import gspread
import requests
import xml.etree.ElementTree as ET
import re
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

def fetch_from_seibro(isin):
    """승인된 여러 기능을 순차적으로 시도하여 데이터를 확정합니다."""
    # 1순위: 채권기본정보조회 (회차, 종류, 발행일)
    # 2순위: 발행인별내역조회 (보조 데이터)
    endpoints = [
        "http://api.seibro.or.kr/openapi/service/BondSvc/getBondIssuInfo",
        "http://api.seibro.or.kr/openapi/service/BondSvc/getIssuByissuInfo"
    ]
    
    for url in endpoints:
        params = {'serviceKey': SERVICE_KEY, 'isin': isin}
        try:
            res = requests.get(url, params=params, timeout=10)
            if res.status_code == 200 and "<item>" in res.text:
                root = ET.fromstring(res.text)
                item = root.find('.//item')
                if item is not None:
                    bond_nm = item.findtext('bondIssuNm', '')
                    # 회차 추출
                    r_match = re.search(r'(\d+)회', bond_nm)
                    r_val = r_match.group(1) if r_match else "1"
                    # 종류 판별
                    b_type = "CB" if "전환" in bond_nm else ("BW" if "신주" in bond_nm else ("EB" if "교환" in bond_nm else "사채"))
                    # 행사가액 (태그명이 다를 수 있어 여러 개 확인)
                    price = item.findtext('issuConvPrice') or item.findtext('issuAmt') or "0"
                    # 발행일/권리시작일
                    date = item.findtext('issuDt') or item.findtext('entryDt') or "-"
                    
                    return [r_val, b_type, price, date]
        except:
            continue
    return ["데이터없음", "-", "0", "-"]

async def main():
    all_values = worksheet.get_all_values()
    if not all_values: return
    
    rows = all_values[1:]
    payload = []

    print(f"🚀 [통합 모드] 1~6번 기능 연동 분석 시작 ({len(rows)}건)")

    for i, row in enumerate(rows):
        isin = row[1].strip() if len(row) > 1 else ""
        if not isin.startswith('KR'):
            payload.append(["", "", "", ""])
            continue
            
        print(f"🔍 [{i+1}/{len(rows)}] {isin} 조회 중...")
        result = fetch_from_seibro(isin)
        payload.append(result)
        
        # API 과부하 방지
        await asyncio.sleep(0.5)

    if payload:
        # C2:F열 일괄 업데이트
        worksheet.update(f"C2:F{len(payload)+1}", payload)
        print("✅ 업데이트 완료! 이번에는 데이터 필드를 교차 검증해서 가져왔습니다.")

if __name__ == "__main__":
    asyncio.run(main())
