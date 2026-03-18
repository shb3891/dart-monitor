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

def get_xml_data(url, params):
    try:
        res = requests.get(url, params=params, timeout=5)
        if res.status_code == 200:
            return ET.fromstring(res.text)
    except: return None
    return None

def get_mezzanine_info(isin):
    # 기본값 설정
    round_val, b_type, price, start_date = "-", "-", "0", "-"
    
    # [Step 1] 기본정보 및 발행일 (상세기능 3번 활용)
    base_url = "http://api.seibro.or.kr/openapi/service/BondSvc/getBondIssuInfo"
    root = get_xml_data(base_url, {'serviceKey': SERVICE_KEY, 'isin': isin})
    if root is not None:
        item = root.find('.//item')
        if item is not None:
            nm = item.findtext('bondIssuNm', '')
            r_match = re.search(r'(\d+)회', nm)
            round_val = r_match.group(1) if r_match else "1"
            b_type = "CB" if "전환" in nm else ("BW" if "신주" in nm else ("EB" if "교환" in nm else "사채"))

    # [Step 2] 권리청구 시작일 (상세기능 5번: 조기상환 정보 활용)
    opt_url = "http://api.seibro.or.kr/openapi/service/BondSvc/getOptnPutCallInfo"
    root = get_xml_data(opt_url, {'serviceKey': SERVICE_KEY, 'isin': isin})
    if root is not None:
        # 행사시작일(exerStartDt) 태그를 찾아 가장 빠른 날짜 추출
        dates = [i.text for i in root.findall('.//exerStartDt') if i.text]
        if dates:
            start_date = min(dates)

    # [Step 3] 최신 행사가액 (상세기능 1번: 주식관련 권리행사 현황)
    stock_url = "http://api.seibro.or.kr/openapi/service/BondSvc/getStockRelBondRightExSittInfo"
    root = get_xml_data(stock_url, {'serviceKey': SERVICE_KEY, 'isin': isin})
    if root is not None:
        # 가장 최근 리픽싱된 행사가액(issuConvPrice) 가져오기
        p_val = root.findtext('.//issuConvPrice')
        if p_val and p_val != '0':
            price = p_val

    return [round_val, b_type, price, start_date]

async def main():
    all_values = worksheet.get_all_values()
    rows = all_values[1:]
    final_payload = [] # 시트에 한 번에 뿌릴 데이터 리스트

    print(f"🚀 메자닌 3단 분석 시작: 총 {len(rows)}건")

    for i, row in enumerate(rows):
        isin = row[1].strip() if len(row) > 1 else ""
        if not isin.startswith('KR'):
            final_payload.append(["-", "-", "0", "-"])
            continue
            
        print(f"🔍 [{i+1}/{len(rows)}] {isin} 분석 중...")
        data = get_mezzanine_info(isin)
        final_payload.append(data)
        await asyncio.sleep(0.3) # 예탁원 API 매너 타임

    # [중요] 429 에러 방지: 리스트를 통째로 한 번에 업데이트 (C2부터 F열까지)
    if final_payload:
        worksheet.update(f"C2:F{len(final_payload)+1}", final_payload)
        print("✅ 시트 업데이트가 완료되었습니다! (범위: C2:F열)")

if __name__ == "__main__":
    asyncio.run(main())
