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

def extract_value(item, tags):
    """API 응답 태그 중 실제 데이터가 있는 첫 번째 값을 반환합니다."""
    for tag in tags:
        val = item.findtext(tag)
        if val and val.strip() and val.strip() not in ['0', '-', 'None']:
            return val.strip()
    return None

def get_mezzanine_full_info(isin):
    # 순서: [회차, 종류, 행사가액, 발행일, 권리청구시작일]
    res_data = ["-", "-", "0", "-", "-"]
    
    # [1] 기본정보 API (회차, 종류, 발행일 추출)
    url1 = "http://api.seibro.or.kr/openapi/service/BondSvc/getBondIssuInfo"
    try:
        r = requests.get(url1, params={'serviceKey': SERVICE_KEY, 'isin': isin}, timeout=5)
        if "<item>" in r.text:
            item = ET.fromstring(r.text).find('.//item')
            nm = item.findtext('bondIssuNm', '')
            # 회차 추출
            res_data[0] = re.search(r'(\d+)회', nm).group(1) if re.search(r'(\d+)회', nm) else "1"
            # 종류 판별
            res_data[1] = "CB" if "전환" in nm else ("BW" if "신주" in nm else ("EB" if "교환" in nm else "사채"))
            # 발행일 (F열용)
            res_data[3] = item.findtext('issuDt') or "-"
    except: pass

    # [2] 행사가액 API (E열용)
    url2 = "http://api.seibro.or.kr/openapi/service/BondSvc/getStockRelBondRightExSittInfo"
    try:
        r = requests.get(url2, params={'serviceKey': SERVICE_KEY, 'isin': isin}, timeout=5)
        if "<item>" in r.text:
            item = ET.fromstring(r.text).find('.//item')
            price = extract_value(item, ['issuConvPrice', 'nextConvPrice', 'stkptPrC', 'currConvPrice'])
            if price: res_data[2] = price
    except: pass

    # [3] 권리청구 시작일 API (G열용)
    url3 = "http://api.seibro.or.kr/openapi/service/BondSvc/getOptnPutCallInfo"
    try:
        r = requests.get(url3, params={'serviceKey': SERVICE_KEY, 'isin': isin}, timeout=5)
        if "<item>" in r.text:
            items = ET.fromstring(r.text).findall('.//item')
            dates = []
            for it in items:
                d = extract_value(it, ['exerStartDt', 'optnExerStartDt', 'putExerStartDt'])
                if d: dates.append(d)
            if dates: res_data[4] = min(dates) # 가장 빠른 행사 가능 날짜
    except: pass

    return res_data

async def main():
    all_values = worksheet.get_all_values()
    rows = all_values[1:]
    
    print(f"🚀 [데이터 동기화] 총 {len(rows)}건 시작 (F:발행일 / G:청구일)")
    
    results = []
    for i, row in enumerate(rows):
        isin = row[1].strip() if len(row) > 1 else ""
        if not isin.startswith('KR'):
            results.append(["-", "-", "0", "-", "-"])
            continue
            
        print(f"🔍 [{i+1}/{len(rows)}] {isin} 분석 중...")
        results.append(get_mezzanine_full_info(isin))
        await asyncio.sleep(0.4) # API 과부하 방지

    # C열(3번째)부터 G열(7번째)까지 데이터 한꺼번에 업데이트
    batch_size = 30
    for j in range(0, len(results), batch_size):
        chunk = results[j : j + batch_size]
        range_str = f"C{j+2}:G{j + len(chunk) + 1}"
        try:
            worksheet.update(range_str, chunk)
            print(f"✅ {range_str} 업데이트 완료")
            time.sleep(1)
        except Exception as e:
            print(f"❌ {range_str} 실패: {e}")

if __name__ == "__main__":
    asyncio.run(main())
