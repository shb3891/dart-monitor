import asyncio
import os
import json
import gspread
import requests
import xml.etree.ElementTree as ET
import re
from google.oauth2.service_account import Credentials

# --- [환경 설정] ---
SERVICE_KEY = '040c722e03bd9f412852134b7984002f9aaab072aebb672ec28d0792cc996a34'
SHEET_ID = '1s73BDNtCPe5mOs9EjBE5npEfcaNtYRyWJxRBUmJI-WA'

# 구글 시트 인증
creds_json = json.loads(os.environ.get('GCP_SERVICE_ACCOUNT_KEY'))
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)
worksheet = sh.get_worksheet(0)

def get_seibro_data(isin_code):
    """SEibro API 호출 및 데이터 추출"""
    # 과장님이 신청하신 서비스의 실제 엔드포인트 중 가장 기본 정보를 주는 곳입니다.
    url = "http://api.seibro.or.kr/openapi/service/BondSvc/getBondIssuInfo"
    params = {'serviceKey': SERVICE_KEY, 'isin': isin_code}
    
    try:
        res = requests.get(url, params=params, timeout=10)
        # XML 응답이 정상인지 확인
        if res.status_code == 200:
            if "<item>" not in res.text:
                return ["-", "-", "0", "-"]
                
            root = ET.fromstring(res.text)
            item = root.find('.//item')
            if item is not None:
                bond_nm = item.findtext('bondIssuNm', '') # 발행 명칭
                
                # 회차 추출
                round_match = re.search(r'(\d+)회', bond_nm)
                r_val = round_match.group(1) if round_match else "1"
                
                # 종류 판별
                b_type = "CB" if "전환" in bond_nm else ("BW" if "신주" in bond_nm else ("EB" if "교환" in bond_nm else "사채"))
                
                # 가격 및 날짜
                price = item.findtext('issuConvPrice', '0') # 행사가액
                date = item.findtext('issuDt', '') # 발행일
                
                return [r_val, b_type, price, date]
    except Exception:
        pass
    return ["-", "-", "0", "-"]

async def main():
    all_values = worksheet.get_all_values()
    if not all_values: return
    
    rows = all_values[1:] # 헤더 제외
    update_data = [] # 한 번에 업데이트할 리스트

    print(f"🚀 [SEibro 실전 업데이트] 총 {len(rows)}건 시작합니다.")

    for i, row in enumerate(rows):
        # B열(인덱스 1)의 ISIN 코드 확인
        isin = row[1].strip() if len(row) > 1 else ""
        
        if not isin.startswith('KR'):
            update_data.append(["", "", "", ""])
            continue

        print(f"🔍 [{i+1}/{len(rows)}] {isin} 처리 중...")
        data = get_seibro_data(isin)
        update_data.append(data)
        
        # 1초당 2~3건 정도로 속도 조절 (API 안정성)
        await asyncio.sleep(0.4)

    # 💎 일괄 업데이트 (C2부터 F열 마지막까지)
    if update_data:
        end_row = len(update_data) + 1
        worksheet.update(f"C2:F{end_row}", update_data)
        print(f"✅ 시트 업데이트가 완료되었습니다! (범위: C2:F{end_row})")

if __name__ == "__main__":
    asyncio.run(main())
