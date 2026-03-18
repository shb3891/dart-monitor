import asyncio
import os
import json
import gspread
import requests
import xml.etree.ElementTree as ET
from google.oauth2.service_account import Credentials

# --- 설정 정보 ---
# 방금 주신 SEibro 서비스키 (Decoding 키 권장)
SERVICE_KEY = '040c722e03bd9f412852134b7984002f9aaab072aebb672ec28d0792cc996a34'
SHEET_ID = '1s73BDNtCPe5mOs9EjBE5npEfcaNtYRyWJxRBUmJI-WA'

creds_json = json.loads(os.environ.get('GCP_SERVICE_ACCOUNT_KEY'))
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)
worksheet = sh.get_worksheet(0)

def get_seibro_bond_info(isin_code):
    """SEibro 채권기본정보조회 API 호출"""
    url = "http://api.seibro.or.kr/openapi/service/BondSvc/getBondIssuInfo"
    params = {
        'serviceKey': SERVICE_KEY,
        'isin': isin_code,
        'numOfRows': '1',
        'pageNo': '1'
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            root = ET.fromstring(response.text)
            item = root.find('.//item')
            
            if item is not None:
                # 1. 발행명칭 (예: 성호전자 2회 전환사채)
                bond_nm = item.findtext('bondIssuNm', '')
                
                # 2. 회차 및 종류 판별
                import re
                round_match = re.search(r'(\d+)회', bond_nm)
                round_val = round_match.group(1) if round_match else "1"
                
                b_type = "CB" if "전환" in bond_nm else ("BW" if "신주" in bond_nm else ("EB" if "교환" in bond_nm else "채권"))
                
                # 3. 발행일/상장일 등 날짜 (필요시 추가)
                issu_dt = item.findtext('issuDt', '') # 발행일
                
                return {
                    "회차": round_val,
                    "종류": b_type,
                    "명칭": bond_nm,
                    "발행일": issu_dt
                }
    except Exception as e:
        print(f"      ⚠️ {isin_code} 조회 실패: {e}")
    return None

async def main():
    all_values = worksheet.get_all_values()
    if not all_values: return
    
    rows = all_values[1:]
    print(f"🚀 [SEibro 마스터 모드] {len(rows)}개 종목 업데이트 시작")

    for i, row in enumerate(rows):
        isin_code = row[1].strip() # B열 ISIN 코드
        if not isin_code or not isin_code.startswith('KR'): continue
        
        print(f"🔍 [{i+1}/{len(rows)}] {isin_code} 조회 중...")
        
        data = get_seibro_bond_info(isin_code)
        
        if data:
            row_idx = i + 2
            # C, D열 (회차, 종류) 업데이트
            # E, F열(가격, 날짜)은 채권정보 외에 '행사가액' 전용 API가 필요할 수 있으나 우선 기본정보부터 채웁니다.
            try:
                worksheet.update(f"C{row_idx}:D{row_idx}", [[data["회차"], data["종류"]]])
                print(f"   ✅ {data['명칭']} 업데이트 완료")
            except Exception as e:
                print(f"   ⚠️ 시트 쓰기 지연 (10초 대기): {e}")
                await asyncio.sleep(10)
        
        # API 및 구글 시트 할당량 보호 (초당 1건 정도)
        await asyncio.sleep(1.2)

if __name__ == "__main__":
    asyncio.run(main())
