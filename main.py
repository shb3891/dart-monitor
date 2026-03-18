import dart_fss as dart
import pandas as pd
import asyncio
import os
import json
import gspread
import re
from google.oauth2.service_account import Credentials
from telegram import Bot
from datetime import datetime

# --- 설정 정보 (기존 시트 ID 사용 완료) ---
DART_API_KEY = 'bfc4e4e445de4727ae0bcc27e80ba5cf0e3818e6'
TELEGRAM_TOKEN = '8491277145:AAHwHfaG1q-5ZjExFu8o3T9T6X5c8HlLSlI'
CHAT_ID = '536635522'
SHEET_ID = '1s73BDNtCPe5mOs9EjBE5npEfcaNtYRyWJxRBUmJI-WA' # 과장님 기존 시트 ID

# 구글 시트 연결 설정 (Secrets 사용)
creds_json = json.loads(os.environ.get('GCP_SERVICE_ACCOUNT_KEY'))
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)
worksheet = sh.get_worksheet(0)

dart.set_api_key(api_key=DART_API_KEY)
corp_list = dart.get_corp_list()

def extract_mezzanine_info(report):
    """[핵심 추가] 공시 본문에서 정밀하게 데이터를 파싱합니다."""
    # 과장님이 요청하신 항목들 (초기화)
    info = {
        "종목명": report.corp_name,
        "행사가액": "",
        "청구시작": "",
        "청구종료": "",
        "리픽싱비율": "",
        "조기상환시작": "",
        "상환지급일": "",
        "콜옵션시작": "",
        "콜옵션종료": ""
    }
    try:
        # 봇이 공시 본문의 '표'들을 다 긁어옴
        tables = report.extract_tables()
        for table in tables:
            df = table.to_df()
            text = df.to_string() # 표 하나를 통째로 글자로 변환
            
            # --- 항목별 키워드 매칭 및 데이터 추출 (정규표현식 Regex 사용) ---
            
            # 1. 행사가액/전환가액 (금액 추출: 1,925원 등)
            if "전환가액" in text or "행사가액" in text:
                match = re.search(r'([\d,]+)\s*원', text)
                if match: info["행사가액"] = match.group(1).replace(",", "") # "," 제거 (1925)
            
            # 2. 권리청구기간 (날짜 추출: 2025.03.27 등)
            if "청구기간" in text:
                dates = re.findall(r'\d{4}\s*\.\s*\d{2}\s*\.\s*\d{2}', text)
                if len(dates) >= 2:
                    info["청구시작"], info["청구종료"] = dates[0], dates[1]
            
            # 3. 리픽싱 (비율 추출: 70% 등)
            if "조정 후 최저가액" in text:
                match = re.search(r'([\d.]+)\s*%', text)
                if match: info["리픽싱비율"] = match.group(1)
            
            # 4. 조기상환 청구기간 (날짜 추출)
            if "조기상환청구" in text:
                dates = re.findall(r'\d{4}\s*\.\s*\d{2}\s*\.\s*\d{2}', text)
                if len(dates) >= 2:
                    info["조기상환시작"], info["상환지급일"] = dates[0], dates[1]
            
            # 5. 매도청구권 (Call Option - 날짜 추출)
            if "매도청구권" in text or "콜옵션" in text:
                dates = re.findall(r'\d{4}\s*\.\s*\d{2}\s*\.\s*\d{2}', text)
                if len(dates) >= 2:
                    info["콜옵션시작"], info["콜옵션종료"] = dates[0], dates[1]
                    
        return info
    except:
        return info

async def update_sheet_upgraded(stock_name, report, row_idx, rows):
    """시트에 공시 정보를 정밀하게 업데이트합니다."""
    try:
        # 본문 파싱
        m_info = extract_mezzanine_info(report)
        
        # 중복 방지: 이미 적혀있는 링크가 같다면 패스
        # (아까 성공했던 중복 방지 로직 유지)
        header = worksheet.row_values(1)
        try:
            link_col_idx = header.index("공시링크") + 1
        except:
            link_col_idx = 5 # 기본 E열
            
        new_link = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={report.rcept_no}"
        existing_link = rows[row_idx - 2][link_col_idx - 1] if len(rows) > row_idx - 2 else ""
        if new_link == existing_link:
            print(f"✅ {stock_name}: 이미 기록된 공시")
            return

        # --- 과장님 시트 컬럼 위치에 맞춰 업데이트 (중요!) ---
        # 봇이 데이터를 긁어왔어도 시트의 컬럼 위치를 모르면 엉뚱하게 씁니다.
        # 아래 컬럼 번호는 과장님이 보여주신 이미지에 맞춰 예시로 설정했습니다.
        
        worksheet.update_cell(row_idx, 1, m_info["종목명"]) # A열
        worksheet.update_cell(row_idx, 4, report.rcept_dt) # D열: 공시날짜
        worksheet.update_cell(row_idx, 5, new_link) # E열: 링크
        if m_info["청구시작"]:
            worksheet.update_cell(row_idx, 10, m_info["청구시작"]) # J열: 청구시작
            worksheet.update_cell(row_idx, 11, m_info["청구종료"]) # K열: 청구종료
        if m_info["행사가액"]:
            worksheet.update_cell(row_idx, 12, m_info["행사가액"]) # L열: 행사가액
        if m_info["리픽싱비율"]:
            worksheet.update_cell(row_idx, 13, f"{m_info['리픽싱비율']}%") # M열: 리픽싱
        # (조기상환, 콜옵션은 컬럼 위치를 몰라 아직 제외했습니다.)

        print(f"✅ {stock_name} 정밀 업데이트 완료")
    except Exception as e:
        print(f"❌ {stock_name} 업데이트 실패: {e}")

async def main():
    bot = Bot(token=TELEGRAM_TOKEN)
    all_values = worksheet.get_all_values()
    if not all_values: return
    
    header = all_values[0]
    rows = all_values[1:]
    
    try:
        # 시트의 '종목명' 컬럼 위치 파악
        name_col_idx = header.index("종목명")
    except ValueError:
        print("❌ '종목명' 컬럼을 찾을 수 없습니다.")
        return

    print(f"🔍 실시간 감시 시작: {len(rows)}개 종목 (업그레이드 버전)")

    for i, row in enumerate(rows):
        stock = row[name_col_idx]
        if not stock: continue
        
        target = corp_list.find_by_corp_name(stock, exactly=True)
        if not target: continue
        
        try:
            today_str = datetime.now().strftime('%Y%m%d')
            reports = target[0].search_filings(bgn_de=today_str)
        except:
            continue
        
        if not reports: continue
        
        for r in reports:
            # 업그레이드된 업데이트 함수 호출
            await update_sheet_upgraded(stock, r, i + 2, rows)
            
            # 텔레그램 알림 발송
            msg = f"🔔 [새 공시] {stock}\n📄 {r.report_nm}\n🔗 https://dart.fss.or.kr/dsaf001/main.do?rcpNo={r.rcept_no}"
            await bot.send_message(chat_id=CHAT_ID, text=msg)
            
            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
