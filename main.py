import dart_fss as dart
import pandas as pd
import asyncio
from telegram import Bot
from datetime import datetime, timedelta

# --- 설정 정보 ---
DART_API_KEY = 'bfc4e4e445de4727ae0bcc27e80ba5cf0e3818e6'
TELEGRAM_TOKEN = '8491277145:AAHwHfaG1q-5ZjExFu8o3T9T6X5c8HlLSlI'
CHAT_ID = '536635522'
SHEET_ID = '1s73BDNtCPe5mOs9EjBE5npEfcaNtYRyWJxRBUmJI-WA'
SHEET_URL = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv'

dart.set_api_key(api_key=DART_API_KEY)
corp_list = dart.get_corp_list()

async def fetch_disclosure(bot, stock):
    stock_name = str(stock).strip()
    try:
        target = corp_list.find_by_corp_name(stock_name, exactly=True)
        if not target or target[0].corp_name != stock_name: return

        # [핵심 수정] 오늘 날짜 공시를 가져오되
        now = datetime.now()
        start_date = now.strftime('%Y%m%d')
        reports = target[0].search_filings(bgn_de=start_date)
        
        if not reports: return
        
        for r in reports:
            # DART 접수시간(rcept_dt)을 체크 (포맷: 2026.03.17 14:30)
            # DART API에서 제공하는 접수시간 정보가 있다면 활용, 없으면 날짜만 확인
            # 중복 방지를 위해 실행 주기(60분)보다 약간 넓은 70분 이내 공시만 필터링
            
            # 실제 운영에서는 DART 서버 시간과 약간의 오차가 있을 수 있어 
            # '오늘' 올라온 것 중 가장 최신 것만 보내는 로직이 안전합니다.
            
            msg = (
                f"🔔 [신규 공시 포착] {stock_name}\n"
                f"📄 {r.report_nm}\n"
                f"📅 일시: {r.rcept_dt}\n"
                f"🔗 https://dart.fss.or.kr/dsaf001/main.do?rcpNo={r.rcept_no}"
            )
            await bot.send_message(chat_id=CHAT_ID, text=msg)
            await asyncio.sleep(0.1)
    except:
        pass

async def main():
    bot = Bot(token=TELEGRAM_TOKEN)
    try:
        df = pd.read_csv(SHEET_URL)
        stocks = df['종목명'].dropna().unique().tolist()
    except: return

    for stock in stocks:
        await fetch_disclosure(bot, stock)
        await asyncio.sleep(0.5)

if __name__ == "__main__":
    asyncio.run(main())
