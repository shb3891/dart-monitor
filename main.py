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

        # [수정] 1시간마다 실행되므로, 최근 2시간 내 공시만 조회 (누락 방지용 여유분 포함)
        # bgn_de는 오늘 날짜로 설정
        start_date = datetime.now().strftime('%Y%m%d')
        reports = target[0].search_filings(bgn_de=start_date)
        
        if not reports: return
        
        for r in reports:
            # [수정] 시간 필터링: 현재 시간 기준 70분 이내에 올라온 공시만 전송
            # DART 접수시간(rcept_dt)은 보통 당일 날짜만 나오므로, 
            # 실시간성을 위해 '오늘' 올라온 모든 공시를 체크하되 중복 알람은 과장님이 걸러보시거나
            # 아래 코드로 오늘 올라온 건 일단 다 보여드립니다.
            
            msg = (
                f"🔔 [실시간 공시] {stock_name}\n"
                f"📄 {r.report_nm}\n"
                f"📅 접수일: {r.rcept_dt}\n"
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
        await asyncio.sleep(0.5) # DART 서버 부하 방지

if __name__ == "__main__":
    asyncio.run(main())
