import dart_fss as dart
import pandas as pd
import asyncio
from telegram import Bot
from datetime import datetime, timedelta

# --- 설정 정보 (수정 금지) ---
DART_API_KEY = 'bfc4e4e445de4727ae0bcc27e80ba5cf0e3818e6'
TELEGRAM_TOKEN = '8491277145:AAHwHfaG1q-5ZjExFu8o3T9T6X5c8HlLSlI'
CHAT_ID = '536635522'
SHEET_ID = '1s73BDNtCPe5mOs9EjBE5npEfcaNtYRyWJxRBUmJI-WA'
SHEET_URL = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv'

dart.set_api_key(api_key=DART_API_KEY)
corp_list = dart.get_corp_list()

async def fetch_disclosure(bot, stock, start_date):
    stock_name = str(stock).strip()
    try:
        target = corp_list.find_by_corp_name(stock_name, exactly=True)
        if not target or target[0].corp_name != stock_name: return

        reports = target[0].search_filings(bgn_de=start_date)
        if not reports: return
        
        for r in reports:
            if any(k in r.report_nm for k in ['전환가액', '신주인수권', '조정', '리픽싱']):
                msg = (
                    f"🚨 [자동 감시 알림] {stock_name}\n"
                    f"📄 {r.report_nm}\n"
                    f"📅 날짜: {r.rcept_dt}\n"
                    f"🔗 https://dart.fss.or.kr/dsaf001/main.do?rcpNo={r.rcept_no}"
                )
                await bot.send_message(chat_id=CHAT_ID, text=msg)
    except:
        pass

async def main():
    bot = Bot(token=TELEGRAM_TOKEN)
    try:
        df = pd.read_csv(SHEET_URL)
        stocks = df['종목명'].dropna().unique().tolist()
    except: return

    # 매일 자동 실행되므로 최근 3일치만 확인 (누락 방지)
    start_date = (datetime.now() - timedelta(days=3)).strftime('%Y%m%d')
    
    # 서버 부하를 줄이기 위해 하나씩 순차 처리
    for stock in stocks:
        await fetch_disclosure(bot, stock, start_date)
        await asyncio.sleep(0.2) 

if __name__ == "__main__":
    asyncio.run(main())
