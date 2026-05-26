"""
bulk_match.py — 보유 종목 일괄 매칭 (초기 1회 또는 신규 종목 추가 시)

처리 로직:
1. 메인 시트(시트1)에서 ISIN과 채권명 읽기
2. 주식코드매칭 시트의 A열(ISIN) 중복 체크 → 신규만 처리
3. 각 ISIN 매칭 시도 (matching.py 사용)
4. 결과를 주식코드매칭 시트에 추가
5. FAILED 종목은 텔레그램 알람 발송

실행: GitHub Actions에서 수동 (workflow_dispatch)
또는: 메인시트 변경 감지 후 자동 (별도 트리거)
"""

import os
import json
import time
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

from matching import (
    load_aliases_from_sheet,
    load_dart_corp_codes,
    match_isin,
    send_telegram_alert,
    format_match_failure_alert,
)


SEIBRO_KEY = os.environ.get('SEIBRO_KEY')
DART_KEY   = os.environ.get('DART_API_KEY')
SHEET_ID   = os.environ.get('SHEET_ID')
TG_TOKEN   = os.environ.get('TELEGRAM_BOT_TOKEN')
TG_CHAT    = os.environ.get('TELEGRAM_CHAT_ID')

MATCH_SHEET = '주식코드매칭'
ALIAS_SHEET = '별칭사전'
MAIN_SHEET  = None  # gspread.get_worksheet(0) — 첫 번째 시트


# ============================================================
# [Google Sheets 연결]
# ============================================================
creds_json = json.loads(os.environ.get('GCP_SERVICE_ACCOUNT_KEY'))
creds = Credentials.from_service_account_info(creds_json, scopes=[
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
])
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)


# ============================================================
# [상태 → 표시 매핑]
# ============================================================
STATUS_DISPLAY = {
    'AUTO':   '✅ 자동매칭',
    'ALIAS':  '⚠️ 별칭매칭(검토)',
    'MANUAL': '🔒 수동확정',
    'FAILED': '❌ 매칭실패',
}


# ============================================================
# [메인]
# ============================================================
def main():
    today = datetime.now().strftime('%Y-%m-%d')
    print(f"🚀 일괄 매칭 시작 ({today})\n")
    
    # 1. 별칭사전 로드
    print("📚 별칭사전 로드 중...")
    try:
        alias_ws = sh.worksheet(ALIAS_SHEET)
    except Exception:
        print(f"  ❌ '{ALIAS_SHEET}' 시트가 없습니다. setup_matching_sheet.py 먼저 실행하세요.")
        return
    
    aliases = load_aliases_from_sheet(alias_ws)
    print(f"  ✅ 별칭 {len(aliases)}개 로드\n")
    
    # 2. DART 기업코드 로드
    print("📥 DART 기업코드 로드 중...")
    dart_corp, dart_corp_name, dart_name = load_dart_corp_codes(DART_KEY)
    
    # 3. 메인 시트에서 보유 종목 리스트 가져오기
    print("\n📋 메인 시트에서 보유 종목 읽는 중...")
    main_ws = sh.get_worksheet(0)
    main_values = main_ws.get_all_values()
    
    # B열(ISIN), A열(종목명)
    holdings = []
    for i, row in enumerate(main_values[1:], 2):
        if len(row) > 1 and row[1].strip().startswith('KR'):
            isin = row[1].strip()
            name = row[0].strip() if row[0] else ''
            holdings.append((isin, name))
    
    print(f"  ✅ 보유 종목 {len(holdings)}개 발견")
    
    # 4. 매칭시트 기존 ISIN 확인 (중복 방지)
    print("\n📋 기존 매칭 결과 확인 중...")
    try:
        match_ws = sh.worksheet(MATCH_SHEET)
    except Exception:
        print(f"  ❌ '{MATCH_SHEET}' 시트가 없습니다. setup_matching_sheet.py 먼저 실행하세요.")
        return
    
    existing_values = match_ws.get_all_values()
    existing_isins = set()
    for row in existing_values[1:]:
        if row and row[0].strip():
            existing_isins.add(row[0].strip())
    
    new_holdings = [(isin, name) for isin, name in holdings if isin not in existing_isins]
    skipped = len(holdings) - len(new_holdings)
    print(f"  ✅ 신규 매칭 대상: {len(new_holdings)}개 (기존 {skipped}개 스킵)")
    
    if not new_holdings:
        print("\n✨ 신규 매칭 대상 없음. 종료.")
        return
    
    # 5. 매칭 시작
    print(f"\n🔍 매칭 시작...\n")
    results = []
    for i, (isin, name) in enumerate(new_holdings, 1):
        print(f"[{i}/{len(new_holdings)}] {isin}  {name}")
        result = match_isin(
            isin, name,
            SEIBRO_KEY, DART_KEY,
            aliases,
            dart_corp, dart_corp_name, dart_name,
        )
        status_label = STATUS_DISPLAY.get(result['status'], result['status'])
        print(f"          → {status_label} | {result['target_corp_name']} ({result['target_stock_code']})")
        if result['reason']:
            print(f"            └ {result['reason']}")
        
        results.append(result)
        time.sleep(1.0)
    
    # 6. 결과 시트에 저장
    print(f"\n📝 매칭 시트에 저장 중...")
    rows_to_add = []
    for r in results:
        rows_to_add.append([
            r['isin'],
            r['bond_name'],
            r['bond_type'],
            r['call_status'],
            r['issuer_stock_code'],
            r['target_corp_name'],
            r['target_stock_code'],
            r['dart_corp_code'],
            STATUS_DISPLAY.get(r['status'], r['status']),
            r['method'],
            today,  # 최초등록일
            today,  # 최근검증일
            r['reason'],
        ])
    
    # 기존 행 다음부터 append
    start_row = len(existing_values) + 1
    if start_row < 2:
        start_row = 2
    end_row = start_row + len(rows_to_add) - 1
    match_ws.update(rows_to_add, range_name=f'A{start_row}:M{end_row}')
    print(f"  ✅ {len(rows_to_add)}개 행 추가 (A{start_row}:M{end_row})")
    
    # 7. 통계 + 알람
    stats = {'AUTO': 0, 'ALIAS': 0, 'MANUAL': 0, 'FAILED': 0}
    failed_items = []
    alias_items = []
    for r in results:
        stats[r['status']] = stats.get(r['status'], 0) + 1
        if r['status'] == 'FAILED':
            failed_items.append(r)
        elif r['status'] == 'ALIAS':
            alias_items.append(r)
    
    print(f"\n📊 매칭 결과:")
    for s in ['AUTO', 'ALIAS', 'MANUAL', 'FAILED']:
        if stats[s]:
            print(f"  {STATUS_DISPLAY[s]}: {stats[s]}개")
    
    # 8. 텔레그램 알람 — 실패 종목만
    if failed_items and TG_TOKEN and TG_CHAT:
        print(f"\n📱 텔레그램 알람 발송: {len(failed_items)}건")
        for r in failed_items:
            msg = format_match_failure_alert(r)
            send_telegram_alert(TG_TOKEN, TG_CHAT, msg)
            time.sleep(0.5)
    
    # 별칭 케이스도 한 번에 묶어서 통보 (선택)
    if alias_items and TG_TOKEN and TG_CHAT:
        lines = ['⚠️ <b>별칭매칭 종목 (검토 권장)</b>\n']
        for r in alias_items:
            lines.append(f"• {r['bond_name']} ({r['isin']})")
            lines.append(f"  └ {r['reason']}")
        msg = '\n'.join(lines)
        send_telegram_alert(TG_TOKEN, TG_CHAT, msg)
    
    print(f"\n🏁 완료!")
    print(f"👉 https://docs.google.com/spreadsheets/d/{SHEET_ID}")


if __name__ == '__main__':
    main()
