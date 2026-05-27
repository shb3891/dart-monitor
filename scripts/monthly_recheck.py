"""
monthly_recheck.py — 매월 자동 재검증

매월 1일 KST 9시에 자동 실행 (수동 실행도 가능).

동작:
1. 메인시트(시트1) + 주식코드매칭 시트 둘 다 로드
2. 메인시트 ISIN 중 매칭시트에 없는 신규 종목 발견 → 자동 매칭 + 시트 추가
3. 매칭시트 기존 행들을 모두 재매칭 (SEIBRO/DART 재조회)
4. 기존값 vs 새값 비교
   - 동일: L열(최근검증일)만 갱신
   - 다름: 변경사항 누적 → 텔레 알람 (한 번에 묶어서)
5. SEIBRO 일시 오류는 스킵 (다음 달 재시도)
6. 변경 감지된 경우 사용자 확인 후 수동 갱신 (자동 덮어쓰기 X)
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
)


SEIBRO_KEY = os.environ.get('SEIBRO_KEY')
DART_KEY   = os.environ.get('DART_API_KEY')
SHEET_ID   = os.environ.get('SHEET_ID')
TG_TOKEN   = os.environ.get('TELEGRAM_BOT_TOKEN')
TG_CHAT    = os.environ.get('TELEGRAM_CHAT_ID')

MATCH_SHEET = '주식코드매칭'
ALIAS_SHEET = '별칭사전'


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
# [기존 매칭 시트 데이터 읽기]
# ============================================================
def load_existing_matches(ws):
    """주식코드매칭 시트에서 기존 데이터 dict로 로드.
    
    Returns:
        dict: {isin: {row_idx, bond_name, target_name, target_code, corp_code, status, ...}}
    """
    rows = ws.get_all_values()
    existing = {}
    for i, row in enumerate(rows[1:], start=2):  # 2번 행부터 (1행은 헤더)
        if not row or not row[0].strip().startswith('KR'):
            continue
        isin = row[0].strip()
        def g(idx): return row[idx].strip() if len(row) > idx else ''
        existing[isin] = {
            'row_idx':      i,
            'bond_name':    g(1),
            'bond_type':    g(2),
            'call_status':  g(3),
            'issuer_code':  g(4),
            'target_name':  g(5),   # F열
            'target_code':  g(6),   # G열
            'corp_code':    g(7),   # H열
            'status':       g(8),   # I열 (UI 라벨)
            'method':       g(9),
            'first_date':   g(10),
            'last_date':    g(11),
            'memo':         g(12),
        }
    return existing


# ============================================================
# [메인]
# ============================================================
def main():
    today = datetime.now().strftime('%Y-%m-%d')
    print(f"🔄 매월 재검증 시작 ({today})\n")

    # === 1. 시트 로드 ===
    try:
        alias_ws = sh.worksheet(ALIAS_SHEET)
        match_ws = sh.worksheet(MATCH_SHEET)
    except Exception as e:
        print(f"❌ 필수 시트 없음: {e}")
        return

    # === 2. 별칭사전 + DART 코드 로드 ===
    print("📚 별칭사전 로드 중...")
    aliases = load_aliases_from_sheet(alias_ws)
    print(f"  ✅ 별칭 {len(aliases)}개 로드\n")

    print("📥 DART 기업코드 로드 중...")
    dart_corp, dart_corp_name, dart_name = load_dart_corp_codes(DART_KEY)

    # === 3. 메인시트(시트1)에서 ISIN 리스트 확인 (신규 종목 발견용) ===
    print("\n📋 메인시트에서 보유 종목 읽는 중...")
    main_ws = sh.get_worksheet(0)
    main_values = main_ws.get_all_values()
    main_holdings = {}
    for row in main_values[1:]:
        if len(row) > 1 and row[1].strip().startswith('KR'):
            isin = row[1].strip()
            name = row[0].strip() if row[0] else ''
            main_holdings[isin] = name
    print(f"  ✅ 메인시트 종목: {len(main_holdings)}개")

    # === 4. 매칭시트 기존 데이터 ===
    print("\n📋 주식코드매칭 시트 기존 데이터 로드...")
    existing = load_existing_matches(match_ws)
    print(f"  ✅ 기존 매칭: {len(existing)}개\n")

    # === 5. 신규 종목 발견 ===
    new_isins = [isin for isin in main_holdings if isin not in existing]
    if new_isins:
        print(f"🆕 신규 종목 {len(new_isins)}개 발견 — 자동 매칭 시도...")
    else:
        print(f"ℹ 신규 종목 없음")

    # === 6. 매칭 작업 ===
    # 6-1. 신규 종목 매칭
    new_match_results = []
    for isin in new_isins:
        name = main_holdings[isin]
        print(f"\n  🆕 {isin} {name}")
        result = match_isin(
            isin, name, SEIBRO_KEY, DART_KEY,
            aliases, dart_corp, dart_corp_name, dart_name,
        )
        status_label = STATUS_DISPLAY.get(result['status'], result['status'])
        print(f"      → {status_label} | {result['target_corp_name']} ({result['target_stock_code']})")
        new_match_results.append(result)
        time.sleep(1.0)

    # 6-2. 기존 종목 재검증
    print(f"\n🔄 기존 {len(existing)}개 종목 재검증 시작...\n")
    changes = []        # 변경 감지 목록
    seibro_failures = [] # SEIBRO 오류 목록
    last_date_updates = []  # L열만 업데이트할 목록 (row_idx, today)

    for idx, (isin, old) in enumerate(existing.items(), 1):
        print(f"[{idx}/{len(existing)}] {isin} {old['bond_name']}")
        new = match_isin(
            isin, old['bond_name'],
            SEIBRO_KEY, DART_KEY,
            aliases, dart_corp, dart_corp_name, dart_name,
        )

        # SEIBRO 오류는 스킵 (기존값 유지, 알람 없음, 다음 달 재시도)
        if new['status'] == 'FAILED' and 'SEIBRO' in new.get('reason', ''):
            print(f"  ⏭ SEIBRO 일시 오류 — 스킵 (기존값 유지)")
            seibro_failures.append({'isin': isin, 'bond_name': old['bond_name']})
            continue

        # 핵심 4개 컬럼 비교
        diff_fields = []
        if old['target_name'] != new['target_corp_name']:
            diff_fields.append(('공시대상종목명', old['target_name'], new['target_corp_name']))
        if old['target_code'] != new['target_stock_code']:
            diff_fields.append(('공시대상주식코드', old['target_code'], new['target_stock_code']))
        if old['corp_code'] != new['dart_corp_code']:
            diff_fields.append(('DARTcorp_code', old['corp_code'], new['dart_corp_code']))

        new_status_label = STATUS_DISPLAY.get(new['status'], new['status'])

        if diff_fields:
            print(f"  ⚠️ 변경 감지: {len(diff_fields)}개 필드")
            for fname, old_v, new_v in diff_fields:
                print(f"     - {fname}: '{old_v}' → '{new_v}'")
            changes.append({
                'isin': isin,
                'bond_name': old['bond_name'],
                'old_status': old['status'],
                'new_status': new_status_label,
                'diffs': diff_fields,
                'is_manual_lock': '🔒' in old['status'],  # 수동확정 행 표시
            })
        else:
            # 동일 — L열만 갱신
            last_date_updates.append((old['row_idx'], today))

        time.sleep(1.0)

    # === 7. 시트 갱신 ===
    print(f"\n📝 시트 갱신 중...")

    # 7-1. 신규 종목 추가
    if new_match_results:
        new_rows = []
        for r in new_match_results:
            new_rows.append([
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
        start_row = max([v['row_idx'] for v in existing.values()] + [1]) + 1
        end_row = start_row + len(new_rows) - 1
        match_ws.update(new_rows, range_name=f'A{start_row}:M{end_row}')
        print(f"  ✅ 신규 {len(new_rows)}개 행 추가")

    # 7-2. 변경 없는 행들 L열 일괄 갱신 (batch update)
    if last_date_updates:
        batch_data = []
        for row_idx, date in last_date_updates:
            batch_data.append({
                'range': f'L{row_idx}',
                'values': [[date]],
            })
        # 50개씩 묶어서 batch update
        BATCH_SIZE = 50
        for i in range(0, len(batch_data), BATCH_SIZE):
            chunk = batch_data[i:i + BATCH_SIZE]
            match_ws.batch_update(chunk)
            time.sleep(0.5)
        print(f"  ✅ {len(last_date_updates)}개 행의 최근검증일 갱신")

    # === 8. 텔레그램 알람 ===
    print(f"\n📱 알람 발송 중...")
    summary_lines = [
        f"🔄 <b>매월 재검증 완료</b>",
        f"📅 {today}",
        f"",
        f"📊 결과 요약:",
        f"• 전체 검증: {len(existing)}개",
        f"• 신규 추가: {len(new_match_results)}개",
        f"• 변경 감지: {len(changes)}개",
        f"• SEIBRO 오류 (스킵): {len(seibro_failures)}개",
        f"• 변동 없음: {len(last_date_updates)}개",
    ]

    # 신규 종목 정보
    if new_match_results:
        summary_lines.append('')
        summary_lines.append('🆕 <b>신규 추가</b>')
        for r in new_match_results:
            status_label = STATUS_DISPLAY.get(r['status'], r['status'])
            summary_lines.append(f"• {r['bond_name']} ({r['isin']})")
            summary_lines.append(f"  └ {status_label} | {r['target_corp_name']}")

    # 변경 감지 종목
    if changes:
        summary_lines.append('')
        summary_lines.append('⚠️ <b>변경 감지 (확인 필요)</b>')
        for c in changes:
            lock_mark = ' 🔒' if c['is_manual_lock'] else ''
            summary_lines.append(f"")
            summary_lines.append(f"• <b>{c['bond_name']}</b> ({c['isin']}){lock_mark}")
            for fname, old_v, new_v in c['diffs']:
                summary_lines.append(f"  └ {fname}")
                summary_lines.append(f"     이전: {old_v or '(없음)'}")
                summary_lines.append(f"     신규: {new_v or '(없음)'}")
        summary_lines.append('')
        summary_lines.append('📝 시트 직접 수정 후 매칭상태 변경 권장')

    msg = '\n'.join(summary_lines)
    
    if TG_TOKEN and TG_CHAT:
        send_telegram_alert(TG_TOKEN, TG_CHAT, msg)
        print(f"  ✅ 텔레그램 알람 전송 완료")

    # === 9. 콘솔 통계 ===
    print(f"\n🏁 완료!")
    print(f"  📊 전체 검증: {len(existing)}개")
    print(f"  🆕 신규 추가: {len(new_match_results)}개")
    print(f"  ⚠️ 변경 감지: {len(changes)}개")
    print(f"  ⏭ SEIBRO 오류 스킵: {len(seibro_failures)}개")
    print(f"  ✅ 변동 없음: {len(last_date_updates)}개")
    print(f"  👉 https://docs.google.com/spreadsheets/d/{SHEET_ID}")


if __name__ == '__main__':
    main()
