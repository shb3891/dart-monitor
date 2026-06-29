# dart-monitor

📊 메자닌 채권(CB/EB/BW) 포트폴리오 자동 모니터링 시스템 - 데이터 수집부

## 🎯 무엇을 하나요?

113개+ 메자닌 채권을 DART/SEIBRO API에서 자동으로 수집하여 Google Sheet에 정리합니다.

## 📦 구성

### 메인 스크립트

| 파일 | 역할 | 실행 주기 |
|---|---|---|
| `main.py` | 시트 자동 갱신 (포트폴리오 + 풋콜스케줄 + 자본변동) | 평일 장중 10분 / 장외 1시간 |
| `audit_check.py` | 감사보고서 추적 | 매일 KST 09:00 |
| `update_holdings.py` | 보유내역 xlsx 자동 처리 | holdings/ 폴더에 push 시 |

### 워크플로우

```
.github/workflows/
├── hourly_monitor.yml    # main.py
├── daily_audit.yml       # audit_check.py
├── update_holdings.yml   # update_holdings.py
└── monthly_recheck.yml   # 보류
```

## 🚀 사용법

### 보유내역 업데이트 (월 1회 권장)

```
1. holdings/ 폴더에 새 xlsx 업로드
   파일명 예시: 260730_보유내역.xlsx
   
2. GitHub에 commit
   → 자동으로 워크플로우 실행
   → 약 3-5분 후 결과 알림 (텔레봇)
```

### 수동 실행

```
1. https://github.com/shb3891/dart-monitor/actions
2. 워크플로우 선택
3. [Run workflow] 클릭
```

## 🔧 환경설정

### Secrets (GitHub Settings → Secrets and variables → Actions)

```
DART_API_KEY           - DART OpenAPI 키
SEIBRO_KEY             - SEIBRO OpenAPI 키
GCP_SERVICE_ACCOUNT_KEY - Google Service Account JSON
SHEET_ID               - Google Sheet ID
TELEGRAM_BOT_TOKEN     - Telegram 봇 토큰
TELEGRAM_CHAT_ID       - Telegram 채팅 ID
```

## 📊 연결된 Google Sheet

- 시트 ID: `1s73BDNtCPe5mOs9EjBE5npEfcaNtYRyWJxRBUmJI-WA`
- 시트 8개: 포트폴리오, 풋콜스케줄, 자본변동이력, 보유내역_변동이력, 주식코드, 주식코드매칭, 별칭사전, 감사보고서

## 🔗 관련 레포

- [susung-alert](https://github.com/shb3891/susung-alert) - 알림 발송

## 📖 자세한 문서

전체 시스템 매뉴얼은 별도 문서 참조.

---

**Tech Stack:** Python 3.11 + gspread + Google Sheets API + DART OpenAPI + SEIBRO OpenAPI + GitHub Actions
