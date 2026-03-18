import requests
import xml.etree.ElementTree as ET

SERVICE_KEY = '040c722e03bd9f412852134b7984002f9aaab072aebb672ec28d0792cc996a34'
BASE_URL = "http://api.seibro.or.kr/openapi/service/BondSvc"

# 스프레드시트 상단 3개 ISIN으로 테스트
TEST_ISINS = ['KR6001081G15', 'KR6002324D70', 'KR6002711E62']

ENDPOINTS = [
    'getBondIssuInfo',    # 채권 기본정보
    'getBondConvInfo',    # CB 전환정보
    'getBondExchInfo',    # EB 교환정보
    'getBondWrantInfo',   # BW 신주인수권정보
]

def diagnose(isin, endpoint):
    params = {'serviceKey': SERVICE_KEY, 'isin': isin}
    try:
        r = requests.get(f"{BASE_URL}/{endpoint}", params=params, timeout=10)
        print(f"\n{'='*60}")
        print(f"ISIN: {isin} | endpoint: {endpoint}")
        print(f"HTTP Status: {r.status_code}")
        print(f"--- Raw XML ---")
        print(r.text[:2000])  # 앞 2000자만 출력
        
        root = ET.fromstring(r.content)
        item = root.find('.//item')
        if item is not None:
            print(f"\n--- 파싱된 필드 목록 ---")
            for child in item:
                print(f"  {child.tag}: {child.text}")
        else:
            print("⚠ <item> 태그 없음")
    except Exception as e:
        print(f"🔥 에러: {e}")

for isin in TEST_ISINS:
    diagnose(isin, 'getBondIssuInfo')
    diagnose(isin, 'getBondConvInfo')
```

이 스크립트를 돌리면 콘솔에 실제 XML과 필드명이 전부 찍힙니다. 그 결과를 여기 붙여넣어 주시면, **정확한 필드명으로 메인 코드를 바로 완성**해드릴 수 있어요.

---

결과를 보내주실 때 이런 형태로 오면 됩니다:
```
ISIN: KR6001081G15 | endpoint: getBondIssuInfo
--- 파싱된 필드 목록 ---
  bondIssuNm: 만호제강제15회무보증전환사채
  issuDt: 20220315
  ...
