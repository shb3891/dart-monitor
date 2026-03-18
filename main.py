import os
import json
import time
import re
import requests
import gspread
import xml.etree.ElementTree as ET
from urllib.parse import urlencode, quote
from google.oauth2.service_account import Credentials

# =========================================================
# 환경설정
# =========================================================
SERVICE_KEY = "040c722e03bd9f412852134b7984002f9aaab072aebb672ec28d0792cc996a34"  # encoded key 그대로
SHEET_ID = "1s73BDNtCPe5mOs9EjBE5npEfcaNtYRyWJxRBUmJI-WA"

GCP_SERVICE_ACCOUNT_KEY = os.environ.get("GCP_SERVICE_ACCOUNT_KEY")
if not GCP_SERVICE_ACCOUNT_KEY:
    raise RuntimeError("환경변수 GCP_SERVICE_ACCOUNT_KEY 가 없습니다.")

creds_json = json.loads(GCP_SERVICE_ACCOUNT_KEY)
scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)
worksheet = sh.get_worksheet(0)

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/xml, text/xml, */*",
})

# =========================================================
# 공통 유틸
# =========================================================
def strip_namespace(root: ET.Element) -> ET.Element:
    for elem in root.iter():
        if "}" in elem.tag:
            elem.tag = elem.tag.split("}", 1)[1]
    return root

def is_html_response(text: str) -> bool:
    t = (text or "").lower()
    return "<!doctype html" in t or "<html" in t

def normalize_text(value, default="-"):
    if value is None:
        return default
    value = str(value).strip()
    return value if value else default

def build_url_preserving_encoded_key(base_url: str, params: dict, encoded_service_key: str) -> str:
    """
    encoded key가 이미 % 인코딩된 상태라고 가정하고,
    requests의 params를 쓰지 않고 직접 URL을 만든다.
    """
    safe_params = []
    safe_params.append(f"serviceKey={encoded_service_key}")  # 절대 재인코딩 금지
    for k, v in params.items():
        safe_params.append(f"{quote(str(k), safe='')}={quote(str(v), safe='')}")
    return base_url + "?" + "&".join(safe_params)

def request_xml(base_url: str, params: dict, timeout: int = 10):
    url = build_url_preserving_encoded_key(base_url, params, SERVICE_KEY)
    r = session.get(url, timeout=timeout)
    return r, url

# =========================================================
# XML 파싱
# =========================================================
def infer_bond_type(text: str) -> str:
    t = (text or "").upper()

    # 우선 한글 기준
    if "전환" in text:
        return "CB"
    if "교환" in text:
        return "EB"
    if "신주인수권" in text or "워런트" in text:
        return "BW"

    # 영문 fallback
    if " CB" in t or t.endswith("CB") or "(CB" in t:
        return "CB"
    if " EB" in t or t.endswith("EB") or "(EB" in t:
        return "EB"
    if " BW" in t or t.endswith("BW") or "(BW" in t:
        return "BW"

    return "-"

def extract_round_no(text: str) -> str:
    if not text:
        return "-"
    patterns = [
        r"제\s*(\d+)\s*회",
        r"(\d+)\s*회",
        r"(\d+)\s*차",
    ]
    for p in patterns:
        m = re.search(p, text)
        if m:
            return m.group(1)
    return "1"

def parse_item_to_row(item: ET.Element) -> list:
    """
    시트 C:H 기준:
    C 회사
    D 종류
    E 행사가액
    F 발행일
    G 권리청구 시작일
    H 상태/비고
    """
    # 가능한 필드명 후보를 넓게 잡음
    bond_name = (
        item.findtext("bondIssuNm")
        or item.findtext("bondNm")
        or item.findtext("isinNm")
        or ""
    )

    company = (
        item.findtext("issucoNm")
        or item.findtext("bondIsurNm")
        or item.findtext("korSecnNm")
        or "-"
    )

    bond_type = infer_bond_type(bond_name)

    strike_price = (
        item.findtext("issuConvPrice")
        or item.findtext("convPric")
        or item.findtext("actnPrc")
        or item.findtext("exerPrc")
        or "0"
    )

    issue_date = (
        item.findtext("issuDt")
        or item.findtext("bondIssuDt")
        or item.findtext("pymtDt")
        or "-"
    )

    rights_start = (
        item.findtext("rghtExerStrtDt")
        or item.findtext("subscrBegDt")
        or item.findtext("wrtExecSrttDt")
        or "-"
    )

    company = normalize_text(company, "-")
    bond_type = normalize_text(bond_type, "-")
    strike_price = normalize_text(strike_price, "0")
    issue_date = normalize_text(issue_date, "-")
    rights_start = normalize_text(rights_start, "-")

    return [company, bond_type, strike_price, issue_date, rights_start]

def parse_xml_response(xml_text: str):
    try:
        root = ET.fromstring(xml_text)
        root = strip_namespace(root)

        # 에러 메시지 먼저 체크
        result_msg = root.findtext(".//resultMsg") or root.findtext(".//returnReasonCode")
        result_code = root.findtext(".//resultCode") or root.findtext(".//returnAuthMsg")

        item = root.find(".//item")
        if item is None:
            return None, f"NO_ITEM resultCode={result_code} resultMsg={result_msg}"

        row = parse_item_to_row(item)
        return row, "OK"

    except ET.ParseError as e:
        return None, f"XML_PARSE_ERROR: {e}"
    except Exception as e:
        return None, f"PARSE_ERROR: {e}"

# =========================================================
# API 조회
# =========================================================
def fetch_bond_info(isin: str):
    """
    여러 방식으로 순차 시도:
    1) seibro 기존 endpoint + isin
    2) seibro 기존 endpoint + bondIsin
    필요시 여기 fallback 추가 가능
    """
    candidates = [
        (
            "https://api.seibro.or.kr/openapi/service/BondSvc/getBondIssuInfo",
            {"isin": isin}
        ),
        (
            "https://api.seibro.or.kr/openapi/service/BondSvc/getBondIssuInfo",
            {"bondIsin": isin}
        ),
    ]

    last_error = "UNKNOWN"

    for idx, (base_url, params) in enumerate(candidates, start=1):
        try:
            r, final_url = request_xml(base_url, params, timeout=10)

            print("=" * 100)
            print(f"[{isin}] TRY {idx}")
            print("URL:", final_url)
            print("STATUS:", r.status_code)
            print("CONTENT-TYPE:", r.headers.get("Content-Type"))
            print("BODY_HEAD:", r.text[:300].replace("\n", " "))

            if r.status_code != 200:
                last_error = f"HTTP_{r.status_code}"
                continue

            if is_html_response(r.text):
                last_error = "HTML_RESPONSE"
                continue

            parsed_row, status = parse_xml_response(r.text)
            if parsed_row:
                return parsed_row, "OK"

            last_error = status

        except requests.Timeout:
            last_error = "TIMEOUT"
        except requests.RequestException as e:
            last_error = f"REQUEST_ERROR: {e}"
        except Exception as e:
            last_error = f"UNKNOWN_ERROR: {e}"

        time.sleep(0.7)

    return ["-", "-", "0", "-", "-"], last_error

# =========================================================
# 메인 실행
# =========================================================
def main():
    all_values = worksheet.get_all_values()
    if len(all_values) < 2:
        print("업데이트할 데이터가 없습니다.")
        return

    rows = all_values[1:]  # 2행부터 끝까지
    print(f"총 {len(rows)}개 종목 업데이트 시작")

    output_values = []
    status_values = []

    for idx, row in enumerate(rows, start=2):
        try:
            stock_name = row[0].strip() if len(row) > 0 else ""
            isin = row[1].strip() if len(row) > 1 else ""

            if not isin or isin == "-":
                output_values.append(["-", "-", "0", "-", "-"])
                status_values.append(["NO_ISIN"])
                print(f"[{idx}] {stock_name} - ISIN 없음")
                continue

            data_row, status = fetch_bond_info(isin)
            output_values.append(data_row)
            status_values.append([status])

            print(f"[{idx}] {stock_name} / {isin} -> {status} / {data_row}")

            # 트래픽/차단 방지
            time.sleep(0.8)

        except Exception as e:
            output_values.append(["-", "-", "0", "-", "-"])
            status_values.append([f"ROW_ERROR: {e}"])
            print(f"[{idx}] 행 처리 중 에러: {e}")

    # C:G 데이터 업데이트
    end_row = len(output_values) + 1
    worksheet.update(
        values=output_values,
        range_name=f"C2:G{end_row}"
    )

    # H열 상태 업데이트
    worksheet.update(
        values=status_values,
        range_name=f"H2:H{end_row}"
    )

    print("시트 업데이트 완료")

if __name__ == "__main__":
    main()
