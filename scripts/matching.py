"""
matching.py — ISIN 매칭 핵심 로직 라이브러리

다른 스크립트에서 import해서 사용:
- bulk_match.py        : 초기 113개 일괄 매칭
- monthly_recheck.py   : 매월 재검증
- alert.py             : /match 명령 처리
- main.py              : 데이터 갱신 시 매칭 결과 활용

매칭 결과는 항상 dict 형태로 반환:
{
    'isin', 'bond_name', 'bond_type', 'call_status',
    'issuer_stock_code', 'target_corp_name', 'target_stock_code',
    'dart_corp_code', 'status', 'method', 'reason'
}

상태값(status) 정의:
- 'AUTO'   : ✅ 자동매칭 성공
- 'ALIAS'  : ⚠️ 별칭사전 매칭
- 'MANUAL' : 🔒 수동확정
- 'FAILED' : ❌ 매칭실패 (텔레 알람 대상)

매칭방법(method) 정의:
- 'ISIN_AUTO'    : ISIN 4-9자리 변환 → DART 매칭
- 'SEIBRO_EB'    : SEIBRO getXrcStkStatInfo → DART 매칭
- 'ALIAS_DICT'   : 별칭사전 매칭
- 'MANUAL_INPUT' : 사용자 /match 명령 입력
- 'FAILED'       : 매칭 실패
"""

import os
import re
import io
import zipfile
import xml.etree.ElementTree as ET
import requests


SEIBRO_BASE = "https://seibro.or.kr/OpenPlatform/callOpenAPI.jsp"
DART_BASE   = "https://opendart.fss.or.kr/api"


# ============================================================
# [별칭사전 시트 로드]
# ============================================================
def load_aliases_from_sheet(worksheet):
    """별칭사전 시트에서 별칭 딕셔너리 로드.
    
    시트 구조: A=원본표기 / B=DART표기 / C=등록방법 / D=등록일
    
    Returns:
        dict: {원본표기: DART표기}
    """
    try:
        records = worksheet.get_all_values()
        aliases = {}
        for row in records[1:]:  # 헤더 제외
            if len(row) >= 2 and row[0].strip() and row[1].strip():
                aliases[row[0].strip()] = row[1].strip()
        return aliases
    except Exception as e:
        print(f"  ⚠ 별칭사전 로드 실패: {e}")
        return {}


# ============================================================
# [DART 기업코드 로드]
# ============================================================
def load_dart_corp_codes(dart_key):
    """DART 기업코드 전체 다운로드.
    
    Returns:
        tuple: (corp_dict, corp_name_dict, name_dict)
            - corp_dict[stock_code] = corp_code
            - corp_name_dict[stock_code] = corp_name
            - name_dict[corp_name] = corp_code
    """
    corp_dict = {}
    corp_name_dict = {}
    name_dict = {}
    
    try:
        r = requests.get(f"{DART_BASE}/corpCode.xml",
                         params={'crtfc_key': dart_key}, timeout=30)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        root = ET.fromstring(z.read('CORPCODE.xml'))
        
        for item in root.findall('.//list'):
            corp_code  = item.findtext('corp_code', '').strip()
            stock_code = item.findtext('stock_code', '').strip()
            corp_name  = item.findtext('corp_name', '').strip()
            if stock_code and len(stock_code) == 6:
                corp_dict[stock_code] = corp_code
                corp_name_dict[stock_code] = corp_name
            if corp_name and corp_code:
                name_dict[corp_name] = corp_code
        
        print(f"  ✅ DART 기업코드 로드: 상장사 {len(corp_dict):,}개, 전체 {len(name_dict):,}개")
    except Exception as e:
        print(f"  ⚠ DART 기업코드 로드 실패: {e}")
    
    return corp_dict, corp_name_dict, name_dict


# ============================================================
# [SEIBRO API 호출]
# ============================================================
def seibro_api(seibro_key, api_id, params_dict):
    """SEIBRO OpenAPI 호출."""
    params_str = ','.join([f"{k}:{v}" for k, v in params_dict.items()])
    full_url = f"{SEIBRO_BASE}?key={seibro_key}&apiId={api_id}&params={params_str}"
    try:
        r = requests.get(full_url, timeout=10)
        r.raise_for_status()
        for enc in ['utf-8', 'euc-kr']:
            try:
                decoded = r.content.decode(enc, errors='strict')
                break
            except UnicodeDecodeError:
                continue
        else:
            decoded = r.content.decode('utf-8', errors='replace')
        cleaned = re.sub(r'<\?xml[^?]*\?>', '', decoded).strip()
        if not cleaned:
            return None
        root = ET.fromstring(cleaned.encode('utf-8'))
        vector = root.find('.//vector')
        if vector is None or vector.get('result', '0') == '0':
            return None
        return root
    except Exception as e:
        print(f"  ⚠ SEIBRO 호출 실패 [{api_id}]: {e}")
        return None


def get_attr(element, tag):
    el = element.find(f'.//{tag}')
    if el is not None:
        return el.get('value', '')
    return ''


# ============================================================
# [채권명 파싱]
# ============================================================
def parse_bond_name(name):
    """채권명에서 (회사명, 회차, 종류, 콜상태) 추출.
    
    예: "다날8CB(콜100%)" → ('다날', '8', 'CB', '콜100%')
    """
    if not name:
        return '', '', '', ''
    
    # 콜상태 추출
    call_status = ''
    m_call = re.search(r'\(콜\s*(\d+)%\)', name)
    if m_call:
        call_status = f'콜{m_call.group(1)}%'
    name_clean = re.sub(r'\(콜[^)]*\)', '', name).strip()
    
    # 회사명 + 회차 + 종류
    m = re.match(r'^(.+?)\s*(\d+)\s*(CB|EB|BW)\s*$', name_clean)
    if m:
        return m.group(1).strip(), m.group(2), m.group(3), call_status
    return name_clean, '', '', call_status


# ============================================================
# [ISIN 변환]
# ============================================================
def isin_to_issuer_stock_code(isin):
    """채권 ISIN에서 발행사 주식코드 추출.
    
    예: KR6043262F84 → 043260
    """
    if not isin or len(isin) < 9 or not isin.startswith('KR6'):
        return ''
    bond_code = isin[3:9]
    if not bond_code.isdigit():
        return ''
    return bond_code[:5] + '0'


def stock_isin_to_code(stock_isin):
    """주식 ISIN(KR7xxxxxxxxxx) → 6자리 주식코드.
    
    예: KR7025550003 → 025550
    """
    if not stock_isin or len(stock_isin) < 9:
        return ''
    code = stock_isin[3:9]
    return code if code.isdigit() else ''


# ============================================================
# [메인 매칭 함수]
# ============================================================
def match_isin(isin, bond_name_hint,
               seibro_key, dart_key,
               aliases,
               dart_corp_dict, dart_corp_name_dict, dart_name_dict):
    """단일 ISIN에 대해 매칭 시도.
    
    Args:
        isin: 채권 ISIN (예: KR6043262F84)
        bond_name_hint: 사용자가 입력한 채권명 (예: 성호전자17CB)
                       SEIBRO 응답이 없을 때 사용. 빈 문자열도 가능.
        seibro_key, dart_key: API 키
        aliases: 별칭사전 dict
        dart_corp_dict, dart_corp_name_dict, dart_name_dict: DART 데이터
    
    Returns:
        dict: 매칭 결과
    """
    result = {
        'isin': isin,
        'bond_name': bond_name_hint,
        'bond_type': '',
        'call_status': '',
        'issuer_stock_code': '',
        'target_corp_name': '',
        'target_stock_code': '',
        'dart_corp_code': '',
        'status': 'FAILED',
        'method': 'FAILED',
        'reason': '',
    }
    
    # === 1단계: 채권명 파싱 (힌트가 있으면) ===
    company_hint, hosu, bond_type_hint, call_status = parse_bond_name(bond_name_hint)
    result['call_status'] = call_status
    
    # === 2단계: SEIBRO에서 정확한 채권명/종류 가져오기 (선택적) ===
    seibro_bond_info = None
    root = seibro_api(seibro_key, 'getBondStatInfo', {'ISIN': isin})
    if root is not None:
        el = root.find('.//result')
        if el is not None:
            secn_nm = get_attr(el, 'KOR_SECN_NM')
            seibro_bond_info = secn_nm
            # SEIBRO 채권명에서 종류 추출
            if 'EB' in secn_nm or '교환' in secn_nm:
                bond_type_hint = 'EB'
            elif 'CB' in secn_nm or '전환' in secn_nm:
                bond_type_hint = 'CB'
            elif 'BW' in secn_nm or '신주인수권' in secn_nm:
                bond_type_hint = 'BW'
    
    result['bond_type'] = bond_type_hint
    
    # === 3단계: 발행사 주식코드 (ISIN 변환) ===
    issuer_code = isin_to_issuer_stock_code(isin)
    result['issuer_stock_code'] = issuer_code
    
    # === 4단계: 종류별 매칭 분기 ===
    if bond_type_hint == 'EB':
        # EB: SEIBRO로 교환대상 추출
        return _match_eb(result, isin, company_hint, seibro_key,
                         aliases, dart_corp_dict, dart_corp_name_dict, dart_name_dict)
    else:
        # CB/BW: 발행사 자신이 공시대상
        return _match_cb_bw(result, issuer_code, company_hint,
                            aliases, dart_corp_dict, dart_corp_name_dict, dart_name_dict)


def _match_cb_bw(result, issuer_code, company_hint,
                 aliases, dart_corp_dict, dart_corp_name_dict, dart_name_dict):
    """CB/BW 매칭: 발행사가 곧 공시대상."""
    
    # 4-1. 주식코드로 DART 매칭
    if issuer_code:
        dart_name = dart_corp_name_dict.get(issuer_code, '')
        dart_code = dart_corp_dict.get(issuer_code, '')
        
        if dart_name and dart_code:
            result['target_corp_name'] = dart_name
            result['target_stock_code'] = issuer_code
            result['dart_corp_code'] = dart_code
            result['status'] = 'AUTO'
            result['method'] = 'ISIN_AUTO'
            return result
    
    # 4-2. ISIN 변환 실패 또는 DART에 없음 → 채권명 힌트로 별칭 시도
    if company_hint:
        # 직접 매칭
        if company_hint in dart_name_dict:
            result['target_corp_name'] = company_hint
            result['dart_corp_code'] = dart_name_dict[company_hint]
            # 주식코드 역추적
            for sc, cn in dart_corp_name_dict.items():
                if cn == company_hint:
                    result['target_stock_code'] = sc
                    break
            result['status'] = 'AUTO'
            result['method'] = 'NAME_DIRECT'
            return result
        
        # 별칭사전 매칭
        alias_target = aliases.get(company_hint)
        if alias_target and alias_target in dart_name_dict:
            result['target_corp_name'] = alias_target
            result['dart_corp_code'] = dart_name_dict[alias_target]
            for sc, cn in dart_corp_name_dict.items():
                if cn == alias_target:
                    result['target_stock_code'] = sc
                    break
            result['status'] = 'ALIAS'
            result['method'] = 'ALIAS_DICT'
            result['reason'] = f'예탁원({company_hint}) → DART({alias_target})'
            return result
    
    result['status'] = 'FAILED'
    result['method'] = 'FAILED'
    result['reason'] = f'ISIN변환({issuer_code}) DART 매칭 실패, 채권명({company_hint}) 별칭도 실패'
    return result


def _match_eb(result, isin, company_hint, seibro_key,
              aliases, dart_corp_dict, dart_corp_name_dict, dart_name_dict):
    """EB 매칭: SEIBRO에서 교환대상 추출."""
    
    root = seibro_api(seibro_key, 'getXrcStkStatInfo', {'BOND_ISIN': isin})
    if root is None:
        result['status'] = 'FAILED'
        result['method'] = 'FAILED'
        result['reason'] = 'SEIBRO getXrcStkStatInfo 응답 없음'
        return result
    
    el = root.find('.//result')
    if el is None:
        result['status'] = 'FAILED'
        result['method'] = 'FAILED'
        result['reason'] = 'SEIBRO 응답에 result 노드 없음'
        return result
    
    xrc_stk_isin = get_attr(el, 'XRC_STK_ISIN')
    xrc_stk_name = get_attr(el, 'STK_SECN_NM')
    target_code = stock_isin_to_code(xrc_stk_isin) if xrc_stk_isin else ''
    
    if not target_code:
        result['status'] = 'FAILED'
        result['method'] = 'FAILED'
        result['reason'] = f'교환대상 주식코드 추출 실패 (xrc_stk_isin={xrc_stk_isin})'
        return result
    
    # 주식코드로 DART 매칭
    dart_name = dart_corp_name_dict.get(target_code, '')
    dart_code = dart_corp_dict.get(target_code, '')
    
    if dart_name and dart_code:
        result['target_stock_code'] = target_code
        result['dart_corp_code'] = dart_code
        result['target_corp_name'] = dart_name
        
        # SEIBRO와 DART 이름 비교
        if dart_name == xrc_stk_name or dart_name in xrc_stk_name or xrc_stk_name in dart_name:
            result['status'] = 'AUTO'
            result['method'] = 'SEIBRO_EB'
        else:
            # 이름 차이 → 별칭사전 확인
            alias_target = aliases.get(xrc_stk_name)
            if alias_target == dart_name:
                result['status'] = 'AUTO'
                result['method'] = 'SEIBRO_EB_ALIAS'
                result['reason'] = f'별칭사전 적용: {xrc_stk_name} → {dart_name}'
            else:
                result['status'] = 'ALIAS'
                result['method'] = 'SEIBRO_EB'
                result['reason'] = f'SEIBRO({xrc_stk_name}) ≠ DART({dart_name}) — 별칭사전 추가 필요'
        return result
    
    # DART에 없음
    result['target_corp_name'] = xrc_stk_name
    result['target_stock_code'] = target_code
    result['status'] = 'FAILED'
    result['method'] = 'FAILED'
    result['reason'] = f'주식코드({target_code})는 추출, DART 매칭 실패'
    return result


# ============================================================
# [텔레그램 알람 헬퍼]
# ============================================================
def send_telegram_alert(bot_token, chat_id, message):
    """텔레그램 알람 전송."""
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        r = requests.post(url, json={
            'chat_id': chat_id,
            'text': message,
            'parse_mode': 'HTML',
        }, timeout=10)
        return r.status_code == 200
    except Exception as e:
        print(f"  ⚠ 텔레그램 전송 실패: {e}")
        return False


def format_match_failure_alert(result):
    """매칭 실패 시 텔레그램 알람 메시지 포맷팅."""
    return f"""🚨 <b>신규 종목 매칭 필요</b>

📌 ISIN: <code>{result['isin']}</code>
📌 채권명: {result['bond_name']}
📌 종류: {result['bond_type'] or '?'}

❌ 매칭 실패
{result['reason']}

✏️ 수동등록 방법:
<code>/match {result['isin']} 종목명 주식코드</code>

예시:
<code>/match {result['isin']} 에르코스 435570</code>"""


def format_change_alert(old, new):
    """매월 재검증에서 변경 감지 시 알람 메시지."""
    return f"""⚠️ <b>매칭 정보 변경 감지</b>

📌 ISIN: <code>{old['isin']}</code>
📌 채권명: {old['bond_name']}

이전 값:
- 종목명: {old['target_corp_name']}
- 주식코드: {old['target_stock_code']}

새 값 (SEIBRO/DART):
- 종목명: {new['target_corp_name']}
- 주식코드: {new['target_stock_code']}

확인 후 수동으로 시트 갱신 필요"""
