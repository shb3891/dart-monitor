import requests, zipfile, io, xml.etree.ElementTree as ET, re

SEIBRO_KEY  = 'e1e03a31bc0583fc0c853d4c41a0dc018dc4d2aa21c363c3d6b1b0b96e85221b'
DART_KEY    = 'bfc4e4e445de4727ae0bcc27e80ba5cf0e3818e6'
SEIBRO_BASE = "https://seibro.or.kr/OpenPlatform/callOpenAPI.jsp"
DART_BASE   = "https://opendart.fss.or.kr/api"

TARGETS = {
    '이연제약': 'KR6102461EA3',
    '만호제강': 'KR6001081G15',
    '한진':    'KR6002324D70',
}

print("📥 DART corpCode.xml 로드 중...")
r0 = requests.get(f"{DART_BASE}/corpCode.xml", params={'crtfc_key': DART_KEY}, timeout=30)
z0 = zipfile.ZipFile(io.BytesIO(r0.content))
corp_root = ET.fromstring(z0.read('CORPCODE.xml'))
stock_to_corp = {}
name_to_corp  = {}
for item in corp_root.findall('.//list'):
    cc = item.findtext('corp_code','').strip()
    sc = item.findtext('stock_code','').strip()
    cn = item.findtext('corp_name','').strip()
    if sc and len(sc)==6:
        stock_to_corp[sc] = cc
    if cn:
        name_to_corp[cn] = cc
print(f"✅ 로드 완료: 상장 {len(stock_to_corp)}개, 전체 {len(name_to_corp)}개\n")

def seibro_call(api_id, params_dict):
    params_str = ','.join([f"{k}:{v}" for k,v in params_dict.items()])
    url = f"{SEIBRO_BASE}?key={SEIBRO_KEY}&apiId={api_id}&params={params_str}"
    r = requests.get(url, timeout=10)
    return r.content.decode('utf-8', errors='replace')

for name, isin in TARGETS.items():
    print(f"\n{'='*70}")
    print(f"🔍 {name} | {isin}")
    print('='*70)

    print(f"\n[1] getBondStatInfo")
    print(seibro_call('getBondStatInfo', {'ISIN': isin})[:2000])

    print(f"\n[2] getXrcStkStatInfo")
    txt2 = seibro_call('getXrcStkStatInfo', {'BOND_ISIN': isin})
    print(txt2[:2000])

    xrc_match    = re.search(r'XRC_STK_ISIN[^>]*value="([^"]*)"', txt2)
    xrc_stk_isin = xrc_match.group(1) if xrc_match else ''
    stock_code_6 = xrc_stk_isin[3:9] if xrc_stk_isin and len(xrc_stk_isin)>=9 else ''
    print(f"\n  → xrc_stk_isin: '{xrc_stk_isin}' / stock_code_6: '{stock_code_6}'")

    print(f"\n[3] corp_code 탐색")
    corp_code = ''
    if stock_code_6:
        corp_code = stock_to_corp.get(stock_code_6, '')
        print(f"  stock_code '{stock_code_6}' → corp_code: '{corp_code}'")
    if not corp_code:
        for try_name in [name, name.replace('(주)','').strip()]:
            if try_name in name_to_corp:
                corp_code = name_to_corp[try_name]
                print(f"  이름 정확매칭 '{try_name}' → corp_code: '{corp_code}'")
                break
        if not corp_code:
            matches = [(n,c) for n,c in name_to_corp.items() if name in n or n in name]
            print(f"  이름 부분매칭: {matches[:10]}")
            if matches:
                corp_code = min(matches, key=lambda x: len(x[0]))[1]
                print(f"  → 선택: '{corp_code}'")

    print(f"\n[4] DART list.json")
    if corp_code:
        for ptype in ['B', '']:
            params = {
                'crtfc_key': DART_KEY, 'corp_code': corp_code,
                'bgn_de': '20220101', 'end_de': '20251231', 'page_count': 40,
            }
            if ptype:
                params['pblntf_ty'] = ptype
            r4  = requests.get(f"{DART_BASE}/list.json", params=params, timeout=10)
            d4  = r4.json()
            label = f"pblntf_ty=B" if ptype else "전체타입"
            print(f"\n  [{label}] status={d4.get('status')}, 건수={len(d4.get('list',[]))}")
            for itm in d4.get('list', []):
                print(f"    {itm.get('rcept_dt')} | {itm.get('report_nm')[:60]} | {itm.get('rcept_no')}")
    else:
        print("  ⚠ corp_code 없음")

# ── 이연제약 공시 본문 파싱 ──
print(f"\n\n{'='*70}")
print("📌 이연제약 공시 본문 - 전환청구기간 섹션 출력")

# 위 [4] 결과에서 이연제약 발행결정 rcept_no 확인 후 아래에 직접 입력
RCEPT_NO = ''  # ← 여기 채워서 2차 실행

if RCEPT_NO:
    r5 = requests.get(f"{DART_BASE}/document.xml",
                      params={'crtfc_key': DART_KEY, 'rcept_no': RCEPT_NO}, timeout=30)
    z5 = zipfile.ZipFile(io.BytesIO(r5.content))
    for fname in z5.namelist():
        if not any(fname.endswith(ext) for ext in ['.xml','.html','.htm']):
            continue
        raw = z5.read(fname)
        for enc in ['utf-8','euc-kr','cp949']:
            try: text = raw.decode(enc); break
            except: continue
        clean = re.sub(r'<[^>]+>', ' ', text)
        clean = re.sub(r'&nbsp;', ' ', clean)
        clean = re.sub(r'\s+', ' ', clean)
        for kw in ['전환청구기간','전환청구 기간','전환권 행사','전환권행사','청구기간','권리행사']:
            idx = clean.find(kw)
            if idx >= 0:
                print(f"\n  ▶ '{kw}' @ {idx}")
                print(f"  {clean[max(0,idx-50):idx+700]}")
                break
else:
    print("  → RCEPT_NO 미입력, 위 [4] 결과 확인 후 채워서 재실행하세요")
