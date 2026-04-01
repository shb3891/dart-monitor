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

# ── 이연제약 공시 본문 파싱 ──
print("="*70)
print("📌 이연제약 공시 본문 - 전환청구기간 섹션 출력")
print("="*70)

RCEPT_NO = '20241108000348'

r5 = requests.get(f"{DART_BASE}/document.xml",
                  params={'crtfc_key': DART_KEY, 'rcept_no': RCEPT_NO}, timeout=30)
print(f"document.xml 응답: {r5.status_code} / {len(r5.content):,} bytes")

z5 = zipfile.ZipFile(io.BytesIO(r5.content))
print(f"ZIP 내 파일: {z5.namelist()}")

for fname in z5.namelist():
    if not any(fname.endswith(ext) for ext in ['.xml', '.html', '.htm']):
        continue
    print(f"\n--- 파일: {fname} ---")
    raw = z5.read(fname)
    text = None
    for enc in ['utf-8', 'euc-kr', 'cp949']:
        try:
            text = raw.decode(enc)
            print(f"  인코딩: {enc}")
            break
        except:
            continue
    if not text:
        print("  ⚠ 디코딩 실패")
        continue

    clean = re.sub(r'<[^>]+>', ' ', text)
    clean = re.sub(r'&nbsp;', ' ', clean)
    clean = re.sub(r'\s+', ' ', clean)

    # 전환청구기간 관련 키워드 전체 탐색
    keywords = [
        '전환청구기간', '전환청구 기간', '전환권 행사', '전환권행사',
        '청구기간', '권리행사', '전환기간', '행사기간',
        '전환청구', '청구권 행사',
    ]
    found_any = False
    for kw in keywords:
        idx = clean.find(kw)
        if idx >= 0:
            print(f"\n  ▶ 키워드 '{kw}' @ {idx}")
            print(f"  {clean[max(0,idx-100):idx+800]}")
            print()
            found_any = True

    if not found_any:
        print("  ⚠ 전환청구 관련 키워드 없음 - 전체 텍스트 앞 3000자 출력:")
        print(clean[:3000])

# ── 만호제강: 발행사 corp_code로 재검색 ──
print("\n\n" + "="*70)
print("📌 만호제강 - 발행사(만호제강) corp_code로 DART 검색")
print("="*70)

# 만호제강 발행사 이름으로 corp_code 찾기
for try_name in ['만호제강', '만호제강(주)', '(주)만호제강']:
    if try_name in name_to_corp:
        mh_corp_code = name_to_corp[try_name]
        print(f"  이름매칭 '{try_name}' → corp_code: '{mh_corp_code}'")
        break
else:
    matches = [(n,c) for n,c in name_to_corp.items() if '만호' in n]
    print(f"  '만호' 포함 기업: {matches[:10]}")
    mh_corp_code = matches[0][1] if matches else ''

if mh_corp_code:
    for ptype in ['B', '']:
        params = {
            'crtfc_key': DART_KEY, 'corp_code': mh_corp_code,
            'bgn_de': '20250101', 'end_de': '20260331', 'page_count': 40,
        }
        if ptype:
            params['pblntf_ty'] = ptype
        r_mh = requests.get(f"{DART_BASE}/list.json", params=params, timeout=10)
        d_mh = r_mh.json()
        label = "pblntf_ty=B" if ptype else "전체타입"
        print(f"\n  [{label}] status={d_mh.get('status')}, 건수={len(d_mh.get('list',[]))}")
        for itm in d_mh.get('list', []):
            print(f"    {itm.get('rcept_dt')} | {itm.get('report_nm')[:60]} | {itm.get('rcept_no')}")

# ── 한진: [첨부추가] 공시 본문 파싱 ──
print("\n\n" + "="*70)
print("📌 한진 - [첨부추가] 공시 본문 파싱")
print("="*70)

HANJIN_RCEPT = '20230711000434'

r6 = requests.get(f"{DART_BASE}/document.xml",
                  params={'crtfc_key': DART_KEY, 'rcept_no': HANJIN_RCEPT}, timeout=30)
print(f"document.xml 응답: {r6.status_code} / {len(r6.content):,} bytes")

z6 = zipfile.ZipFile(io.BytesIO(r6.content))
print(f"ZIP 내 파일: {z6.namelist()}")

for fname in z6.namelist():
    if not any(fname.endswith(ext) for ext in ['.xml', '.html', '.htm']):
        continue
    print(f"\n--- 파일: {fname} ---")
    raw = z6.read(fname)
    text = None
    for enc in ['utf-8', 'euc-kr', 'cp949']:
        try:
            text = raw.decode(enc)
            break
        except:
            continue
    if not text:
        continue

    clean = re.sub(r'<[^>]+>', ' ', text)
    clean = re.sub(r'&nbsp;', ' ', clean)
    clean = re.sub(r'\s+', ' ', clean)

    keywords = [
        '전환청구기간', '전환청구 기간', '전환권 행사', '전환권행사',
        '청구기간', '권리행사', '전환기간', '행사기간',
    ]
    found_any = False
    for kw in keywords:
        idx = clean.find(kw)
        if idx >= 0:
            print(f"\n  ▶ 키워드 '{kw}' @ {idx}")
            print(f"  {clean[max(0,idx-100):idx+800]}")
            found_any = True

    if not found_any:
        print("  ⚠ 키워드 없음 - 전체 텍스트 앞 3000자:")
        print(clean[:3000])
