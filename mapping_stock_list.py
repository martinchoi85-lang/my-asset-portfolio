import pandas as pd
import re
from io import StringIO
from datetime import datetime
from supabase import create_client, Client
import os
from dotenv import load_dotenv

# .env 파일에서 환경 변수 로드 (API 키를 안전하게 관리)
# NOTE: secrets.txt 대신 .env 파일을 사용하는 것이 더 표준적입니다.
load_dotenv()

# ----------------------------------------------------
# 1. Supabase 연결 설정
# ----------------------------------------------------
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("FATAL ERROR: Supabase URL 또는 Key가 설정되지 않았습니다. .env 파일을 확인하세요.")
    exit()

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
print("Supabase 연결 성공!")


# --- 1. Supabase 연동 헬퍼 함수 (기존 함수 재사용 및 accounts 추가) ---
def get_asset_lookup():
    """Supabase에서 'assets' 테이블을 조회하여 종목명(name_kr)과 ID를 매핑하는 딕셔너리를 반환합니다."""
    # 사용자 정의 함수를 그대로 사용합니다.
    try:
        response = supabase.table('assets').select('id, name_kr').execute()
        asset_map = {item['name_kr']: item['id'] for item in response.data}
        print(f"✅ assets 테이블에서 {len(asset_map)}건의 자산 ID 매핑 데이터 로드 완료.")
        return asset_map
    except Exception as e:
        print(f"❌ assets 테이블 조회 오류: {e}")
        return {}

def get_account_lookup():
    """Supabase에서 'accounts' 테이블을 조회하여 계좌명(name)과 ID를 매핑하는 딕셔너리를 반환합니다."""
    try:
        # accounts 테이블에서 name과 id 컬럼만 조회
        response = supabase.table('accounts').select('id, name').execute()
        account_map = {item['name']: item['id'] for item in response.data}
        print(f"✅ accounts 테이블에서 {len(account_map)}건의 계좌 ID 매핑 데이터 로드 완료.")
        return account_map
    except Exception as e:
        print(f"❌ accounts 테이블 조회 오류: {e}")
        return {}
    

# --- 2. 데이터 클리닝 및 파싱 헬퍼 함수 ---
def clean_currency_string(value):
    """'1,234,567원', '$123.45', '123' 형태의 문자열을 float으로 변환합니다."""
    if pd.isna(value) or value in (None, ''):
        return 0.0

    # 따옴표 제거 (CSV 파싱 오류 방지)
    value = str(value).strip().replace('"', '').replace("'", "")
    
    # 숫자와 소수점만 남기도록 통화 기호, 콤마, '원' 등을 제거
    cleaned_value = re.sub(r'[₩$,%a-zA-Z\s]', '', value)
    
    try:
        return float(cleaned_value)
    except ValueError:
        return 0.0

def parse_csv_content(csv_content):
    """
    비표준 CSV 텍스트에서 자산 상세 목록 부분만 파싱하여 DataFrame으로 반환합니다.
    """
    # 상세 목록 헤더를 기준으로 데이터 시작 지점을 찾습니다.
    data_start_line = csv_content.find(',,증권사,티커(코드),종목명,잔고수량')
    if data_start_line == -1:
        raise ValueError("CSV 파일에서 상세 데이터 섹션을 찾을 수 없습니다.")

    # 상세 데이터 섹션만 추출
    detail_section = csv_content[data_start_line:].split('\n')
    
    # 종목번호(티커) 정보 테이블 직전까지의 유효 데이터만 필터링 (불필요한 공백 행, 합계 행 제거)
    data_lines = []
    
    # 40번째 라인 이후부터 종목 상세정보 라인들만 추출
    for i, line in enumerate(detail_section):
        # 1. 헤더 라인 제거
        if i == 0: continue
        
        # 2. 다음 섹션 (종목번호/평균단가) 시작 전까지 (비고 필드가 아닌 빈 줄이 나오면 종료)
        if line.startswith(',,,종목번호') or line.strip() == '':
            break
            
        # 3. 데이터가 들어있지 않은 라인 제거
        if len(line.strip().replace(',', '')) < 10:
             continue
        
        data_lines.append(line.strip())

    # 추출된 데이터를 StringIO 객체로 변환하여 Pandas read_csv로 처리
    # 컬럼은 15개 (선행 2개 빈칸 + 13개 데이터 컬럼)
    csv_data = "\n".join(data_lines)
    
    # 컬럼명 임시 지정 (이후 필요한 컬럼만 추출)
    temp_df = pd.read_csv(StringIO(csv_data), header=None, skiprows=0)

    # 엑셀 시트 기반의 컬럼 인덱스 (0부터 시작)
    # A=0, B=1, C=2, D=3, E=4, F=5, G=6, H=7, I=8, J=9, K=10, L=11, M=12, N=13, O=14
    
    # 필요한 컬럼만 추출 및 새 컬럼명 부여
    df = temp_df.iloc[:, [2, 3, 4, 5, 6, 7, 10, 11]].copy()
    df.columns = [
        'account_name', 'ticker', 'asset_name', 'quantity', 
        'current_price', 'avg_purchase_price', 'valuation_amount', 'purchase_amount'
    ]
    
    # 'account_name'이 비어있는 행 제거 (잔고 상세가 아닌 기타 헤더 행일 수 있음)
    df = df.dropna(subset=['account_name'])
    
    return df

# --- 3. 메인 실행 함수 ---

def prepare_data_for_supabase(file_content):
    # 1. DB 매핑 데이터 로드
    asset_lookup_map = get_asset_lookup()
    account_lookup_map = get_account_lookup()
    
    if not account_lookup_map:
        print("🚨 accounts 테이블 매핑 데이터가 비어있습니다. DB 연결 및 데이터 삽입을 확인하세요.")
        return pd.DataFrame()

    # 2. CSV 파일 파싱
    print("⏳ CSV 데이터 파싱 시작...")
    df = parse_csv_content(file_content)
    print(f"✅ 총 {len(df)}건의 자산 상세 데이터 파싱 완료.")

    # 3. 데이터 클리닝
    print("⏳ 데이터 클리닝 및 변환 시작...")
    
    # 통화 문자열을 숫자(float)로 변환
    currency_cols = [
        'current_price', 'avg_purchase_price', 'valuation_amount', 'purchase_amount'
    ]
    for col in currency_cols:
        df[col] = df[col].apply(clean_currency_string)

    # 잔고수량(quantity)도 숫자로 변환
    df['quantity'] = df['quantity'].fillna(0).apply(clean_currency_string)
    
    # 4. ID 매핑 적용
    df['account_id'] = df['account_name'].map(account_lookup_map)
    df['asset_id'] = df['asset_name'].map(asset_lookup_map)
    
    # 5. 특수 항목 처리 (Asset ID가 없는 현금/펀드 등)
    # - asset_id가 NaN이지만 valuation_amount가 0이 아닌 경우:
    #   해당 자산(종목명)을 assets 테이블에 먼저 추가해야 합니다.
    #   여기서는 미등록 자산 목록을 출력하여 사용자에게 DB 추가를 요청합니다.
    missing_assets = df[df['asset_id'].isna() & (df['valuation_amount'] > 0)]['asset_name'].unique()
    
    if len(missing_assets) > 0:
        print("\n⚠️ 경고: 다음 종목들은 'assets' 테이블에 ID가 매핑되지 않았습니다. DB에 먼저 등록해야 합니다.")
        for asset in missing_assets:
            print(f" - {asset}")
        print("데이터 삽입을 계속하려면, 이 자산들을 'assets' 테이블에 추가하고 다시 시도하세요.")
        # 매핑되지 않은 자산 제거 (혹은 에러 발생)
        # 여기서는 안전하게 매핑된 데이터만 필터링합니다.
        df_final = df.dropna(subset=['asset_id', 'account_id']).copy()
        
    else:
        df_final = df.dropna(subset=['asset_id', 'account_id']).copy()

    # 6. 최종 스냅샷 테이블 구조로 정리
    
    # ❗️ 중요: 스냅샷 날짜 설정
    # 이 데이터는 한 시점의 잔고이므로, 날짜를 명시적으로 지정합니다.
    snapshot_date = datetime.now().strftime('%Y-%m-%d')
    df_final['snapshot_date'] = snapshot_date
    
    # 최종 컬럼 순서 및 이름 지정 (account_snapshots 테이블 스키마 가정)
    final_cols = {
        'account_id': 'account_id',
        'asset_id': 'asset_id',
        'snapshot_date': 'date', # DB 컬럼명은 'date'로 가정
        'quantity': 'quantity',
        'current_price': 'valuation_price',
        'avg_purchase_price': 'purchase_price',
        'valuation_amount': 'valuation_amount',
        'purchase_amount': 'purchase_amount',
    }
    
    df_snapshots = df_final[list(final_cols.keys())].rename(columns=final_cols)
    
    print(f"\n✅ 최종 'account_snapshots' 준비 데이터: {len(df_snapshots)}건")
    print(f"   스냅샷 날짜: {snapshot_date}")
    
    return df_snapshots

# --- 4. 실행 ---

# 이전에 제공된 CSV 파일의 전체 내용
file_content = """
The following table:
,,,,한국 개별,한국 ETF,미국 개별,국내상장 미국ETF,채권,현금,국내주식,해외주식,배당주,금,채권,달러,합계
... (중략) ...
""" # 실제로는 의 전체 내용이 들어와야 함

# 실제 파일 내용을 변수에 할당 (제공된 파일의 `fullContent` 전체를 사용)
csv_file_content = """
,,증권사,티커(코드),종목명,잔고수량,현재가,평균매입가,평가손익,수익률,평가 금액,매입 금액,수수료,세금,구성,비고
,,미래에셋연금저축,213630,PLUS 미국다우존스고배당주(합성 H),10,"18,535원","18,021원","4,669원",2.59%,"185,350원","180,210원",7원,463원,배당주,국내상장 미국ETF
... (중략: 상세 데이터 목록) ...
,,미래에셋연금저축m(예),,미래에셋솔로몬장기국공채,,,,,,"3,000,000원","3,000,000원",,,채권,현금
,,,종목번호,종목명 (국문),,,평균단가
... (후략) ...
"""

# 위의 prepare_data_for_supabase 함수에 `File Fetcher`에서 가져온 전체 텍스트를 인수로 전달하여 실행하세요.
# 예: final_snapshots_df = prepare_data_for_supabase(csv_file_content)

# final_snapshots_df를 Supabase에 bulk insert 합니다.
# 예: response = supabase.table('account_snapshots').insert(final_snapshots_df.to_dict('records')).execute()