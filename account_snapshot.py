import pandas as pd
import numpy as np
import io
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


# 사용자로부터 받은 CSV 내용
stock_list_csv_content = """
{fullContent}
"""

# CSV 데이터를 DataFrame으로 로드합니다.
# 데이터가 복잡한 구조를 가지고 있으므로, 실제 주식 잔고 목록이 시작되는 행을 찾아야 합니다.
# 10행('증권사'로 시작하는 행)부터 데이터가 시작됩니다.
df = pd.read_csv(io.StringIO(stock_list_csv_content), header=9)

# 필요한 컬럼만 선택하고 이름을 정리합니다.
df = df.iloc[:, [2, 3, 4, 5, 6, 9, 10, 14]]
df.columns = ['account_name', 'ticker', 'asset_name', 'quantity', 'current_price', 'current_value_krw', 'purchase_price_krw', 'note']

# 불필요한 행 제거 (합계, 빈 값 등)
df = df.dropna(subset=['account_name', 'asset_name']).copy()
df = df[~df['account_name'].str.contains('합계|총 합계|총 평가 금액|총 매입 금액', na=False)]
df = df.dropna(subset=['asset_name'])
df = df[df['asset_name'] != '종목명 (국문)']

# 달러 자산 목록이 아래쪽에도 별도로 있으나, 위쪽 표에 '신한금융투자' 계좌에 USD 자산이 원화로 평가되어 포함되어 있으므로, 상단 목록을 중심으로 정리합니다.

# '잔고수량'이 없는 행(예금, 현금성 자산 등)은 별도 처리 (수량은 1, '평가 금액'을 잔고로 간주)
df_no_quantity = df[df['quantity'].isna()]
df_has_quantity = df[df['quantity'].notna()]

print("--- 전처리된 데이터 프레임 (상위 5개) ---")
print(df_has_quantity.head())
print("\n--- 현금성 자산 데이터 프레임 (상위 5개) ---")
print(df_no_quantity.head())


# 콤마, 통화 기호 등 제거 및 숫자형 변환 함수
def clean_numeric_column(series):
    return series.astype(str).str.replace('[\$,₩원]', '', regex=True).str.replace(',', '', regex=False).replace('', np.nan).astype(float)

# 'current_price', 'current_value_krw', 'purchase_price_krw' 정리
df_has_quantity.loc[:, 'quantity'] = clean_numeric_column(df_has_quantity['quantity'])
df_has_quantity.loc[:, 'current_price'] = clean_numeric_column(df_has_quantity['current_price'])
df_has_quantity.loc[:, 'current_value_krw'] = clean_numeric_column(df_has_quantity['current_value_krw'])
df_has_quantity.loc[:, 'purchase_price_krw'] = clean_numeric_column(df_has_quantity['purchase_price_krw'])

# 현금성 자산 정리
df_no_quantity.loc[:, 'quantity'] = 1 # 현금성 자산은 수량을 1로 설정
df_no_quantity.loc[:, 'current_value_krw'] = clean_numeric_column(df_no_quantity['current_price'])
df_no_quantity.loc[:, 'purchase_price_krw'] = clean_numeric_column(df_no_quantity['current_value_krw'])
df_no_quantity.loc[:, 'current_price'] = df_no_quantity['current_value_krw'] # '현재가' 컬럼에 잔고액을 임시로 넣습니다.

# 두 데이터프레임 다시 결합
final_snapshots_df = pd.concat([df_has_quantity, df_no_quantity], ignore_index=True)

# 잔고/평가액이 없는 행 최종 제거
final_snapshots_df = final_snapshots_df.dropna(subset=['current_value_krw']).copy()

# 최종 스냅샷 데이터 확인
print("\n--- 최종 통합 데이터 프레임 (정리 후 상위 10개) ---")
print(final_snapshots_df.head(10))
print(f"\n최종 스냅샷 데이터 행 수: {len(final_snapshots_df)}")


# account_id 매핑을 위한 헬퍼 함수
def get_account_lookup():
    """Supabase에서 'accounts' 테이블을 조회하여 계좌명(name)과 ID를 매핑하는 딕셔너리를 반환합니다."""
    try:
        # accounts 테이블에서 name과 id 컬럼만 조회
        # 주의: 이 함수를 사용하기 전에 'supabase' 객체가 정의되어 있어야 합니다.
        response = supabase.table('accounts').select('id, name').execute()
        account_map = {item['name']: item['id'] for item in response.data}
        print(f"✅ accounts 테이블에서 {len(account_map)}건의 계좌 ID 매핑 데이터 로드 완료.")
        return account_map
    except Exception as e:
        print(f"❌ accounts 테이블 조회 오류: {e}")
        return {}

# 사용 예시 (이 코드는 2단계 전처리 코드 이후에 실행되어야 합니다)
account_lookup_map = get_account_lookup()

# 'account_name'을 사용하여 accounts 테이블의 'id'를 매핑
final_snapshots_df['account_id'] = final_snapshots_df['account_name'].map(account_lookup_map)