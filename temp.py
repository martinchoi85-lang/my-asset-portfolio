import pandas as pd
import numpy as np
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


# asset_id 매핑을 위한 헬퍼 함수
def get_asset_lookup():
    """Supabase에서 'assets' 테이블을 조회하여 종목명(name_kr)과 ID를 매핑하는 딕셔너리를 반환합니다."""
    try:
        # assets 테이블에서 name_kr과 id 컬럼만 조회
        response = supabase.table('assets').select('id, name_kr').execute()
        asset_map = {item['name_kr']: item['id'] for item in response.data}
        print(f"✅ assets 테이블에서 {len(asset_map)}건의 자산 ID 매핑 데이터 로드 완료.")
        return asset_map
    except Exception as e:
        print(f"❌ assets 테이블 조회 오류: {e}")
        return {}
    

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
print("Supabase 연결 성공!")
df = pd.read_csv("./trade_journal.csv")

# DB 스키마에 없는 불필요한 컬럼 삭제 (옵션)
df = df.drop(columns=['실현손익', '수익률', '총 체결 금액'], errors='ignore')

# 1. 컬럼 이름 정리 및 매핑
df = df.rename(columns={
    '종목명': 'asset_name',  # 임시로 'asset_name' 사용
    '증권사': 'account_name',
    '매매 구분': 'trade_type',
    '체결 일자': 'transaction_date',
    '체결 단가': 'price',
    '체결 수량': 'quantity',
    '매매 비용\n(수수료+제세금)': 'commission',
    '매매 이유': 'memo'
})

# 2. 필수 숫자 컬럼의 클리닝 (통화 기호, 쉼표, NaN 처리)
numeric_cols = ['price', 'quantity', 'commission']
for col in numeric_cols:
    if col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str).str.strip()
            # 🚨 핵심 수정: $ 및 ₩ 기호 제거
            df[col] = df[col].str.replace('$', '', regex=False).str.replace('₩', '', regex=False).str.replace(',', '', regex=False)
        
        # 숫자 변환 실패 시 NaN으로, NaN은 0으로 치환
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)  

# 3. 거래 유형 정리
df['trade_type'] = df['trade_type'].astype(str).str.strip().str.upper().str.replace('매수', 'BUY').str.replace('매도', 'SELL')

# 4. 날짜 형식 변환
df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce').dt.strftime('%Y-%m-%dT%H:%M:%S+09:00')

# 5. [FK 해결] assets 테이블에서 ID 매핑 정보 가져오기
asset_lookup_map = get_asset_lookup()

# asset_name을 사용하여 assets 테이블의 'id'를 매핑
df['asset_id'] = df['asset_name'].map(asset_lookup_map)

# 6. transactions 테이블 DB 스키마에 맞는 컬럼만 선택
# 매핑에 실패했거나 (NaN), 필수 컬럼이 없는 행 제거
final_df = df[['asset_id', 'account_name', 'trade_type', 'transaction_date', 
                'quantity', 'price', 'commission', 'memo']].dropna(subset=['asset_id', 'transaction_date'])

# 7. [최종 점검] NaN/Inf 값 처리 및 강제 정수형 변환 (DB INT 가정)

# Inf 값 처리 (Out of range float 오류의 또 다른 원인)
final_df.replace([np.inf, -np.inf], 0, inplace=True) 

# 데이터 타입을 DB 스키마에 맞게 강제 변환 (정수형으로)
try:
    # 소수점 반올림 후 정수형으로 변환하여 DB 정수형 스키마 불일치 문제를 최종적으로 방지
    final_df['quantity'] = final_df['quantity'].round(0).astype(int)
    final_df['price'] = final_df['price'].round(0).astype(int)
    final_df['commission'] = final_df['commission'].round(0).astype(int)    
    final_df['asset_id'] = final_df['asset_id'].round(0).astype(int)
except Exception as e:
    print(f"🚨 경고: 숫자 컬럼 강제 int 변환 오류: {e}. float 형태로 삽입을 시도합니다.")
    # float 형태를 유지하고 NaN/Inf만 처리된 상태로 진행합니다. (price/commission에 소수점 허용 시)

print("Debug: 거래 기록 삽입 전 데이터 확인")
print(final_df.head(50))
print(final_df.info())
final_df.to_csv("./debug_final_transactions.csv", index=False)  # 디버그용 CSV 저장