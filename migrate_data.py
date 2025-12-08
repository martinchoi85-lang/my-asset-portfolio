import pandas as pd
import numpy as np
from supabase import create_client, Client
import os
from dotenv import load_dotenv

# .env 파일에서 환경 변수 로드 (API 키를 안전하게 관리)
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


# ----------------------------------------------------
# 2. 거래 기록(Journal)을 DB에 삽입 (Transaction 테이블)
# ----------------------------------------------------
def get_asset_lookup():
    """Supabase에서 'assets' 테이블을 조회하여 종목명(name_kr)과 ID를 매핑하는 딕셔너리를 반환합니다."""
    try:
        response = supabase.table('assets').select('id, name_kr').execute()
        asset_map = {item['name_kr']: item['id'] for item in response.data}
        print(f"✅ assets 테이블에서 {len(asset_map)}건의 자산 ID 매핑 데이터 로드 완료.")
        return asset_map
    except Exception as e:
        print(f"❌ assets 테이블 조회 오류: {e}")
        return {}

def migrate_transactions(file_path):
    print("\n거래 기록 마이그레이션 시작...")
    df = pd.read_csv(file_path)

    # ... (기존 migrate_transactions 코드) ...
    
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
    except Exception as e:
        print(f"🚨 경고: 숫자 컬럼 강제 int 변환 오류: {e}. float 형태로 삽입을 시도합니다.")
    
    print("Debug: 거래 기록 삽입 전 데이터 확인")
    print(final_df.head(2))

    records = final_df.to_dict('records')

    try:
        response = supabase.table('transactions').insert(records).execute()
        rejected_count = len(df) - len(final_df)
        print(f"✅ 총 {len(records)} 건의 거래 기록 삽입 완료.")
        if rejected_count > 0:
            print(f"⚠️ 경고: {rejected_count} 건의 거래 기록이 'asset_id' 매핑 실패 또는 필수 필드 누락으로 제외되었습니다.")
    except Exception as e:
        print(f"❌ 거래 기록 삽입 중 오류 발생: {e}")

# ----------------------------------------------------
# 3. 자산 목록(Stock List)을 DB에 삽입 (Assets 테이블)
# ----------------------------------------------------
def migrate_assets(file_path):
    print("자산 목록 마이그레이션 시작...")
    # ... (기존 migrate_assets 코드) ...
    print("Debug: 자산 목록 삽입 전 데이터 확인")
    print(df.head(9))

    records = df.to_dict('records')

    try:
        response = supabase.table('assets').insert(records).execute()
        print(f"✅ 총 {len(records)} 건의 고유 자산 목록 삽입 완료.")
    except Exception as e:
        print(f"❌ 자산 목록 삽입 중 오류 발생: {e}")

# ----------------------------------------------------
# 4. 포트폴리오 P&L 역사 기록을 DB에 삽입 (portfolio_pnl_history 테이블)
# ----------------------------------------------------
def migrate_pnl_history(file_path):
    print("\n포트폴리오 P&L 기록 마이그레이션 시작...")
    
    # 1. 파일 로드 (History.csv)
    df = pd.read_csv(file_path)
    
    # 2. 컬럼 이름 정리 및 매핑
    df = df.rename(columns={
        '날짜': 'date',
        '총 매입 금액': 'cumulative_contribution',
        '총 평가 금액': 'cumulative_valuation_amount',
        '수익률': 'portfolio_return_rate'
    })

    # 3. 필수 숫자 컬럼의 클리닝 (통화 기호, 쉼표, NaN 처리)
    numeric_cols = ['cumulative_contribution', 'cumulative_valuation_amount', 'portfolio_return_rate']
    
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            # 쉼표, '원', '%' 기호 제거
            df[col] = df[col].str.replace('원', '', regex=False).str.replace('%', '', regex=False).str.replace(',', '', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0) 
    
    # 4. 필수 계산 필드 추가
    df = df.sort_values(by='date') # 날짜순 정렬 (diff 계산을 위해 필수)
    
    # 누적 평가 손익: 총 평가 금액 - 총 매입 금액
    df['cumulative_pnl'] = df['cumulative_valuation_amount'] - df['cumulative_contribution']

    # 순 입출금 금액: 총 매입 금액의 일별 변화량
    # 첫 행은 이전 데이터가 없으므로 0으로 설정하거나, 첫날 매입 금액으로 설정합니다.
    # 여기서는 첫날 매입 금액을 net_contribution으로 간주합니다.
    df['net_contribution'] = df['cumulative_contribution'].diff().fillna(df['cumulative_contribution'].iloc[0] if not df.empty else 0)

    # 일별 평가 금액: cumulative_valuation_amount와 동일하게 설정 (일별 스냅샷이므로)
    df['daily_valuation_amount'] = df['cumulative_valuation_amount']
    
    # 5. 날짜 형식 변환
    # YYYY-MM-DD 형식으로 변환 (PostgreSQL DATE 타입에 맞춤)
    df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.strftime('%Y-%m-%d')
    
    # 6. 최종 컬럼 선택 및 NaN/Inf 처리
    final_df = df[['date', 'cumulative_valuation_amount', 'cumulative_contribution', 
                   'cumulative_pnl', 'portfolio_return_rate', 'daily_valuation_amount', 
                   'net_contribution']].dropna(subset=['date'])

    # Inf 값 처리
    final_df.replace([np.inf, -np.inf], 0, inplace=True)
    
    print("Debug: P&L 기록 삽입 전 데이터 확인 (상위 2개)")
    print(final_df.head(2))

    records = final_df.to_dict('records')

    # 7. Supabase 삽입
    try:
        # P&L 기록은 날짜(date)가 기본 키이며, 중복 삽입 시 에러가 발생할 수 있습니다.
        # on_conflict='date'를 사용해 upsert를 수행하여 데이터 충돌을 방지합니다.
        response = supabase.table('portfolio_pnl_history').upsert(records, on_conflict='date').execute()
        print(f"✅ 총 {len(records)} 건의 P&L 기록 삽입 완료 또는 업데이트 완료.")
    except Exception as e:
        print(f"❌ P&L 기록 삽입 중 오류 발생. (테이블 생성 및 스키마 확인 필요): {e}")


if __name__ == "__main__":
    if not os.path.exists(".env"):
        print("\n=======================================================")
        print("🚨 경고: .env 파일이 없습니다. SUPABASE_URL 및 KEY를 포함하여 생성해주세요.")
        print("=======================================================\n")
    
    # 💡 마이그레이션 실행 순서: 
    # 1. assets (자산 목록)을 먼저 삽입해야 transactions에서 asset_id를 매핑할 수 있습니다.
    # 2. transactions (거래 기록)을 삽입합니다.
    # 3. portfolio_pnl_history (P&L 기록)을 삽입합니다.
    
    # 파일 경로는 사용자가 저장한 실제 CSV 파일 이름으로 지정합니다.
    # migrate_assets('./승엽민희_금융자산 포트폴리오 - Stock List.csv')
    # migrate_transactions('./승엽민희_금융자산 포트폴리오 - Stock Trading Journal.csv')
    migrate_pnl_history('./승엽민희_금융자산 포트폴리오 - History.csv')