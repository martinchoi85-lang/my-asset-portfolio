# 데이터베이스 연결과 조회 로직만 담당합니다. UI 코드는 넣지 않습니다.
import os
import pandas as pd
import streamlit as st
from supabase import create_client
from dotenv import load_dotenv
from FinanceDataReader import data as fdr
import yfinance as yf
import numpy as np
from datetime import datetime, date

# -------------------------------------------------------------------
# 1. Supabase 연결 초기화
# -------------------------------------------------------------------
load_dotenv()

@st.cache_resource
def init_connection():
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        st.error("❌ Supabase 환경 변수(SUPABASE_URL, SUPABASE_KEY)를 확인해주세요.")
        return None
    return create_client(url, key)

# -------------------------------------------------------------------
# 2. 데이터 조회 함수 (캐싱 적용)
# -------------------------------------------------------------------
@st.cache_data(ttl=600)
def fetch_data(table_name):
    """테이블 또는 뷰(View)의 모든 데이터를 가져와 DataFrame으로 변환합니다."""
    supabase = init_connection()
    if not supabase: return pd.DataFrame()

    try:
        response = supabase.table(table_name).select("*").execute()
        df = pd.DataFrame(response.data)

        # 📌 [핵심] 'transactions' 테이블의 날짜 컬럼을 datetime 형식으로 변환
        if table_name == 'transactions' and 'transaction_date' in df.columns:
            df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')
        
        return df
    except Exception as e:
        st.error(f"⚠️ 데이터 조회 실패 ({table_name}): {e}")
        return pd.DataFrame()
    
# -------------------------------------------------------------------
# 3. 환율 조회 함수 (캐싱 적용)
# -------------------------------------------------------------------
@st.cache_data(ttl=3600)
def fetch_usd_exchange_rate():
    """
    [Warning 해결] 최신 FinanceDataReader 권장 방식을 사용하여 USD/KRW 환율을 가져옵니다.
    """
    try:
        df_rate = fdr.DataReader('FRED:DEXKOUS', start='2025-01-01')
        
        if df_rate is not None and not df_rate.empty:
            return df_rate.iloc[-1]['DEXKOUS']
        return 1300.0
    except Exception:
        return 1300.0
    
# -------------------------------------------------------------------
# 4. 데이터 업데이트 함수 (신규: 현재가 업데이트)
# -------------------------------------------------------------------
def fetch_current_prices(df_assets):
    """
    yfinance를 사용하여 자산의 현재가를 업데이트합니다.
    """
    if df_assets.empty or 'ticker' not in df_assets.columns:
        return df_assets

    tickers = df_assets['ticker'].dropna().unique().tolist()
    current_prices = {}
    
    for ticker in tickers:
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="1d")
            if not hist.empty:
                current_prices[ticker] = hist['Close'].iloc[-1]
            else:
                current_prices[ticker] = None
        except Exception:
            current_prices[ticker] = None

    price_df = pd.DataFrame(
        list(current_prices.items()), 
        columns=['ticker', 'current_price_fetched']
    )
    
    df_assets = pd.merge(df_assets, price_df, on='ticker', how='left')
    df_assets['current_price'] = df_assets['current_price_fetched'].combine_first(df_assets['current_price'])
    df_assets = df_assets.drop(columns=['current_price_fetched'], errors='ignore')

    return df_assets

# -------------------------------------------------------------------
# 5. 조회용 룩업 데이터 (Asset, Account, Code) 로드 함수
# -------------------------------------------------------------------
@st.cache_data(ttl=3600)
def get_lookup_data():
    """드롭다운 선택지(룩업 데이터)를 미리 로드하여 딕셔너리로 반환"""
    
    df_assets = fetch_data("assets")
    required_asset_cols = ['id', 'name_kr', 'asset_type', 'currency', 'market']
    if not df_assets.empty and all(c in df_assets.columns for c in required_asset_cols):
        asset_lookup = df_assets[required_asset_cols].copy()
    else:
        asset_lookup = pd.DataFrame(columns=required_asset_cols)

    asset_name_to_id = asset_lookup.set_index('name_kr')['id'].to_dict() if not asset_lookup.empty else {}
    asset_id_to_name = asset_lookup.set_index('id')['name_kr'].to_dict() if not asset_lookup.empty else {}

    df_accounts = fetch_data("accounts")
    required_account_cols = ['id', 'name', 'brokerage', 'type', 'owner']
    if not df_accounts.empty and all(c in df_accounts.columns for c in required_account_cols):
        account_lookup = df_accounts[required_account_cols].copy()
        account_lookup['display_name'] = account_lookup['name'] + ' (' + account_lookup['brokerage'] + ')'
    else:
        account_lookup = pd.DataFrame(columns=required_account_cols + ['display_name'])

    account_id_to_name_db = account_lookup.set_index('id')['name'].to_dict() if not account_lookup.empty else {}
    account_name_to_id_db = account_lookup.set_index('name')['id'].to_dict() if not account_lookup.empty else {}
    account_name_to_id_display = account_lookup.set_index('display_name')['id'].to_dict() if not account_lookup.empty else {}
    account_id_to_name_display = account_lookup.set_index('id')['display_name'].to_dict() if not account_lookup.empty else {}

    is_asset_empty = asset_lookup.empty
    is_account_empty = account_lookup.empty

    code_map = {
        'asset_type': {'stock': '주식', 'stock': '주식', 'cash': '현금', 'fund': '펀드', 
                       'bond': '채권', 'gold': '금', 'etf': 'ETF'},
        'currency': {'won': '한화', 'usd': '달러', 'jpy': '엔화', '': '기타'},
        'market': {'korea': '한국', 'us': '미국', 'jp': '일본', '': '기타'},
    }
    
    code_lookup = {
        'trade_types': ["BUY", "SELL"],
        'asset_types': list(code_map['asset_type'].values()),
        'currencies': list(code_map['currency'].values()),
        'markets': list(code_map['market'].values()),
        'account_owners': account_lookup['owner'].dropna().unique().tolist() if not is_account_empty else ["승엽", "민희"],
        'account_types': account_lookup['type'].dropna().unique().tolist() if not is_account_empty else ["일반", "ISA", "DC", "IRP", "연금저축"],
        'type_to_kr': {'stock': '주식', 'stock': '주식', 'cash': '현금', 'fund': '펀드', 
                       'bond': '채권', 'gold': '금', 'etf': 'ETF'}, 
        'code_map': code_map,
    }

    kr_to_code_map = {
        key: {v: k for k, v in value.items()} 
        for key, value in code_map.items()
    }

    return {
        'asset_id_to_name': asset_id_to_name,
        'asset_name_to_id': asset_name_to_id,
        'account_id_to_name_display': account_id_to_name_display,
        'account_name_to_id_display': account_name_to_id_display,
        'account_id_to_name_db': account_id_to_name_db,
        'account_name_to_id_db': account_name_to_id_db,
        'kr_to_code_map': kr_to_code_map,
        'codes': code_lookup,
        'asset_lookup_df': asset_lookup,
        'account_lookup_df': account_lookup
    }

# -------------------------------------------------------------------
# 6. 📊 [신규] Transactions 기반 asset_summary 재계산 함수
# -------------------------------------------------------------------
def recalculate_asset_summary():
    """
    transactions 테이블에 기반하여 asset_summary 테이블의 내용을 재계산하고 덮어씁니다.
    asset_summary는 이제 뷰가 아닌 테이블입니다.
    """
    supabase = init_connection()
    if not supabase: return False

    SUMMARY_TABLE_NAME = "asset_summary"
    
    try:
        st.info("🔄 'transactions' 기반으로 'asset_summary' 테이블을 재계산합니다.")
        
        # 1. 계산에 필요한 기초 데이터 로드 (캐시 사용)
        df_transactions = fetch_data("transactions")
        df_assets = fetch_data("assets")
        df_accounts = fetch_data("accounts")

        if df_transactions.empty or df_assets.empty:
            st.warning("⚠️ 거래 기록(transactions) 또는 자산 정보(assets)가 없어 요약을 생성할 수 없습니다.")
            return False

        # 2. 데이터 병합 (Merge)
        # transactions + assets (자산 정보) 병합
        df_merged = pd.merge(df_transactions, 
                             df_assets[['id', 'ticker', 'name_kr', 'asset_type', 'currency', 'current_price']], 
                             left_on='asset_id', 
                             right_on='id', 
                             suffixes=('', '_asset'), 
                             how='left')
        
        # transactions + accounts (계좌 ID) 병합
        df_merged = pd.merge(df_merged, 
                             df_accounts[['id', 'account_id']], 
                             left_on='account_name', 
                             right_on='name', 
                             suffixes=('', '_account'), 
                             how='left')
        
        # account_id 컬럼 정리 (DB에 account_id가 없으므로 수동 매핑 로직 필요)
        # 📌 'account_name'을 이용하여 'accounts' 테이블에서 'id'를 가져와 'account_id'로 사용
        account_name_to_id = df_accounts.set_index('name')['id'].to_dict()
        df_merged['account_id'] = df_merged['account_name'].map(account_name_to_id)
        
        # 필요한 숫자형 컬럼을 숫자로 변환 (계산의 정확성 확보)
        df_merged['quantity'] = pd.to_numeric(df_merged['quantity'], errors='coerce')
        df_merged['price'] = pd.to_numeric(df_merged['price'], errors='coerce')
        df_merged['current_price'] = pd.to_numeric(df_merged['current_price'], errors='coerce')
        
        # 3. 자산 요약 핵심 계산 로직
        
        # 3-1. 수량(Total Quantity) 및 총 매수 금액(Total Purchase Amount) 계산
        # BUY: +quantity, SELL: -quantity
        df_merged['signed_quantity'] = df_merged.apply(
            lambda row: row['quantity'] if row['trade_type'] == 'BUY' else -row['quantity'], 
            axis=1
        )
        # 총 매수 금액: BUY일 때만 계산
        df_merged['purchase_amount'] = df_merged.apply(
            lambda row: row['quantity'] * row['price'] if row['trade_type'] == 'BUY' else 0, 
            axis=1
        )

        # 그룹화 기준: asset_id와 account_id
        df_summary_base = df_merged.groupby(['asset_id', 'account_id']).agg(
            total_quantity=('signed_quantity', 'sum'), # 최종 보유 수량
            total_purchase_amount=('purchase_amount', 'sum') # 총 매수 금액
        ).reset_index()

        # 3-2. 평균 매수 가격 및 평가 금액 계산
        # total_quantity가 0인 경우(전량 매도), 평균 매수 가격 계산에서 제외 (0으로 설정)
        df_summary_base['average_purchase_price'] = np.where(
            df_summary_base['total_quantity'] > 0,
            df_summary_base['total_purchase_amount'] / df_summary_base['total_quantity'],
            0.0 # 수량이 0이면 평균 매수 가격은 0
        )
        
        # 자산 정보 (ticker, current_price 등)를 다시 병합
        df_summary_final = pd.merge(df_summary_base, 
                                     df_assets[['id', 'ticker', 'name_kr', 'asset_type', 'currency', 'current_price']],
                                     left_on='asset_id', 
                                     right_on='id', 
                                     how='left')
        
        # 3-3. 평가 관련 지표 계산
        # 현재 평가 가격 (current_valuation_price): current_price를 사용
        df_summary_final['current_valuation_price'] = df_summary_final['current_price']
        
        # 총 평가 금액 (total_valuation_amount)
        df_summary_final['total_valuation_amount'] = (
            df_summary_final['total_quantity'] * df_summary_final['current_valuation_price']
        )
        
        # 평가 손익 (unrealized_pnl)
        df_summary_final['unrealized_pnl'] = (
            df_summary_final['total_valuation_amount'] - df_summary_final['total_purchase_amount']
        )
        
        # 수익률 (unrealized_return_rate)
        df_summary_final['unrealized_return_rate'] = np.where(
            df_summary_final['total_purchase_amount'] > 0,
            (df_summary_final['unrealized_pnl'] / df_summary_final['total_purchase_amount']) * 100,
            0.0 # 매수 금액이 0이면 수익률도 0
        )

        # 4. 최종 DataFrame 정리 및 필터링
        # 보유 수량이 0보다 큰 경우만 필터링 (완전 매도된 종목 제외)
        df_new_summary = df_summary_final[df_summary_final['total_quantity'] > 0].copy()
        
        # asset_summary 테이블 스키마에 맞게 컬럼명 정리
        df_new_summary = df_new_summary.rename(columns={'id_x': 'id'}) # id_x가 transactions의 id였다면
        
        # 필요한 최종 컬럼 선택 (DB 테이블 스키마와 일치해야 함)
        # 스키마: asset_id, account_id, ticker, name_kr, currency, asset_type, total_quantity, 
        #         current_valuation_price, total_purchase_amount, total_valuation_amount, 
        #         average_purchase_price, unrealized_pnl, unrealized_return_rate
        final_cols = [
            'asset_id', 'account_id', 'ticker', 'name_kr', 'currency', 'asset_type', 
            'total_quantity', 'current_valuation_price', 'total_purchase_amount', 
            'total_valuation_amount', 'average_purchase_price', 'unrealized_pnl', 
            'unrealized_return_rate'
        ]
        
        # 데이터 타입 조정 (Postgres numeric/bigint에 맞추기 위해)
        for col in ['total_quantity', 'current_valuation_price', 'total_purchase_amount', 
                    'total_valuation_amount', 'average_purchase_price', 'unrealized_pnl']:
            df_new_summary[col] = pd.to_numeric(df_new_summary[col], errors='coerce').round(4)
        df_new_summary['unrealized_return_rate'] = pd.to_numeric(df_new_summary['unrealized_return_rate'], errors='coerce').round(8)
        
        df_new_summary = df_new_summary[final_cols]

        # 5. DB에 저장 (Delete 후 Upsert)
        
        # 📌 [해결] asset_summary가 이제 테이블이므로 DELETE 명령이 정상 작동합니다.
        # 기존 데이터를 모두 삭제하여 재계산된 새 데이터만 남깁니다.
        # 'asset_id' != 0 조건은 필요 없을 수도 있지만, 안전을 위해 유지합니다.
        supabase.table(SUMMARY_TABLE_NAME).delete().neq('asset_id', 0).execute()  # 기존 요약 데이터 전체 삭제
        
        new_records = df_new_summary.where(pd.notnull(df_new_summary), None).to_dict('records')

        # 📌 데이터 타입 클리닝: 정수형 실수(38.0)를 정수(38)로 변환 (bigint 에러 방지)
        cleaned_records = []
        for record in new_records:
            cleaned_record = {}
            for key, value in record.items():
                if value is None or pd.isna(value):
                    cleaned_record[key] = None
                elif isinstance(value, (float, np.floating, np.float64, np.float32)):
                    if float(value).is_integer():
                        cleaned_record[key] = int(value)
                    else:
                        cleaned_record[key] = float(value)
                else:
                    cleaned_record[key] = value
            cleaned_records.append(cleaned_record)
            
        # 📌 계산된 새 데이터 삽입 (asset_id가 Primary Key 역할을 하므로 upsert 사용)
        supabase.table(SUMMARY_TABLE_NAME).upsert(cleaned_records).execute()
        
        st.success("✅ asset_summary 테이블 재계산 및 업데이트 완료!")
        st.cache_data.clear() # 캐시 초기화하여 대시보드에서 최신 데이터 로드 유도
        return True
    
    except Exception as e:
        st.error(f"❌ asset_summary 재계산 실패: {e}")
        import traceback
        st.error(traceback.format_exc())
        st.warning("DB에서 'asset_summary' 뷰를 삭제하고, 동일한 이름의 테이블로 생성했는지 확인해주세요.")
        return False

# -------------------------------------------------------------------
# 7. 데이터 변경 함수 (저장/수정/삭제)
# -------------------------------------------------------------------
def update_data(table_name, df_changes):
    """
    Supabase 테이블에 데이터를 저장합니다.
    transactions 테이블은 INSERT / UPDATE를 분리하여
    id = NULL 문제를 구조적으로 차단합니다.
    """

    supabase = init_connection()
    if not supabase:
        return False

    PK_COL = 'id'

    try:
        lookup = get_lookup_data()

        # =====================================================
        # 1️⃣ transactions 전처리 (자산 / 계좌 변환)
        # =====================================================
        if table_name == 'transactions':

            if 'name_kr' in df_changes.columns:
                df_changes['asset_id'] = df_changes['name_kr'].map(
                    lookup['asset_name_to_id']
                )
                df_changes.drop(columns=['name_kr'], inplace=True, errors='ignore')

            if 'account_display_name' in df_changes.columns:
                df_changes['account_id'] = df_changes['account_display_name'].map(
                    lookup['account_name_to_id_display']
                )
                df_changes['account_name'] = df_changes['account_id'].map(
                    lookup['account_id_to_name_db']
                )
                df_changes.drop(
                    columns=['account_display_name'], inplace=True, errors='ignore'
                )

        # =====================================================
        # 2️⃣ DataFrame → dict 변환 (NaN → None)
        # =====================================================
        records = (
            df_changes
            .where(pd.notnull(df_changes), None)
            .to_dict('records')
        )

        insert_rows = []
        update_rows = []

        # =====================================================
        # 3️⃣ INSERT / UPDATE 분리 (🔥 핵심)
        # =====================================================
        for record in records:
            cleaned = {}

            for key, value in record.items():
                if value is None or pd.isna(value):
                    cleaned[key] = None

                elif isinstance(value, (pd.Timestamp, datetime, date)):
                    if key == 'transaction_date':
                        cleaned[key] = value.strftime('%Y-%m-%d %H:%M:%S+00')
                    else:
                        cleaned[key] = value.strftime('%Y-%m-%d')

                elif isinstance(value, (float, np.floating)):
                    cleaned[key] = int(value) if float(value).is_integer() else float(value)

                elif isinstance(value, (int, np.integer)):
                    cleaned[key] = int(value)

                elif isinstance(value, np.bool_):
                    cleaned[key] = bool(value)

                else:
                    cleaned[key] = value

            # id 기준으로 분기
            if cleaned.get(PK_COL) is None:
                cleaned.pop(PK_COL, None)  # INSERT 시 id 제거
                insert_rows.append(cleaned)
            else:
                update_rows.append(cleaned)

        # =====================================================
        # 🔍 디버깅 출력
        # =====================================================
        print("🟢 INSERT rows", insert_rows[-5:-1])
        print("🟡 UPDATE rows", update_rows[-5:-1])

        # =====================================================
        # 4️⃣ DB 반영
        # =====================================================
        if insert_rows:
            supabase.table(table_name).insert(insert_rows).execute()

        for row in update_rows:
            supabase.table(table_name) \
                .update(row) \
                .eq(PK_COL, row[PK_COL]) \
                .execute()

        st.cache_data.clear()

        if table_name == 'transactions':
            st.info("🔄 거래 내역 저장 완료 → asset_summary 재계산")
            recalculate_asset_summary()
        else:
            st.success("✅ 데이터 저장 완료")

        return True

    except Exception as e:
        st.error(f"❌ 데이터 저장 실패: {e}")
        raise


# -------------------------------------------------------------------
# 8. 📌 [신규] 데이터 삭제 함수 구현
# -------------------------------------------------------------------
def delete_data(table_name, record_id):
    """
    특정 레코드를 DB에서 삭제합니다.
    
    Args:
        table_name (str): 테이블명
        record_id (int/str): 삭제할 레코드의 ID (PK)
    
    Returns:
        bool: 성공 여부
    """
    supabase = init_connection()
    if not supabase:
        st.error("❌ DB 연결 실패")
        return False
    
    try:
        # Supabase delete API 사용 (id 컬럼 기준)
        response = supabase.table(table_name).delete().eq('id', record_id).execute()
        
        # 캐시 초기화
        st.cache_data.clear()
        
        # 📊 transactions 테이블 삭제 후 asset_summary 자동 재계산
        if table_name == 'transactions':
            recalculate_asset_summary()
        
        return True
        
    except Exception as e:
        st.error(f"❌ 데이터 삭제 실패 (ID: {record_id}): {e}")
        return False

# -------------------------------------------------------------------
# 9. 🔮 [향후 확장용] 일별 스냅샷 자동 생성 함수
# -------------------------------------------------------------------
def create_daily_snapshot(snapshot_date=None):
    """
    특정 날짜 기준으로 account_snapshots 테이블에 스냅샷을 생성합니다.
    
    Args:
        snapshot_date (str): 'YYYY-MM-DD' 형식. None이면 오늘 날짜 사용
    
    Returns:
        bool: 성공 여부
    """
    from datetime import datetime
    
    supabase = init_connection()
    if not supabase:
        return False
    
    if snapshot_date is None:
        snapshot_date = datetime.now().strftime('%Y-%m-%d')
    
    try:
        # asset_summary 데이터 로드
        df_summary = fetch_data("asset_summary")
        if df_summary.empty:
            st.warning("⚠️ asset_summary가 비어있어 스냅샷을 생성할 수 없습니다.")
            return False
        
        # 스냅샷 데이터 생성 (account_snapshots 스키마에 맞춤)
        snapshots = df_summary.copy()
        snapshots['date'] = snapshot_date
        
        # 컬럼 매핑
        snapshots = snapshots.rename(columns={
            'total_quantity': 'quantity',
            'current_valuation_price': 'valuation_price',
            'average_purchase_price': 'purchase_price',
            'total_valuation_amount': 'valuation_amount',
            'total_purchase_amount': 'purchase_amount'
        })
        
        # cost 컬럼 추가 (기본값 0)
        snapshots['cost'] = 0
        
        # 필요한 컬럼만 선택
        final_cols = [
            'account_id', 'asset_id', 'date', 'currency', 
            'quantity', 'valuation_price', 'purchase_price',
            'valuation_amount', 'purchase_amount', 'cost'
        ]
        
        snapshots_final = snapshots[final_cols].copy()
        
        # DB에 삽입
        records = snapshots_final.where(pd.notnull(snapshots_final), None).to_dict('records')
        supabase.table("account_snapshots").upsert(records).execute()
        
        st.success(f"✅ {snapshot_date} 스냅샷이 생성되었습니다. (총 {len(snapshots_final)}개)")
        return True
        
    except Exception as e:
        st.error(f"❌ 스냅샷 생성 실패: {e}")
        return False