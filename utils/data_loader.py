# 데이터베이스 연결과 조회 로직만 담당합니다. UI 코드는 넣지 않습니다.
# 기존 app.py에 있던 init_connection, fetch_data... 등의 함수를 이곳으로 옮기고, 데이터 저장/업데이트 기능을 보강했습니다.
import os
import pandas as pd
import streamlit as st
from supabase import create_client
from dotenv import load_dotenv
from FinanceDataReader import data as fdr
import yfinance as yf # yfinance 패키지 추가

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
        # 📌 [Warning 반영] 'data_source' 인자 대신 심볼에 출처를 명시하는 방식으로 변경
        # FRED:DEXKOUS는 USD/KRW (달러당 원화)
        df_rate = fdr.DataReader('FRED:DEXKOUS', start='2025-01-01') # 충분히 최근 날짜로 시작
        
        if df_rate is not None and not df_rate.empty:
            # 컬럼명은 'DEXKOUS'입니다.
            return df_rate.iloc[-1]['DEXKOUS']
        return 1300.0
    except Exception:
        # fdr 문제 발생 시 임시 환율 리턴
        return 1300.0
    
# -------------------------------------------------------------------
# 3. 데이터 업데이트 함수 (신규: 현재가 업데이트)
# -------------------------------------------------------------------
def fetch_current_prices(df_assets):
    """
    [2-1 요청 반영] yfinance를 사용하여 자산의 현재가를 업데이트합니다.
    """
    if df_assets.empty or 'ticker' not in df_assets.columns:
        return df_assets

    # Ticker 리스트 준비 (null 및 중복 제거)
    tickers = df_assets['ticker'].dropna().unique().tolist()
    
    current_prices = {}
    
    for ticker in tickers:
        try:
            # yfinance를 사용하여 현재가 조회 (한국 주식은 fdr이 더 정확할 수 있으나, yf로 통일)
            stock = yf.Ticker(ticker)
            # period="1d"는 최신 1일 데이터를 가져옴
            hist = stock.history(period="1d")
            if not hist.empty:
                current_prices[ticker] = hist['Close'].iloc[-1]
            else:
                current_prices[ticker] = None
        except Exception:
            # 예외 발생 시 가격을 None으로 처리
            current_prices[ticker] = None

    # 업데이트된 가격을 DataFrame으로 변환
    price_df = pd.DataFrame(
        list(current_prices.items()), 
        columns=['ticker', 'current_price_fetched']
    )
    
    # 기존 df_assets에 merge하여 current_price를 업데이트
    df_assets = pd.merge(df_assets, price_df, on='ticker', how='left')
    
    # NaN이 아닌 경우에만 덮어쓰기
    df_assets['current_price'] = df_assets['current_price_fetched'].combine_first(df_assets['current_price'])
    df_assets = df_assets.drop(columns=['current_price_fetched'], errors='ignore')

    return df_assets

# -------------------------------------------------------------------
# 4. 조회용 룩업 데이터 (Asset, Account, Code) 로드 함수
# -------------------------------------------------------------------
@st.cache_data(ttl=3600) # 룩업 데이터는 자주 변경되지 않으므로 캐시 시간 길게 설정
def get_lookup_data():
    """드롭다운 선택지(룩업 데이터)를 미리 로드하여 딕셔너리로 반환"""
    
    # 1. 자산 정보 (변경 없음)
    df_assets = fetch_data("assets")
    required_asset_cols = ['id', 'name_kr', 'asset_type', 'currency', 'market']
    if not df_assets.empty and all(c in df_assets.columns for c in required_asset_cols):
        asset_lookup = df_assets[required_asset_cols].copy()
    else:
        asset_lookup = pd.DataFrame(columns=required_asset_cols)

    asset_name_to_id = asset_lookup.set_index('name_kr')['id'].to_dict() if not asset_lookup.empty else {}
    asset_id_to_name = asset_lookup.set_index('id')['name_kr'].to_dict() if not asset_lookup.empty else {}

    # 2. 계좌 정보 
    df_accounts = fetch_data("accounts")
    required_account_cols = ['id', 'name', 'brokerage', 'type', 'owner']
    if not df_accounts.empty and all(c in df_accounts.columns for c in required_account_cols):
        account_lookup = df_accounts[required_account_cols].copy()
        
        # Display Name (거래 기록 뷰에서 사용할 표시 이름)
        account_lookup['display_name'] = account_lookup['name'] + ' (' + account_lookup['brokerage'] + ')'
    else:
        account_lookup = pd.DataFrame(columns=required_account_cols + ['display_name'])

    # 📌 [1번 요청 반영] 'account_name' (DB에 저장된 값) -> 'id' 맵핑 추가
    account_id_to_name_db = account_lookup.set_index('id')['name'].to_dict() if not account_lookup.empty else {} # id -> 계좌 이름 (DB 값)
    account_name_to_id_db = account_lookup.set_index('name')['id'].to_dict() if not account_lookup.empty else {} # 계좌 이름 (DB 값) -> id

    # Display Name ('이름 (증권사)') 맵핑 (트랜잭션 편집 UI에 사용)
    account_name_to_id_display = account_lookup.set_index('display_name')['id'].to_dict() if not account_lookup.empty else {}
    account_id_to_name_display = account_lookup.set_index('id')['display_name'].to_dict() if not account_lookup.empty else {}

    # 3. 코드성 데이터 (드롭다운 옵션)
    is_asset_empty = asset_lookup.empty
    is_account_empty = account_lookup.empty

    code_map = {
        'asset_type': {'stock': '주식', 'us_stock': '미국 주식', 'cash': '현금', 'fund': '펀드', 
                       'bond': '채권', 'gold': '금', 'etf': 'ETF', 'commodity': '원자재'},
        'currency': {'won': '한화', 'usd': '달러', 'jpy': '엔화', '': '기타'},
        'market': {'korea': '한국', 'us': '미국', 'jp': '일본', '': '기타'},
    }
    
    code_lookup = {
        'trade_types': ["BUY", "SELL"],
        'asset_types': list(code_map['asset_type'].values()), # 한글로 옵션 제공
        'currencies': list(code_map['currency'].values()),
        'markets': list(code_map['market'].values()),'account_owners': account_lookup['owner'].dropna().unique().tolist() if not is_account_empty else ["승엽", "민희"],
        'account_types': account_lookup['type'].dropna().unique().tolist() if not is_account_empty else ["일반", "ISA", "DC", "IRP", "연금저축"],
        'type_to_kr': {'stock': '주식', 'us_stock': '미국 주식', 'cash': '현금', 'fund': '펀드', 
                       'bond': '채권', 'gold': '금', 'etf': 'ETF', 'commodity': '원자재'}, 
        'code_map': code_map, # 코드 <-> 한글 맵핑 데이터
    }

    # 📌 [룩업 변환용] 한글 -> 코드 (저장 시 사용)
    kr_to_code_map = {
        key: {v: k for k, v in value.items()} 
        for key, value in code_map.items()
    }

    return {
        'asset_id_to_name': asset_id_to_name, # 자산 ID <-> 한글명
        'asset_name_to_id': asset_name_to_id, 
        
        'account_id_to_name_display': account_id_to_name_display, # 계좌 ID <-> Display Name (UI용)
        'account_name_to_id_display': account_name_to_id_display, 
        
        'account_id_to_name_db': account_id_to_name_db, # 계좌 ID <-> 계좌명 (DB값)
        'account_name_to_id_db': account_name_to_id_db, # 계좌명 (DB값) <-> ID (저장용)
        
        'kr_to_code_map': kr_to_code_map, # 한글 -> 코드 맵핑
        'codes': code_lookup,
        'asset_lookup_df': asset_lookup,
        'account_lookup_df': account_lookup
    }

# -------------------------------------------------------------------
# 5. 데이터 변경 함수 (저장/수정/삭제) - 룩업 데이터 변환 로직 추가
# -------------------------------------------------------------------
def update_data(table_name, df_changes):
    supabase = init_connection()
    if not supabase: return

    try:
        lookup = get_lookup_data()
        
        # assets 테이블 저장 시 한글 -> 코드로 변환
        if table_name == 'assets':
            kr_to_code = lookup['kr_to_code_map']
            
            # 한글 컬럼을 코드 컬럼으로 변환
            for kr_col, code_col in [('asset_type_kr', 'asset_type'), 
                                     ('currency_kr', 'currency'), 
                                     ('market_kr', 'market')]:
                if kr_col in df_changes.columns:
                    # 한글을 코드로 맵핑. 맵핑 실패 시 기존 코드 값을 유지
                    code_type = code_col.split('_')[0] # 'asset'
                    df_changes[code_col] = df_changes[kr_col].map(kr_to_code[code_col]).fillna(df_changes[code_col])
                    df_changes = df_changes.drop(columns=[kr_col], errors='ignore')

        if table_name == 'transactions':
            # 📌 [1번 요청 반영] 거래 기록 저장 시 'account_name' (Display Name)을 ID로 변환
            
            # 자산명 변환 (기존과 동일)
            if 'name_kr' in df_changes.columns:
                df_changes['asset_id'] = df_changes['name_kr'].map(lookup['asset_name_to_id'])
                df_changes = df_changes.drop(columns=['name_kr'], errors='ignore')

            # 계좌명 변환 (UI Display Name -> DB ID)
            if 'account_display_name' in df_changes.columns:
                # UI에서 선택된 Display Name을 ID로 변환하여 DB에 저장
                df_changes['account_id'] = df_changes['account_display_name'].map(lookup['account_name_to_id_display'])
                # DB 스키마에는 'account_name'이 있으므로, 이 필드에도 DB 계좌명을 넣어줍니다.
                # 이는 DB 스키마가 account_name 필드를 요구할 때의 방어 로직입니다. (필요 시 제거 가능)
                # Display Name의 역변환을 통해 계좌 이름만 추출하거나,
                # 아니면 Display Name -> ID -> DB Name 순서로 맵핑합니다.
                df_changes['account_name'] = df_changes['account_id'].map(lookup['account_id_to_name_db'])
                
                df_changes = df_changes.drop(columns=['account_display_name'], errors='ignore')
            
            # DB에 'account_id'가 없는 경우를 대비하여 ID 컬럼을 제거하지 않습니다.
            # 만약 DB에 'account_id'가 있다면, 위에서 생성된 'account_id'가 저장됩니다.
            # 만약 DB에 'account_id'가 없고 'account_name'만 있다면, 새로 생성된 'account_name'이 저장됩니다.

        # 날짜/시간 처리 및 DB 전송
        if 'transaction_date' in df_changes.columns:
            # datetime 객체를 DB가 요구하는 문자열 형식으로 변환 (NaT는 None으로 처리)
            df_changes['transaction_date'] = df_changes['transaction_date'].dt.strftime('%Y-%m-%d').where(df_changes['transaction_date'].notnull(), None)
            
        records = df_changes.where(pd.notnull(df_changes), None).to_dict('records')
        
        supabase.table(table_name).upsert(records).execute()
        
        st.cache_data.clear()
        st.success("✅ 데이터가 성공적으로 저장되었습니다!")
        return True
    except Exception as e:
        st.error(f"❌ 데이터 저장 실패: {e}")
        return False