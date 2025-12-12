# 모든 기능을 하나로 묶어주는 파일입니다. 기존의 3탭 구조를 완벽하게 복원했습니다.
# main.py
import streamlit as st
from datetime import datetime

# 모듈 불러오기
from utils.data_loader import fetch_data, fetch_usd_exchange_rate#, get_lookup_data
from views.dashboard import show_dashboard
from views.transaction_editor import show_transaction_editor
from views.asset_editor import show_asset_editor
from views.account_editor import show_account_editor


# ----------------------------------------------------
# 임시(디버깅 중)
# ----------------------------------------------------
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

# ----------------------------------------------------
# 1. 페이지 설정
# ----------------------------------------------------
st.set_page_config(
    page_title="승엽민희 포트폴리오",
    layout="wide",
    initial_sidebar_state="collapsed" # 모바일 최적화 (사이드바 닫기)
)

# ----------------------------------------------------
# 2. 데이터 로드 (Main에서 한 번에 로드)
# ----------------------------------------------------
# with st.container():
#     # 📌 [4번 요청 디버깅 지원] 캐시 초기화 버튼
#     if st.button("🔄 전체 캐시 초기화 (DB 재연결)", key='clear_all_cache', type='warning'):
#         st.cache_data.clear() 
#         st.cache_resource.clear()
#         # 현재가 업데이트 플래그도 초기화
#         if 'current_prices_fetched' in st.session_state:
#             st.session_state['current_prices_fetched'] = False
#         st.rerun()
        
with st.spinner("데이터를 동기화 중입니다..."):
    # 1) 대시보드용 뷰
    df_summary = fetch_data("asset_summary") 
    # 2) 편집용 원본 테이블
    df_transactions = fetch_data("transactions") 
    # 3) accounts 테이블
    df_accounts = fetch_data("accounts")
    # 4) 자산 데이터
    df_assets = fetch_data("assets")
    # 5) 룩업 데이터
    lookup_data = get_lookup_data() 
    # 6) 환율
    usd_rate = fetch_usd_exchange_rate()

# ----------------------------------------------------
# 3. UI 헤더
# ----------------------------------------------------
st.title("💰 금융 자산 포트폴리오")
st.caption(f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# ----------------------------------------------------
# 4. 탭 구성 (기존 3개 탭 구조 복원)
# ----------------------------------------------------
tab1, tab2, tab3, tab4 = st.tabs(["📊 대시보드", "📝 거래 기록 편집", "💼 자산 정보 관리", "🏦 계좌 정보 관리"]) # 📌 [수정] 탭 4개

lookup_data = get_lookup_data()

with tab1:
    show_dashboard(df_summary, usd_rate, lookup_data) 

with tab2:
    show_transaction_editor(df_transactions, lookup_data) 

with tab3:
    show_asset_editor(df_assets, lookup_data) 
    
with tab4: # 📌 [추가] 계좌 관리 탭
    show_account_editor(df_accounts, lookup_data) 