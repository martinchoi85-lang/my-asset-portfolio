# 모든 기능을 하나로 묶어주는 파일입니다. 기존의 3탭 구조를 완벽하게 복원했습니다.
# main.py
import streamlit as st
from datetime import datetime

# 모듈 불러오기
from utils.data_loader import fetch_data, fetch_usd_exchange_rate, get_lookup_data
from views.dashboard import show_dashboard
from views.transaction_editor import show_transaction_editor
from views.asset_editor import show_asset_editor
from views.account_editor import show_account_editor

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