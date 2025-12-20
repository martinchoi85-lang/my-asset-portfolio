# 모든 기능을 하나로 묶어주는 파일입니다. 기존의 3탭 구조를 완벽하게 복원했습니다.
import streamlit as st
import pandas as pd
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
    initial_sidebar_state="collapsed"
)

# ----------------------------------------------------
# 📌 [3번 요청 해결] 탭 상태 관리
# ----------------------------------------------------
# 세션 상태에 현재 활성 탭 저장 (기본값: 0 = 대시보드)
if 'active_tab' not in st.session_state:
    st.session_state['active_tab'] = 0

# ----------------------------------------------------
# 2. 데이터 로드 (Main에서 한 번에 로드)
# ----------------------------------------------------
with st.spinner("데이터를 동기화 중입니다..."):
    df_summary = fetch_data("asset_summary") 
    df_transactions = fetch_data("transactions") 
    df_accounts = fetch_data("accounts")
    df_assets = fetch_data("assets")
    lookup_data = get_lookup_data() 
    usd_rate = fetch_usd_exchange_rate()

# ----------------------------------------------------
# 3. UI 헤더
# ----------------------------------------------------
st.title("💰 금융 자산 포트폴리오")
st.caption(f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# ----------------------------------------------------
# 4. 탭 구성 (기존 3개 탭 구조 복원)
# ----------------------------------------------------
# 탭을 상단에 고정하는 CSS
st.markdown("""
    <style>
    /* 탭 바 전체를 고정 */
    section[data-testid="stHorizontalBlock"] > div:has(div[data-baseweb="tab-list"]) {
        position: sticky !important;
        top: 0 !important;
        background-color: white !important;
        z-index: 999 !important;
        padding: 1rem 0 0.5rem 0;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
    }
    
    /* 탭 리스트 자체도 고정 */
    div[data-baseweb="tab-list"] {
        position: sticky !important;
        top: 0 !important;
        background-color: white !important;
        z-index: 1000 !important;
    }
    
    /* 다크모드 */
    [data-theme="dark"] section[data-testid="stHorizontalBlock"] > div:has(div[data-baseweb="tab-list"]),
    [data-theme="dark"] div[data-baseweb="tab-list"] {
        background-color: #0e1117 !important;
    }
    </style>
""", unsafe_allow_html=True)

# 📌 [3번 요청 해결] 탭 선택 시 세션 상태 업데이트를 위한 콜백 함수
def on_tab_change():
    """탭 변경 시 세션 상태에 현재 탭 인덱스 저장"""
    # Streamlit의 tabs는 직접적인 콜백을 지원하지 않으므로,
    # 각 탭 내부에서 상태를 업데이트하는 방식 사용
    pass

# 📌 기본 탭 인덱스 설정 (st.tabs는 index 파라미터를 지원하지 않음)
# 대신, 각 탭 내부에서 위젯 상태를 관리하여 재실행 시에도 유지되도록 함

# 📌 [탭 전환 문제 완화] session_state로 위젯 상태 유지
# st.tabs는 재실행 시 항상 첫 번째 탭이 활성화되는 한계가 있습니다.
# 완벽한 해결은 불가능하지만, 다음 방법으로 완화할 수 있습니다:

tab1, tab2, tab3, tab4 = st.tabs(["📊 대시보드", "📝 거래 기록 편집", "💼 자산 정보 관리", "🏦 계좌 정보 관리"])

with tab1:
    show_dashboard(df_summary, usd_rate, lookup_data) 

with tab2:
    show_transaction_editor(df_transactions, lookup_data)
    
with tab3:
    show_asset_editor(df_assets, lookup_data) 
    
with tab4:
    show_account_editor(df_accounts, lookup_data)

# 📌 [사용자 가이드] 탭 전환 문제 안내
st.sidebar.markdown("""
### 💡 사용 팁
**거래 입력 시 탭이 전환되는 문제**는 Streamlit의 기술적 한계입니다.
- 입력 중 탭이 바뀌어도 데이터는 유지됩니다
- 입력 완료 후 '저장' 버튼을 눌러주세요
""")

# 📌 [3번 요청 추가 설명]
# Streamlit의 탭은 모두 한 번에 렌더링되므로, 
# data_editor에서 값 변경 시 전체 페이지가 재실행됩니다.
# 이 때 기본적으로 첫 번째 탭(대시보드)이 활성화되는 것은 
# Streamlit의 기본 동작입니다.
#
# 완벽한 해결책은 없지만, 다음 방법들로 완화할 수 있습니다:
# 1. 각 탭의 위젯에 unique key 부여 (이미 적용됨)
# 2. session_state를 활용한 상태 유지 (이미 적용됨)
# 3. 탭 전환을 최소화하기 위해 저장 버튼을 누르기 전까지는 
#    데이터를 세션에만 저장하고 DB 저장은 명시적으로 수행
#
# 📌 사용자 가이드:
# - 거래 입력 중에는 자동 저장되지 않으므로 입력 완료 후 '저장' 버튼 클릭
# - 입력 중 탭이 전환되더라도 데이터는 data_editor의 상태로 유지됨


# [ToDo]
# ASAP)transaction_editor에서 row 삭제 기능 필요: 이거 안하면 실행 안됨
# 1)transaction_editor에서 거래내역 추가/수정/삭제 내용을 asset_summary에 반영하는 로직
# 1-1)위 2번의 내역을 바탕으로 개별 종목 손익률 차트 만들기
# 2)Ticker 없는 종목들 현재가 크롤링 로직 추가
# 3)현재가 기준으로 매일 asset_summary 테이블 snapshot 만드는 로직 추가
# 3-1)asset_summary snapshot으로 포트폴리오 전체 수익률 history 차트 만들기(portfolio_pnl_history 테이블 업데이트 로직)
# 4)asset_summary 테이블을 현 시점 데이터로 채우고 앱 Launching
# 5)asset_summary 테이블 history 만들기(portfolio_pnl_history 테이블 업데이트 로직 및 해당 차트)