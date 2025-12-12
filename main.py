# 모든 기능을 하나로 묶어주는 파일입니다. 기존의 3탭 구조를 완벽하게 복원했습니다.
# main.py
import streamlit as st
from datetime import datetime

# 모듈 불러오기
from utils.data_loader import fetch_data, fetch_usd_exchange_rate
from views.dashboard import show_dashboard
from views.transaction_editor import show_transaction_editor
from views.asset_editor import show_asset_editor

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
with st.spinner("데이터를 동기화 중입니다..."):
    # 1) 대시보드용 뷰
    df_summary = fetch_data("asset_summary") 
    # 2) 편집용 원본 테이블
    df_transactions = fetch_data("transactions") 
    df_assets = fetch_data("assets")
    # 3) 환율
    usd_rate = fetch_usd_exchange_rate()

# ----------------------------------------------------
# 3. UI 헤더
# ----------------------------------------------------
st.title("💰 금융 자산 포트폴리오")
st.caption(f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# ----------------------------------------------------
# 4. 탭 구성 (기존 3개 탭 구조 복원)
# ----------------------------------------------------
tab1, tab2, tab3 = st.tabs(["📊 대시보드", "📝 거래 기록 편집", "💼 자산 정보 관리"])

with tab1:
    # 모바일 최적화된 대시보드 뷰 호출
    show_dashboard(df_summary, usd_rate)

with tab2:
    # 거래 기록 편집 뷰 호출
    show_transaction_editor(df_transactions)

with tab3:
    # 자산 정보 편집 뷰 호출
    show_asset_editor(df_assets)