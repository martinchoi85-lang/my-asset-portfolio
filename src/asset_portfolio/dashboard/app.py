# src/asset_portfolio/dashboard/app.py
import os
import streamlit as st
import streamlit.components.v1 as components

from asset_portfolio.backend.infra import query
from asset_portfolio.dashboard.render import (
    render_asset_return_section,
    render_kpi_section,
    render_benchmark_comparison_section,
    render_asset_contribution_section_full,
    render_account_selector,
    render_period_selector,
    render_asset_weight_section,
    render_portfolio_treemap,
    render_transactions_table_section,
    render_latest_snapshot_table,
    render_asset_grouping_pie_section,
    render_portfolio_trend_chart,
    render_asset_transaction_history,  # 자산별 거래 내역 조회
    render_period_performance_section, # 기간별 성과 분석
)
from asset_portfolio.dashboard.transaction_editor import render_transaction_editor
from asset_portfolio.dashboard.transaction_importer import render_transaction_importer
from asset_portfolio.dashboard.asset_editor import render_asset_editor
from asset_portfolio.dashboard.price_updater import render_price_updater
from asset_portfolio.dashboard.snapshot_editor import render_snapshot_editor
from asset_portfolio.dashboard.recurring_order_editor import render_recurring_order_editor

st.set_page_config(
    page_title="Asset Portfolio Dashboard",
    layout="wide"
)

def _inject_mobile_redirect():
    mobile_url = os.environ.get("MOBILE_URL")
    if not mobile_url:
        return

    html = f"""
        <script>
        (function() {{
          const ua = (navigator.userAgent || "").toLowerCase();
          const isMobile = /iphone|android|ipad|ipod|mobile|opera mini|blackberry|iemobile/.test(ua);
          if (!isMobile) return;

          // 디버깅/예외 처리를 위해 no_mobile_redirect=1 이면 리다이렉트하지 않는다.
          if (window.location.search.includes("no_mobile_redirect=1")) return;

          const base = "{mobile_url}".replace(/\\/$/, "");
          const target = base + "/?from=streamlit";
          window.location.replace(target);
        }})();
        </script>
        """
    components.html(html, height=0)

def render_login_page():
    """Renders the login page."""
    st.title("🔒 포트폴리오 로그인")
    password = st.text_input("비밀번호를 입력하세요.", type="password", key="password_input")

    if st.button("로그인"):
        if not password:
            st.warning("비밀번호를 입력해주세요.")
            st.stop()
        
        user = query.get_user_by_password(password)
        if user:
            st.session_state.user = user
            st.rerun()
        else:
            st.error("비밀번호가 올바르지 않습니다.")
    
    st.stop()

def render_main_dashboard():
    """Renders the main dashboard after user is logged in."""
    user = st.session_state.user
    user_id = user['id']
    username = user['username']

    # --- Sidebar ---
    with st.sidebar:
        st.success(f"'{username}'님으로 로그인")
        if st.button("로그아웃"):
            del st.session_state.user
            st.rerun()

        page = st.sidebar.radio(
            "화면 선택",
            ["자산 종합/분석", "거래내역 수정", "정기매수 관리", "자산가격 업데이트", "자산 정보 수정", "스냅샷 수정", "Transaction Importer"],
            index=0,
        )

    # --- Page Routing ---
    if page == "거래내역 수정":
        render_transaction_editor(user_id=user_id)
        st.stop()
    if page == "정기매수 관리":
        render_recurring_order_editor(user_id=user_id)
        st.stop()   
    if page == "자산가격 업데이트":
        render_price_updater()
        st.stop()
    if page == "자산 정보 수정":
        render_asset_editor()
        st.stop()
    if page == "스냅샷 수정":
        render_snapshot_editor(user_id=user_id)
        st.stop()
    if page == "Transaction Importer":
        render_transaction_importer(user_id=user_id)
        st.stop()

    # --- Main Dashboard Content ---
    portfolio_title = "지온이의 포트폴리오" if username == "지온이" else "승엽&민희 자산 포트폴리오"
    
    mobile_url = os.environ.get("MOBILE_URL")
    title_cols = st.columns([0.05, 0.95], vertical_alignment="center")
    with title_cols[0]:
        if st.button("📊", help="모바일 페이지로 전환", disabled=not mobile_url):
            target = f"{mobile_url.rstrip('/')}/?from=streamlit"
            components.html(
                f"<script>window.location.replace('{target}');</script>",
                height=0,
            )
            st.stop()
    with title_cols[1]:
        st.title(portfolio_title)
    
    user_accounts = query.get_accounts(user_id)
    if not user_accounts:
        st.warning("표시할 계좌가 없습니다. 먼저 계좌를 추가해주세요.")
        st.stop()

    account_id = render_account_selector(user_accounts)
    if not account_id:
        st.stop()
    
    start_date, end_date = render_period_selector(user_id, account_id)
    
    tab1, tab2, tab3, tab4 = st.tabs(["대시보드", "자산 분석", "자산별 거래", "거래 내역"])

    with tab1:
        render_kpi_section(user_id, account_id, start_date, end_date)
        st.divider()
        render_portfolio_trend_chart(user_id, account_id, start_date, end_date)
        st.divider()
        render_latest_snapshot_table(user_id, account_id)
        st.divider()
        render_portfolio_treemap(user_id, account_id, start_date, end_date)

    with tab2:
        render_period_performance_section(user_id, account_id, start_date, end_date)
        st.divider()
        render_asset_grouping_pie_section(user_id, account_id)
        st.divider()
        render_benchmark_comparison_section(user_id, account_id, start_date, end_date)
        st.divider()
        render_asset_contribution_section_full(user_id, account_id, start_date, end_date)
        st.divider()
        render_asset_return_section(user_id, account_id, start_date, end_date)
        st.divider()
        render_asset_weight_section(user_id, account_id, start_date, end_date)

    with tab3:
        # 보유 중인 자산별 거래 내역 조회
        render_asset_transaction_history(user_id, account_id)

    with tab4:
        render_transactions_table_section(user_id, account_id, start_date, end_date)


# --- Main app execution logic ---
_inject_mobile_redirect()

if "user" not in st.session_state:
    render_login_page()
else:
    render_main_dashboard()
