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
    render_asset_transaction_history,  # 자산별 거래 내역 조회
    render_period_performance_section, # 기간별 성과 분석
    render_asset_contribution_stacked_area,
    render_realized_pnl_charts,
    render_portfolio_trend_chart
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
          const target = base + "/"; // Streamlit Cloud might not like query params initially or we just go to root
          
          // Streamlit component runs in an iframe. We need to redirect the top window.
          try {{
              window.top.location.href = target;
          }} catch(e) {{
              // Fallback if cross-origin rules block top navigation (unlikely for simple redirects but possible)
              console.error("Top navigation failed:", e);
              window.location.href = target;
          }}
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
    print("user_id: ", user_id)
    print("username: ", username)
    print("user: ", user)

    # --- Sidebar ---
    with st.sidebar:
        st.success(f"'{username}'님으로 로그인")
        if st.button("로그아웃"):
            del st.session_state.user
            st.rerun()

        # 기능 그룹화
        menu_items = {
            "🏠 대시보드": ["대시보드"],
            "✍️ 거래 관리": ["거래내역 입력", "정기매수 관리", "거래내역 업로드"],
            "💼 자산 관리": ["자산 정보 수정", "자산가격 업데이트"],
            "🛠️ 시스템 관리": ["스냅샷 수정"],
        }
        
        # 1. 메인 카테고리 선택
        selected_category = st.selectbox("메뉴 그룹", list(menu_items.keys()))
        
        # 2. 서브 메뉴 선택
        # "대시보드" 처럼 서브메뉴가 1개인 경우 바로 선택된 것으로 처리하거나,
        # 숨기고 싶다면 radio를 조건부로 보여줄 수 있음.
        # 여기서는 직관성을 위해 항상 radio를 보여주되, 선택지가 1개면 그것이 선택됨.
        sub_options = menu_items[selected_category]
        if len(sub_options) == 1:
            page = sub_options[0]
        else:
            page = st.radio("기능 선택", sub_options)

    # --- Page Routing ---
    if page == "거래내역 입력":
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
    if page == "거래내역 업로드":
        render_transaction_importer(user_id=user_id)
        st.stop()

    # --- Main Dashboard Content (page == "대시보드") ---
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
    
    # 탭 재구성: 요약 / 성과 / 이력
    tab1, tab2, tab3 = st.tabs(["🏠 요약 (Overview)", "📈 성과 (Performance)", "📜 이력 (History)"])

    with tab1:
        st.caption("현재 자산 상태 요약")
        # 1. KPI (기존 대시보드 상단)
        render_kpi_section(user_id, account_id, start_date, end_date)
        st.divider()
        
        # 2. 보유 종목 리스트 (Snapshot Table)
        render_latest_snapshot_table(user_id, account_id)
        st.divider()
        
        # 3. 자산 비중 (Pie + Bar)
        c1, c2 = st.columns(2)
        with c1:
            render_asset_grouping_pie_section(user_id, account_id)
        with c2:
            # st.subheader("📊 자산 비중 상세")
            render_asset_weight_section(user_id, account_id, start_date, end_date)

    with tab2:
        st.caption("기간별 투자 성과 분석")
        # 1. 기간별 성과 요약 (Period Analysis)
        render_period_performance_section(user_id, account_id, start_date, end_date)
        st.divider()

        # 2. 총자산 추세 (Trend Chart)
        render_portfolio_trend_chart(user_id, account_id, start_date, end_date)
        st.divider()

        # 3. 벤치마크 비교
        render_benchmark_comparison_section(user_id, account_id, start_date, end_date)
        st.divider()

        # 4. 수익 기여도 & 자산별 수익률
        t2_c1, t2_c2 = st.columns(2)
        with t2_c1:
            render_asset_contribution_section_full(user_id, account_id, start_date, end_date)
        with t2_c2:
            render_asset_return_section(user_id, account_id, start_date, end_date)
            
        st.divider()

        # 5. 누적 기여도 (Stacked Area)
        render_asset_contribution_stacked_area(user_id, account_id, start_date, end_date)

        st.divider()

        # 6. 트리맵
        render_portfolio_treemap(user_id, account_id, start_date, end_date)
        
        st.divider()

        # 7. 실현손익 분석
        render_realized_pnl_charts(user_id, account_id, start_date, end_date, key_suffix="performance_tab")

    with tab3:
        st.caption("전체 거래 내역 및 실현손익 분석")
        
        # 1. 기간 내 실현손익 분석 차트
        render_realized_pnl_charts(user_id, account_id, start_date, end_date, key_suffix="history_tab")
        st.divider()
        
        # 2. 자산별 거래 내역 조회
        render_asset_transaction_history(user_id, account_id)
        st.divider()
        # 2. 전체 거래 내역 테이블
        render_transactions_table_section(user_id, account_id, start_date, end_date)


# --- Main app execution logic ---
_inject_mobile_redirect()

if "user" not in st.session_state:
    render_login_page()
else:
    render_main_dashboard()
