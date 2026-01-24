# src/asset_portfolio/dashboard/app.py
import os
import streamlit as st
import streamlit.components.v1 as components
from asset_portfolio.dashboard.render import (
    render_asset_return_section, 
    render_kpi_section,
    render_benchmark_comparison_section, 
    render_asset_contribution_section_full,
    render_account_selector, 
    render_period_selector, 
    render_asset_weight_section,
    render_asset_contribution_stacked_area, 
    render_portfolio_treemap,
    render_transactions_table_section,
    render_latest_snapshot_table,
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

    html = """
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
        """.format(mobile_url=mobile_url)
    components.html(html, height=0)

_inject_mobile_redirect()

# 페이지 전환
page = st.sidebar.radio(
    "화면 선택",
    ["자산 종합/분석", "거래내역 수정", "정기매수 관리", "자산가격 업데이트", "자산 정보 수정", "스냅샷 수정", "Transaction Importer"],
    index=0,
)

if page == "거래내역 수정":
    render_transaction_editor()
    st.stop()

if page == "정기매수 관리":
    render_recurring_order_editor()
    st.stop()   


if page == "자산가격 업데이트":
    render_price_updater()
    st.stop()


if page == "자산 정보 수정":
    render_asset_editor()
    st.stop()


if page == "스냅샷 수정":
    render_snapshot_editor()
    st.stop()

if page == "Transaction Importer":
    render_transaction_importer()
    st.stop()

# =========================
# Main Dashboard 기존 로직
# =========================
st.title("📊 승엽&민희 자산 포트폴리오")

account_id = render_account_selector()

if not account_id:
    st.stop()
    
start_date, end_date = render_period_selector(account_id)


# --- 디버그: 단일 날짜 고정 모드 (원인 규명용) ---
# with st.sidebar.expander("🧪 디버그 옵션", expanded=False):
#     debug_single_day = st.checkbox("단일 날짜로 고정", value=False)
#     debug_day = st.date_input("조회 날짜", value=end_date)

# if debug_single_day:
#     start_date = debug_day
#     end_date = debug_day

# st.sidebar.caption(f"DEBUG date_range: {start_date} ~ {end_date}")
# --- 디버그: 단일 날짜 고정 모드 (원인 규명용) 끝 ---


tab1, tab2 = st.tabs(["Dashboard", "Transactions"])

with tab1:
    render_kpi_section(account_id, start_date, end_date)
    st.divider()
    render_latest_snapshot_table(account_id)
    st.divider()
    render_benchmark_comparison_section(account_id, start_date, end_date)
    st.divider()
    render_asset_contribution_section_full(account_id, start_date, end_date)
    st.divider()
    render_asset_return_section(account_id, start_date, end_date)
    st.divider()
    render_asset_weight_section(account_id, start_date, end_date)
    # st.divider()
    # render_asset_contribution_stacked_area(account_id, start_date, end_date)
    st.divider()
    render_portfolio_treemap(account_id, start_date, end_date)

with tab2:
    render_transactions_table_section(account_id, start_date, end_date)
