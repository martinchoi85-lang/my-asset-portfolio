# src/asset_portfolio/dashboard/app.py
import streamlit as st
from asset_portfolio.dashboard.render import (
    render_asset_return_section, 
    render_portfolio_return_section, 
    render_asset_contribution_section_full,
    render_account_selector, 
    render_period_selector, 
    render_asset_weight_section,
    render_asset_contribution_stacked_area, 
    render_portfolio_treemap,
    render_transactions_table_section
)
from asset_portfolio.dashboard.transaction_editor import render_transaction_editor
from asset_portfolio.dashboard.asset_editor import render_asset_editor
from asset_portfolio.dashboard.price_updater import render_price_updater

st.set_page_config(
    page_title="Asset Portfolio Dashboard",
    layout="wide"
)

# 페이지 전환
page = st.sidebar.radio(
    "화면 선택",
    ["Main Dashboard", "Transaction Editor", "Price Updater", "Asset Editor"],
    index=0,
)

if page == "Transaction Editor":
    render_transaction_editor()
    st.stop()


if page == "Price Updater":
    render_price_updater()
    st.stop()


if page == "Asset Editor":
    render_asset_editor()
    st.stop()

# =========================
# Main Dashboard 기존 로직
# =========================
st.title("📊 포트폴리오 수익률 대시보드")

account_id = render_account_selector()

if not account_id:
    st.stop()
    
start_date, end_date = render_period_selector()

tab1, tab2 = st.tabs(["Dashboard", "Transactions"])

with tab1:
    render_portfolio_return_section(account_id, start_date, end_date)
    st.divider()
    render_asset_return_section(account_id, start_date, end_date)
    st.divider()
    render_asset_contribution_section_full(account_id, start_date, end_date)
    st.divider()
    render_asset_weight_section(account_id, start_date, end_date)
    st.divider()
    render_asset_contribution_stacked_area(account_id, start_date, end_date)
    st.divider()
    render_portfolio_treemap(account_id, start_date, end_date)

with tab2:
    render_transactions_table_section(account_id, start_date, end_date)