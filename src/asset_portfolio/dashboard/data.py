import pandas as pd
import streamlit as st
from asset_portfolio.backend.infra.supabase_client import get_supabase_client
from asset_portfolio.backend.infra.query import build_daily_snapshots_query


def load_asset_contribution_data(account_id: str, start_date: str, end_date: str):
    query = build_daily_snapshots_query(
        select_cols="date, asset_id, valuation_amount",
        start_date=start_date,
        end_date=end_date,
        account_id=account_id,
    )
    response = query.order("date").execute()
    return response.data or []


@st.cache_data(ttl=3600)
def load_assets_lookup() -> pd.DataFrame:
    """
    asset_id → 자산명(name_kr) 매핑용 lookup 로드
    """
    supabase = get_supabase_client()

    resp = (
        supabase.table("assets")
        .select("id, name_kr, ticker, asset_type, currency, market")
        .execute()
    )

    rows = resp.data or []
    if not rows:
        return pd.DataFrame(columns=["asset_id", "name_kr", "ticker", "asset_type", "currency", "market"])

    df = pd.DataFrame(rows).rename(columns={"id": "asset_id"})
    return df



