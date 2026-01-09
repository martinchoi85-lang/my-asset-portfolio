import pandas as pd
import streamlit as st
from asset_portfolio.backend.infra.supabase_client import get_supabase_client


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


