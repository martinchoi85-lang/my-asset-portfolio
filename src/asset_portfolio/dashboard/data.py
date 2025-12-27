import pandas as pd
import streamlit as st
from typing import Optional
from asset_portfolio.backend.infra.supabase_client import get_supabase_client

ALL_ACCOUNT_TOKEN = "__ALL__"


def load_asset_contribution_data(account_id: str, start_date: str, end_date: str):
    supabase = get_supabase_client()
    q = (
        supabase.table("daily_snapshots")
        .select("date, asset_id, valuation_amount")
        .gte("date", start_date)
        .lte("date", end_date)
    )

    # ✅ ALL이 아닌 경우에만 계좌 필터 적용
    if account_id and account_id != "__ALL__":
        q = q.eq("account_id", account_id)

    response = q.execute()

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


def build_daily_snapshots_query(
    select_cols: str,
    start_date: str,
    end_date: str,
    account_id: Optional[str] = None,
):
    """
    daily_snapshots 공통 쿼리 빌더
    - account_id가 "__ALL__"이면 account 필터를 걸지 않는다.
    - execute()는 여기서 하지 않는다(호출자가 마지막에 execute)
    """
    supabase = get_supabase_client()

    q = (
        supabase.table("daily_snapshots")
        .select(select_cols)
        .gte("date", start_date)
        .lte("date", end_date)
        .order("date")
    )

    # if start_date:
    #     query = query.gte("date", start_date)

    # if end_date:
    #     query = query.lte("date", end_date)

    if account_id and account_id != ALL_ACCOUNT_TOKEN:
        q = q.eq("account_id", account_id)

    return q
