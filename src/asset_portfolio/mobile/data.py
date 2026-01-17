from __future__ import annotations

from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd

from asset_portfolio.backend.infra.query import load_asset_contribution_data
from asset_portfolio.backend.infra.supabase_client import get_supabase_client
from asset_portfolio.backend.services.manual_cost_basis_service import attach_manual_cost_basis
from asset_portfolio.backend.services.portfolio_service import (
    calculate_asset_contributions,
    get_portfolio_return_series,
)
from asset_portfolio.backend.services.portfolio_weight_service import load_latest_asset_weights


ALL_ACCOUNT_TOKEN = "__ALL__"


def _date_range_from_days(days: int) -> Tuple[str, str]:
    """최근 n일 범위를 (start, end) 문자열로 반환합니다."""
    # 초보자용 설명:
    # - date.today()는 오늘 날짜를 의미합니다.
    # - timedelta(days=...)로 날짜를 쉽게 빼서 기간을 만듭니다.
    end_date = date.today()
    safe_days = max(int(days), 1)
    start_date = end_date - timedelta(days=safe_days - 1)
    return start_date.isoformat(), end_date.isoformat()


def _json_safe_records(df: pd.DataFrame) -> List[Dict]:
    """NaN/NaT 값을 JSON에서 이해 가능한 None으로 치환합니다."""
    # Pandas의 NaN은 JSON으로 직렬화할 때 문제가 될 수 있어요.
    # 그래서 딕셔너리로 변환하기 전에 None으로 바꿉니다.
    clean_df = df.where(pd.notna(df), None)
    return clean_df.to_dict(orient="records")


def list_accounts() -> List[Dict]:
    """계좌 목록을 조회합니다."""
    supabase = get_supabase_client()
    response = (
        supabase.table("accounts")
        .select("id, name, brokerage, owner, type")
        .order("brokerage")
        .execute()
    )
    return response.data or []


def load_assets_lookup() -> pd.DataFrame:
    """자산 정보 lookup을 조회합니다."""
    supabase = get_supabase_client()
    response = (
        supabase.table("assets")
        .select("id, name_kr, ticker, asset_type, currency, market")
        .execute()
    )
    rows = response.data or []
    if not rows:
        return pd.DataFrame(columns=["asset_id", "name_kr", "ticker", "asset_type", "currency", "market"])
    return pd.DataFrame(rows).rename(columns={"id": "asset_id"})


def get_kpi_summary(account_id: str, days: int) -> Dict[str, Optional[float]]:
    """전체 포트폴리오 KPI를 계산합니다."""
    start_date, end_date = _date_range_from_days(days)
    portfolio_df = get_portfolio_return_series(account_id, start_date, end_date)

    if portfolio_df.empty:
        return {
            "total_valuation": None,
            "total_purchase": None,
            "profit": None,
            "profit_rate": None,
            "portfolio_return_pct": None,
        }

    pf_valid = portfolio_df.dropna(subset=["portfolio_return"]).copy()

    if not pf_valid.empty:
        last = pf_valid.sort_values("date").iloc[-1]
        total_val = float(last["valuation_amount"])
        total_buy = float(last["purchase_amount"])
        pnl = total_val - total_buy
        pnl_rate = (pnl / total_buy * 100) if total_buy > 0 else 0.0
        portfolio_return_pct = float(last["portfolio_return"]) * 100
    else:
        total_val = float(portfolio_df["valuation_amount"].dropna().iloc[-1]) if portfolio_df["valuation_amount"].notna().any() else 0.0
        total_buy = float(portfolio_df["purchase_amount"].dropna().iloc[-1]) if portfolio_df["purchase_amount"].notna().any() else 0.0
        pnl = total_val - total_buy
        pnl_rate = (pnl / total_buy * 100) if total_buy > 0 else 0.0
        portfolio_return_pct = 0.0

    return {
        "total_valuation": total_val,
        "total_purchase": total_buy,
        "profit": pnl,
        "profit_rate": pnl_rate,
        "portfolio_return_pct": portfolio_return_pct,
    }


def get_latest_snapshot_table(account_id: str) -> Dict[str, Optional[List[Dict]]]:
    """가장 최신 스냅샷 테이블 데이터를 반환합니다."""
    supabase = get_supabase_client()

    latest_query = (
        supabase.table("daily_snapshots")
        .select("date")
        .order("date", desc=True)
        .limit(1)
    )
    if account_id != ALL_ACCOUNT_TOKEN:
        latest_query = latest_query.eq("account_id", account_id)

    latest_row = latest_query.execute().data or []
    if not latest_row:
        return {"latest_date": None, "rows": []}

    latest_date = latest_row[0]["date"]

    rows_query = (
        supabase.table("daily_snapshots")
        .select(
            "date, account_id, asset_id, quantity, purchase_price, valuation_price, "
            "valuation_amount, purchase_amount, currency, "
            "assets (name_kr, asset_type, price_source), accounts (name)"
        )
        .eq("date", latest_date)
    )
    if account_id != ALL_ACCOUNT_TOKEN:
        rows_query = rows_query.eq("account_id", account_id)

    rows = rows_query.execute().data or []
    if not rows:
        return {"latest_date": latest_date, "rows": []}

    df = pd.json_normalize(rows, sep=".")

    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df = df[df["quantity"].fillna(0) != 0]
    if df.empty:
        return {"latest_date": latest_date, "rows": []}

    # ✅ manual 자산 원금(cost basis) 정보 붙이기
    df = attach_manual_cost_basis(df)

    df["purchase_amount"] = pd.to_numeric(df["purchase_amount"], errors="coerce")
    df["valuation_amount"] = pd.to_numeric(df["valuation_amount"], errors="coerce")
    if "manual_principal" in df.columns:
        df["manual_principal"] = pd.to_numeric(df["manual_principal"], errors="coerce")

    # 초보자 설명:
    # - manual 자산은 manual_principal(원금) 기준으로 수익률을 계산합니다.
    # - 그 외 자산은 purchase_amount(매수금액)을 기준으로 계산합니다.
    df["profit_base_amount"] = df["purchase_amount"]
    manual_mask = df["assets.price_source"].fillna("").str.lower().str.strip().eq("manual")
    df.loc[manual_mask, "profit_base_amount"] = df.loc[manual_mask, "manual_principal"]

    df["profit_amount"] = df["valuation_amount"] - df["profit_base_amount"]
    df["profit_rate"] = df.apply(
        lambda r: (r["profit_amount"] / r["profit_base_amount"] * 100)
        if float(r["profit_base_amount"] or 0) > 0
        else 0.0,
        axis=1,
    )

    df = df.rename(
        columns={
            "accounts.name": "account_name",
            "assets.name_kr": "asset_name",
            "quantity": "quantity",
            "purchase_price": "purchase_price",
            "valuation_price": "valuation_price",
            "manual_principal": "manual_principal",
            "valuation_amount": "valuation_amount",
            "profit_amount": "profit_amount",
            "profit_rate": "profit_rate",
            "currency": "currency",
            "assets.asset_type": "asset_type",
        }
    )

    columns = [
        "account_name",
        "asset_name",
        "quantity",
        "purchase_price",
        "valuation_price",
        "manual_principal",
        "valuation_amount",
        "profit_amount",
        "profit_rate",
        "currency",
        "asset_type",
    ]

    return {
        "latest_date": latest_date,
        "rows": _json_safe_records(df[columns].copy()),
    }


def get_recent_transactions(account_id: str, days: int) -> List[Dict]:
    """최근 n일 동안의 거래 내역을 조회합니다."""
    start_date, end_date = _date_range_from_days(days)

    supabase = get_supabase_client()
    query = (
        supabase.table("transactions")
        .select(
            "id, transaction_date, trade_type, quantity, price, fee, tax, memo, "
            "assets ( ticker, name_kr, currency ), accounts ( name, brokerage, owner, type )"
        )
        .order("transaction_date", desc=True)
    )

    if account_id and account_id != ALL_ACCOUNT_TOKEN:
        query = query.eq("account_id", account_id)

    if start_date is not None:
        query = query.gte("transaction_date", start_date)

    if end_date is not None:
        query = query.lte("transaction_date", end_date)

    rows = query.execute().data or []
    if not rows:
        return []

    df = pd.DataFrame(rows)

    trade_type_map = {
        "BUY": "매수",
        "SELL": "매도",
        "DEPOSIT": "입금",
        "WITHDRAW": "출금",
        "INIT": "초기",
    }

    df["transaction_date"] = pd.to_datetime(df["transaction_date"]).dt.date
    df["trade_type"] = df["trade_type"].map(trade_type_map).fillna(df["trade_type"])

    df["ticker"] = df["assets"].apply(lambda x: (x or {}).get("ticker"))
    df["asset_name"] = df["assets"].apply(lambda x: (x or {}).get("name_kr"))
    df["asset_currency"] = df["assets"].apply(lambda x: (x or {}).get("currency"))
    df["account_name"] = df["accounts"].apply(lambda x: (x or {}).get("name"))

    df = df.drop(columns=["assets", "accounts"], errors="ignore")

    columns = [
        "transaction_date",
        "trade_type",
        "ticker",
        "asset_name",
        "asset_currency",
        "quantity",
        "price",
        "fee",
        "tax",
        "account_name",
        "memo",
    ]

    return _json_safe_records(df[columns])


def get_top_contributions(account_id: str, days: int, top_k: int) -> List[Dict]:
    """최근 n일 누적 기여도 기준 Top K 종목을 반환합니다."""
    start_date, end_date = _date_range_from_days(days)

    snapshots = load_asset_contribution_data(account_id, start_date, end_date)
    df = calculate_asset_contributions(snapshots)

    if df.empty:
        return []

    assets = load_assets_lookup()
    df = df.merge(
        assets[["asset_id", "name_kr", "asset_type", "market"]],
        on="asset_id",
        how="left",
    )
    df["name_kr"] = df["name_kr"].fillna(df["asset_id"].astype(str))

    df = df.sort_values(["asset_id", "date"])
    df["cum_contribution"] = df.groupby("asset_id")["contribution"].cumsum()
    df["cum_contribution_pct"] = df["cum_contribution"] * 100

    latest = (
        df.groupby(["asset_id", "name_kr", "asset_type", "market"], as_index=False)
        .last()[["asset_id", "name_kr", "asset_type", "market", "cum_contribution", "cum_contribution_pct"]]
        .sort_values("cum_contribution", ascending=False)
    )

    top_k_safe = max(int(top_k), 1)
    latest = latest.head(top_k_safe)

    return _json_safe_records(latest)


def get_portfolio_treemap(account_id: str, days: int) -> Dict[str, Optional[List[Dict]]]:
    """포트폴리오 Treemap용 데이터를 반환합니다."""
    start_date, end_date = _date_range_from_days(days)

    df = load_latest_asset_weights(account_id, start_date, end_date)
    if df.empty:
        return {"latest_date": None, "rows": []}

    assets = load_assets_lookup()
    df = df.merge(assets[["asset_id", "name_kr", "asset_type", "market"]], on="asset_id", how="left")
    df["name_kr"] = df["name_kr"].fillna(df["asset_id"].astype(str))

    value_col = "valuation_amount_krw" if "valuation_amount_krw" in df.columns else "valuation_amount"

    df = df.rename(columns={value_col: "value"})

    rows = df[["date", "asset_id", "name_kr", "asset_type", "market", "value"]].copy()

    latest_date = rows["date"].max().date().isoformat() if not rows.empty else None

    return {
        "latest_date": latest_date,
        "rows": _json_safe_records(rows),
    }
