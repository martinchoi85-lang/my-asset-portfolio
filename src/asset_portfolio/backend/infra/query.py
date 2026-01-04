from typing import Optional
from datetime import date, datetime
from asset_portfolio.backend.infra.supabase_client import get_supabase_client


ALL_ACCOUNT_TOKEN = "__ALL__"

def _as_date_str(x):
    if x is None:
        return None
    if isinstance(x, (date, datetime)):
        return x.strftime("%Y-%m-%d")
    s = str(x)
    return s[:10]  # "YYYY-MM-DD..." -> "YYYY-MM-DD"


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
        .order("date")
    )

    # ✅ start_date, end_date, account_id가 있을 때만 필터 적용 (None-safe)
    if start_date is not None:
        q = q.gte("date", _as_date_str(start_date))

    if end_date is not None:
        q = q.lte("date", _as_date_str(end_date))

    if account_id and account_id != ALL_ACCOUNT_TOKEN:
        q = q.eq("account_id", account_id)

    return q


def load_asset_contribution_data(
    account_id: str,
    start_date: str,
    end_date: str,
):
    query = build_daily_snapshots_query(
        select_cols="date, asset_id, valuation_amount",
        start_date=start_date,
        end_date=end_date,
        account_id=account_id,
    )
    response = query.order("date").execute()
    return response.data or []
