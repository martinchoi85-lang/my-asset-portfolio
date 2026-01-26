from typing import Optional, List
from datetime import date, datetime
from asset_portfolio.backend.infra.supabase_client import get_supabase_client


ALL_ACCOUNT_TOKEN = "__ALL__"


def get_user_by_password(password: str) -> Optional[dict]:
    """비밀번호로 사용자를 조회합니다."""
    supabase = get_supabase_client()
    response = supabase.table("users").select("*").eq("password", password).limit(1).execute()
    if response.data:
        return response.data[0]
    return None


def get_accounts(user_id: str) -> List[dict]:
    """특정 사용자의 모든 계좌 정보를 불러옵니다."""
    supabase = get_supabase_client()
    response = supabase.table("accounts").select("*").eq("user_id", user_id).order("name").execute()
    return response.data or []


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
    user_id: str,
    account_id: Optional[str] = None,
):
    """
    daily_snapshots 공통 쿼리 빌더
    - account_id가 "__ALL__"이면 user_id에 속한 모든 계좌를 조회한다.
    - execute()는 여기서 하지 않는다(호출자가 마지막에 execute)
    """
    supabase = get_supabase_client()

    q = (
        supabase.table("daily_snapshots")
        .select(select_cols)
        .order("date")
    )

    if start_date is not None:
        q = q.gte("date", _as_date_str(start_date))

    if end_date is not None:
        q = q.lte("date", _as_date_str(end_date))

    if account_id and account_id != ALL_ACCOUNT_TOKEN:
        q = q.eq("account_id", account_id)
    else:
        user_accounts = get_accounts(user_id)
        user_account_ids = [acc['id'] for acc in user_accounts]
        if not user_account_ids:
            return q.eq("account_id", "00000000-0000-0000-0000-000000000000") # Return empty
        q = q.in_("account_id", user_account_ids)

    return q


def load_asset_contribution_data(
    user_id: str,
    account_id: str,
    start_date: str,
    end_date: str,
):
    query = build_daily_snapshots_query(
        select_cols="date, asset_id, valuation_amount",
        start_date=start_date,
        end_date=end_date,
        user_id=user_id,
        account_id=account_id,
    )
    response = query.order("date").execute()
    return response.data or []


def get_transactions(user_id: str) -> List[dict]:
    """사용자의 모든 거래내역을 불러옵니다."""
    supabase = get_supabase_client()
    user_accounts = get_accounts(user_id)
    user_account_ids = [acc['id'] for acc in user_accounts]
    if not user_account_ids:
        return []
    response = supabase.table("transactions").select("*").in_("account_id", user_account_ids).execute()
    return response.data or []


def get_recurring_orders(user_id: str) -> List[dict]:
    """사용자의 모든 정기주문을 불러옵니다."""
    supabase = get_supabase_client()
    user_accounts = get_accounts(user_id)
    user_account_ids = [acc['id'] for acc in user_accounts]
    if not user_account_ids:
        return []
    response = supabase.table("recurring_orders").select("*").in_("account_id", user_account_ids).execute()
    return response.data or []


def get_assets() -> List[dict]:
    """모든 자산 정보를 불러옵니다."""
    supabase = get_supabase_client()
    response = supabase.table("assets").select("*").execute()
    return response.data or []
