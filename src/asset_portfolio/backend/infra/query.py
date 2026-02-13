from typing import Optional, List, Any
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
    return fetch_all_pagination(query.order("date"))


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




def load_asset_prices(asset_id: int, start_date: str, end_date: str) -> List[dict]:
    """특정 자산의 가격 데이터를 조회합니다."""
    supabase = get_supabase_client()
    q = (
        supabase.table("asset_prices")
        .select("price_date, close_price, currency")
        .eq("asset_id", asset_id)
        .order("price_date")
    )

    if start_date:
        q = q.gte("price_date", _as_date_str(start_date))
    if end_date:
        q = q.lte("price_date", _as_date_str(end_date))
        
    return fetch_all_pagination(q)


def fetch_all_pagination(query_builder: Any, batch_size: int = 1000) -> List[dict]:
    """
    Supabase 1000행 제한을 우회하기 위한 페이지네이션 헬퍼.
    query_builder는 .select()까지 완료된 상태여야 함.
    """
    all_rows = []
    start = 0
    while True:
        # .range(start, end)는 inclusive index
        end = start + batch_size - 1
        response = query_builder.range(start, end).execute()
        rows = response.data or []
        
        all_rows.extend(rows)
        
        if len(rows) < batch_size:
            break
            
        start += batch_size
        
    return all_rows
