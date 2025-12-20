import pandas as pd
from asset_portfolio.backend.infra.supabase_client import get_supabase_client
from src.asset_portfolio.backend.services.portfolio_service import calculate_asset_return_series


def apply_transactions(transactions):
    """
    거래 내역을 순서대로 처리하여
    - 잔여 수량
    - 평균 매입가
    - 누적 실현 손익
    을 계산한다.
    """

    total_quantity = 0.0
    total_cost = 0.0
    realized_pnl = 0.0

    for tx in transactions:
        qty = float(tx["quantity"])
        price = float(tx["price"])

        # =========================
        # 매수 처리
        # =========================
        if tx["type"] == "BUY":
            total_cost += qty * price
            total_quantity += qty

        # =========================
        # 매도 처리 (부분 매도 포함)
        # =========================
        elif tx["type"] == "SELL":
            if total_quantity <= 0:
                raise ValueError("보유 수량 없이 매도할 수 없습니다.")

            avg_cost = total_cost / total_quantity  # 평균 매입가 계산
            sell_cost = qty * avg_cost  # 매도한 수량의 원가
            sell_value = qty * price    # 매도한 수량의 매출액

            realized_pnl += sell_value - sell_cost  # 실현 손익 계산

            total_quantity -= qty    # 잔여 수량 차감
            total_cost -= sell_cost  # 잔여 원가 차감

    avg_price = total_cost / total_quantity if total_quantity > 0 else 0.0

    return {
        "quantity": total_quantity,
        "average_price": avg_price,
        "realized_pnl": realized_pnl,
        "remaining_cost": total_cost,
    }


def calculate_asset_return_series_from_snapshots(
    snapshots: list[dict],
) -> pd.DataFrame:
    """
    daily_snapshots 조회 결과(list of dict)를 받아
    자산 수익률 시계열을 계산하는 순수 함수
    """

    if not snapshots:
        return pd.DataFrame()

    df = pd.DataFrame(snapshots)

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    df["cumulative_return"] = (
        (df["valuation_amount"] - df["purchase_amount"])
        / df["purchase_amount"]
    )

    df["daily_return"] = df["valuation_amount"].pct_change()

    return df


def calculate_asset_return_series(
    asset_id: int,
    account_id: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """
    DB에서 daily_snapshots를 조회한 뒤
    순수 계산 함수로 위임
    """

    supabase = get_supabase_client()

    response = (
        supabase
        .table("daily_snapshots")
        .select("date, valuation_amount, purchase_amount")
        .eq("asset_id", asset_id)
        .eq("account_id", account_id)
        .gte("date", start_date)
        .lte("date", end_date)
        .order("date")
        .execute()
    )

    return calculate_asset_return_series_from_snapshots(response.data)

