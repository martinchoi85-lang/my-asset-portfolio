# src/asset_portfolio/backend/services/portfolio_service.py
from typing import List, Dict
from asset_portfolio.backend.infra.supabase_client import get_supabase_client
from asset_portfolio.backend.services.portfolio_calculator import (
    calculate_asset_return_series_from_snapshots, calculate_portfolio_return_series_from_snapshots,
)

"""
portfolio_service.py

[역할]
- Supabase에서 daily_snapshots 조회
- 조회 결과를 calculator에 전달
- '서비스 계층'으로서 orchestration만 담당
"""

def get_asset_return_series(
    asset_id: int,
    account_id: str,
    start_date: str,
    end_date: str,
) -> List[Dict]:
    """
    특정 자산 + 계좌의 기간별 수익률 시계열 조회

    [흐름]
    1. daily_snapshots 조회
    2. calculator로 전달
    3. 계산 결과 반환
    """

    supabase = get_supabase_client()

    response = (
        supabase
        .table("daily_snapshots")
        .select(
            "date, purchase_amount, valuation_amount"
        )
        .eq("asset_id", asset_id)
        .eq("account_id", account_id)
        .gte("date", start_date)
        .lte("date", end_date)
        .order("date")
        .execute()
    )

    snapshots = response.data or []

    # calculator는 DB를 모른다
    return calculate_asset_return_series_from_snapshots(snapshots)



def load_portfolio_daily_snapshots(
    account_id: str,
    start_date: str,
    end_date: str,
):
    """
    daily_snapshots에서
    특정 계좌의 포트폴리오 단위 데이터를 date 기준으로 집계
    """
    supabase = get_supabase_client()

    response = (
        supabase.table("daily_snapshots")
        .select("date, valuation_amount, purchase_amount")
        .eq("account_id", account_id)
        .gte("date", start_date)
        .lte("date", end_date)
        .execute()
    )

    rows = response.data or []

    # =========================
    # date 기준으로 합산
    # =========================
    daily_map = {}

    for r in rows:
        d = r["date"]
        if d not in daily_map:
            daily_map[d] = {
                "date": d,
                "valuation_amount": 0,
                "purchase_amount": 0,
            }

        daily_map[d]["valuation_amount"] += float(r["valuation_amount"])
        daily_map[d]["purchase_amount"] += float(r["purchase_amount"])

    return sorted(
        daily_map.values(),
        key=lambda x: x["date"]
    )



def get_portfolio_return_series(
    account_id: str,
    start_date: str,
    end_date: str,
):
    """
    Streamlit / API에서 사용하는 최종 함수
    """
    snapshots = load_portfolio_daily_snapshots(
        account_id, start_date, end_date
    )

    return calculate_portfolio_return_series_from_snapshots(snapshots)