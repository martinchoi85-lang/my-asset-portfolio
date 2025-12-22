# src/asset_portfolio/backend/services/portfolio_service.py
import pandas as pd
import numpy as np
from typing import List, Dict
from asset_portfolio.backend.infra.supabase_client import get_supabase_client
from asset_portfolio.backend.services.portfolio_calculator import calculate_asset_return_series_from_snapshots

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



# def calculate_asset_summary(transactions: pd.DataFrame, assets: pd.DataFrame) -> pd.DataFrame:
#     """
#     DB / Supabase / Streamlit에 전혀 의존하지 않는
#     순수 계산 함수
#     """

#     tx = transactions.copy()

#     tx["quantity"] = pd.to_numeric(tx["quantity"], errors="coerce").fillna(0)
#     tx["price"] = pd.to_numeric(tx["price"], errors="coerce").fillna(0)

#     def signed_qty(row):
#         if row["trade_type"] in ("BUY", "INIT"):
#             return row["quantity"]
#         if row["trade_type"] == "SELL":
#             return -row["quantity"]
#         return 0

#     tx["signed_quantity"] = tx.apply(signed_qty, axis=1)

#     tx["purchase_amount"] = np.where(
#         tx["trade_type"].isin(["BUY", "INIT"]),
#         tx["quantity"] * tx["price"],
#         0
#     )

#     tx["income_amount"] = np.where(
#         tx["trade_type"].isin(["DIVIDEND", "DISTRIBUTION"]),
#         tx["price"],
#         0
#     )

#     grouped = tx.groupby(
#         ["asset_id", "account_id"], dropna=False
#     ).agg(
#         total_quantity=("signed_quantity", "sum"),
#         total_purchase_amount=("purchase_amount", "sum"),
#         total_income=("income_amount", "sum")
#     ).reset_index()

#     df = grouped.merge(
#         assets,
#         left_on="asset_id",
#         right_on="id",
#         how="left"
#     )

#     df["average_purchase_price"] = np.where(
#         df["total_quantity"] > 0,
#         df["total_purchase_amount"] / df["total_quantity"],
#         0
#     )

#     df["current_valuation_price"] = df["current_price"].fillna(0)

#     df["total_valuation_amount"] = (
#         df["total_quantity"] * df["current_valuation_price"]
#     )

#     df["unrealized_pnl"] = (
#         df["total_valuation_amount"]
#         - df["total_purchase_amount"]
#         + df["total_income"]
#     )

#     df["unrealized_return_rate"] = np.where(
#         df["total_purchase_amount"] > 0,
#         df["unrealized_pnl"] / df["total_purchase_amount"] * 100,
#         0
#     )

#     return df[df["total_quantity"] != 0]




# def calculate_asset_return_series(daily_snapshots):
#     """
#     daily_snapshots: [
#       {
#         date,
#         purchase_amount,
#         valuation_amount
#       }
#     ]
#     """
#     results = []

#     for s in daily_snapshots:
#         purchase = float(s["purchase_amount"])
#         valuation = float(s["valuation_amount"])

#         if purchase == 0:
#             return_rate = 0.0
#         else:
#             return_rate = (valuation - purchase) / purchase

#         results.append({
#             "date": s["date"],
#             "return_rate": return_rate,
#             "valuation_amount": valuation,
#             "purchase_amount": purchase,
#         })

#     return results
