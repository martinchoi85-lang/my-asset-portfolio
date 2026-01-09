# src/asset_portfolio/backend/services/portfolio_service.py
import pandas as pd
from typing import List, Dict
from asset_portfolio.backend.infra.query import build_daily_snapshots_query
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
    query = build_daily_snapshots_query(
        select_cols="date, purchase_amount, valuation_amount",
        start_date=start_date,
        end_date=end_date,
        account_id=account_id,
    )

    snapshots = query.execute().data or []

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
    query = build_daily_snapshots_query(
        select_cols="date, purchase_amount, valuation_amount",
        start_date=start_date,
        end_date=end_date,
        account_id=account_id,
    )

    snapshots = query.execute().data or []
    
    # =========================
    # date 기준으로 합산
    # =========================
    daily_map = {}

    for r in snapshots:
        d = r["date"]
        if d not in daily_map:
            daily_map[d] = {
                "date": d,
                "valuation_amount": 0,
                "purchase_amount": 0,
            }

        daily_map[d]["valuation_amount"] += float(r["valuation_amount"] or 0)
        daily_map[d]["purchase_amount"] += float(r["purchase_amount"] or 0)

    # return sorted(
    #     daily_map.values(),
    #     key=lambda x: x["date"]
    # )
    # date 순서 정렬 안정화
    result = list(daily_map.values())
    result.sort(key=lambda x: x["date"])
    return result


def get_portfolio_return_series(account_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Streamlit / API에서 사용하는 최종 함수
    """
    snapshots = load_portfolio_daily_snapshots(account_id, start_date, end_date)
    return calculate_portfolio_return_series_from_snapshots(snapshots)


def calculate_asset_contributions(
    snapshots: List[Dict],
) -> pd.DataFrame:
    """
    daily_snapshots 기반 자산별 수익률 기여도 계산

    반환:
    date | asset_id | contribution | contribution_pct
    """

    if not snapshots:
        return pd.DataFrame()

    df = pd.DataFrame(snapshots)
    df["date"] = pd.to_datetime(df["date"])

    # =========================
    # date, asset_id 기준 정렬
    # =========================
    df = df.sort_values(["asset_id", "date"])

    # =========================
    # 자산별 평가금액 변화
    # =========================
    df["prev_valuation"] = df.groupby("asset_id")["valuation_amount"].shift(1)
    df["delta_valuation"] = df["valuation_amount"] - df["prev_valuation"]

    # =========================
    # 포트폴리오 전일 총 평가금액
    # =========================
    portfolio_prev = (
        df.groupby("date")["prev_valuation"]
        .sum()
        .rename("portfolio_prev_valuation")
        .reset_index()
    )

    df = df.merge(portfolio_prev, on="date", how="left")

    # =========================
    # 기여도 계산
    # =========================
    
    # 기여도 "inf%" 표시 방어 로직: inf / NaN 제거
    # 1) 자산 전일값 없는 행 제거 (첫날)
    # 2) 포트폴리오 전일 총액이 0/NaN이면 제거
    df = df.dropna(subset=["prev_valuation", "portfolio_prev_valuation"])
    df = df[df["portfolio_prev_valuation"] > 0]

    df["contribution"] = df["delta_valuation"] / df["portfolio_prev_valuation"]

    df = df.dropna(subset=["contribution"])

    df["contribution_pct"] = df["contribution"] * 100

    return df[
        [
            "date",
            "asset_id",
            "contribution",
            "contribution_pct",
        ]
    ]