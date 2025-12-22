"""
portfolio_aggregator.py

[역할]
- 여러 자산의 daily_snapshot을 날짜 기준으로 합산
- 포트폴리오 단위 daily_return / cumulative_return 계산
- DB, Supabase, API 전혀 모름 (순수 계산 레이어)
"""

from typing import List, Dict
import pandas as pd


def calculate_portfolio_return_series(
    asset_snapshots_by_date: List[Dict]
) -> List[Dict]:
    """
    포트폴리오 단위 수익률 계산 (TWR 방식)

    Parameters
    ----------
    asset_snapshots_by_date : List[Dict]
        [
            {
                "date": "2025-01-01",
                "purchase_amount": 3000,
                "valuation_amount": 3200,
            },
            ...
        ]

    Returns
    -------
    List[Dict]
        [
            {
                "date": "2025-01-01",
                "purchase_amount": 3000,
                "valuation_amount": 3200,
                "daily_return": 0.0,
                "cumulative_return": 0.0,
            },
            ...
        ]
    """

    if not asset_snapshots_by_date:
        return []

    # =========================
    # DataFrame 변환
    # =========================
    df = pd.DataFrame(asset_snapshots_by_date)

    # 날짜 기준 정렬 (안전장치)
    df = df.sort_values("date").reset_index(drop=True)

    # =========================
    # 일별 수익률 계산
    # =========================
    df["daily_return"] = df["valuation_amount"].pct_change()

    # 첫 날은 기준점이므로 수익률 0
    df.loc[0, "daily_return"] = 0.0

    # =========================
    # 누적 수익률 (TWR)
    # =========================
    df["cumulative_return"] = (1 + df["daily_return"]).cumprod() - 1

    # =========================
    # 결과 정리
    # =========================
    return df[
        [
            "date",
            "purchase_amount",
            "valuation_amount",
            "daily_return",
            "cumulative_return",
        ]
    ].to_dict(orient="records")