# src/asset_portfolio/backend/services/tests/test_asset_return_series.py

from asset_portfolio.backend.services.portfolio_calculator import (calculate_asset_return_series_from_snapshots)


def test_cumulative_return_calculation():
    """
    daily_snapshots 기반 누적 수익률 계산 테스트
    (DB 접근 없이 순수 계산만 검증)
    """

    snapshots = [
        {
            "date": "2024-01-01",
            "purchase_amount": 900,
            "valuation_amount": 1000,
        },
        {
            "date": "2024-01-02",
            "purchase_amount": 900,
            "valuation_amount": 1100,
        },
    ]

    result = calculate_asset_return_series_from_snapshots(snapshots)

    assert len(result) == 2

    # Day 1
    assert round(result[0]["return_rate"], 4) == round((1000 - 900) / 900, 4)

    # Day 2
    assert round(result[1]["return_rate"], 4) == round((1100 - 900) / 900, 4)
