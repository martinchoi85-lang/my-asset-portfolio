# src/asset_portfolio/backend/services/tests/test_portfolio_aggregator.py

import pytest
from asset_portfolio.backend.services.portfolio_aggregator import (
    calculate_portfolio_return_series
)


def test_single_day_portfolio():
    """
    하루만 있는 포트폴리오는 수익률 0
    """

    snapshots = [
        {
            "date": "2025-01-01",
            "purchase_amount": 3000,
            "valuation_amount": 3000,
        }
    ]

    result = calculate_portfolio_return_series(snapshots)

    assert result[0]["daily_return"] == 0.0
    assert result[0]["cumulative_return"] == 0.0


def test_two_day_portfolio_growth():
    """
    2일간 포트폴리오 가치 상승
    """

    snapshots = [
        {
            "date": "2025-01-01",
            "purchase_amount": 3000,
            "valuation_amount": 3000,
        },
        {
            "date": "2025-01-02",
            "purchase_amount": 3000,
            "valuation_amount": 3300,
        },
    ]

    result = calculate_portfolio_return_series(snapshots)

    assert result[1]["daily_return"] == pytest.approx(0.10)
    assert result[1]["cumulative_return"] == pytest.approx(0.10)


def test_multiple_days_twr():
    """
    여러 날 누적 수익률 (TWR)
    """

    snapshots = [
        {
            "date": "2025-01-01",
            "purchase_amount": 3000,
            "valuation_amount": 3000,
        },
        {
            "date": "2025-01-02",
            "purchase_amount": 3000,
            "valuation_amount": 3300,  # +10%
        },
        {
            "date": "2025-01-03",
            "purchase_amount": 3000,
            "valuation_amount": 2970,  # -10%
        },
    ]

    result = calculate_portfolio_return_series(snapshots)

    # Day 2: +10%
    assert result[1]["daily_return"] == pytest.approx(0.10)

    # Day 3: -10%
    assert result[2]["daily_return"] == pytest.approx(-0.10)

    # TWR = (1.1 * 0.9) - 1 = -0.01
    assert result[2]["cumulative_return"] == pytest.approx(-0.01)
