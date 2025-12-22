import pytest
from src.asset_portfolio.backend.services.portfolio_calculator import (apply_transactions, calculate_asset_return_series_from_snapshots)

# =========================
# TC1: 단일 매수, 가격 상승
# =========================
def test_single_buy_price_up():
    snapshots = [
        {
            "date": "2025-01-01",
            "purchase_amount": 1000,
            "valuation_amount": 1000,
        },
        {
            "date": "2025-01-02",
            "purchase_amount": 1000,
            "valuation_amount": 1100,
        },
    ]

    result = calculate_asset_return_series_from_snapshots(snapshots)

    assert result[-1]["return_rate"] == pytest.approx(0.10)


# =========================
# TC2: 단일 매수, 가격 하락
# =========================
def test_single_buy_price_down():
    snapshots = [
        {
            "date": "2025-01-01",
            "purchase_amount": 2000,
            "valuation_amount": 2000,
        },
        {
            "date": "2025-01-02",
            "purchase_amount": 2000,
            "valuation_amount": 1800,
        },
    ]

    result = calculate_asset_return_series_from_snapshots(snapshots)

    assert result[-1]["return_rate"] == pytest.approx(-0.10)


# =========================
# TC3: 현금성 자산
# =========================
def test_cash_asset():
    snapshots = [
        {
            "date": "2025-01-01",
            "purchase_amount": 5000,
            "valuation_amount": 5000,
        }
    ]

    result = calculate_asset_return_series_from_snapshots(snapshots)

    assert result[0]["return_rate"] == 0.0


# =========================
# TC4: 배당 포함
# =========================
def test_dividend_included():
    snapshots = [
        {
            "date": "2025-01-01",
            "purchase_amount": 1000,
            "valuation_amount": 1000,
        },
        {
            "date": "2025-01-02",
            "purchase_amount": 1000,
            "valuation_amount": 1050,  # 배당 50 포함
        },
    ]

    result = calculate_asset_return_series_from_snapshots(snapshots)

    assert result[-1]["return_rate"] == pytest.approx(0.05)


# =========================
# TC5: 여러 날짜 누적
# =========================
def test_multiple_days_consistency():
    snapshots = [
        {
            "date": "2025-01-01",
            "purchase_amount": 1000,
            "valuation_amount": 1000,
        },
        {
            "date": "2025-01-02",
            "purchase_amount": 1000,
            "valuation_amount": 1100,
        },
        {
            "date": "2025-01-03",
            "purchase_amount": 1000,
            "valuation_amount": 1200,
        },
    ]

    result = calculate_asset_return_series_from_snapshots(snapshots)

    assert result[-1]["return_rate"] == pytest.approx(0.20)


# =========================
# TC6: 부분 매도 처리
# =========================
def test_partial_sell_calculation():
    """
    [테스트 시나리오]
    - 10주를 100원에 매수
    - 4주를 120원에 부분 매도

    기대 결과:
    - 잔여 수량: 6주
    - 평균 매입가: 100원
    - 실현 손익: (120 - 100) * 4 = 80
    - 잔여 매입 원가: 600
    """

    transactions = [
        {
            "type": "BUY",
            "quantity": 10,
            "price": 100
        },
        {
            "type": "SELL",
            "quantity": 4,
            "price": 120
        }
    ]

    result = apply_transactions(transactions)

    # =========================
    # 수량 검증
    # =========================
    assert result["quantity"] == 6

    # =========================
    # 평균 매입가 검증
    # =========================
    assert result["average_price"] == 100

    # =========================
    # 실현 손익 검증
    # =========================
    assert result["realized_pnl"] == 80

    # =========================
    # 잔여 매입 원가 검증
    # =========================
    assert result["remaining_cost"] == 600
