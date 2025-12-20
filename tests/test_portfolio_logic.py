# tests/test_portfolio_logic.py
import pandas as pd
from src.asset_portfolio.backend.services.portfolio_service import calculate_asset_summary


def test_basic_buy_sell_dividend():
    transactions = pd.DataFrame([
        {"asset_id": 1, "account_id": "A", "trade_type": "INIT", "quantity": 10, "price": 100},
        {"asset_id": 1, "account_id": "A", "trade_type": "BUY", "quantity": 5, "price": 120},
        {"asset_id": 1, "account_id": "A", "trade_type": "DIVIDEND", "quantity": 0, "price": 50},
    ])

    assets = pd.DataFrame([
        {"id": 1, "current_price": 130}
    ])

    result = calculate_asset_summary(transactions, assets)

    row = result.iloc[0]

    assert row["total_quantity"] == 15
    assert row["total_purchase_amount"] == (10 * 100 + 5 * 120)
    assert row["total_income"] == 50
    assert row["total_valuation_amount"] == 15 * 130
    assert row["unrealized_pnl"] == (15 * 130) - row["total_purchase_amount"] + 50
