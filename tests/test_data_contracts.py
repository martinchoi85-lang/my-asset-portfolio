import pandas as pd

from asset_portfolio.backend.services.data_contracts import (
    WEIGHT_COLUMNS,
    BENCHMARK_COLUMNS,
    CONTRIBUTION_COLUMNS,
    normalize_weight_df,
    normalize_benchmark_df,
    normalize_contribution_df,
)
from asset_portfolio.backend.services.portfolio_service import calculate_asset_contributions


def test_normalize_weight_df_empty_has_contract_columns():
    df = normalize_weight_df(pd.DataFrame())
    assert list(df.columns) == WEIGHT_COLUMNS


def test_normalize_weight_df_fills_weight_column():
    raw = pd.DataFrame(
        {
            "date": ["2024-01-01"],
            "asset_id": [1],
            "valuation_amount_krw": [1000.0],
            "total_amount_krw": [1000.0],
            "weight_krw": [1.0],
        }
    )
    df = normalize_weight_df(raw)
    assert df.loc[0, "weight"] == 1.0
    assert df.loc[0, "weight_krw"] == 1.0


def test_calculate_asset_contributions_contract():
    snapshots = [
        {"date": "2024-01-01", "asset_id": 1, "valuation_amount": 100.0},
        {"date": "2024-01-02", "asset_id": 1, "valuation_amount": 110.0},
        {"date": "2024-01-01", "asset_id": 2, "valuation_amount": 200.0},
        {"date": "2024-01-02", "asset_id": 2, "valuation_amount": 210.0},
    ]
    df = calculate_asset_contributions(snapshots)
    assert list(df.columns) == CONTRIBUTION_COLUMNS
    assert not df.empty
    assert df["date"].iloc[0] == pd.to_datetime(df["date"].iloc[0]).date()


def test_normalize_benchmark_df_empty_has_contract_columns():
    df = normalize_benchmark_df(pd.DataFrame())
    assert list(df.columns) == BENCHMARK_COLUMNS


def test_normalize_contribution_df_handles_missing_pct():
    raw = pd.DataFrame(
        {
            "date": ["2024-01-02"],
            "asset_id": [1],
            "contribution": [0.1],
        }
    )
    df = normalize_contribution_df(raw)
    assert list(df.columns) == CONTRIBUTION_COLUMNS
    assert df.loc[0, "contribution_pct"] == 10.0
