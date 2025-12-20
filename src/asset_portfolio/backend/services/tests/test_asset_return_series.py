import pandas as pd
from src.asset_portfolio.backend.services.portfolio_calculator import calculate_asset_return_series_from_snapshots

def test_cumulative_return_calculation(monkeypatch):
    """
    daily_snapshots 기반 누적 수익률 계산 테스트
    """

    fake_data = [
        {
            "date": "2024-01-01",
            "valuation_amount": 1000,
            "purchase_amount": 900,
        },
        {
            "date": "2024-01-02",
            "valuation_amount": 1100,
            "purchase_amount": 900,
        },
    ]

    # =========================
    # Supabase 호출을 가짜로 치환
    # =========================
    class FakeResponse:
        data = fake_data

    class FakeTable:
        def select(self, *args, **kwargs): return self
        def eq(self, *args, **kwargs): return self
        def gte(self, *args, **kwargs): return self
        def lte(self, *args, **kwargs): return self
        def order(self, *args, **kwargs): return self
        def execute(self): return FakeResponse()

    class FakeClient:
        def table(self, *args, **kwargs): return FakeTable()

    monkeypatch.setattr(
        "asset_portfolio.backend.services.portfolio_calculator.get_supabase_client",
        lambda: FakeClient()
    )

    df = calculate_asset_return_series_from_snapshots(
        asset_id=1,
        account_id="test-account",
        start_date="2024-01-01",
        end_date="2024-01-02"
    )

    # =========================
    # 검증
    # =========================
    assert len(df) == 2
    assert round(df.iloc[0]["cumulative_return"], 4) == round((1000 - 900) / 900, 4)
    assert round(df.iloc[1]["cumulative_return"], 4) == round((1100 - 900) / 900, 4)
