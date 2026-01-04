# src/asset_portfolio/backend/services/tests/test_daily_snapshot_integration.py

from datetime import date
from asset_portfolio.backend.services.daily_snapshot_generator import (
    generate_daily_snapshots
)

# =========================
# Fake Supabase Layer
# =========================

class FakeTable:
    def __init__(self, name, storage):
        self.name = name
        self.storage = storage
        self.filters = []

    def select(self, *args, **kwargs):
        return self

    def eq(self, key, value):
        self.filters.append(("eq", key, value))
        return self

    def in_(self, key, values):
        # Supabase in_() 대응
        self.filters.append(("in", key, values))
        return self

    def gte(self, key, value):
        self.filters.append(("gte", key, value))
        return self

    def lte(self, key, value):
        self.filters.append(("lte", key, value))
        return self

    def order(self, *args, **kwargs):
        return self

    def delete(self):
        self.storage[self.name] = []
        return self

    def upsert(self, payload, on_conflict=None):
        self.storage[self.name].extend(payload)
        return self

    def execute(self):
        # 현재 테스트에서는 실제 필터링이 필요 없음
        return type("Resp", (), {"data": self.storage.get(self.name, [])})



class FakeSupabaseClient:
    def __init__(self):
        self.storage = {
            "transactions": [],
            "assets": [],
            "daily_snapshots": []
        }

    def table(self, name):
        return FakeTable(name, self.storage)


# =========================
# 테스트 본문
# =========================

def test_daily_snapshot_generation(monkeypatch):
    """
    거래 → calculator → daily_snapshot 생성 통합 테스트
    """

    fake_client = FakeSupabaseClient()

    # -------------------------
    # Fake Supabase 주입
    # -------------------------
    monkeypatch.setattr(
        "asset_portfolio.backend.services.daily_snapshot_generator.get_supabase_client",
        lambda: fake_client
    )

    monkeypatch.setattr(
        "asset_portfolio.backend.services.portfolio_calculator.get_supabase_client",
        lambda: fake_client
    )

    # -------------------------
    # 테스트 데이터 준비
    # -------------------------
    fake_client.storage["assets"] = [
        {"id": 1, "current_price": 120, "currency": "krw"}
    ]

    fake_client.storage["transactions"] = [
        {
            "asset_id": 1,
            "trade_type": "BUY",
            "quantity": 10,
            "price": 100,
            "transaction_date": "2025-01-01"
        },
        {
            "asset_id": 1,
            "trade_type": "SELL",
            "quantity": 4,
            "price": 110,
            "transaction_date": "2025-01-02"
        },
        {
            "asset_id": 1,
            "trade_type": "DIVIDEND",
            "quantity": 0,
            "price": 50,
            "transaction_date": "2025-01-03"
        }
    ]

    # -------------------------
    # Snapshot 생성
    # -------------------------
    generate_daily_snapshots(
        account_id="A-001",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 3),
        overwrite=True
    )

    snapshots = fake_client.storage["daily_snapshots"]

    # =========================
    # 검증
    # =========================

    # 날짜별 snapshot 3개
    assert len(snapshots) == 3

    last = snapshots[-1]

    # 부분 매도 반영 (10 - 4)
    assert last["quantity"] == 6

    # 평균단가 유지
    assert last["purchase_price"] == 100

    # 배당 반영 (원금 +50)
    assert last["purchase_amount"] == 650

    # 평가금액
    assert last["valuation_amount"] == 6 * 120
