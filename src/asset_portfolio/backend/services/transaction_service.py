from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional, Dict, Any, List

from asset_portfolio.backend.infra.supabase_client import get_supabase_client
from asset_portfolio.backend.services.portfolio_calculator import calculate_daily_snapshots_for_asset


# V1 trade types
TRADE_TYPES = {"BUY", "SELL", "INIT", "DEPOSIT", "WITHDRAW"}

# 자산명/티커로 판별하지 않고, asset_type='cash' 등을 쓰면 가장 좋지만
# 현재 스키마에서 확실히 보장되지 않으므로, V1에서는 ticker/name_kr로도 보조 확인 가능
# 사용자가 CASH_KRW, CASH_USD를 만든다고 했으니 id를 읽어 "캐시 여부"를 판별하는 방식이 안전합니다.


@dataclass(frozen=True)
class CreateTransactionRequest:
    account_id: str
    asset_id: int
    transaction_date: date
    trade_type: str
    quantity: float
    price: float
    fee: float = 0.0
    tax: float = 0.0
    memo: Optional[str] = None


class TransactionService:
    @staticmethod
    def _iso_date(d: date) -> str:
        return d.isoformat()

    @staticmethod
    def _chunk(rows: List[Dict[str, Any]], size: int = 500):
        for i in range(0, len(rows), size):
            yield rows[i:i + size]

    @staticmethod
    def _get_asset_cash_flag(asset_id: int) -> bool:
        """
        CASH_KRW / CASH_USD 여부 판단:
        - V1: assets.ticker 또는 assets.name_kr 기준으로 판별(사용자가 수동 생성한다는 전제)
        - 더 좋은 방식: assets.asset_type='cash' 같은 명시 컬럼이 있으면 그걸 쓰는 것
        """
        supabase = get_supabase_client()
        row = (
            supabase.table("assets")
            .select("id, ticker, name_kr, asset_type")
            .eq("id", asset_id)
            .single()
            .execute()
            .data
        )
        if not row:
            raise ValueError(f"assets.id={asset_id} not found")

        ticker = (row.get("ticker") or "").upper()
        name_kr = (row.get("name_kr") or "").upper()
        asset_type = (row.get("asset_type") or "").lower()

        if asset_type == "cash":
            return True
        # if "CASH" in ticker:
        #     return True
        # if "CASH" in name_kr:
        #     return True
        return False

    @staticmethod
    def validate_request(req: CreateTransactionRequest) -> None:
        if req.trade_type not in TRADE_TYPES:
            raise ValueError(f"Unsupported trade_type: {req.trade_type}")

        if req.quantity <= 0:
            raise ValueError("quantity must be > 0")

        if req.trade_type in {"BUY", "SELL", "INIT"}:
            if req.price <= 0:
                raise ValueError("price must be > 0 for BUY/SELL/INIT")

        if req.trade_type in {"DEPOSIT", "WITHDRAW"}:
            # cash only
            if not TransactionService._get_asset_cash_flag(req.asset_id):
                raise ValueError("DEPOSIT/WITHDRAW is allowed only for CASH assets")
            # cash는 단가 1 고정(V1)
            if req.price != 1:
                raise ValueError("For CASH transactions, price must be 1")

    @staticmethod
    def create_transaction(req: CreateTransactionRequest) -> Dict[str, Any]:
        """
        transactions insert (단일)
        """
        TransactionService.validate_request(req)

        supabase = get_supabase_client()

        payload = {
            "account_id": req.account_id,
            "asset_id": req.asset_id,
            "transaction_date": req.transaction_date.isoformat(),  # timestamp with tz 컬럼이라도 iso string은 허용되는 편
            "trade_type": req.trade_type,
            "quantity": req.quantity,
            "price": req.price,
            "fee": req.fee,
            "tax": req.tax,
            "memo": req.memo,
        }

        resp = supabase.table("transactions").insert(payload).execute()
        rows = resp.data or []
        if not rows:
            raise RuntimeError("Insert failed: no rows returned")
        return rows[0]

    @staticmethod
    def rebuild_daily_snapshots_for_asset(
        account_id: str,
        asset_id: int,
        start_date: date,
        end_date: date,
        *,
        delete_first: bool = True,
    ) -> int:
        """
        (account_id, asset_id, date range)에 대해 daily_snapshots를 리빌드한다.
        - V1: delete 후 upsert로 멱등성 확보
        - end_date는 date.today() 기본 사용(호출자가 전달)
        """
        supabase = get_supabase_client()

        # 1) 계산
        snapshots = calculate_daily_snapshots_for_asset(
            asset_id=asset_id,
            account_id=account_id,
            start_date=start_date,
            end_date=end_date,
        )
        if not snapshots:
            # 거래가 없으면 스냅샷을 0으로 유지할지 정책이 필요하지만,
            # V1에서는 "해당 기간 row를 삭제"하고 종료하는 것이 자연스럽습니다.
            if delete_first:
                (
                    supabase.table("daily_snapshots")
                    .delete()
                    .eq("account_id", account_id)
                    .eq("asset_id", asset_id)
                    .gte("date", start_date.isoformat())
                    .lte("date", end_date.isoformat())
                    .execute()
                )
            return 0

        # 2) date 직렬화 + 수치 타입 정규화(안전)
        for r in snapshots:
            if isinstance(r.get("date"), date):
                r["date"] = r["date"].isoformat()

        # 3) delete (권장)
        if delete_first:
            (
                supabase.table("daily_snapshots")
                .delete()
                .eq("account_id", account_id)
                .eq("asset_id", asset_id)
                .gte("date", start_date.isoformat())
                .lte("date", end_date.isoformat())
                .execute()
            )

        # 4) upsert (chunk)
        inserted = 0
        for chunk in TransactionService._chunk(snapshots, size=500):
            supabase.table("daily_snapshots").upsert(
                chunk,
                on_conflict="date,asset_id,account_id",
            ).execute()
            inserted += len(chunk)

        return inserted

    @staticmethod
    def create_transaction_and_rebuild(req: CreateTransactionRequest) -> Dict[str, Any]:
        """
        UI에서 호출할 단일 진입점.
        1) 거래 insert
        2) 해당 자산에 대해 tx_date~today 리빌드
        """
        tx_row = TransactionService.create_transaction(req)

        start = req.transaction_date
        end = date.today()

        # 리빌드: 해당 asset만 (V1 최소)
        rebuilt_rows = TransactionService.rebuild_daily_snapshots_for_asset(
            account_id=req.account_id,
            asset_id=req.asset_id,
            start_date=start,
            end_date=end,
            delete_first=True,
        )

        return {
            "transaction": tx_row,
            "rebuilt_rows": rebuilt_rows,
            "rebuilt_start_date": start.isoformat(),
            "rebuilt_end_date": end.isoformat(),
        }
