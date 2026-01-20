from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional, Dict, Any, List

from asset_portfolio.backend.infra.supabase_client import get_supabase_client
from asset_portfolio.backend.services.portfolio_calculator import calculate_daily_snapshots_for_asset


# V1 trade types
TRADE_TYPES = {"BUY", "SELL", "INIT", "DEPOSIT", "WITHDRAW"}
MANUAL_PRICE_SOURCES = {"manual"}  # 필요시 {"manual","snapshot"} 등 확장

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
    def _normalize_currency(currency: Optional[str]) -> Optional[str]:
        if not currency:
            return None
        return str(currency).strip().upper()

    @staticmethod
    def _iso_date(d: date) -> str:
        return d.isoformat()

    @staticmethod
    def _to_date(value: Any) -> date:
        """
        Supabase에서 내려오는 날짜 타입을 date로 통일한다.
        - date/datetime/ISO 문자열을 모두 지원
        """
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, str):
            return datetime.fromisoformat(value).date()
        raise ValueError(f"Unsupported date value: {value!r}")

    @staticmethod
    def _chunk(rows: List[Dict[str, Any]], size: int = 500):
        for i in range(0, len(rows), size):
            yield rows[i:i + size]

    @staticmethod
    def _is_manual_asset(asset_id: int) -> bool:
        """
        ✅ 수동평가 자산 여부 판단
        - price_source == 'manual' 이면, daily_snapshots는 Snapshot Editor가 관리하는 것으로 간주
        - 이 자산들은 'delete 후 재생성' 리빌드를 금지한다 (수동 입력값이 날아감)
        """
        supabase = get_supabase_client()
        row = (
            supabase.table("assets")
            .select("price_source, asset_type")
            .eq("id", asset_id)
            .single()
            .execute()
            .data
        ) or {}

        price_source = (row.get("price_source") or "").lower().strip()
        # asset_type으로도 보조 판정하고 싶으면 여기에 추가 가능
        return price_source in MANUAL_PRICE_SOURCES
    
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

        ✅ 중요 정책:
        - manual 자산(price_source='manual')은 Snapshot Editor가 관리한다.
          → 자동 리빌드로 삭제/재생성하면 사용자가 입력한 평가금액이 날아가므로 리빌드 금지.
        """
        # =========================
        # 0) manual 자산은 리빌드 제외
        # =========================
        if TransactionService._is_manual_asset(asset_id):
            # 사용자가 원할 경우: "manual도 리빌드" 옵션을 별도로 만들 수 있으나
            # 기본값은 반드시 제외가 안전합니다.
            return 0

        supabase = get_supabase_client()

        # 1) 계산
        snapshots = calculate_daily_snapshots_for_asset(
            asset_id=asset_id,
            account_id=account_id,
            start_date=start_date,
            end_date=end_date,
        )

        # 거래가 없으면 기간 삭제(기존 정책)
        if not snapshots:
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

        # 2) date 직렬화
        for r in snapshots:
            if isinstance(r.get("date"), date):
                r["date"] = r["date"].isoformat()

        # 3) delete-range
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

        # 4) upsert
        inserted = 0
        for chunk in TransactionService._chunk(snapshots, size=500):
            supabase.table("daily_snapshots").upsert(
                chunk,
                on_conflict="date,asset_id,account_id",
            ).execute()
            inserted += len(chunk)

        return inserted


    @staticmethod
    def _get_asset_currency(asset_id: int) -> str:
        """✅ 원자산의 통화를 조회하여, 어떤 CASH 자산을 움직여야 하는지 결정합니다."""
        supabase = get_supabase_client()
        row = (
            supabase.table("assets")
            .select("currency")
            .eq("id", asset_id)
            .single()
            .execute()
            .data
        )
        if not row or not row.get("currency"):
            raise ValueError(f"assets.id={asset_id} currency not found")
        return TransactionService._normalize_currency(row["currency"])

    @staticmethod
    def _get_account_currency(account_id: str) -> Optional[str]:
        """✅ 계좌에 통화 컬럼이 있는 경우, 그 값을 우선 사용합니다."""
        supabase = get_supabase_client()
        resp = (
            supabase.table("accounts")
            .select("currency")
            .eq("id", account_id)
            .single()
            .execute()
        )
        row = resp.data or {}
        return TransactionService._normalize_currency(row.get("currency"))


    @staticmethod
    def _get_cash_asset_id_by_currency(currency: str) -> int:
        """
        ✅ 통화에 맞는 CASH 자산을 찾습니다.
        - 정책: assets.asset_type='cash' AND assets.currency=통화
        - CASH_KRW/CASH_USD를 수동으로 추가하셨으므로 여기서 자동 매칭됩니다.
        """
        supabase = get_supabase_client()
        normalized_currency = TransactionService._normalize_currency(currency)
        rows = (
            supabase.table("assets")
            .select("id, asset_type, currency")
            .execute()
            .data or []
        )
        cash_rows = [
            row
            for row in rows
            if str(row.get("asset_type") or "").lower() == "cash"
            and TransactionService._normalize_currency(row.get("currency")) == normalized_currency
        ]
        if not cash_rows:
            raise ValueError(f"cash asset not found for currency={currency}. assets.asset_type='cash' 확인")
        return int(cash_rows[0]["id"])


    @staticmethod
    def _build_cash_mirror_request(
        req: CreateTransactionRequest,
        *,
        cash_asset_id: int,
        memo_suffix: str,
    ) -> CreateTransactionRequest:
        """
        BUY/SELL 거래를 CASH 입출금으로 미러링한다.
        - BUY  -> WITHDRAW (매수금액 + 수수료/세금)
        - SELL -> DEPOSIT  (매도금액 - 수수료/세금)
        """
        gross = req.quantity * req.price
        fees = (req.fee or 0.0) + (req.tax or 0.0)

        if req.trade_type == "BUY":
            cash_trade_type = "WITHDRAW"
            cash_amount = gross + fees
        else:
            cash_trade_type = "DEPOSIT"
            cash_amount = max(0.0, gross - fees)

        return CreateTransactionRequest(
            account_id=req.account_id,
            asset_id=cash_asset_id,
            transaction_date=req.transaction_date,
            trade_type=cash_trade_type,
            quantity=float(cash_amount),
            price=1.0,
            fee=0.0,
            tax=0.0,
            memo=f"[AUTO] {req.trade_type} cash mirror {memo_suffix}",
        )

    @staticmethod
    def _find_auto_cash_transactions(
        *,
        account_id: str,
        cash_asset_id: int,
        tx_date: date,
    ) -> List[Dict[str, Any]]:
        """
        AUTO CASH 미러 거래를 찾는다.
        - 조건: account_id + cash_asset_id + transaction_date + memo startswith '[AUTO]'
        """
        supabase = get_supabase_client()
        rows = (
            supabase.table("transactions")
            .select("id, transaction_date, trade_type, quantity, price, fee, tax, memo")
            .eq("account_id", account_id)
            .eq("asset_id", cash_asset_id)
            .eq("transaction_date", tx_date.isoformat())
            .ilike("memo", "[AUTO]%")
            .execute()
            .data or []
        )
        return rows

    @staticmethod
    def get_transaction_by_id(tx_id: int) -> Dict[str, Any]:
        supabase = get_supabase_client()
        row = (
            supabase.table("transactions")
            .select(
                "id, account_id, asset_id, transaction_date, trade_type, "
                "quantity, price, fee, tax, memo"
            )
            .eq("id", tx_id)
            .single()
            .execute()
            .data
        )
        if not row:
            raise ValueError(f"transactions.id={tx_id} not found")
        return row

    @staticmethod
    def create_transaction_and_rebuild(req: CreateTransactionRequest, *, auto_cash: bool = True) -> Dict[str, Any]:
        """
        ✅ 거래 입력 단일 진입점(확장)
        1) 원자산 거래 insert
        2) BUY/SELL이면 cash mirror 거래 자동 insert
        3) 원자산 + cash 자산 스냅샷 리빌드

        ⚠️ 원자성 주의:
        - 기존에 사용자가 cash를 수동 입력해둔 데이터가 있으면,
          auto_cash=True 상태에서 BUY/SELL을 입력할 때 현금이 이중 반영될 수 있습니다.
        - 그래서 UI 체크박스로 on/off를 제공하는 것이 안전합니다.
        """
        # 1) 원자산 거래 insert
        tx_row = TransactionService.create_transaction(req)

        cash_tx_row = None
        cash_asset_id = None

        # ✅ auto_cash 옵션이 켜져 있고, BUY/SELL인 경우에만 현금 자동 반영
        if auto_cash and req.trade_type in {"BUY", "SELL"}:
            # accounts 테이블에 currency가 없으므로 자산 통화를 기준으로 CASH를 선택한다.
            asset_ccy = TransactionService._get_asset_currency(req.asset_id)
            cash_currency = asset_ccy
            cash_asset_id = TransactionService._get_cash_asset_id_by_currency(cash_currency)

            # ✅ 현금 변동액 산정: 매수=출금(매수대금+비용), 매도=입금(매도대금-비용)
            gross = req.quantity * req.price
            fees = (req.fee or 0.0) + (req.tax or 0.0)

            if req.trade_type == "BUY":
                cash_trade_type = "WITHDRAW"
                cash_amount = gross + fees
            else:
                cash_trade_type = "DEPOSIT"
                cash_amount = max(0.0, gross - fees)  # ✅ 음수 방어

            cash_req = CreateTransactionRequest(
                account_id=req.account_id,
                asset_id=cash_asset_id,
                transaction_date=req.transaction_date,
                trade_type=cash_trade_type,
                quantity=float(cash_amount),  # ✅ cash는 quantity=잔고금액 모델
                price=1.0,                    # ✅ cash 단가 1 고정
                fee=0.0,
                tax=0.0,
                memo=f"[AUTO] {req.trade_type} cash mirror (gross={gross}, fees={fees})",
            )

            # ✅ cash 거래 insert
            cash_tx_row = TransactionService.create_transaction(cash_req)

        # 2) 리빌드 범위: 거래일~오늘
        start = req.transaction_date
        end = date.today()

        # ✅ 원자산 리빌드
        rebuilt_main = TransactionService.rebuild_daily_snapshots_for_asset(
            account_id=req.account_id,
            asset_id=req.asset_id,
            start_date=start,
            end_date=end,
            delete_first=True,
        )

        # ✅ cash 거래가 생성된 경우에만 cash 자산도 리빌드
        rebuilt_cash = 0
        if cash_asset_id is not None:
            rebuilt_cash = TransactionService.rebuild_daily_snapshots_for_asset(
                account_id=req.account_id,
                asset_id=cash_asset_id,
                start_date=start,
                end_date=end,
                delete_first=True,
            )

        return {
            "transaction": tx_row,
            "cash_transaction": cash_tx_row,
            "auto_cash": auto_cash,
            "rebuilt_rows_main": rebuilt_main,
            "rebuilt_rows_cash": rebuilt_cash,
            "rebuilt_start_date": start.isoformat(),
            "rebuilt_end_date": end.isoformat(),
        }

    @staticmethod
    def update_transaction_and_rebuild(
        tx_id: int,
        updated_req: CreateTransactionRequest,
        *,
        auto_cash: bool = True,
    ) -> Dict[str, Any]:
        """
        거래 수정 + 스냅샷 리빌드
        - 기존 거래일/수정 거래일 중 더 빠른 날짜부터 리빌드한다.
        - auto_cash=True면 기존 AUTO CASH 미러를 제거하고 재생성한다.
        """
        # 거래 유효성 검증을 먼저 수행해 잘못된 업데이트를 막는다.
        TransactionService.validate_request(updated_req)

        supabase = get_supabase_client()
        original = TransactionService.get_transaction_by_id(tx_id)

        original_date = TransactionService._to_date(original["transaction_date"])
        updated_date = updated_req.transaction_date

        payload = {
            "account_id": updated_req.account_id,
            "asset_id": updated_req.asset_id,
            "transaction_date": updated_req.transaction_date.isoformat(),
            "trade_type": updated_req.trade_type,
            "quantity": updated_req.quantity,
            "price": updated_req.price,
            "fee": updated_req.fee,
            "tax": updated_req.tax,
            "memo": updated_req.memo,
        }

        # 1) 본 거래 업데이트
        supabase.table("transactions").update(payload).eq("id", tx_id).execute()

        removed_cash_ids: List[int] = []
        cash_asset_ids: List[int] = []
        created_cash_tx: Optional[Dict[str, Any]] = None

        if auto_cash:
            # 2) 기존 AUTO CASH 미러 제거(가능한 경우에만)
            if original["trade_type"] in {"BUY", "SELL"}:
                original_cash_asset_id = TransactionService._get_cash_asset_id_by_currency(
                    TransactionService._get_asset_currency(int(original["asset_id"]))
                )
                cash_asset_ids.append(original_cash_asset_id)
                auto_rows = TransactionService._find_auto_cash_transactions(
                    account_id=original["account_id"],
                    cash_asset_id=original_cash_asset_id,
                    tx_date=original_date,
                )
                if len(auto_rows) == 1:
                    supabase.table("transactions").delete().eq("id", auto_rows[0]["id"]).execute()
                    removed_cash_ids.append(int(auto_rows[0]["id"]))

            # 3) 수정된 거래 기준 AUTO CASH 미러 생성
            if updated_req.trade_type in {"BUY", "SELL"}:
                updated_cash_asset_id = TransactionService._get_cash_asset_id_by_currency(
                    TransactionService._get_asset_currency(updated_req.asset_id)
                )
                cash_asset_ids.append(updated_cash_asset_id)
                cash_req = TransactionService._build_cash_mirror_request(
                    updated_req,
                    cash_asset_id=updated_cash_asset_id,
                    memo_suffix=f"(source_tx_id={tx_id})",
                )
                created_cash_tx = TransactionService.create_transaction(cash_req)

        # 4) 리빌드 범위: 기존/수정 거래일 중 빠른 날짜 ~ 오늘
        rebuild_start = min(original_date, updated_date)
        rebuild_end = date.today()

        # 자산 변경 가능성을 고려해 양쪽 모두 리빌드
        asset_ids = {int(original["asset_id"]), int(updated_req.asset_id)}
        rebuilt_main = 0
        for aid in asset_ids:
            rebuilt_main += TransactionService.rebuild_daily_snapshots_for_asset(
                account_id=updated_req.account_id,
                asset_id=aid,
                start_date=rebuild_start,
                end_date=rebuild_end,
                delete_first=True,
            )

        rebuilt_cash = 0
        for cash_asset_id in set(cash_asset_ids):
            rebuilt_cash += TransactionService.rebuild_daily_snapshots_for_asset(
                account_id=updated_req.account_id,
                asset_id=cash_asset_id,
                start_date=rebuild_start,
                end_date=rebuild_end,
                delete_first=True,
            )

        return {
            "transaction_id": tx_id,
            "removed_cash_ids": removed_cash_ids,
            "created_cash_transaction": created_cash_tx,
            "rebuilt_rows_main": rebuilt_main,
            "rebuilt_rows_cash": rebuilt_cash,
            "rebuilt_start_date": rebuild_start.isoformat(),
            "rebuilt_end_date": rebuild_end.isoformat(),
        }

    @staticmethod
    def delete_transaction_and_rebuild(
        tx_id: int,
        *,
        auto_cash: bool = True,
    ) -> Dict[str, Any]:
        """
        거래 삭제 + 스냅샷 리빌드
        - 삭제된 거래일 ~ 오늘 범위를 리빌드한다.
        - auto_cash=True면 AUTO CASH 미러도 같이 삭제를 시도한다.
        """
        supabase = get_supabase_client()
        original = TransactionService.get_transaction_by_id(tx_id)
        original_date = TransactionService._to_date(original["transaction_date"])

        # 1) 본 거래 삭제
        supabase.table("transactions").delete().eq("id", tx_id).execute()

        removed_cash_ids: List[int] = []
        cash_asset_ids: List[int] = []

        if auto_cash and original["trade_type"] in {"BUY", "SELL"}:
            original_cash_asset_id = TransactionService._get_cash_asset_id_by_currency(
                TransactionService._get_asset_currency(int(original["asset_id"]))
            )
            cash_asset_ids.append(original_cash_asset_id)
            auto_rows = TransactionService._find_auto_cash_transactions(
                account_id=original["account_id"],
                cash_asset_id=original_cash_asset_id,
                tx_date=original_date,
            )
            if len(auto_rows) == 1:
                supabase.table("transactions").delete().eq("id", auto_rows[0]["id"]).execute()
                removed_cash_ids.append(int(auto_rows[0]["id"]))

        rebuild_start = original_date
        rebuild_end = date.today()

        rebuilt_main = TransactionService.rebuild_daily_snapshots_for_asset(
            account_id=original["account_id"],
            asset_id=int(original["asset_id"]),
            start_date=rebuild_start,
            end_date=rebuild_end,
            delete_first=True,
        )

        rebuilt_cash = 0
        for cash_asset_id in set(cash_asset_ids):
            rebuilt_cash += TransactionService.rebuild_daily_snapshots_for_asset(
                account_id=original["account_id"],
                asset_id=cash_asset_id,
                start_date=rebuild_start,
                end_date=rebuild_end,
                delete_first=True,
            )

        return {
            "deleted_transaction_id": tx_id,
            "removed_cash_ids": removed_cash_ids,
            "rebuilt_rows_main": rebuilt_main,
            "rebuilt_rows_cash": rebuilt_cash,
            "rebuilt_start_date": rebuild_start.isoformat(),
            "rebuilt_end_date": rebuild_end.isoformat(),
        }
