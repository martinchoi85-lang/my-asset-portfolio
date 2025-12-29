from __future__ import annotations
from dataclasses import dataclass
import math
import yfinance as yf
from datetime import date
from typing import Optional, Dict, Any, List, Tuple
from asset_portfolio.backend.infra.supabase_client import get_supabase_client
from asset_portfolio.backend.services.transaction_service import TransactionService


@dataclass
class PriceUpdateResult:
    asset_id: int
    ticker: str
    ok: bool
    price: Optional[float] = None
    reason: Optional[str] = None


class PriceUpdaterService:
    """
    ✅ 목적:
    - assets 테이블의 current_price를 yfinance로 업데이트
    - ticker가 있어도 업데이트 실패하는 케이스가 많으므로, 실패 사유를 함께 반환하여 UI에서 확인 가능

    ✅ 방어전략:
    - ticker 없음/빈 값 → 실패
    - yfinance에서 데이터 없음 → 실패
    - price가 NaN/0/음수 등 비정상 → 실패
    - 한국 종목처럼 ticker 포맷이 다를 수 있으므로 market 기반으로 후보 ticker를 생성해서 시도
    """

    @staticmethod
    def _safe_float(v) -> Optional[float]:
        try:
            x = float(v)
            if math.isnan(x) or math.isinf(x):
                return None
            return x
        except Exception:
            return None

    @staticmethod
    def _candidate_tickers(ticker: str, market: Optional[str]) -> List[str]:
        """
        ✅ yfinance ticker 후보 생성
        - 미국: 보통 그대로
        - 한국: 숫자 6자리면 .KS / .KQ를 시도(코스피/코스닥 구분이 불명확하면 둘 다 시도)
        - 이미 접미사가 있으면 그대로 사용
        """
        t = (ticker or "").strip()
        if not t:
            return []

        # 이미 접미사가 있으면 그대로 우선
        if "." in t:
            return [t]

        m = (market or "").lower()

        # 한국 시장 추정: tickers like '213630'
        if m in {"korea", "kr", "kor"} and t.isdigit() and len(t) == 6:
            return [f"{t}.KS", f"{t}.KQ", t]  # ✅ 둘 다 시도 + 원본도 fallback
        return [t]

    @staticmethod
    def fetch_price_from_yfinance(ticker: str, market: Optional[str] = None) -> Tuple[Optional[float], Optional[str], Optional[str]]:
        """
        return: (price, used_ticker, reason)
        """
        candidates = PriceUpdaterService._candidate_tickers(ticker, market)
        if not candidates:
            return None, None, "ticker가 비어있음"

        last_err = None

        for t in candidates:
            try:
                tk = yf.Ticker(t)

                # ✅ 1) fast_info가 있으면 가장 빠르고 간단합니다.
                fi = getattr(tk, "fast_info", None)
                if fi:
                    p = PriceUpdaterService._safe_float(fi.get("last_price"))
                    if p and p > 0:
                        return p, t, None

                # ✅ 2) fast_info가 없거나 last_price가 없으면 history로 fallback
                hist = tk.history(period="5d", interval="1d")
                if hist is None or hist.empty:
                    last_err = "yfinance history가 비어있음"
                    continue

                # ✅ 종가 기반으로 현재가 근사(시장시간 외에는 close가 유효한 경우가 많음)
                p = PriceUpdaterService._safe_float(hist["Close"].iloc[-1])
                if p and p > 0:
                    return p, t, None

                last_err = "Close 가격이 비정상(NaN/0)"
            except Exception as e:
                last_err = f"yfinance 예외: {e}"

        return None, None, last_err or "알 수 없는 실패"

    @staticmethod
    def update_asset_price(asset_id: int) -> PriceUpdateResult:
        supabase = get_supabase_client()

        # ✅ 자산 메타(티커/시장) 조회
        row = (
            supabase.table("assets")
            .select("id, ticker, market, asset_type")
            .eq("id", asset_id)
            .single()
            .execute()
            .data
        )
        if not row:
            return PriceUpdateResult(asset_id=asset_id, ticker="", ok=False, reason="assets에서 자산을 찾지 못함")

        ticker = (row.get("ticker") or "").strip()
        market = row.get("market")
        asset_type = (row.get("asset_type") or "").lower()

        # ✅ 현금은 현재가 업데이트 대상이 아닙니다(1로 고정)
        if asset_type == "cash":
            # 필요하면 current_price를 1로 정리만 해줘도 됨
            supabase.table("assets").update({"current_price": 1}).eq("id", asset_id).execute()
            return PriceUpdateResult(asset_id=asset_id, ticker=ticker, ok=True, price=1.0, reason="cash는 1 고정")

        if not ticker:
            return PriceUpdateResult(asset_id=asset_id, ticker="", ok=False, reason="ticker가 없음")

        price, used_ticker, reason = PriceUpdaterService.fetch_price_from_yfinance(ticker, market)
        if price is None:
            return PriceUpdateResult(asset_id=asset_id, ticker=ticker, ok=False, reason=reason or "가격 조회 실패")

        # ✅ DB 업데이트
        supabase.table("assets").update({"current_price": price}).eq("id", asset_id).execute()
        return PriceUpdateResult(asset_id=asset_id, ticker=ticker, ok=True, price=price, reason=f"used_ticker={used_ticker}")

    @staticmethod
    def update_many(asset_ids: List[int]) -> List[PriceUpdateResult]:
        results = []
        for aid in asset_ids:
            results.append(PriceUpdaterService.update_asset_price(aid))
        return results


    @staticmethod
    def _get_accounts_holding_asset(asset_id: int) -> List[str]:
        """
        ✅ 해당 자산이 거래된 계좌 목록을 조회
        - transactions에서 asset_id로 필터 후 account_id를 중복 제거
        """
        supabase = get_supabase_client()
        rows = (
            supabase.table("transactions")
            .select("account_id")
            .eq("asset_id", asset_id)
            .execute()
            .data or []
        )
        return sorted({r["account_id"] for r in rows if r.get("account_id")})
    

    @staticmethod
    def _get_first_transaction_date(asset_id: int, account_id: str) -> date:
        """
        ✅ (asset_id, account_id)의 최초 거래일을 조회합니다.
        - order asc + limit 1 방식으로 최소 날짜를 가져옵니다.
        """
        supabase = get_supabase_client()
        rows = (
            supabase.table("transactions")
            .select("transaction_date")
            .eq("asset_id", asset_id)
            .eq("account_id", account_id)
            .order("transaction_date", desc=False)
            .limit(1)
            .execute()
            .data or []
        )
        if not rows:
            return date.today()

        # ✅ timestamp string에서 YYYY-MM-DD만 떼어 date로 변환
        s = str(rows[0]["transaction_date"])
        return date.fromisoformat(s[:10])

    
    @staticmethod
    def rebuild_snapshots_for_updated_assets(asset_ids: List[int]) -> Dict[str, int]:
        """
        ✅ 가격 업데이트 성공한 자산들에 대해 스냅샷을 자동 리빌드합니다.
        - 각 자산이 거래된 계좌들을 찾아
        - 최초 거래일 ~ 오늘 범위를 리빌드합니다.
        """
        rebuilt_total_rows = 0

        for asset_id in asset_ids:
            account_ids = PriceUpdaterService._get_accounts_holding_asset(asset_id)

            for account_id in account_ids:
                start = PriceUpdaterService._get_first_transaction_date(asset_id, account_id)
                end = date.today()

                # ✅ 멱등 리빌드(delete-range + upsert)
                rebuilt = TransactionService.rebuild_daily_snapshots_for_asset(
                    account_id=account_id,
                    asset_id=asset_id,
                    start_date=start,
                    end_date=end,
                    delete_first=True,
                )
                rebuilt_total_rows += rebuilt

        return {"rebuilt_total_rows": rebuilt_total_rows}