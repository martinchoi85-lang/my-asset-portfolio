from __future__ import annotations
from dataclasses import dataclass
import math
import yfinance as yf
from datetime import date, datetime, timezone
from typing import Optional, Dict, Any, List, Tuple
from asset_portfolio.backend.infra.supabase_client import get_supabase_client
from asset_portfolio.backend.services.transaction_service import TransactionService


@dataclass
class PriceUpdateResult:
    """
    ✅ 가격 업데이트 결과를 UI에 그대로 표시하기 위한 데이터 구조
    """
    asset_id: int
    ticker: str
    ok: bool
    old_price: Optional[float] = None
    new_price: Optional[float] = None
    reason: Optional[str] = None


class PriceUpdaterService:
    """
    ✅ 목적:
    - assets 테이블의 current_price를 yfinance로 업데이트
    - (선택) 업데이트 성공한 자산들에 대해 스냅샷 자동 리빌드까지 연결
    - ticker가 있어도 업데이트 실패하는 케이스가 많으므로, 실패 사유를 함께 반환하여 UI에서 확인 가능

    ✅ 방어전략:
    - ticker 없음/빈 값 → 실패
    - yfinance에서 데이터 없음 → 실패
    - price가 NaN/0/음수 등 비정상 → 실패
    - 한국 종목처럼 ticker 포맷이 다를 수 있으므로 market 기반으로 후보 ticker를 생성해서 시도
    """

    @staticmethod
    def _normalize_ticker_for_yf(ticker: str, market: Optional[str]) -> str:
        """
        ✅ yfinance가 요구하는 티커 형식으로 정규화
        - 한국 종목은 보통 '005930.KS' 또는 '005930.KQ' 형태가 필요합니다.
        - 이미 '.'이 포함된 경우(예: SPY, AAPL)는 그대로 둡니다.

        ⚠️ 이 규칙은 100% 자동화가 어려워서,
           V1에서는 시장(market) 기반의 단순 규칙을 적용합니다.
        """
        t = (ticker or "").strip()
        if not t:
            return t

        # 이미 suffix가 있거나 미국 티커처럼 보이면 그대로
        if "." in t:
            return t

        m = (market or "").lower().strip()
        if m in {"korea", "kr", "kor"}:
            # ✅ 한국 상장(단순): KS로 가정
            # - 코스닥(KQ)까지 자동 판별하려면 별도 매핑 테이블이 필요
            return f"{t}.KS"
        return t  # ✅ 기본: 미국/기타는 그대로
    
    
    @staticmethod
    def _fetch_last_close_price(yf_ticker: str) -> float:
        """
        ✅ yfinance로 최근 종가(또는 마지막 close)를 가져옵니다.
        - 데이터가 비어있거나 NaN이면 예외 처리합니다.
        """
        if not yf_ticker:
            raise ValueError("empty ticker")

        tk = yf.Ticker(yf_ticker)

        # ✅ 가장 단순/안전: 최근 5일 hist에서 마지막 close 사용
        hist = tk.history(period="5d")
        if hist is None or hist.empty:
            raise ValueError("no price data (empty history)")

        last_close = float(hist["Close"].dropna().iloc[-1])
        if last_close <= 0:
            raise ValueError("invalid close price")
        return last_close
    
    
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
        """
        ✅ 단일 자산 current_price 업데이트 + 메타데이터 기록
        - 성공: current_price, price_updated_at, status=ok, error=NULL, source=yfinance
        - 실패: (current_price 유지) status=failed, error 저장, source=yfinance
        """
        supabase = get_supabase_client()

        row = (
            supabase.table("assets")
            .select("id, ticker, market, current_price")
            .eq("id", asset_id)
            .single()
            .execute()
            .data
        )
        if not row:
            return PriceUpdateResult(asset_id=asset_id, ticker="", ok=False, reason="asset not found")

        ticker = str(row.get("ticker") or "")
        market = row.get("market")
        old_price = float(row.get("current_price") or 0.0)

        # ✅ 한국 종목 정확도 개선: 후보(.KS/.KQ) + fast_info/history fallback을 쓰는 함수 활용
        price, used_ticker, reason = PriceUpdaterService.fetch_price_from_yfinance(ticker, market)

        now = datetime.now(timezone.utc)

        if price is None or float(price) <= 0:
            # ✅ 실패: 기존 current_price는 유지하고, 메타만 기록
            supabase.table("assets").update({
                "price_updated_at": now.isoformat(),
                "price_update_status": "failed",
                "price_update_error": (str(reason)[:300] if reason else "unknown error"),
                "price_source": "yfinance",
            }).eq("id", asset_id).execute()

            return PriceUpdateResult(
                asset_id=asset_id,
                ticker=ticker,
                ok=False,
                old_price=old_price,
                new_price=None,
                reason=f"{reason} (used={used_ticker})" if used_ticker else str(reason),
            )

        new_price = float(price)

        # ✅ 성공: current_price + 메타 기록
        supabase.table("assets").update({
            "current_price": new_price,
            "price_updated_at": now.isoformat(),
            "price_update_status": "ok",
            "price_update_error": None,
            "price_source": "yfinance",
        }).eq("id", asset_id).execute()

        return PriceUpdateResult(
            asset_id=asset_id,
            ticker=ticker,
            ok=True,
            old_price=old_price,
            new_price=new_price,
            reason=f"used={used_ticker}" if used_ticker else None,
        )
                
            
    @staticmethod
    def update_many(asset_ids: List[int]) -> List[PriceUpdateResult]:
        """
        ✅ 여러 자산 가격 업데이트
        """
        results: List[PriceUpdateResult] = []
        for aid in asset_ids:
            results.append(PriceUpdaterService.update_asset_price(int(aid)))
        return results


    # (핵심) 업데이트 후 스냅샷 자동 리빌드 연결
    @staticmethod
    def _get_accounts_holding_asset(asset_id: int) -> List[str]:
        """
        ✅ 해당 자산이 거래된 계좌 목록을 조회합니다.
        - transactions에서 asset_id로 필터 후 account_id distinct
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