from __future__ import annotations
from dataclasses import dataclass
import math
import pandas as pd
import yfinance as yf
from datetime import date, datetime, timezone
from typing import Optional, Dict, Any, List, Tuple
from asset_portfolio.backend.infra.supabase_client import get_supabase_client
from asset_portfolio.backend.services.transaction_service import TransactionService
from asset_portfolio.backend.services.daily_snapshot_generator import generate_daily_snapshots
from asset_portfolio.backend.services.krx_price_fetcher import KRXPriceFetcher


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
        - 한국: 6자리(숫자/문자혼합)면 .KS / .KQ를 시도(코스피/코스닥 구분이 불명확하면 둘 다 시도)
        - 이미 접미사가 있으면 그대로 사용
        """
        t = (ticker or "").strip()
        if not t:
            return []

        # 이미 접미사가 있으면 그대로 우선
        if "." in t:
            return [t]

        m = (market or "").lower()

        # 한국 시장 추정: tickers like '213630' or '0064K0'
        if m in {"korea", "kr", "kor"} and len(t) == 6 and t.isalnum():
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
        - (중요) price_source는 '정책 컬럼'이므로 절대 덮어쓰지 않는다.
        - price_source='manual'이면 무조건 스킵한다. (수동평가 자산 보호)
        """
        supabase = get_supabase_client()

        # ✅ price_source까지 반드시 조회해서 manual 보호 로직을 적용
        row = (
            supabase.table("assets")
            .select("id, ticker, asset_type, market, current_price, price_source")
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

        now = datetime.now(timezone.utc)
        
        # =========================
        # ✅ 0) manual 자산 보호: 절대 yfinance로 덮지 않음
        # =========================
        policy_source = (row.get("price_source") or "").lower().strip()
        asset_type = (row.get("asset_type") or "").lower().strip()
        
        if policy_source == "manual" or asset_type == "cash":
            # manual/cash는 스킵하되, 원하면 메타에 skipped 기록 가능
            supabase.table("assets").update({
                "price_updated_at": now.isoformat(),
                "price_update_status": "skipped",
                # "price_update_error": "manual or cash",
                # "price_source": row.get("price_source"),  # ❌ 아예 포함하지 않는 것이 더 안전
            }).eq("id", asset_id).execute()

            return PriceUpdateResult(
                asset_id=asset_id,
                ticker=ticker,
                ok=False,
                old_price=old_price,
                new_price=None,
                reason="skipped (manual or cash)",
            )

        # =========================
        # 1) yfinance fetch
        # =========================
        price, used_ticker, reason = PriceUpdaterService.fetch_price_from_yfinance(ticker, market)

        if price is None or float(price) <= 0:
            # ✅ 실패: current_price 유지 + 메타만 기록
            # (중요) price_source 절대 변경 금지
            supabase.table("assets").update({
                "price_updated_at": now.isoformat(),
                "price_update_status": "failed",
                "price_update_error": (str(reason)[:300] if reason else "unknown error"),
                # "price_source": "yfinance",  # ❌ 절대 쓰면 안 됨
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

        # ✅ 성공: current_price + 메타 기록(정책 컬럼 price_source는 건드리지 않음)
        # ✅ 성공: current_price + 메타 기록(정책 컬럼 price_source는 건드리지 않음)
        supabase.table("assets").update({
            "current_price": new_price,
            "price_updated_at": now.isoformat(),
            "price_update_status": "ok",
            "price_update_error": None,
            # "price_source": "yfinance",  # ❌ 절대 쓰면 안 됨
        }).eq("id", asset_id).execute()

        # ✅ [New] history 테이블에도 저장 (스냅샷 생성 시 참조됨)
        # - 이를 저장해야 generate_daily_snapshots가 오늘자 가격으로 평가를 수행함
        supabase.table("asset_prices").upsert({
            "price_date": now.date().isoformat(),
            "asset_id": asset_id,
            "close_price": new_price,
            "currency": row.get("currency") or "",
            "source": "yfinance",
            "fetched_at": now.isoformat(),
        }, on_conflict="price_date,asset_id").execute()

        return PriceUpdateResult(
            asset_id=asset_id,
            ticker=ticker,
            ok=True,
            old_price=old_price,
            new_price=new_price,
            reason=f"used={used_ticker}" if used_ticker else None,
        )
        
    
    @staticmethod
    def update_many(asset_ids: list[int]) -> list[PriceUpdateResult]:
        results: list[PriceUpdateResult] = []

        for aid in asset_ids:
            try:
                r = PriceUpdaterService.update_asset_price(int(aid))

                # ✅ 절대 None이 results에 들어가지 않게 방어
                if r is None:
                    results.append(
                        PriceUpdateResult(
                            asset_id=int(aid),
                            ticker="",
                            ok=False,
                            old_price=None,
                            new_price=None,
                            reason="update_asset_price returned None",
                        )
                    )
                else:
                    results.append(r)

            except Exception as e:
                results.append(
                    PriceUpdateResult(
                        asset_id=int(aid),
                        ticker="",
                        ok=False,
                        old_price=None,
                        new_price=None,
                        reason=f"exception: {e}",
                    )
                )

        return results

    # =========================
    # ✅ KRX price source helpers
    # =========================
    @staticmethod
    def _load_price_sources(asset_ids: list[int]) -> Dict[int, list[Dict[str, Any]]]:
        """
        ✅ asset_price_sources 테이블에서 price source 설정을 읽어옵니다.
        - asset_id별로 우선순위(priority) 기반으로 정렬하여 저장합니다.
        - 설정이 없는 자산은 빈 리스트로 처리합니다.
        """
        if not asset_ids:
            return {}

        supabase = get_supabase_client()
        rows = (
            supabase.table("asset_price_sources")
            .select("asset_id, source_type, priority, source_params, active")
            .in_("asset_id", [int(x) for x in asset_ids])
            .execute()
            .data or []
        )

        mapping: Dict[int, list[Dict[str, Any]]] = {}
        for r in rows:
            if not r.get("active", True):
                continue
            aid = int(r["asset_id"])
            mapping.setdefault(aid, []).append(r)

        for aid in mapping:
            mapping[aid].sort(key=lambda x: int(x.get("priority") or 0))

        return mapping

    @staticmethod
    def _fetch_price_from_sources(
        *,
        asset_row: Dict[str, Any],
        source_rows: list[Dict[str, Any]],
    ) -> Tuple[Optional[float], Optional[str], Optional[str]]:
        """
        ✅ price source 우선순위에 따라 가격을 조회합니다.
        - 현재 단계에서는 KRX만 지원
        - 향후 yfinance, paid API 등 확장 가능
        """
        if not source_rows:
            return None, None, "price source 설정 없음"

        last_reason = None
        for src in source_rows:
            source_type = (src.get("source_type") or "").lower().strip()
            params = src.get("source_params") or {}

            # Future: add new source_type handlers here for deposit/savings/fund crawling,
            # and map those assets to asset_price_sources with the right params.
            if source_type == "krx":
                # ✅ KRX 가격 조회
                result = KRXPriceFetcher.fetch_reference_price(
                    code=str(params.get("code") or asset_row.get("ticker") or ""),
                    source_params=params,
                )
                if result.price is not None:
                    return result.price, "krx", f"used_trade_date={result.used_trade_date}"
                if result.reason:
                    last_reason = result.reason
                # KRX 실패 시, 문자혼합 코드(yfinance 지원) fallback
                ticker = str(asset_row.get("ticker") or "")
                market = asset_row.get("market")
                if ticker and any(ch.isalpha() for ch in ticker):
                    yf_price, yf_used, yf_reason = PriceUpdaterService.fetch_price_from_yfinance(ticker, market)
                    if yf_price is not None:
                        return yf_price, "yfinance", f"fallback_from_krx used={yf_used}"
                    if yf_reason:
                        last_reason = yf_reason
                # 실패 시 다음 소스로 넘어감
                continue

        return None, None, last_reason or "모든 price source 실패"

    @staticmethod
    def _carry_forward_last_price(
        asset_id: int,
        price_date: date,
    ) -> Optional[float]:
        """
        ✅ 가격 조회 실패 시 가장 최근 가격을 가져와 당일 가격으로 사용합니다.
        - 실패하더라도 일자별 시계열을 끊지 않기 위한 정책
        """
        supabase = get_supabase_client()
        rows = (
            supabase.table("asset_prices")
            .select("price_date, close_price")
            .eq("asset_id", asset_id)
            .lte("price_date", price_date.isoformat())
            .order("price_date", desc=True)
            .limit(1)
            .execute()
            .data or []
        )
        if not rows:
            return None
        return float(rows[0].get("close_price") or 0) or None

    @staticmethod
    def update_asset_prices_for_date(
        *,
        asset_ids: list[int],
        price_date: date,
        carry_forward_on_fail: bool = True,
    ) -> Dict[str, Any]:
        """
        ✅ price source 기반으로 asset_prices 테이블을 갱신합니다.
        - 주로 KRX 기반 가격을 저장하는 목적
        - 실패 시 carry-forward 정책을 선택적으로 적용
        """
        if not asset_ids:
            return {"inserted": 0, "failed": 0}

        supabase = get_supabase_client()
        assets = (
            supabase.table("assets")
            .select("id, ticker, currency, asset_type, price_source, market")
            .in_("id", [int(x) for x in asset_ids])
            .execute()
            .data or []
        )
        if not assets:
            return {"inserted": 0, "failed": 0}

        source_map = PriceUpdaterService._load_price_sources([a["id"] for a in assets])

        payload = []
        failed = 0
        details: list[Dict[str, Any]] = []

        for row in assets:
            asset_type = (row.get("asset_type") or "").lower().strip()
            if asset_type == "cash":
                details.append({
                    "asset_id": int(row["id"]),
                    "ok": False,
                    "source": None,
                    "reason": "skipped cash",
                })
                continue

            aid = int(row["id"])
            sources = source_map.get(aid, [])

            # ✅ price source 우선순위를 돌면서 가능한 가격을 찾습니다.
            # - 현재는 KRX만 지원하지만, 나중에 yfinance/유료 API가 추가될 수 있습니다.
            price, used_source, reason = PriceUpdaterService._fetch_price_from_sources(
                asset_row=row,
                source_rows=sources,
            )

            if price is None and carry_forward_on_fail:
                # ✅ 오늘 가격이 없으면 "가장 최근 가격"으로 보간합니다.
                # - 시계열이 끊기는 것을 막기 위한 안정성 정책입니다.
                price = PriceUpdaterService._carry_forward_last_price(aid, price_date)
                if price is not None:
                    used_source = "carry_forward"
                    reason = "fallback to last price"

            if price is None:
                failed += 1
                # ✅ 실패는 로그만 남기고 job을 중단하지 않습니다.
                supabase.table("assets").update({
                    "price_updated_at": datetime.now(timezone.utc).isoformat(),
                    "price_update_status": "failed",
                    "price_update_error": (str(reason)[:300] if reason else "unknown error"),
                }).eq("id", aid).execute()
                details.append({
                    "asset_id": aid,
                    "ok": False,
                    "source": used_source,
                    "reason": reason,
                })
                continue

            payload.append({
                "price_date": price_date.isoformat(),
                "asset_id": aid,
                "close_price": float(price),
                "currency": row.get("currency") or "",
                "source": used_source or "krx",
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            })
            details.append({
                "asset_id": aid,
                "ok": True,
                "source": used_source or "krx",
                "reason": reason,
            })

        if payload:
            supabase.table("asset_prices").upsert(
                payload,
                on_conflict="price_date,asset_id",
            ).execute()

        return {"inserted": len(payload), "failed": failed, "details": details}

    
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
        acc_ids = sorted({r["account_id"] for r in rows if r.get("account_id")})
        return acc_ids
    
    
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
    def rebuild_snapshots_for_updated_assets(updated_asset_ids: list[int]) -> None:
        """
        ✅ 가격 업데이트 후 스냅샷 자동 리빌드
        - 리팩터링 중 내부 helper가 없어져도 동작하도록
          여기서 직접 transactions를 조회해 account 목록을 만든다.
        """
        supabase = get_supabase_client()

        summary = {
            "rebuilt_total_rows": 0,
            "rebuilt_pairs": 0,
            "accounts": [],
            "errors": [],
        }

        if not updated_asset_ids:
            return summary

        # =========================
        # 1) 업데이트된 자산이 등장한 계좌 목록 조회
        # =========================
        tx_rows = (
            supabase.table("transactions")
            .select("account_id, asset_id, transaction_date")
            .in_("asset_id", [int(x) for x in updated_asset_ids])
            .execute()
            .data or []
        )
        if not tx_rows:
            return summary

        tx_df = pd.DataFrame(tx_rows)
        if tx_df.empty:
            return summary

        # ✅ account_id별로 최소 시작일을 잡아 리빌드 비용을 줄임
        tx_df["transaction_date"] = pd.to_datetime(tx_df["transaction_date"], errors="coerce")
        tx_df = tx_df.dropna(subset=["account_id", "transaction_date"])  # ✅ NaT/None 제거        
        
        if tx_df.empty:
            return summary
    
        start_by_account = (
            tx_df.groupby("account_id")["transaction_date"]
                .min()
                .dt.date
                .to_dict()
        )

        # =========================
        # 2) 계좌별로 스냅샷 리빌드
        # - end_date는 사용자 정책대로 date.today()
        # =========================
        for account_id, start_date in start_by_account.items():
            try:
                # ✅ generate_daily_snapshots가 total_rows를 반환하도록(아래 1-B 참고)
                res = generate_daily_snapshots(
                    account_id=str(account_id),
                    start_date=start_date,
                    end_date=date.today(),
                ) or {}

                summary["accounts"].append(str(account_id))
                summary["rebuilt_total_rows"] += int(res.get("total_rows", 0))
                summary["rebuilt_pairs"] += int(res.get("asset_count", 0))

            except Exception as e:
                summary["errors"].append(f"account={account_id}: {e}")

        return summary
