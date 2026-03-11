from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import yfinance as yf


@dataclass
class FxRate:
    """
    ✅ FX 결과를 명확히 남기기 위한 구조
    """
    pair: str
    rate: float
    asof: datetime
    source: str


class FxService:
    """
    ✅ USDKRW 환율을 '대시보드 합산용'으로 제공
    - DB 스냅샷은 원통화 유지
    - UI/집계에서만 환산
    """

    @staticmethod
    def fetch_usdkrw() -> FxRate:
        """
        ✅ USD/KRW 환율(근사)을 yfinance로 가져옵니다.
        - yfinance ticker: 'KRW=X' 는 'USD->KRW' 환율로 널리 사용됩니다.
        - 실패 시 예외를 던지기보다는 안전한 fallback을 제공합니다.
        """
        now = datetime.now(timezone.utc)

        try:
            tk = yf.Ticker("KRW=X")
            hist = tk.history(period="5d", interval="1d")
            if hist is None or hist.empty:
                raise ValueError("empty fx history")

            v = float(hist["Close"].dropna().iloc[-1])
            if v <= 0:
                raise ValueError("invalid fx close")

            return FxRate(pair="USDKRW", rate=v, asof=now, source="yfinance")
        except Exception:
            # ✅ fallback (운영 안정성): 비정상일 때 1300 같은 하드 fallback도 가능하지만,
            #    여기서는 1.0으로 두면 USD가 작게 보이는 문제가 남습니다.
            #    따라서 최소한의 “현실적인” fallback을 둡니다.
            return FxRate(pair="USDKRW", rate=1300.0, asof=now, source="fallback")

    @staticmethod
    def apply_fx_to_df(
        df: "pd.DataFrame",
        usd_krw: float,
        amount_cols: list[str],
        currency_col: str = "currency",
    ) -> "pd.DataFrame":
        """
        DataFrame에서 USD 행의 금액 컬럼을 KRW로 일괄 환산한다.

        - 원본 df를 변경하지 않고 복사본을 반환 (immutable)
        - currency_col 값이 'usd'(대소문자 무관)인 행만 * usd_krw 적용
        - currency_col 이 없거나 amount_cols 가 없으면 원본 그대로 반환
        """
        import pandas as pd
        if df.empty:
            return df

        result = df.copy()

        # currency 컬럼이 없으면 환산 불가 → 원본 반환
        if currency_col not in result.columns:
            return result

        # USD 행 마스크 (대소문자 무관, 공백 trim)
        usd_mask = result[currency_col].astype(str).str.strip().str.lower() == "usd"

        for col in amount_cols:
            if col not in result.columns:
                continue
            # 숫자 타입으로 강제 변환 (혹시 문자열 섞인 경우 대비)
            result[col] = pd.to_numeric(result[col], errors="coerce")
            result.loc[usd_mask, col] = result.loc[usd_mask, col] * usd_krw

        return result

    @staticmethod
    def fx_caption(usd_krw: float, source: str) -> str:
        """
        표준화된 환율 안내 캡션 문자열을 반환.
        """
        src_label = "실시간 (yfinance)" if source == "yfinance" else f"fallback ({usd_krw:,.0f}원 고정)"
        return f"💱 적용 환율: 1 USD = {usd_krw:,.2f} 원  |  출처: {src_label}"

    @staticmethod
    def fetch_historical_usdkrw(start_date: "date", end_date: "date") -> "pd.DataFrame":
        """
        특정 기간의 USD/KRW 과거 환율을 가져옵니다.
        - 조회기간 외 휴장일 보정을 위해 7일 전부터 조회합니다.
        """
        import pandas as pd
        from datetime import timedelta
        import logging
        import yfinance as yf
        
        fetch_start = start_date - timedelta(days=7)
        
        try:
            tk = yf.Ticker("KRW=X")
            hist = tk.history(start=fetch_start.isoformat(), end=(end_date + timedelta(days=1)).isoformat())
            
            if hist is None or hist.empty:
                raise ValueError("empty historical fx from yfinance")
                
            hist = hist.reset_index()
            if "Date" in hist.columns:
                hist["date"] = pd.to_datetime(hist["Date"]).dt.date
            else:
                hist["date"] = pd.to_datetime(hist["index"]).dt.date
                
            hist["fx_rate"] = hist["Close"]
            
            # 주말/휴장일 채우기 위해 1일 간격 Full Dataframe 생성
            full_dates = pd.DataFrame({"date": pd.date_range(fetch_start, end_date).date})
            df = pd.merge(full_dates, hist[["date", "fx_rate"]], on="date", how="left")
            
            df["fx_rate"] = df["fx_rate"].ffill()
            df["fx_rate"] = df["fx_rate"].fillna(1300.0)
            
            df = df[(df["date"] >= start_date) & (df["date"] <= end_date)]
            return df
            
        except Exception as e:
            logging.error(f"FX History fetch failed: {e}")
            full_dates = pd.DataFrame({"date": pd.date_range(start_date, end_date).date})
            full_dates["fx_rate"] = 1300.0
            return full_dates

    @staticmethod
    def apply_historical_fx_to_df(
        df: "pd.DataFrame",
        fx_history_df: "pd.DataFrame",
        amount_cols: list[str],
        currency_col: str = "currency",
        date_col: str = "date"
    ) -> "pd.DataFrame":
        """
        스냅샷 DataFrame에 날짜(date)별 과거 환율을 매핑하여 USD 금액을 KRW로 일괄 환산한다.
        """
        import pandas as pd
        if df.empty or fx_history_df is None or fx_history_df.empty:
            return df
            
        result = df.copy()
        if currency_col not in result.columns or date_col not in result.columns:
            return result
            
        result["_match_date"] = pd.to_datetime(result[date_col]).dt.date
        fx_hist = fx_history_df.copy()
        fx_hist["_match_date"] = pd.to_datetime(fx_hist["date"]).dt.date
        
        result = pd.merge(result, fx_hist[["_match_date", "fx_rate"]], on="_match_date", how="left")
        result["fx_rate"] = result["fx_rate"].fillna(1300.0)
        
        usd_mask = result[currency_col].astype(str).str.strip().str.lower() == "usd"
        
        for col in amount_cols:
            if col not in result.columns:
                continue
            result[col] = pd.to_numeric(result[col], errors="coerce")
            result.loc[usd_mask, col] = result.loc[usd_mask, col] * result.loc[usd_mask, "fx_rate"]
            
        result = result.drop(columns=["_match_date", "fx_rate"])
        return result
