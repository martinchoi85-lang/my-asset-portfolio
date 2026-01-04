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
