"""
fx_utils.py

[역할]
- USD → KRW FX 변환 로직을 중앙화한 유틸리티 모듈
- 각 렌더 컴포넌트가 직접 FxService를 호출하지 않고 이 모듈만 참조하면 됨

[설계 원칙]
- DB 스냅샷은 원통화(USD) 유지 — FxService 정책과 동일
- UI/집계 시에만 KRW로 환산
- @st.cache_data 로 세션 내 환율을 1회만 조회 (TTL=600초)
"""
from __future__ import annotations

from typing import List

import pandas as pd
import streamlit as st

from asset_portfolio.backend.services.fx_service import FxService


@st.cache_data(ttl=600)
def get_usdkrw_rate() -> tuple[float, str]:
    """
    USD/KRW 환율을 Streamlit 세션 캐시에서 반환.
    동일 세션에서 여러 컴포넌트가 호출해도 yfinance 요청은 1번만 발생.

    반환값:
        (rate: float, source: str)
        - rate  : 환율 (예: 1446.28)
        - source: "yfinance" 또는 "fallback"
    """
    try:
        fx = FxService.fetch_usdkrw()
        return fx.rate, fx.source
    except Exception:
        # FxService 내부에서도 fallback을 처리하지만 이중 방어
        return 1300.0, "fallback"


def apply_fx_to_df(
    df: pd.DataFrame,
    usd_krw: float,
    amount_cols: List[str],
    currency_col: str = "currency",
) -> pd.DataFrame:
    """
    DataFrame에서 USD 행의 금액 컬럼을 KRW로 일괄 환산한다.

    - 원본 df를 변경하지 않고 복사본을 반환 (immutable)
    - currency_col 값이 'usd'(대소문자 무관)인 행만 * usd_krw 적용
    - currency_col 이 없거나 amount_cols 가 없으면 원본 그대로 반환

    Args:
        df          : 원본 DataFrame
        usd_krw     : USD→KRW 환율 (예: 1446.28)
        amount_cols : 환산 적용할 금액 컬럼 목록 (예: ["valuation_amount", "purchase_amount"])
        currency_col: 통화 정보를 담은 컬럼명 (기본값: "currency")

    Returns:
        FX 환산이 적용된 새 DataFrame
    """
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


def fx_caption(usd_krw: float, source: str) -> str:
    """
    표준화된 환율 안내 캡션 문자열을 반환.
    각 컴포넌트에서 st.caption(fx_caption(...))으로 사용.
    """
    src_label = "실시간 (yfinance)" if source == "yfinance" else f"fallback ({usd_krw:,.0f}원 고정)"
    return f"💱 적용 환율: 1 USD = {usd_krw:,.2f} 원  |  출처: {src_label}"
