import pandas as pd
import streamlit as st
from asset_portfolio.backend.services.asset_service import AssetService
from asset_portfolio.backend.services.fx_service import FxService

@st.cache_data(ttl=3600)
def load_assets_lookup() -> pd.DataFrame:
    """
    asset_id → 자산명(name_kr) 매핑용 lookup 로드
    """
    return AssetService.get_assets_lookup_df()

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

@st.cache_data(ttl=3600)  # 과거 환율은 1시간 캐시 (자주 안바뀜)
def get_historical_usdkrw_rate(start_date: "date", end_date: "date") -> pd.DataFrame:
    """
    지정된 기간의 일일 USD/KRW 환율 이력을 가져옵니다.
    """
    return FxService.fetch_historical_usdkrw(start_date, end_date)
