import streamlit as st
import pandas as pd

from asset_portfolio.backend.services.asset_service import AssetService
from asset_portfolio.dashboard.transaction_editor import _load_assets_df  # 이미 있다면 재사용

def render_asset_editor():
    st.title("🧩 Asset Editor (V1)")

    assets_df = _load_assets_df()
    if assets_df.empty:
        st.info("등록된 자산이 없습니다.")
        return

    # ✅ 보기 좋은 정렬(원하시면 더 정교하게)
    assets_df = assets_df.sort_values(["market", "asset_type", "underlying_asset_class", "ticker"])

    # ✅ 자산 선택
    selected_label = st.selectbox("자산 선택", assets_df["label"].tolist())
    row = assets_df.loc[assets_df["label"] == selected_label].iloc[0]
    asset_id = int(row["id"])

    st.subheader("✏️ 자산 정보 수정")

    # ✅ 핵심 필드들만 V1에서 노출
    ticker = st.text_input("티커", value=str(row["ticker"]), disabled=True)  # 안전하게 비활성
    name_kr = st.text_input("자산명(한글)", value=str(row["name_kr"]))

    market = st.selectbox("시장", ["Korea", "US", "etc"], index=["korea","us","etc"].index(str(row.get("market") or "etc")))
    asset_type = st.selectbox("자산유형", ["cash", "Deposit", "ETF", "Fund", "TDF"], index=["cash", "deposit", "etf", "fund", "tdf"].index(str(row.get("asset_type") or "etc")))
    currency = st.selectbox("통화", ["krw", "usd"], index=["krw","usd"].index(str(row.get("currency") or "krw").lower()))

    # 분류는 V1에서는 선택 옵션을 최소화
    underlying_asset_class = st.text_input("자산군(underlying_asset_class)", value=str(row.get("underlying_asset_class") or "Unknown"))
    economic_exposure_region = st.text_input("노출 지역(economic_exposure_region)", value=str(row.get("economic_exposure_region") or "Unknown"))
    vehicle_type = st.text_input("상품 형태(vehicle_type)", value=str(row.get("vehicle_type") or "Unknown"))

    current_price = st.number_input("현재가(current_price)", min_value=0.0, value=float(row.get("current_price") or 0.0))

    lookthrough_available = st.checkbox("룩스루 가능(ETF/TDF/Fund의 내부 구성 자산을 분해해서 보는 기능)", value=bool(row.get("lookthrough_available") or False))

    st.divider()
    col1, col2 = st.columns([1, 1])
    with col1:
        save = st.button("저장", type="primary")
    with col2:
        st.button("새로고침", on_click=lambda: st.rerun())

    if save:
        try:
            with st.spinner("자산 정보를 저장 중..."):
                updates = {
                    "name_kr": name_kr,
                    "market": market,
                    "asset_type": asset_type,
                    "currency": currency.lower() if currency in ("krw","usd") else currency,
                    "underlying_asset_class": underlying_asset_class,
                    "economic_exposure_region": economic_exposure_region,
                    "vehicle_type": vehicle_type,
                    "current_price": current_price,
                    "lookthrough_available": lookthrough_available,
                }
                # ✅ 빈 값이 들어가지 않도록 최소 방어(원하면 더 강화 가능)
                updates = {k: v for k, v in updates.items() if v is not None}

                AssetService.update_asset(asset_id, updates)

            st.success("저장 완료")
            st.cache_data.clear()
            st.rerun()
        except Exception as e:
            st.error(f"저장 실패: {e}")
