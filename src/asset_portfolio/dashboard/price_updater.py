import streamlit as st
import pandas as pd

from asset_portfolio.backend.infra.supabase_client import get_supabase_client
from asset_portfolio.backend.services.price_updater_service import PriceUpdaterService


@st.cache_data(ttl=3600)
def _load_assets_df() -> pd.DataFrame:
    supabase = get_supabase_client()
    rows = (
        supabase.table("assets")
        .select("id, ticker, name_kr, asset_type, market, currency, current_price")
        .order("ticker")
        .execute()
        .data or []
    )
    return pd.DataFrame(rows)


def render_price_updater():
    st.title("💹 Price Updater (yfinance)")

    df = _load_assets_df()
    if df.empty:
        st.error("assets 테이블에 데이터가 없습니다.")
        return

    # ✅ 현금은 업데이트 대상이 아니므로 기본 제외(원하면 토글로 포함 가능)
    show_cash = st.checkbox("현금(CASH)도 포함해서 보기", value=False)
    if not show_cash:
        df = df[df["asset_type"].fillna("").str.lower() != "cash"]

    st.caption("yfinance로 current_price를 갱신합니다. ticker가 있어도 종종 실패할 수 있으므로 실패 사유를 표로 제공합니다.")

    mode = st.radio("업데이트 방식", ["선택한 자산만", "표에 보이는 전체"], index=0)

    selected_ids = []
    if mode == "선택한 자산만":
        df["label"] = df.apply(lambda r: f"{r['ticker']} | {r['name_kr']} (id={r['id']})", axis=1)
        labels = st.multiselect("업데이트할 자산 선택", df["label"].tolist(), default=[])
        if labels:
            selected_ids = [int(df.loc[df["label"] == lb, "id"].iloc[0]) for lb in labels]
    else:
        selected_ids = [int(x) for x in df["id"].tolist()]

    auto_rebuild = st.checkbox("가격 업데이트 후 스냅샷 자동 리빌드", value=True)

    if st.button("가격 업데이트 실행", type="primary", disabled=(len(selected_ids) == 0)):
        with st.spinner("가격 업데이트 중..."):
            results = PriceUpdaterService.update_many(selected_ids)

        # ✅ 성공한 asset_id만 추출
        ok_asset_ids = [r.asset_id for r in results if r.ok]

        # ✅ 결과 표시 (기존)
        # res_df = pd.DataFrame([r.__dict__ for r in results])
        # st.dataframe(res_df, width='stretch')

        # ✅ 선택: 업데이트 후 자동 리빌드
        if auto_rebuild and ok_asset_ids:
            with st.spinner("스냅샷 자동 리빌드 중..."):
                summary = PriceUpdaterService.rebuild_snapshots_for_updated_assets(ok_asset_ids)

            st.success(f"스냅샷 리빌드 완료: 총 {summary['rebuilt_total_rows']}행 업서트")

        # ✅ 결과를 표로 보여줘서 “어떤 종목이 왜 실패했는지”를 즉시 확인 가능
        res_df = pd.DataFrame([r.__dict__ for r in results])
        res_df = res_df.rename(columns={
            "asset_id": "자산ID",
            "ticker": "티커",
            "ok": "성공여부",
            "price": "가격",
            "reason": "비고/실패사유",
        })
        st.dataframe(res_df, width='stretch')

        # ✅ 캐시 무효화: 업데이트된 current_price가 다른 화면에도 바로 반영되도록 처리
        st.cache_data.clear()
        st.success("완료되었습니다. (실패 종목은 사유를 확인하세요)")
