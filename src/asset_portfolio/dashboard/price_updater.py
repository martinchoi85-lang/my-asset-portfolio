import streamlit as st
import pandas as pd

from asset_portfolio.dashboard.transaction_editor import _load_assets_df
from asset_portfolio.backend.services.price_updater_service import PriceUpdaterService

        
def render_price_updater():
    st.title("💹 Price Updater (yfinance)")

    # ✅ 실행 중 플래그
    if "price_busy" not in st.session_state:
        st.session_state["price_busy"] = False

    df = _load_assets_df()
    if df.empty:
        st.error("assets 테이블에 데이터가 없습니다.")
        return

    # ✅ 필터 섹션: stale/failed 자산만 빠르게 골라내기
    st.subheader("필터")
    colA, colB, colC = st.columns([1, 1, 1])

    with colA:
        show_cash = st.checkbox("현금(CASH)도 포함해서 보기", value=False)
    with colB:
        only_failed = st.checkbox("실패 자산만 보기", value=False)
    with colC:
        only_stale = st.checkbox("스테일(오래된) 자산만 보기", value=False)

    stale_days = st.number_input("스테일 기준(일)", min_value=1, value=3, step=1)

    if not show_cash:
        df = df[df["asset_type"].fillna("").str.lower() != "cash"]

    # ✅ 스테일 판정: price_updated_at이 NULL이거나 N일보다 오래되면 stale
    # - timezone이 섞일 수 있으니 UTC 기준으로 비교
    now_utc = pd.Timestamp.utcnow()
    df["price_updated_at"] = pd.to_datetime(df.get("price_updated_at"), errors="coerce", utc=True)
    df["is_stale"] = df["price_updated_at"].isna() | ((now_utc - df["price_updated_at"]) > pd.Timedelta(days=int(stale_days)))

    if only_failed:
        df = df[df["price_update_status"].fillna("").str.lower() == "failed"]
    if only_stale:
        df = df[df["is_stale"] == True]

    st.caption("yfinance로 current_price를 갱신합니다. 실패해도 기존가 유지 + 실패 사유/시각을 기록합니다.")

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

    run_clicked = st.button("가격 업데이트 실행", type="primary", disabled=(len(selected_ids) == 0))

    if run_clicked:
        st.session_state["price_busy"] = True

        try:
            with st.spinner("가격 업데이트 중..."):
                results = PriceUpdaterService.update_many(selected_ids)

            # ✅ 결과표: old_price/new_price 기준으로 표시(기존 'price' rename 버그 수정)
            res_df = pd.DataFrame([r.__dict__ for r in results]).rename(columns={
                "asset_id": "자산ID",
                "ticker": "티커",
                "ok": "성공여부",
                "old_price": "기존가",
                "new_price": "신규가",
                "reason": "비고/실패사유",
            })
            st.dataframe(res_df, width="stretch")

            ok_asset_ids = [int(r.asset_id) for r in results if r.ok]

            if auto_rebuild and ok_asset_ids:
                with st.spinner("스냅샷 자동 리빌드 중..."):
                    summary = PriceUpdaterService.rebuild_snapshots_for_updated_assets(ok_asset_ids) or {}
                    rebuilt_rows = int(summary.get("rebuilt_total_rows", 0))
                    rebuilt_pairs = summary.get("rebuilt_pairs", "?")
                    
                    if summary.get("errors"):
                        st.warning("일부 계좌 리빌드 실패: " + " | ".join(summary["errors"][:3]))

                    st.success(f"스냅샷 리빌드 완료: 총 {rebuilt_rows}행 (대상 {rebuilt_pairs} 조합)")

            st.cache_data.clear()
            st.success("완료되었습니다. (실패 종목은 사유/스테일 상태를 확인하세요)")
        except Exception as e:
            st.error(f"실행 실패: {e}")
        finally:
            st.session_state["price_busy"] = False