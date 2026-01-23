from datetime import date
import streamlit as st
import pandas as pd

from asset_portfolio.dashboard.transaction_editor import _load_assets_df
from asset_portfolio.backend.infra.supabase_client import get_supabase_client
from asset_portfolio.backend.services.price_updater_service import PriceUpdaterService

        
def render_price_updater():
    st.title("💹 Price Updater (yfinance + krx)")

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

    st.caption("yfinance 및 krx에서 현재 가격(current_price)을 갱신합니다. 실패해도 기존가 유지 + 실패 사유/시각을 기록합니다.")

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
    include_krx = st.checkbox("KRX price source도 함께 업데이트", value=True)

    run_clicked = st.button("가격 업데이트 실행", type="primary", disabled=(len(selected_ids) == 0))

    if run_clicked:
        st.session_state["price_busy"] = True

        try:
            with st.spinner("가격 업데이트 중..."):
                results = PriceUpdaterService.update_many(selected_ids)

            asset_name_map = df.set_index("id")["name_kr"].to_dict()
            krx_detail_map = {}

            source_asset_ids = []
            if include_krx:
                supabase = get_supabase_client()
                rows = (
                    supabase.table("asset_price_sources")
                    .select("asset_id")
                    .eq("active", True)
                    .execute()
                    .data or []
                )
                source_asset_ids = sorted({int(r["asset_id"]) for r in rows if r.get("asset_id") is not None})
                if mode == "선택한 자산만":
                    selected_set = set(selected_ids)
                    source_asset_ids = [aid for aid in source_asset_ids if aid in selected_set]

            ok_asset_ids = [int(r.asset_id) for r in results if r.ok]

            if auto_rebuild and ok_asset_ids:
                with st.spinner("스냅샷 자동 리빌드 중..."):
                    summary = PriceUpdaterService.rebuild_snapshots_for_updated_assets(ok_asset_ids) or {}
                    rebuilt_rows = int(summary.get("rebuilt_total_rows", 0))
                    rebuilt_pairs = summary.get("rebuilt_pairs", "?")
                    
                    if summary.get("errors"):
                        st.warning("일부 계좌 리빌드 실패: " + " | ".join(summary["errors"][:3]))

                    st.success(f"스냅샷 리빌드 완료: 총 {rebuilt_rows}행 (대상 {rebuilt_pairs} 조합)")

            if include_krx and source_asset_ids:
                with st.spinner("KRX price source 업데이트 중..."):
                    # Future sources (deposit/fund/crawling) should be handled by adding
                    # new source_type branches in PriceUpdaterService._fetch_price_from_sources.
                    source_result = PriceUpdaterService.update_asset_prices_for_date(
                        asset_ids=source_asset_ids,
                        price_date=date.today(),
                        carry_forward_on_fail=True,
                    )
                details = source_result.get("details") or []
                for d in details:
                    aid = d.get("asset_id")
                    if aid is None:
                        continue
                    ok = bool(d.get("ok"))
                    source = d.get("source") or "krx"
                    reason = d.get("reason")
                    status = "ok" if ok else "failed"
                    note = f"krx:{status}({source})"
                    if reason:
                        note = f"{note} {reason}"
                    krx_detail_map[int(aid)] = note
                st.info(
                    "KRX price source 업데이트: "
                    f"inserted={source_result.get('inserted')}, "
                    f"failed={source_result.get('failed')}"
                )

            res_df = pd.DataFrame([r.__dict__ for r in results])
            if not res_df.empty:
                res_df["asset_name"] = res_df["asset_id"].map(asset_name_map)
                if krx_detail_map:
                    res_df["reason"] = res_df["reason"].fillna("")
                    res_df["reason"] = res_df.apply(
                        lambda r: (f"{r['reason']} | {krx_detail_map[int(r['asset_id'])]}"
                                   if krx_detail_map.get(int(r["asset_id"]))
                                   else r["reason"]),
                        axis=1,
                    )
            res_df = res_df.rename(columns={
                "asset_id": "asset_id",
                "asset_name": "asset_name",
                "ticker": "ticker",
                "ok": "ok",
                "old_price": "old_price",
                "new_price": "new_price",
                "reason": "reason",
            })
            st.dataframe(res_df, width="stretch")

            st.cache_data.clear()
            st.success("완료되었습니다. (실패 종목은 사유/스테일 상태를 확인하세요)")
        except Exception as e:
            st.error(f"실행 실패: {e}")
        finally:
            st.session_state["price_busy"] = False
