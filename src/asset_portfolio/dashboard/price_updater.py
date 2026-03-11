from datetime import date
import streamlit as st
import pandas as pd

from asset_portfolio.dashboard.transaction_editor import _load_assets_df
from asset_portfolio.backend.infra.supabase_client import get_supabase_client
from asset_portfolio.backend.services.price_updater_service import PriceUpdaterService


def _render_auto_updater(df: pd.DataFrame):
    st.subheader("필터 (자동업데이트 전용)")
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

    run_clicked = st.button("자동 가격 업데이트 실행", type="primary", disabled=(len(selected_ids) == 0) or st.session_state.get("price_busy", False))

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


def _render_manual_updater(df: pd.DataFrame):
    st.subheader("수동 자산 가격 입력")
    st.caption("price_source가 **manual_price**인 펀드, 주식 등 **수량과 단가가 분리된 자산**의 기준가를 직접 입력합니다.")
    
    # Filter manual assets that are set to manual_price
    manual_assets = df[(df["price_source"].fillna("").str.lower() == "manual_price")]
    
    if manual_assets.empty:
        st.info("수동으로 단가를 입력할 자산이 없습니다. (단가 입력을 원하시면 자산 편집기에서 price_source를 'manual_price'로 변경하세요. 일반 예적금/펀드의 총 잔액 입력은 '스냅샷 수정' 메뉴를 이용하세요)")
        return
        
    price_date = st.date_input("기준일 (가격 업데이트 날짜)", value=date.today())
    
    edit_df = manual_assets[["id", "name_kr", "ticker", "currency", "current_price"]].copy()
    edit_df["입력 단가(close_price)"] = edit_df["current_price"]
    
    st.caption("아래 표에서 '입력 단가'를 수정한 후 저장 버튼을 누르세요. (저장 시 거래내역 수량을 바탕으로 스냅샷이 자동 리빌드됩니다)")
    edited = st.data_editor(
        edit_df,
        width='stretch',
        disabled=st.session_state.get("price_busy", False),
        column_config={
            "id": st.column_config.NumberColumn("ID", disabled=True),
            "name_kr": st.column_config.TextColumn("자산명", disabled=True),
            "ticker": st.column_config.TextColumn("Ticker", disabled=True),
            "currency": st.column_config.TextColumn("통화", disabled=True),
            "current_price": st.column_config.NumberColumn("기존 단가", disabled=True),
            "입력 단가(close_price)": st.column_config.NumberColumn("입력 단가", min_value=0.0, step=10.0),
        }
    )
    
    if st.button("수동 가격 저장 및 스냅샷 리빌드", type="primary", disabled=st.session_state.get("price_busy", False)):
        st.session_state["price_busy"] = True
        try:
            with st.spinner("저장 중..."):
                supabase = get_supabase_client()
                
                prices_to_upsert = []
                asset_ids_to_rebuild = []
                now_iso = pd.Timestamp.utcnow().isoformat()
                
                for idx, row in edited.iterrows():
                    aid = int(row["id"])
                    new_price = float(row["입력 단가(close_price)"])
                    old_price = float(row["current_price"])
                    
                    if new_price <= 0:
                        continue
                        
                    # 1. Update assets.current_price
                    supabase.table("assets").update({
                        "current_price": new_price,
                        "price_updated_at": now_iso,
                        "price_update_status": "ok",
                        "price_update_error": None
                    }).eq("id", aid).execute()
                    
                    # 2. Insert into asset_prices
                    prices_to_upsert.append({
                        "price_date": price_date.isoformat(),
                        "asset_id": aid,
                        "close_price": new_price,
                        "currency": row.get("currency") or "",
                        "source": "manual_entry",
                        "fetched_at": now_iso
                    })
                    
                    if aid not in asset_ids_to_rebuild:
                        asset_ids_to_rebuild.append(aid)
                    
                if prices_to_upsert:
                    supabase.table("asset_prices").upsert(
                        prices_to_upsert,
                        on_conflict="price_date,asset_id"
                    ).execute()
                    
                rebuilt_rows = 0
                if asset_ids_to_rebuild:
                    summary = PriceUpdaterService.rebuild_snapshots_for_updated_assets(asset_ids_to_rebuild)
                    rebuilt_rows = summary.get("rebuilt_total_rows", 0)
                    
            st.success(f"수동 가격 저장 및 스냅샷 리빌드 완료! (영향 받은 스냅샷: {rebuilt_rows} 건)")
            st.cache_data.clear()
            st.rerun()
        except Exception as e:
            st.error(f"오류: {e}")
        finally:
            st.session_state["price_busy"] = False


def render_price_updater():
    st.title("💹 자산 가격 업데이트 (자동/수동)")

    if "price_busy" not in st.session_state:
        st.session_state["price_busy"] = False

    df = _load_assets_df()
    if df.empty:
        st.error("assets 테이블에 데이터가 없습니다.")
        return

    tab1, tab2 = st.tabs(["자동 업데이트 (yfinance / KRX)", "수동 단가 입력 (펀드 / 기타)"])
    
    with tab1:
        _render_auto_updater(df)
        
    with tab2:
        _render_manual_updater(df)
