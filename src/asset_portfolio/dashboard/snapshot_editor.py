from __future__ import annotations

from datetime import date
import pandas as pd
import streamlit as st

from asset_portfolio.backend.infra.supabase_client import get_supabase_client
from asset_portfolio.backend.services.manual_cost_basis_service import record_cost_basis_events
from asset_portfolio.dashboard.transaction_editor import _load_accounts_df, _load_assets_df

MANUAL_TYPES = {"manual", "deposit", "bond", "pension"}


def _load_manual_assets_df() -> pd.DataFrame:
    df = _load_assets_df()
    if df.empty:
        return df

    df["price_source_norm"] = df["price_source"].fillna("").str.lower().str.strip()
    # 'manual' (총액 입력 기반) 인 자산들만 스냅샷 에디터에 표시
    df = df[df["price_source_norm"].isin(MANUAL_TYPES)].copy()

    # ✅ 표시 라벨: ticker만 애매하면 name_kr가 더 중요하므로 둘 다 노출 + id도 붙임
    df["label"] = df.apply(
        lambda r: f"{r['name_kr']} ({r['ticker']}) [{str(r.get('currency','')).upper()}]  #id={r['id']}",
        axis=1,
    )
    return df


def _load_snapshots_for_date_multi(account_ids: list[str], snap_date: date, asset_ids: list[int]) -> pd.DataFrame:
    """
    ✅ 여러 계좌에 대해 (date=고정) 스냅샷 로드
    """
    supabase = get_supabase_client()

    rows = (
        supabase.table("daily_snapshots")
        .select("date, account_id, asset_id, quantity, valuation_price, purchase_price, valuation_amount, purchase_amount")
        .eq("date", snap_date.isoformat())
        .in_("account_id", account_ids)
        .in_("asset_id", asset_ids)
        .execute()
        .data or []
    )
    return pd.DataFrame(rows)


def _upsert_snapshots(rows: list[dict]) -> None:
    if not rows:
        return
    supabase = get_supabase_client()
    supabase.table("daily_snapshots").upsert(rows).execute()


def _upsert_asset_prices(rows: list[dict]) -> None:
    """
    수동자산 평가 입력 시점에 asset_prices도 함께 저장한다.
    - price_date + asset_id 기준으로 업서트
    """
    if not rows:
        return
    supabase = get_supabase_client()
    supabase.table("asset_prices").upsert(
        rows,
        on_conflict="price_date,asset_id",
    ).execute()


def render_snapshot_editor(user_id: str):
    st.title("🏦 정적 자산 평가액 갱신")

    if "snap_busy" not in st.session_state:
        st.session_state["snap_busy"] = False

    acc_df = _load_accounts_df(user_id)
    if acc_df.empty:
        st.warning("accounts 데이터가 없습니다.")
        return

    manual_assets = _load_manual_assets_df()
    if manual_assets.empty:
        st.info("수동평가 대상 자산(asset_type)이 없습니다. assets에 manual/bond/deposit/pension 등을 지정하세요.")
        return

    # ✅ 편집 대상: 사용자의 모든 계좌
    account_ids = acc_df["id"].astype(str).tolist()

    snap_date = st.date_input("스냅샷 날짜", value=date.today(), disabled=st.session_state["snap_busy"])

    # =========================
    # 2) 자산 선택 (기본: price_source='manual' 우선 선택은 사용자가 이미 반영 완료하셨다고 하셨으므로,
    #    여기서는 '선택된 자산 라벨'만 받아서 asset_id를 뽑습니다.
    # =========================
    # ✅ 사용자의 모든 manual 자산 ID 추출
    selected_asset_ids = manual_assets["id"].astype(int).tolist()

    # =========================
    # 3) 스냅샷 로드 (멀티 계좌)
    # =========================
    snap_df = _load_snapshots_for_date_multi(account_ids, snap_date, selected_asset_ids)

    # =========================
    # ✅ (중요) 멀티 편집에서는 '전체 곱'이 아니라
    #         실제 존재하는 (account_id, asset_id) pair만 로드합니다.
    # =========================
    pairs_df = _load_existing_pairs_for_manual_assets(
        account_ids=account_ids,
        asset_ids=selected_asset_ids,
        snap_date=snap_date,
    )

    if pairs_df.empty:
        st.info("선택한 계좌들에서 현재 날짜 기준으로 존재하는 수동평가 자산이 없습니다. (스냅샷/거래 기반 pair가 없음)")
        st.stop()

    grid = pairs_df.copy()

    if snap_df.empty:
        base_df = grid.copy()
        base_df["date"] = snap_date.isoformat()
        base_df["quantity"] = 0.0
        base_df["valuation_price"] = 1.0
        base_df["purchase_price"] = 1.0
        base_df["valuation_amount"] = 0.0
        base_df["purchase_amount"] = 0.0
    else:
        snap_df["date"] = snap_date.isoformat()
        base_df = grid.merge(snap_df, on=["account_id", "asset_id"], how="left")

        base_df["date"] = base_df["date"].fillna(snap_date.isoformat())
        for c in ["quantity", "valuation_amount", "purchase_amount"]:
            base_df[c] = pd.to_numeric(base_df[c], errors="coerce").fillna(0.0)
        for c in ["valuation_price", "purchase_price"]:
            base_df[c] = pd.to_numeric(base_df[c], errors="coerce").fillna(1.0)

    # =========================
    # 4) 보기용 메타 조인: 계좌 라벨 + 자산 라벨
    # =========================
    # 계좌 라벨
    acc_map = acc_df[["id", "label"]].rename(columns={"id": "account_id", "label": "계좌"})
    base_df = base_df.merge(acc_map, on="account_id", how="left")

    # 자산 메타 (price_source가 있어야 원금 정보 로드 가능)
    ast_map = manual_assets[["id", "name_kr", "ticker", "currency", "asset_type", "price_source"]].rename(columns={"id": "asset_id"})
    base_df = base_df.merge(ast_map, on="asset_id", how="left")

    # ✅ 최신 원금(Cost Basis) 정보 가져오기 (1회성 보정 및 편집용)
    from asset_portfolio.backend.services.manual_cost_basis_service import attach_manual_cost_basis
    # attach_manual_cost_basis는 "assets.price_source" 컬럼을 기본으로 찾으므로 임시로 연결
    base_df["assets.price_source"] = base_df["price_source"]
    base_df = attach_manual_cost_basis(base_df, user_id=user_id)
    
    if "manual_principal" not in base_df.columns or base_df["manual_principal"].isna().all():
        # 못 가져온 경우 기존 스냅샷의 purchase_amount 활용
        base_df["manual_principal"] = base_df["manual_principal"].fillna(base_df["purchase_amount"])
    
    base_df["manual_principal"] = pd.to_numeric(base_df["manual_principal"], errors="coerce").fillna(0.0)

    # ✅ 사용자가 편집할 필드
    base_df["평가금액"] = pd.to_numeric(base_df["valuation_amount"], errors="coerce").fillna(0.0)
    base_df["납입원금"] = base_df["manual_principal"]

    # 표시 컬럼
    view_cols = ["계좌", "name_kr", "ticker", "currency", "유형", "평가금액", "납입원금"]
    base_df = base_df.rename(columns={"asset_type": "유형"})

    st.caption("※ 수동평가 자산은 quantity=평가금액으로 관리됩니다. 이 화면에서 **평가액**과 **납입원금**을 직접 수정할 수 있습니다.")

    edited = st.data_editor(
        base_df[view_cols],
        width='stretch',
        disabled=st.session_state["snap_busy"],
        column_config={
            "계좌": st.column_config.TextColumn("계좌명", disabled=True),
            "name_kr": st.column_config.TextColumn("자산명", disabled=True),
            "ticker": st.column_config.TextColumn("Ticker", disabled=True),
            "currency": st.column_config.TextColumn("통화", disabled=True),
            "유형": st.column_config.TextColumn("유형", disabled=True),
            "평가금액": st.column_config.NumberColumn("평가금액", min_value=0.0, step=1000.0),
            "납입원금": st.column_config.NumberColumn("납입원금", min_value=0.0, step=1000.0),
        },
    )

    # =========================
    # 5) 저장(upsert)
    # =========================
    save = st.button("저장(스냅샷 반영)", type="primary", disabled=st.session_state["snap_busy"])
    if save:
        st.session_state["snap_busy"] = True
        try:
            with st.spinner("스냅샷 저장 중..."):
                save_rows = []
                cost_basis_events = []
                new_transactions = []

                # edited는 account_id/asset_id가 없으므로 base_df의 동일 index를 이용해 매핑
                for i, row in edited.iterrows():
                    account_id = str(base_df.iloc[i]["account_id"])
                    asset_id = int(base_df.iloc[i]["asset_id"])
                    ccy = str(base_df.iloc[i].get("currency") or "").upper() or None
                    amt = float(row["평가금액"] or 0.0)
                    new_principal = float(row["납입원금"] or 0.0)
                    old_principal = float(base_df.iloc[i].get("manual_principal", 0.0))
                    delta = new_principal - old_principal

                    # ✅ 기존 스냅샷(평가금액)과 비교해 차이를 리밸류에이션 거래로 기록
                    original_amt = float(base_df.iloc[i].get("valuation_amount", 0.0))
                    revaluation_qty = amt - original_amt
                    
                    # ✅ 원금(purchase_amount)은 수동 자산의 경우 history 보다는 스냅샷 시점의 원금을 의미함
                    #    (단, manual_cost_basis_service가 우선순위가 높으므로 동기화가 중요)
                    save_rows.append({
                        "date": snap_date.isoformat(),
                        "account_id": account_id,
                        "asset_id": asset_id,
                        "quantity": amt,
                        "valuation_price": 1.0,
                        "purchase_price": new_principal / amt if amt > 0 else 0.0,
                        "valuation_amount": amt,
                        "purchase_amount": new_principal,
                        "currency": ccy,
                    })
                    
                    if abs(revaluation_qty) > 0.001:
                        new_transactions.append({
                            "account_id": account_id,
                            "asset_id": asset_id,
                            "trade_type": "REVALUATION",
                            "quantity": revaluation_qty,
                            "price": 1.0,
                            "transaction_date": snap_date.isoformat(),
                            "fee": 0.0,
                            "tax": 0.0,
                            "memo": "Manual Snapshot Update",
                        })

                    if abs(delta) > 0.001:
                        # 수동 자산의 원금 수정은 cost basis 이벤트로 기록한다.
                        cost_basis_events.append({
                            "account_id": account_id,
                            "asset_id": asset_id,
                            "event_date": snap_date.isoformat(),
                            "delta_amount": delta,
                            "currency": ccy or "KRW",
                            "reason": "snapshot_editor_correction",
                            "memo": "원금 직접 수정 보정",
                        })

                if new_transactions:
                    supabase = get_supabase_client()
                    supabase.table("transactions").insert(new_transactions).execute()

                _upsert_snapshots(save_rows)
                # 수동자산은 평가 입력 시점에만 가격 히스토리를 저장한다.
                # 동일 자산이 여러 계좌에 있어도 가격은 동일하므로 자산 기준으로만 업서트한다.
                price_rows = []
                seen_assets = set()
                for r in save_rows:
                    if r["asset_id"] in seen_assets:
                        continue
                    seen_assets.add(r["asset_id"])
                    price_rows.append({
                        "price_date": r["date"],
                        "asset_id": r["asset_id"],
                        "close_price": r["valuation_price"],
                        "currency": r.get("currency") or "",
                        "source": "manual_snapshot",
                        "fetched_at": None,
                    })
                _upsert_asset_prices(price_rows)
                # 원금 증감 입력이 있으면 cost basis current까지 갱신한다.
                if cost_basis_events:
                    record_cost_basis_events(cost_basis_events)

            st.success("저장 완료. 대시보드에 즉시 반영됩니다.")
            st.cache_data.clear()
            st.rerun()
        except Exception as e:
            st.error(f"저장 실패: {e}")
        finally:
            st.session_state["snap_busy"] = False


def _load_existing_pairs_for_manual_assets(
    account_ids: list[str],
    asset_ids: list[int],
    snap_date: date,
) -> pd.DataFrame:
    """
    ✅ 멀티 편집에서 '해당 계좌에 실제로 존재하는 자산'만 보여주기 위한 (account_id, asset_id) pair 조회

    우선순위:
    1) daily_snapshots: snap_date 기준 valuation_amount > 0 인 pair
    2) (없으면) transactions: 과거 어떤 거래라도 있는 pair
    """
    supabase = get_supabase_client()

    # =========================
    # 1) daily_snapshots 기반(당일 존재하는 자산)
    # =========================
    snap_rows = (
        supabase.table("daily_snapshots")
        .select("account_id, asset_id, valuation_amount")
        .eq("date", snap_date.isoformat())
        .in_("account_id", account_ids)
        .in_("asset_id", asset_ids)
        .execute()
        .data or []
    )
    snap_df = pd.DataFrame(snap_rows)
    if not snap_df.empty:
        snap_df["valuation_amount"] = pd.to_numeric(snap_df["valuation_amount"], errors="coerce").fillna(0.0)
        snap_df = snap_df[snap_df["valuation_amount"] > 0].copy()
        if not snap_df.empty:
            return snap_df[["account_id", "asset_id"]].drop_duplicates()

    # =========================
    # 2) transactions 기반(과거라도 거래가 있던 자산)
    # - 스냅샷이 아직 없을 수도 있으니 fallback으로 사용
    # =========================
    tx_rows = (
        supabase.table("transactions")
        .select("account_id, asset_id")
        .in_("account_id", account_ids)
        .in_("asset_id", asset_ids)
        .execute()
        .data or []
    )
    tx_df = pd.DataFrame(tx_rows)
    if tx_df.empty:
        return tx_df

    return tx_df[["account_id", "asset_id"]].drop_duplicates()
