import streamlit as st
import pandas as pd
from datetime import date
from asset_portfolio.backend.infra.supabase_client import get_supabase_client
from asset_portfolio.backend.services.transaction_service import (
    TransactionService, CreateTransactionRequest
)

@st.cache_data(ttl=3600)
def _load_accounts_df() -> pd.DataFrame:
    supabase = get_supabase_client()
    rows = (
        supabase.table("accounts")
        .select("id, name, brokerage, owner, type")
        .order("brokerage")
        .execute()
        .data or []
    )
    return pd.DataFrame(rows)

@st.cache_data(ttl=3600)
def _load_assets_df() -> pd.DataFrame:
    supabase = get_supabase_client()
    rows = (
        supabase.table("assets")
        .select("id, ticker, name_kr, asset_type, currency, market, underlying_asset_class")
        .execute()
        .data or []
    )
    df = pd.DataFrame(rows)

    if not df.empty:
        # None 정렬 안정화
        for c in ["market", "asset_type", "underlying_asset_class", "ticker"]:
            if c in df.columns:
                df[c] = df[c].fillna("")
                
        # 자산 선택 드롭다운 정렬: market → asset_type → underlying_asset_class 순
        df = df.sort_values(
            by=["market", "asset_type", "underlying_asset_class", "ticker"],
            ascending=[True, True, True, True],
            kind="mergesort",  # 안정 정렬
        )

    return df


def render_transaction_editor():
    st.title("🧾 Transaction Editor (V1)")

    acc_df = _load_accounts_df()
    ast_df = _load_assets_df()

    if acc_df.empty:
        st.error("accounts 테이블에 데이터가 없습니다.")
        return
    if ast_df.empty:
        st.error("assets 테이블에 데이터가 없습니다.")
        return

    # ALL은 거래 입력 대상이 아니므로 제외
    acc_df["label"] = acc_df.apply(
        lambda r: f"{r['brokerage']} | {r['name']} ({r['owner']})",
        axis=1
    )
    selected_acc_label = st.selectbox("계좌 선택", acc_df["label"].tolist())
    account_id = acc_df.loc[acc_df["label"] == selected_acc_label, "id"].iloc[0]

    ast_df["label"] = ast_df.apply(
        lambda r: f"{r['ticker']} | {r['name_kr']} [{r.get('currency','')}]",
        axis=1
    )
    selected_asset_label = st.selectbox("자산 선택", ast_df["label"].tolist())
    asset_id = int(ast_df.loc[ast_df["label"] == selected_asset_label, "id"].iloc[0])

    TRADE_TYPE_LABEL_TO_CODE = {
        "매수": "BUY",
        "매도": "SELL",
        "입금": "DEPOSIT",
        "출금": "WITHDRAW",
    }

    trade_type_label = st.selectbox("거래 타입", list(TRADE_TYPE_LABEL_TO_CODE.keys()))
    trade_type = TRADE_TYPE_LABEL_TO_CODE[trade_type_label]  # DB 저장용

    tx_date = st.date_input("거래일", value=date.today())

    quantity = st.number_input("수량(또는 현금 금액)", min_value=0.0, value=0.0, step=1.0)

    # CASH 거래는 price=1 고정(입력 숨김/비활성)
    is_cash_type = trade_type in {"DEPOSIT", "WITHDRAW"}
    price = 1.0
    if not is_cash_type:
        price = st.number_input("단가", min_value=0.0, value=0.0, step=1.0)

    fee = st.number_input("수수료", min_value=0.0, value=0.0, step=1.0)
    tax = st.number_input("세금", min_value=0.0, value=0.0, step=1.0)
    memo = st.text_input("메모", value="")

    st.caption("※ 제출 시: transactions insert → (거래일~오늘) 해당 자산 daily_snapshots 리빌드")

    col1, col2 = st.columns([1, 1])
    with col1:
        submit = st.button("거래 저장 및 스냅샷 반영", type="primary")
    with col2:
        st.button("화면 새로고침", on_click=lambda: st.rerun())

    if submit:
        try:
            req = CreateTransactionRequest(
                account_id=str(account_id),
                asset_id=int(asset_id),
                transaction_date=tx_date,
                trade_type=str(trade_type),
                quantity=float(quantity),
                price=float(price),
                fee=float(fee),
                tax=float(tax),
                memo=memo if memo else None,
            )
            st.session_state["last_tx_req"] = req  # ✅ 저장

            result = TransactionService.create_transaction_and_rebuild(req)
            st.success(
                f"저장 완료. 스냅샷 {result['rebuilt_rows']}행 리빌드 "
                f"({result['rebuilt_start_date']} ~ {result['rebuilt_end_date']})"
            )
            # 캐시 무효화(assets/accounts 조회 캐시 등은 TTL이지만, 즉시 반영 원하면 clear)
            st.cache_data.clear()
            st.rerun()

        except Exception as e:
            st.error(f"처리 실패: {e}")
            st.info("네트워크/일시 오류일 수 있습니다. 동일 내용을 다시 제출하거나, 재시도 후에도 안되면 로그를 확인하세요.")

        req = st.session_state.get("last_tx_req")
        if st.session_state.get("last_rebuild_failed") and req:
            if st.button("실패한 리빌드 재시도"):
                try:
                    rebuilt = TransactionService.rebuild_daily_snapshots_for_asset(
                        account_id=req.account_id,
                        asset_id=req.asset_id,
                        start_date=req.transaction_date,
                        end_date=date.today(),
                        delete_first=True,
                    )
                    st.success(f"리빌드 재시도 성공: {rebuilt}행")
                    st.session_state["last_rebuild_failed"] = False
                    st.rerun()
                except Exception as e:
                    st.error(f"재시도 실패: {e}")
