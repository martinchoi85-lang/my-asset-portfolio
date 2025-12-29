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

    # ✅ BUY/SELL 시 현금 자동 반영 옵션(기본 True 권장)
    # - 기존 데이터에 수동 cash 입력이 많다면 기본 False로 두는 것도 방법입니다.
    auto_cash = st.checkbox("BUY/SELL 시 현금(CASH) 자동 반영", value=True)
    st.caption("※ '현금 자동 반영'을 켠 경우, BUY/SELL 입력 시 CASH 거래가 자동 생성됩니다. (현금 수동 입력과 중복 주의)")

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

    ########################## 삽입 부분 시작
    assets_df = _load_assets_df()  # id, ticker, name_kr, asset_type, currency, market ... 포함

    # ✅ 자동 현금 반영이 켜져 있고 BUY/SELL이면, 자산 선택 목록에서 CASH 제거
    if auto_cash and trade_type in {"BUY", "SELL"}:
        assets_df = assets_df[assets_df["asset_type"].fillna("").str.lower() != "cash"].copy()

    # ✅ 입금/출금일 때는 CASH를 자동 선택/고정
    fixed_asset_id = None
    if trade_type in {"DEPOSIT", "WITHDRAW"}:
        # 1) 통화 선택(입출금은 결국 현금 통화가 필요)
        cash_ccy = st.selectbox("입출금 통화", ["KRW", "USD"], index=0)

        # 2) 해당 통화의 CASH 자산 id 자동 선택
        cash_rows = assets_df[
            (assets_df["asset_type"].fillna("").str.lower() == "cash")
            & (assets_df["currency"].fillna("").str.upper() == cash_ccy)
        ]
        if cash_rows.empty:
            st.error(f"{cash_ccy} CASH 자산이 없습니다. assets에 asset_type='cash' & currency='{cash_ccy}' 자산을 추가하세요.")
            st.stop()

        fixed_asset_id = int(cash_rows.iloc[0]["id"])

        # 3) 사용자에게 고정 자산을 명시적으로 보여줌
        st.info(f"입금/출금은 현금(CASH) 자산으로만 입력됩니다: {cash_rows.iloc[0]['ticker']} | {cash_rows.iloc[0]['name_kr']}")

        # 4) 자산 드롭다운은 비활성화(대신 고정)
        asset_id = fixed_asset_id
    else:
        # 일반 BUY/SELL: 사용자가 자산을 선택
        asset_label = st.selectbox("자산 선택", assets_df["label"].tolist())
        asset_id = int(assets_df.loc[assets_df["label"] == asset_label, "id"].iloc[0])

    # ✅ price 입력: 입금/출금이면 price=1로 고정하고 입력창 비활성화
    if trade_type in {"DEPOSIT", "WITHDRAW"}:
        price = st.number_input("단가", value=1.0, disabled=True)  # ✅ 고정
        quantity = st.number_input("금액", min_value=0.0, value=0.0)  # ✅ 라벨을 '금액'으로
    else:
        price = st.number_input("단가", min_value=0.0, value=0.0)
        quantity = st.number_input("수량", min_value=0.0, value=0.0)

    ########################## 삽입 부분 끝


    # 거래 타입(trade_type)이 결정된 이후, assets_df를 만들기 전에/후에 적용
    if trade_type == "SELL" and account_id:
        holding_ids = _load_latest_holding_asset_ids(str(account_id))

        # ✅ holding이 없으면 SELL 대상이 없으므로 안내
        if not holding_ids:
            st.info("해당 계좌에 보유 중인 자산이 없습니다. (SELL 불가)")
        else:
            assets_df = assets_df[assets_df["id"].isin(holding_ids)].copy()

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
                trade_type=str(trade_type),  # (이미 한글→영문 코드 매핑 적용한 상태라고 가정)
                quantity=float(quantity),
                price=float(price),
                fee=float(fee),
                tax=float(tax),
                memo=memo if memo else None,
            )
            st.session_state["last_tx_req"] = req
            
            # ✅ auto_cash 옵션 전달
            result = TransactionService.create_transaction_and_rebuild(req, auto_cash=auto_cash)

            # ✅ 결과 메시지: cash 자동 반영 여부에 따라 안내 강화
            if result.get("cash_transaction"):
                st.success(
                    f"저장 완료. (원자산 리빌드 {result['rebuilt_rows_main']}행 + CASH 리빌드 {result['rebuilt_rows_cash']}행)\n"
                    f"기간: {result['rebuilt_start_date']} ~ {result['rebuilt_end_date']}"
                )
            else:
                st.success(
                    f"저장 완료. (원자산 리빌드 {result['rebuilt_rows_main']}행)\n"
                    f"기간: {result['rebuilt_start_date']} ~ {result['rebuilt_end_date']}"
                )

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


@st.cache_data(ttl=60)
def _load_latest_holding_asset_ids(account_id: str) -> set[int]:
    """
    ✅ 특정 계좌의 '최신 스냅샷 날짜' 기준 보유 자산(asset_id) 집합을 반환
    - SELL 시 자산 드롭다운을 보유 자산으로 제한하기 위해 사용
    """
    supabase = get_supabase_client()

    # 1) 최신 날짜 조회
    latest_row = (
        supabase.table("daily_snapshots")
        .select("date")
        .eq("account_id", account_id)
        .order("date", desc=True)
        .limit(1)
        .execute()
        .data or []
    )
    if not latest_row:
        return set()

    latest_date = latest_row[0]["date"]

    # 2) 최신 날짜의 보유 자산 조회(quantity>0)
    rows = (
        supabase.table("daily_snapshots")
        .select("asset_id, quantity")
        .eq("account_id", account_id)
        .eq("date", latest_date)
        .gt("quantity", 0)
        .execute()
        .data or []
    )

    return {int(r["asset_id"]) for r in rows if r.get("asset_id") is not None}