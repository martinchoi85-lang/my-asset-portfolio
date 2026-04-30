import streamlit as st
import pandas as pd
from datetime import date
from typing import Optional

from asset_portfolio.backend.infra.supabase_client import get_supabase_client
from asset_portfolio.backend.services.transaction_service import TransactionService
from asset_portfolio.backend.services.asset_service import AssetService
from asset_portfolio.backend.services.transaction_service import CreateTransactionRequest


@st.cache_data(ttl=300)
def _load_accounts_df(user_id: str) -> pd.DataFrame:
    supabase = get_supabase_client()
    rows = (
        supabase.table("accounts")
        .select("id, name, brokerage, old_owner, type")
        .eq("user_id", user_id)
        .order("brokerage")
        .order("type")
        .order("name")
        .execute()
        .data or []
    )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["label"] = df.apply(lambda r: f"{r['brokerage']} | {r['name']} ({r['type']})", axis=1)
    return df


@st.cache_data(ttl=300)
def _load_assets_df() -> pd.DataFrame:
    supabase = get_supabase_client()
    rows = (
        supabase.table("assets")
        .select(
            "id, ticker, name_kr, asset_type, currency, market, underlying_asset_class, "
            "economic_exposure_region, asset_nature, vehicle_type, fx_exposure_type, "
            "return_driver, strategy_type, "
            "current_price, price_updated_at, price_update_status, price_update_error, price_source"
        )
        .order("market")
        .order("asset_type")
        .order("underlying_asset_class")
        .order("ticker")
        .execute()
        .data or []
    )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["label"] = df.apply(lambda r: f"{r['ticker']} | {r['name_kr']} [{str(r.get('currency','')).upper()}]", axis=1)
    return df


@st.cache_data(ttl=60)
def _load_latest_holding_asset_ids(account_id: str) -> set[int]:
    supabase = get_supabase_client()
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


def _find_cash_asset_id(currency: str) -> int:
    df = _load_assets_df()
    currency = str(currency).upper().strip()
    cash_rows = df[
        (df["asset_type"].fillna("").str.lower() == "cash")
        & (df["currency"].fillna("").str.upper() == currency)
    ]
    if cash_rows.empty:
        raise ValueError(f"{currency} CASH 자산이 없습니다. assets에 asset_type='cash' & currency='{currency}' 자산을 추가하세요.")
    return int(cash_rows.iloc[0]["id"])


def render_transaction_editor(user_id: str):
    st.title("🧾 Transaction Editor (V1)")

    if "tx_busy" not in st.session_state:
        st.session_state["tx_busy"] = False

    acc_df = _load_accounts_df(user_id)
    if acc_df.empty:
        st.error("해당 사용자의 계좌가 없습니다. 계좌를 먼저 추가해주세요.")
        return

    selected_acc_label = st.selectbox("계좌 선택", acc_df["label"].tolist())
    account_id = str(acc_df.loc[acc_df["label"] == selected_acc_label, "id"].iloc[0])

    TRADE_TYPE_LABEL_TO_CODE = {
        "매수": "BUY",
        "매도": "SELL",
        "입금": "DEPOSIT",
        "출금": "WITHDRAW",
    }
    trade_type_label = st.selectbox("거래 타입", list(TRADE_TYPE_LABEL_TO_CODE.keys()))
    trade_type = TRADE_TYPE_LABEL_TO_CODE[trade_type_label]

    auto_cash = st.checkbox("BUY/SELL 시 현금(CASH) 자동 반영", value=True)
    st.caption("※ 자동 현금 반영 ON: BUY/SELL 입력 시 CASH 거래가 자동 생성됩니다. (기존 현금 수동 입력과 중복 주의)")

    if trade_type in {"BUY", "SELL"}:
        asset_mode = st.radio("자산 선택 방식", ["기존 자산에서 선택", "새 자산 생성 후 거래"], horizontal=True)
    else:
        asset_mode = "기존 자산에서 선택"

    assets_df = _load_assets_df()
    if assets_df.empty and not (trade_type in {"BUY", "SELL"} and asset_mode == "새 자산 생성 후 거래"):
        st.error("assets 테이블에 자산이 없습니다.")
        return

    asset_id: Optional[int] = None
    if trade_type in {"DEPOSIT", "WITHDRAW"}:
        cash_ccy = st.selectbox("입출금 통화", ["krw", "usd"], index=0)
        try:
            asset_id = _find_cash_asset_id(cash_ccy)
        except Exception as e:
            st.error(str(e))
            return
        cash_row = _load_assets_df().loc[_load_assets_df()["id"] == asset_id].iloc[0]
        st.info(f"입금/출금은 현금(CASH) 자산으로만 입력됩니다: {cash_row['ticker']} | {cash_row['name_kr']} [{cash_ccy}]")
        price = 1.0

    elif asset_mode == "새 자산 생성 후 거래":
        st.subheader("➕ 새 자산 생성")
        new_ticker = st.text_input("티커(중복 불가)", value="")
        new_name = st.text_input("자산명(한글)", value="")
        new_currency = st.selectbox("통화", ["krw", "usd"], index=0)
        new_asset_type = st.selectbox("자산 유형", ["stock", "etf", "fund", "cash", "etc"], index=1)
        new_market = st.selectbox("시장", ["korea", "usa", "etc"], index=0)
        st.caption("※ V1에서는 최소 필드로 assets에 등록하고, 분류(underlying_asset_class 등)는 추후 Asset Editor에서 보강합니다.")

        if st.button("새 자산 생성", disabled=st.session_state["tx_busy"]):
            st.session_state["tx_busy"] = True
            try:
                with st.spinner("새 자산 생성 중..."):
                    created = AssetService.create_asset_minimal(
                        ticker=new_ticker, name_kr=new_name, asset_type=new_asset_type,
                        currency=new_currency, market=new_market,
                    )
                st.success(f"자산 생성 완료: id={created['id']}, ticker={created['ticker']}")
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"자산 생성 실패: {e}")
            finally:
                st.session_state["tx_busy"] = False
        st.divider()
        st.subheader("🧾 거래 입력")
        assets_df = _load_assets_df()
        if not assets_df.empty:
            if auto_cash:
                assets_df = assets_df[assets_df["asset_type"].fillna("").str.lower() != "cash"].copy()
            selected_asset_label = st.selectbox("자산 선택(생성 완료된 자산 포함)", assets_df["label"].tolist())
            asset_id = int(assets_df.loc[assets_df["label"] == selected_asset_label, "id"].iloc[0])
        else:
            st.warning("assets 목록을 불러올 수 없습니다. 자산 생성 후 다시 시도하세요.")
            return
    else:
        if auto_cash and trade_type in {"BUY", "SELL"} and not assets_df.empty:
            assets_df = assets_df[assets_df["asset_type"].fillna("").str.lower() != "cash"].copy()
        if trade_type == "SELL":
            holding_ids = _load_latest_holding_asset_ids(account_id)
            if not holding_ids:
                st.info("해당 계좌에 보유 중인 자산이 없습니다. (SELL 입력 불가)")
                return
            assets_df = assets_df[assets_df["id"].isin(list(holding_ids))].copy()
        if assets_df.empty:
            st.error("선택 가능한 자산이 없습니다.")
            return
        selected_asset_label = st.selectbox("자산 선택", assets_df["label"].tolist())
        asset_id = int(assets_df.loc[assets_df["label"] == selected_asset_label, "id"].iloc[0])
        price = None

    tx_date = st.date_input("거래일", value=date.today())
    if trade_type in {"DEPOSIT", "WITHDRAW"}:
        quantity = st.number_input("금액", min_value=0.0, value=0.0, step=1.0)
        st.number_input("단가", value=1.0, disabled=True)
        price = 1.0
    else:
        quantity = st.number_input("수량", min_value=0.0, value=0.0, step=1.0)
        price = st.number_input("단가", min_value=0.0, value=0.0, step=1.0)

    fee = st.number_input("수수료", min_value=0.0, value=0.0, step=1.0)
    tax = st.number_input("세금", min_value=0.0, value=0.0, step=1.0)
    memo = st.text_input("메모", value="")

    st.divider()
    st.caption("※ 제출 시: transactions insert → (거래일~오늘) 해당 자산 daily_snapshots 리빌드 (auto_cash=ON이면 CASH도 함께 반영)")
    st.caption("※ 중복 클릭 방지를 위해 제출 처리 중에는 버튼이 비활성화됩니다.")

    submit = st.button("거래 저장 및 스냅샷 반영", type="primary", disabled=st.session_state["tx_busy"])
    if st.button("화면 새로고침", disabled=st.session_state["tx_busy"]):
        st.rerun()

    if submit:
        st.session_state["tx_busy"] = True
        try:
            with st.spinner("거래 저장 및 스냅샷 반영 중..."):
                if asset_id is None:
                    raise ValueError("asset_id가 확정되지 않았습니다. (새 자산 생성 후 선택이 필요합니다)")
                req = CreateTransactionRequest(
                    account_id=str(account_id), asset_id=int(asset_id), transaction_date=tx_date,
                    trade_type=str(trade_type), quantity=float(quantity), price=float(price),
                    fee=float(fee), tax=float(tax), memo=memo or None,
                )
                result = TransactionService.create_transaction_and_rebuild(req, auto_cash=auto_cash)
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
            st.info("네트워크/일시 오류일 수 있습니다. 동일 내용을 재시도해보세요.")
        finally:
            st.session_state["tx_busy"] = False
