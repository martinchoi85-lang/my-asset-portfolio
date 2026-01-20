import streamlit as st
import pandas as pd
from datetime import date

from asset_portfolio.backend.infra.supabase_client import get_supabase_client


@st.cache_data(ttl=300)
def _load_accounts_df() -> pd.DataFrame:
    supabase = get_supabase_client()
    rows = (
        supabase.table("accounts")
        .select("id, name, brokerage, owner, type")
        .order("brokerage")
        .order("type")
        .order("owner")
        .execute()
        .data or []
    )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["label"] = df.apply(lambda r: f"{r['brokerage']} | {r['name']} ({r['type']}/{r['owner']})", axis=1)
    return df


@st.cache_data(ttl=300)
def _load_assets_df() -> pd.DataFrame:
    supabase = get_supabase_client()
    rows = (
        supabase.table("assets")
        .select("id, ticker, name_kr, asset_type, currency, market")
        .order("market")
        .order("asset_type")
        .order("ticker")
        .execute()
        .data or []
    )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["label"] = df.apply(lambda r: f"{r['ticker']} | {r['name_kr']} [{str(r.get('currency','')).upper()}]", axis=1)
    return df


def render_recurring_order_editor():
    st.title("📅 정기 매수 관리")

    supabase = get_supabase_client()

    acc_df = _load_accounts_df()
    if acc_df.empty:
        st.error("accounts 테이블에 계좌가 없습니다.")
        return

    selected_acc_label = st.selectbox("계좌 선택", acc_df["label"].tolist())
    account_id = str(acc_df.loc[acc_df["label"] == selected_acc_label, "id"].iloc[0])

    assets_df = _load_assets_df()
    if assets_df.empty:
        st.error("assets 테이블에 자산이 없습니다.")
        return

    # 정기 매수는 현금(CASH) 자산을 제외하고 노출
    assets_df = assets_df[assets_df["asset_type"].fillna("").str.lower() != "cash"].copy()
    if assets_df.empty:
        st.error("정기 매수 등록 가능한 자산이 없습니다.")
        return

    st.subheader("정기 매수 등록")

    asset_label = st.selectbox("매수 자산", assets_df["label"].tolist())
    asset_row = assets_df.loc[assets_df["label"] == asset_label].iloc[0]
    asset_id = int(asset_row["id"])
    asset_currency = str(asset_row.get("currency") or "").upper()

    frequency = st.selectbox("주기", ["MONTHLY", "WEEKLY"])

    # 주기별 스케줄 필드 분기
    day_of_month = None
    day_of_week = None
    if frequency == "MONTHLY":
        day_of_month = int(st.number_input("매수일(1~31)", min_value=1, max_value=31, value=1, step=1))
    else:
        day_of_week = st.selectbox(
            "요일",
            options=[0, 1, 2, 3, 4, 5, 6],
            format_func=lambda v: ["월", "화", "수", "목", "금", "토", "일"][v],
        )

    timezone = st.text_input("타임존", value="Asia/Seoul")

    # 수량/금액 중 하나 이상 입력하도록 유도
    quantity = st.number_input("수량(선택)", min_value=0.0, value=0.0, step=1.0)
    price = st.number_input("지정가(선택)", min_value=0.0, value=0.0, step=1.0)
    amount = st.number_input(f"매수금액(선택, {asset_currency})", min_value=0.0, value=0.0, step=1.0)

    start_date = st.date_input("시작일", value=date.today())
    use_end_date = st.checkbox("종료일 사용", value=False)
    end_date = st.date_input("종료일", value=date.today()) if use_end_date else None

    active = st.checkbox("활성화", value=True)
    memo = st.text_input("메모", value="")

    if st.button("정기 매수 등록", type="primary"):
        if quantity <= 0 and amount <= 0:
            st.error("수량 또는 매수금액 중 하나는 0보다 커야 합니다.")
        else:
            payload = {
                "account_id": account_id,
                "asset_id": asset_id,
                "trade_type": "BUY",
                "frequency": frequency,
                "day_of_month": day_of_month,
                "day_of_week": day_of_week,
                "timezone": timezone,
                "quantity": float(quantity) if quantity > 0 else None,
                "price": float(price) if price > 0 else None,
                "amount": float(amount) if amount > 0 else None,
                "currency": asset_currency or None,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat() if end_date else None,
                "active": active,
                "memo": memo if memo else None,
            }
            supabase.table("recurring_orders").insert(payload).execute()
            st.success("정기 매수가 등록되었습니다.")
            st.cache_data.clear()
            st.rerun()

    st.divider()
    st.subheader("등록된 정기 매수")

    existing_rows = (
        supabase.table("recurring_orders")
        .select(
            "id, account_id, asset_id, trade_type, frequency, day_of_month, day_of_week, "
            "timezone, quantity, price, amount, currency, start_date, end_date, active, memo, "
            "created_at, updated_at, assets (ticker, name_kr)"
        )
        .eq("account_id", account_id)
        .order("created_at", desc=True)
        .execute()
        .data or []
    )

    if not existing_rows:
        st.info("등록된 정기 매수가 없습니다.")
        return

    df_orders = pd.DataFrame(existing_rows)
    df_orders["asset_label"] = df_orders["assets"].apply(
        lambda x: f"{(x or {}).get('ticker', '')} | {(x or {}).get('name_kr', '')}".strip(" |")
    )
    st.dataframe(
        df_orders[[
            "id", "asset_label", "frequency", "day_of_month", "day_of_week",
            "quantity", "price", "amount", "currency", "start_date", "end_date", "active", "memo"
        ]],
        width="stretch",
    )

    with st.expander("✏️ 정기 매수 수정/삭제"):
        order_rows = df_orders.to_dict("records")
        order_label_map = {
            r["id"]: f"{r.get('asset_label', '')} | {r.get('frequency')} | id={r['id']}"
            for r in order_rows
        }
        selected_id = st.selectbox(
            "수정/삭제 대상",
            options=[r["id"] for r in order_rows],
            format_func=lambda v: order_label_map.get(v, str(v)),
        )
        selected = next(r for r in order_rows if r["id"] == selected_id)

        edit_frequency = st.selectbox(
            "주기(수정)",
            options=["MONTHLY", "WEEKLY"],
            index=0 if selected["frequency"] == "MONTHLY" else 1,
        )

        edit_day_of_month = selected.get("day_of_month")
        edit_day_of_week = selected.get("day_of_week")
        if edit_frequency == "MONTHLY":
            edit_day_of_month = int(st.number_input(
                "매수일(1~31, 수정)",
                min_value=1,
                max_value=31,
                value=int(edit_day_of_month or 1),
                step=1,
            ))
            edit_day_of_week = None
        else:
            edit_day_of_week = st.selectbox(
                "요일(수정)",
                options=[0, 1, 2, 3, 4, 5, 6],
                index=int(edit_day_of_week or 0),
                format_func=lambda v: ["월", "화", "수", "목", "금", "토", "일"][v],
            )
            edit_day_of_month = None

        edit_timezone = st.text_input("타임존(수정)", value=selected.get("timezone") or "Asia/Seoul")
        edit_quantity = st.number_input(
            "수량(수정, 선택)",
            min_value=0.0,
            value=float(selected.get("quantity") or 0.0),
            step=1.0,
        )
        edit_price = st.number_input(
            "지정가(수정, 선택)",
            min_value=0.0,
            value=float(selected.get("price") or 0.0),
            step=1.0,
        )
        edit_amount = st.number_input(
            "매수금액(수정, 선택)",
            min_value=0.0,
            value=float(selected.get("amount") or 0.0),
            step=1.0,
        )

        edit_start_date = st.date_input(
            "시작일(수정)",
            value=pd.to_datetime(selected.get("start_date") or date.today()).date(),
        )
        edit_use_end = st.checkbox("종료일 사용(수정)", value=selected.get("end_date") is not None)
        edit_end_date = (
            st.date_input(
                "종료일(수정)",
                value=pd.to_datetime(selected.get("end_date") or date.today()).date(),
            )
            if edit_use_end
            else None
        )
        edit_active = st.checkbox("활성화(수정)", value=bool(selected.get("active", True)))
        edit_memo = st.text_input("메모(수정)", value=selected.get("memo") or "")

        col_u, col_d = st.columns(2)
        with col_u:
            update_clicked = st.button("정기 매수 수정", type="primary")
        with col_d:
            delete_clicked = st.button("정기 매수 삭제", type="secondary")

        if update_clicked:
            if edit_quantity <= 0 and edit_amount <= 0:
                st.error("수량 또는 매수금액 중 하나는 0보다 커야 합니다.")
            else:
                payload = {
                    "frequency": edit_frequency,
                    "day_of_month": edit_day_of_month,
                    "day_of_week": edit_day_of_week,
                    "timezone": edit_timezone,
                    "quantity": float(edit_quantity) if edit_quantity > 0 else None,
                    "price": float(edit_price) if edit_price > 0 else None,
                    "amount": float(edit_amount) if edit_amount > 0 else None,
                    "start_date": edit_start_date.isoformat(),
                    "end_date": edit_end_date.isoformat() if edit_end_date else None,
                    "active": edit_active,
                    "memo": edit_memo if edit_memo else None,
                }
                supabase.table("recurring_orders").update(payload).eq("id", selected_id).execute()
                st.success("정기 매수가 수정되었습니다.")
                st.cache_data.clear()
                st.rerun()

        if delete_clicked:
            supabase.table("recurring_orders").delete().eq("id", selected_id).execute()
            st.success("정기 매수가 삭제되었습니다.")
            st.cache_data.clear()
            st.rerun()
