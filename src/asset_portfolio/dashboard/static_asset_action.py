import streamlit as st
import pandas as pd
from asset_portfolio.backend.infra import query
from asset_portfolio.backend.services.transaction_service import TransactionService, CreateTransactionRequest

def render_static_asset_actions(user_id: str):
    st.subheader("💸 만기 / 해지 / 출금 관리")
    st.caption("정적 자산의 전액 출금, 이자 수령, 연결 계좌 이체 등을 간편하게 처리합니다.")

    user_accounts = query.get_accounts(user_id)
    if not user_accounts:
        st.warning("계좌 정보가 없습니다.")
        return

    from asset_portfolio.dashboard.render import render_account_selector
    account_id = render_account_selector(user_accounts)
    if not account_id:
        return

    supabase = query.get_supabase_client()
    latest_query = supabase.table("daily_snapshots").select("date").order("date", desc=True).limit(1)
    
    # 계좌 하나만 선택 가능하므로
    if account_id == "__ALL__":
        st.warning("단일 계좌를 선택해 주세요.")
        return
        
    latest_query = latest_query.eq("account_id", account_id)
    latest_row = latest_query.execute().data or []
    if not latest_row:
        st.info("데이터가 없습니다.")
        return
    latest_date = latest_row[0]["date"]

    rows_query = (
        supabase.table("daily_snapshots")
        .select(
            "date, account_id, asset_id, valuation_amount, purchase_amount, currency, "
            "assets (name_kr, asset_type, price_source, underlying_asset_class)"
        )
        .eq("date", latest_date)
        .eq("account_id", account_id)
    )

    rows = rows_query.execute().data or []
    if not rows:
        st.info("정적 자산 보유 내역이 없습니다.")
        return

    df = pd.json_normalize(rows, sep=".")
    df = df[df["assets.price_source"].fillna("").str.lower().str.strip() == "manual"]
    df["valuation_amount"] = pd.to_numeric(df["valuation_amount"], errors="coerce").fillna(0)
    df = df[df["valuation_amount"] > 0]
    
    if df.empty:
        st.info("현재 보유 중인 정적 자산(잔액 > 0)이 없습니다.")
        return

    from asset_portfolio.backend.services.manual_cost_basis_service import attach_manual_cost_basis
    df = attach_manual_cost_basis(df, user_id=user_id)
    if "manual_principal" not in df.columns:
        df["manual_principal"] = pd.to_numeric(df["purchase_amount"], errors="coerce").fillna(0)
    else:
        df["manual_principal"] = pd.to_numeric(df["manual_principal"], errors="coerce").fillna(0)

    # 자산 선택 드롭다운
    asset_options = []
    for _, row in df.iterrows():
        asset_options.append({
            "label": f"{row['assets.name_kr']} (평가액: {row['valuation_amount']:,.0f} / 원금: {row['manual_principal']:,.0f})",
            "asset_id": row["asset_id"],
            "valuation": row["valuation_amount"],
            "principal": row["manual_principal"]
        })
    
    selected_label = st.selectbox("출금할 자산 선택", [opts["label"] for opts in asset_options])
    selected_asset = next(opts for opts in asset_options if opts["label"] == selected_label)
    
    st.divider()
    
    with st.form("static_asset_withdrawal_form"):
        import datetime
        action_date = st.date_input("출금 일자", value=datetime.date.today())
        
        col1, col2 = st.columns(2)
        with col1:
            withdraw_principal = st.number_input("출금 원금", min_value=0.0, max_value=float(selected_asset["principal"]), value=float(selected_asset["principal"]), step=1000.0)
        with col2:
            interest_income = st.number_input("이자/수익 수령액", min_value=0.0, value=float(selected_asset["valuation"] - selected_asset["principal"]) if selected_asset["valuation"] > selected_asset["principal"] else 0.0, step=1000.0)
            
        auto_cash = st.checkbox("현금 계좌(예수금)로 자동 입금 처리", value=True)
        is_full_termination = st.checkbox("전액 해지 (잔고 0원 처리)", value=True, help="체크 시, 입력한 출금 금액과 상관없이 남은 스냅샷 잔액(평가액)을 모두 팔아 0원으로 정리합니다.")
        memo = st.text_input("메모", value="정적 자산 출금/해지")
        
        submitted = st.form_submit_button("출금 / 만기 해지 실행")
        
        if submitted:
            if withdraw_principal <= 0 and interest_income <= 0:
                st.error("출금할 금액을 입력해주세요.")
            else:
                try:
                    # 1. 원금 상환 / 해지 매도 기록 (SELL)
                    sell_qty = float(selected_asset["valuation"]) if is_full_termination else withdraw_principal
                    
                    if sell_qty > 0:
                        req_sell = CreateTransactionRequest(
                            account_id=account_id,
                            asset_id=selected_asset["asset_id"],
                            transaction_date=action_date,
                            trade_type="SELL",
                            quantity=sell_qty,
                            price=1.0,  # 수동자산은 단가 1.0 취급 (잔액 베이스)
                            memo=memo
                        )
                        # 여기서는 현금 입금을 아래에서 별도 DEPOSIT로 직접 꽂아주므로 auto_cash=False
                        TransactionService.create_transaction_and_rebuild(req=req_sell, auto_cash=False)
                        
                        # 수동 자산 원금(cost basis) 차감 이벤트 기록 (0.0은 DB 제약조건 위반이므로 필터링)
                        from asset_portfolio.backend.services.manual_cost_basis_service import record_cost_basis_events
                        
                        delta_amount = -float(selected_asset["principal"]) if is_full_termination else -withdraw_principal
                        if abs(delta_amount) > 0.001:
                            record_cost_basis_events(user_id, [{
                                "account_id": account_id,
                                "asset_id": selected_asset["asset_id"],
                                "event_date": action_date.isoformat(),
                                "delta_amount": delta_amount,
                                "currency": "KRW",
                                "reason": "withdrawal",
                                "memo": memo
                            }])
                        
                    # 2. 이자 수령 및 총액 예수금 입금
                    if auto_cash:
                        total_cash_received = withdraw_principal + interest_income
                        if total_cash_received > 0:
                            cash_asset_id = TransactionService._get_cash_asset_id_by_currency("KRW")
                            req_interest = CreateTransactionRequest(
                                account_id=account_id,
                                asset_id=cash_asset_id,
                                transaction_date=action_date,
                                trade_type="DEPOSIT",
                                quantity=total_cash_received,
                                price=1.0,
                                memo=f"출금/수령 합계: {memo}"
                            )
                            TransactionService.create_transaction_and_rebuild(req=req_interest, auto_cash=False) # 이미 cash이므로 미러링 안함
                            
                    st.success("출금 및 만기 처리가 완료되었습니다.")
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"처리 중 오류 발생: {e}")
