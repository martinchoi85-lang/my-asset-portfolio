import streamlit as st
import pandas as pd
from asset_portfolio.backend.infra import query

def render_account_editor(user_id: str):
    st.subheader("🏦 계좌 관리")
    st.caption("새로운 계좌를 추가하고 관리합니다. 기존 데이터의 안전성을 위해 수정/삭제는 지원하지 않습니다.")

    # 1. 기존 계좌 목록 조회
    accounts = query.get_accounts(user_id)
    if accounts:
        st.markdown("##### 📌 등록된 계좌 목록")
        df = pd.DataFrame(accounts)
        # 필요한 컬럼만 추출 후 이름 변경
        display_cols = ["name", "brokerage", "old_owner", "type"]
        existing_cols = [c for c in display_cols if c in df.columns]
        display_df = df[existing_cols].copy()
        
        rename_map = {
            "name": "전체 계좌명 (자동 생성)",
            "brokerage": "증권사/은행",
            "old_owner": "소유자",
            "type": "계좌 타입"
        }
        display_df.rename(columns=rename_map, inplace=True)
        st.dataframe(display_df, width='stretch', hide_index=True)
    else:
        st.info("등록된 계좌가 없습니다. 하단에서 새 계좌를 추가해주세요.")

    st.divider()

    # 2. 신규 계좌 추가 폼
    st.markdown("##### ➕ 신규 계좌 추가")
    with st.form("add_account_form"):
        st.markdown("**자동 네이밍 규칙**: `[증권사]_[계좌닉네임]_[소유자명]` 형태로 저장됩니다.")
        
        col1, col2 = st.columns(2)
        with col1:
            brokerage = st.text_input("증권사/은행명 *", placeholder="예: 키움증권, NH투자증권")
            owner = st.text_input("소유자명 (old_owner) *", placeholder="예: 홍길동")
        with col2:
            nickname = st.text_input("계좌 닉네임 *", placeholder="예: 연금저축, ISA, 미장")
            acc_type = st.selectbox("계좌 타입", ["STOCK", "CASH", "PENSION", "ETC"])

        submitted = st.form_submit_button("계좌 생성", width='stretch')
        
        if submitted:
            if not brokerage or not owner or not nickname:
                st.error("증권사명, 소유자명, 계좌 닉네임은 필수 입력 항목입니다.")
            else:
                account_name = f"{brokerage.strip()}_{nickname.strip()}_{owner.strip()}"
                
                # 중복 확인
                existing_names = [acc['name'] for acc in accounts]
                if account_name in existing_names:
                    st.error(f"이미 존재하는 계좌 이름입니다: {account_name}")
                else:
                    new_acc = {
                        "user_id": user_id,
                        "name": account_name,
                        "brokerage": brokerage.strip(),
                        "old_owner": owner.strip(),
                        "type": acc_type
                    }
                    query.create_account(new_acc)
                    st.success(f"'{account_name}' 계좌가 성공적으로 생성되었습니다!")
                    st.rerun()
