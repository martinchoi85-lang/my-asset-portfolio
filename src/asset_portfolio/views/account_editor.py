# views/account_editor.py (수정 버전)
import streamlit as st
import pandas as pd
from utils.data_loader import update_data

def show_account_editor(df_accounts, lookup_data):
    st.markdown("### 🏦 계좌 정보 관리")
    st.caption("새로운 계좌를 추가하거나 기존 계좌 정보를 수정합니다.")

    codes = lookup_data['codes']
    
    # 📌 [3-1 요청 반영] df_accounts가 비어있는 경우, 새 행 추가를 위해 컬럼을 확보
    if df_accounts.empty:
        # accounts 테이블의 필수 컬럼을 기준으로 빈 DataFrame 생성
        df_accounts = pd.DataFrame(columns=['id', 'name', 'brokerage', 'owner', 'type'])

    # ---------------------------------------------------------
    # 데이터 에디터 설정 (드롭다운 적용)
    # ---------------------------------------------------------
    
    column_config = {
        "id": None, # PK 숨김
        "name": st.column_config.TextColumn("계좌명", required=True, width='medium'),
        "brokerage": st.column_config.TextColumn("증권사", required=True),
        "owner": st.column_config.SelectboxColumn(
            "소유자",
            options=codes['account_owners'], # 예: ["승엽", "민희"]
            required=True
        ),
        "type": st.column_config.SelectboxColumn(
            "계좌 유형",
            options=codes['account_types'], # 예: ["일반", "DC", "IRP", "연금저축"]
            required=True
        )
    }

    display_cols = [c for c in df_accounts.columns if c != 'id']

    row_count = len(df_accounts)
    calculated_height = min(35 * row_count + 38, 2000)  # 최대 2000px

    edited_df = st.data_editor(
        df_accounts[display_cols].sort_values(by=['owner', 'brokerage', 'name'], ascending=[False, True, True]),
        num_rows="dynamic",
        height=calculated_height,
        width='stretch',
        # hide_index=True,
        column_config=column_config,
        key="account_editor"
    )

    if st.button("💾 계좌 정보 저장", key="save_accounts", type="primary", width='stretch'):
        if update_data("accounts", edited_df):
            st.rerun()