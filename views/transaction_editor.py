# 기존 앱의 편집 기능을 구현합니다. st.data_editor를 사용하여 엑셀처럼 편집할 수 있습니다.
import streamlit as st
import pandas as pd
from utils.data_loader import update_data, delete_data

def show_transaction_editor(df_transactions):
    st.markdown("### 📝 거래 기록 관리")
    st.caption("여기서 데이터를 수정/추가하면 'transactions' 테이블에 반영됩니다.")

    if df_transactions.empty:
        st.warning("거래 데이터가 없습니다.")
        # 빈 프레임 생성 (새로 추가할 수 있도록)
        df_transactions = pd.DataFrame(columns=[
            'id', 'transaction_date', 'ticker', 'type', 'quantity', 'price', 'amount'
        ])

    # ---------------------------------------------------------
    # 데이터 에디터 설정
    # ---------------------------------------------------------
    # num_rows="dynamic": 행 추가/삭제 가능하게 설정
    edited_df = st.data_editor(
        df_transactions,
        num_rows="dynamic", 
        width='stretch',
        column_config={
            "transaction_date": st.column_config.DateColumn("거래일"),
            "price": st.column_config.NumberColumn("가격", format="%.2f"),
            "amount": st.column_config.NumberColumn("총액", format="%d")
        },
        key="transaction_editor"
    )

    # ---------------------------------------------------------
    # 저장 버튼 로직
    # ---------------------------------------------------------
    col_l, col_r = st.columns([4, 1])
    with col_r:
        if st.button("💾 변경사항 저장", type="primary", width='stretch'):
            # 실제 변경된 데이터만 찾아서 업데이트하는 로직이 이상적이나,
            # 편의상 전체/변경된 행을 업데이트 함수로 넘깁니다.
            # (Streamlit data_editor는 변경된 상태인 edited_df를 바로 반환합니다)
            
            # 주의: 새로 추가된 행은 id가 없을 수 있음. Supabase가 처리하도록 맡기거나 처리 필요.
            if update_data("transactions", edited_df):
                st.rerun() # 저장 후 새로고침