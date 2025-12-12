# 기존 앱의 편집 기능을 구현합니다. st.data_editor를 사용하여 엑셀처럼 편집할 수 있습니다.
import streamlit as st
import pandas as pd
from utils.data_loader import update_data

def show_transaction_editor(df_transactions, lookup_data):
    st.markdown("### 📝 거래 기록 관리")
    st.caption("여기서 데이터를 수정/추가하면 'transactions' 테이블에 반영됩니다.")

    # 룩업 데이터 가져오기
    asset_id_to_name = lookup_data['asset_id_to_name']
    # 📌 UI용 룩업 맵 (ID -> Display Name)
    account_id_to_name_display = lookup_data['account_id_to_name_display']
    # 📌 DB용 룩업 맵 (DB 계좌명 -> ID)
    account_name_to_id_db = lookup_data['account_name_to_id_db']
    trade_types = lookup_data['codes']['trade_types']

    # 📌 [KeyError 해결] transactions 테이블에 account_id가 없으므로,
    # 기존 'account_name' 컬럼을 Display Name으로 변환할 임시 맵핑을 만듭니다.
    
    # 1. DB의 account_name (예: 미래에셋IRP)을 ID로 찾고 -> ID를 Display Name으로 변환
    name_to_display_map = {
        db_name: account_id_to_name_display.get(account_name_to_id_db.get(db_name))
        for db_name in account_name_to_id_db.keys()
    }
    
    if not df_transactions.empty:
        df_transactions['name_kr'] = df_transactions['asset_id'].map(asset_id_to_name)
        # 📌 [KeyError 해결] 기존 account_name (DB 값)을 UI용 Display Name으로 변환
        df_transactions['account_display_name'] = df_transactions['account_name'].map(name_to_display_map)
    else:
        # 빈 프레임 생성 시에도 'name_kr', 'account_display_name' 컬럼 포함
        empty_cols = list(df_transactions.columns) + ['name_kr', 'account_display_name']
        df_transactions = pd.DataFrame(columns=empty_cols)

    # ---------------------------------------------------------
    # 데이터 에디터 설정(드롭다운 적용)
    # ---------------------------------------------------------
    # num_rows="dynamic": 행 추가/삭제 가능하게 설정
    # 선택 가능한 Display Name 리스트
    display_name_options = list(lookup_data['account_name_to_id_display'].keys())
    
    column_config = {
        "name_kr": st.column_config.SelectboxColumn(
            "자산명 (name_kr)",
            options=list(asset_id_to_name.values()),
            required=True,
            width='medium'
        ),
        # 📌 [KeyError 해결] Display Name을 보여주고 드롭다운으로 선택
        "account_display_name": st.column_config.SelectboxColumn(
            "계좌명 (증권사)",
            options=display_name_options, 
            required=True,
            width='medium'
        ),
        "type": st.column_config.SelectboxColumn(
            "거래 유형",
            options=trade_types,
            required=True
        ),
        "transaction_date": st.column_config.DateColumn("거래일"),
        "price": st.column_config.NumberColumn("가격", format="%.2f"),
        "amount": st.column_config.NumberColumn("총액", format="%d"),
        # 기존 account_name 필드와 ID 필드는 숨김
        "account_name": None,
        "asset_id": None, 
        "account_id": None,
        "id": None 
    }

    display_cols = [
        'transaction_date', 'name_kr', 'account_display_name', 'trade_type', 
        'quantity', 'price'
    ]
    
    edited_df = st.data_editor(
        df_transactions[display_cols],
        num_rows="dynamic", 
        # 📌 [Warning 반영] use_container_width=True 대신 width='stretch' 사용
        width='stretch', 
        column_config=column_config,
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