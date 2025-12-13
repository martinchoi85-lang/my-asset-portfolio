# 기존 앱의 편집 기능을 구현합니다. st.data_editor를 사용하여 엑셀처럼 편집할 수 있습니다.
import streamlit as st
import pandas as pd
from utils.data_loader import update_data#, delete_data

def show_transaction_editor(df_transactions, lookup_data):
    st.markdown("### 📝 거래 기록 관리")
    st.caption("여기서 데이터를 수정/추가/삭제하면 'transactions' 테이블에 반영됩니다.")

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

    # 📌 [추가] 삭제용 체크박스 컬럼 추가
    df_transactions.insert(0, '선택', False)

    # ---------------------------------------------------------
    # 데이터 에디터 설정(드롭다운 적용)
    # ---------------------------------------------------------
    # 선택 가능한 Display Name 리스트
    display_name_options = list(lookup_data['account_name_to_id_display'].keys())
    
    column_config = {
        # 📌 [추가] 체크박스 컬럼 설정 (width 제거로 자동 크기 조정)
        "선택": st.column_config.CheckboxColumn(
            "☑",  # 짧은 헤더로 변경
            help="삭제할 거래를 선택하세요",
            # width="small",   ############################## 값을 지정해서 넣을 수는 없는지?
            default=False
        ),
        "transaction_date": st.column_config.DateColumn("거래일", required=True),
        "name_kr": st.column_config.SelectboxColumn(
            "자산명",
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
        "trade_type": st.column_config.SelectboxColumn(
            "거래 유형",
            options=trade_types,
            required=True,
            width="small"
        ),
        "quantity": st.column_config.NumberColumn(
            "수량",
            format="%.2f",
            width="small"
        ),
        "price": st.column_config.NumberColumn(
            "가격",
            format="%.2f",
            width="small"
        ),
        # 기존 account_name 필드와 ID 필드는 숨김
        "account_name": None,
        "asset_id": None, 
        "account_id": None,
        "id": None 
    }

    display_cols = [
        '선택', 'transaction_date', 'name_kr', 'account_display_name', 
        'trade_type', 'quantity', 'price'
    ]

    # 날짜 기준 내림차순 정렬 (최신 거래가 위로)
    df_transactions = df_transactions.sort_values(by='transaction_date', ascending=False)
    
    # 📌 [중요] 정렬 후 인덱스를 Range Index로 리셋 (경고 해결)
    df_transactions = df_transactions.reset_index(drop=True)

    # 📌 슬라이더로 행 수 선택
    rows_num = st.slider(
        "표시할 테이블 행 수",
        min_value=20,
        max_value=min(100, len(df_transactions)) if len(df_transactions) > 20 else 20,
        value=20,
        step=5,
        key='transaction_rows_slider'
    )

    # 📌 높이 계산
    calculated_height = min(35 * rows_num + 38, 2000)

    # 📌 테이블 표시
    edited_df = st.data_editor(
        df_transactions[display_cols],
        num_rows="dynamic", 
        height=calculated_height,
        width='stretch',
        hide_index=True,  # 이제 정상 작동!
        column_config=column_config,
        key="transaction_editor"
    )

    # ---------------------------------------------------------
    # 📌 [추가] 버튼 영역 (삭제 + 저장)
    # ---------------------------------------------------------
    col_info, col_delete, col_save = st.columns([2, 1, 1])
    
    with col_info:
        selected_count = edited_df['선택'].sum()
        if selected_count > 0:
            st.info(f"📌 {int(selected_count)}개 거래 선택됨")
    
    with col_delete:
        if st.button("🗑️ 선택 삭제", type="secondary", width='stretch'):
            # 체크된 행 찾기
            rows_to_delete = edited_df[edited_df['선택'] == True]
            
            if len(rows_to_delete) > 0:
                # 원본 df에서 id 찾기 (인덱스로 매칭)
                delete_indices = rows_to_delete.index
                ids_to_delete = df_transactions.loc[delete_indices, 'id'].dropna().tolist()
                
                # DB 삭제
                success_count = 0
                for del_id in ids_to_delete:
                    if delete_data("transactions", int(del_id)):
                        success_count += 1
                
                st.success(f"✅ {success_count}개 거래가 삭제되었습니다.")
                st.rerun()
            else:
                st.warning("⚠️ 삭제할 거래를 선택해주세요.")
    
    with col_save:
        if st.button("💾 변경사항 저장", type="primary", width='stretch'):
            # '선택' 컬럼 제거
            save_df = edited_df.drop(columns=['선택'])
            
            # 원본 df의 id, asset_id, account_name 컬럼 복원
            if 'id' in df_transactions.columns:
                save_df['id'] = df_transactions['id']
            
            # 📌 [중요] Display Name -> DB의 account_name으로 역변환
            display_to_name_map = {v: k for k, v in name_to_display_map.items()}
            save_df['account_name'] = save_df['account_display_name'].map(display_to_name_map)
            
            # 📌 [중요] 자산명(한글) -> asset_id로 역변환
            name_to_asset_id = {v: k for k, v in asset_id_to_name.items()}
            save_df['asset_id'] = save_df['name_kr'].map(name_to_asset_id)
            
            # UI용 컬럼 제거
            save_df = save_df.drop(columns=['name_kr', 'account_display_name'])
            
            # DB 저장
            if update_data("transactions", save_df):
                st.success("✅ 변경사항이 저장되었습니다.")
                st.rerun()
            else:
                st.error("❌ 저장 중 오류가 발생했습니다.")