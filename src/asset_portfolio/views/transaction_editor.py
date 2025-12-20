# 기존 앱의 편집 기능을 구현합니다. st.data_editor를 사용하여 엑셀처럼 편집할 수 있습니다.
import streamlit as st
import pandas as pd
from utils.data_loader import update_data, delete_data

def show_transaction_editor(df_transactions, lookup_data):
    st.markdown("### 📝 거래 기록 관리")
    st.caption("여기서 데이터를 수정/추가/삭제하면 'transactions' 테이블에 반영됩니다.")

    # 룩업 데이터 가져오기
    asset_id_to_name = lookup_data['asset_id_to_name']
    account_id_to_name_display = lookup_data['account_id_to_name_display']
    account_name_to_id_db = lookup_data['account_name_to_id_db']
    trade_types = lookup_data['codes']['trade_types']

    # 📌 계좌명 매칭 (DB의 account_name -> Display Name)
    name_to_display_map = {
        db_name: account_id_to_name_display.get(account_name_to_id_db.get(db_name))
        for db_name in account_name_to_id_db.keys()
    }
    
    # 📌 [2번 요청] assets 테이블 로드 (평균 매수가 계산용)
    df_assets = pd.DataFrame()
    if 'asset_lookup_df' in lookup_data:
        df_assets = lookup_data['asset_lookup_df']
    
    if not df_transactions.empty:
        df_transactions['name_kr'] = df_transactions['asset_id'].map(asset_id_to_name)
        df_transactions['account_display_name'] = df_transactions['account_name'].map(name_to_display_map)
        
        # 📌 [2번 요청] 손익금과 손익률 계산
        # SELL 거래의 경우: (매도가 - 평균매수가) × 수량
        # BUY 거래의 경우: null
        
        # 먼저 각 자산의 평균 매수가를 계산 (해당 계좌의 BUY 거래만 사용)
        buy_transactions = df_transactions[df_transactions['trade_type'] == 'BUY'].copy()
        
        if not buy_transactions.empty:
            # 📌 [Warning 해결] apply 대신 agg 사용 (더 빠르고 안전함)
            # 계좌별-자산별 평균 매수가 계산: (가격 × 수량의 합) / 수량의 합
            avg_prices = buy_transactions.groupby(['account_name', 'asset_id']).agg(
                total_cost=('price', lambda x: (buy_transactions.loc[x.index, 'price'] * 
                                               buy_transactions.loc[x.index, 'quantity']).sum()),
                total_quantity=('quantity', 'sum')
            ).reset_index()
            
            avg_prices['avg_purchase_price'] = avg_prices['total_cost'] / avg_prices['total_quantity']
            avg_prices = avg_prices[['account_name', 'asset_id', 'avg_purchase_price']]
            
            # 원본 df에 merge
            df_transactions = df_transactions.merge(
                avg_prices, 
                on=['account_name', 'asset_id'], 
                how='left'
            )
        else:
            df_transactions['avg_purchase_price'] = None
        
        # 📌 realized_pnl 계산 (SELL인 경우만)
        df_transactions['calculated_pnl'] = df_transactions.apply(
            lambda row: (row['price'] - row['avg_purchase_price']) * row['quantity'] 
            if row['trade_type'] == 'SELL' and pd.notna(row['avg_purchase_price'])
            else None,
            axis=1
        )
        
        # 📌 realized_return_rate 계산 (SELL인 경우만)
        df_transactions['calculated_return_rate'] = df_transactions.apply(
            lambda row: ((row['price'] - row['avg_purchase_price']) / row['avg_purchase_price'] * 100)
            if row['trade_type'] == 'SELL' and pd.notna(row['avg_purchase_price']) and row['avg_purchase_price'] != 0
            else None,
            axis=1
        )
        
        # 📌 DB에 저장된 realized_pnl이 있으면 그것을 우선 사용
        if 'realized_pnl' in df_transactions.columns:
            df_transactions['calculated_pnl'] = df_transactions['realized_pnl'].combine_first(df_transactions['calculated_pnl'])
        
    else:
        # 빈 프레임 생성 시에도 필요한 컬럼 포함
        empty_cols = list(df_transactions.columns) + ['name_kr', 'account_display_name', 'calculated_pnl', 'calculated_return_rate']
        df_transactions = pd.DataFrame(columns=empty_cols)

    # 📌 삭제용 체크박스 컬럼 추가
    df_transactions.insert(0, '선택', False)

    # ---------------------------------------------------------
    # 데이터 에디터 설정(드롭다운 적용)
    # ---------------------------------------------------------
    display_name_options = list(lookup_data['account_name_to_id_display'].keys())
    
    column_config = {
        "선택": st.column_config.CheckboxColumn(
            "☑",
            help="삭제할 거래를 선택하세요",
            default=False
        ),
        "transaction_date": st.column_config.DateColumn("거래일", required=True),
        "name_kr": st.column_config.SelectboxColumn(
            "자산명",
            options=list(asset_id_to_name.values()),
            required=True,
            width='medium'
        ),
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
            format="%.0f",
            width="small"
        ),
        # 📌 [2번 요청] 손익금과 손익률 컬럼 추가 (읽기 전용)
        "calculated_pnl": st.column_config.NumberColumn(
            "손익금",
            format="%.0f 원",
            help="SELL 거래의 경우 실현 손익",
            disabled=True  # 읽기 전용
        ),
        "calculated_return_rate": st.column_config.NumberColumn(
            "손익률",
            format="%.2f %%",
            help="SELL 거래의 경우 실현 수익률",
            disabled=True  # 읽기 전용
        ),
        # 기존 필드 숨김
        "account_name": None,
        "asset_id": None, 
        "account_id": None,
        "id": None,
        "avg_purchase_price": None,  # 계산용 필드 숨김
        "realized_pnl": None  # DB 필드 숨김
    }

    display_cols = [
        '선택', 'transaction_date', 'name_kr', 'account_display_name', 
        'trade_type', 'quantity', 'price', 'calculated_pnl', 'calculated_return_rate'
    ]

    # 날짜 기준 내림차순 정렬 (최신 거래가 위로)
    df_transactions = df_transactions.sort_values(by='transaction_date', ascending=False)
    df_transactions = df_transactions.reset_index(drop=True)

    # 📌 슬라이더로 행 수 선택
    rows_num = st.slider(
        "표시할 테이블 행 수",
        min_value=10,
        max_value=min(100, len(df_transactions)) if len(df_transactions) > 20 else 20,
        value=10,
        step=5,
        key='transaction_rows_slider'
    )

    calculated_height = min(35 * rows_num + 38, 2000)

    # 📌 [2번 요청] 조건부 스타일링 함수
    def highlight_pnl_transaction(row):
        """손익금과 손익률 컬럼을 양수/음수에 따라 빨강/파랑으로 표시"""
        styles = [''] * len(row)
        
        pnl_val = row.get('calculated_pnl')
        
        if pd.notna(pnl_val):
            color = 'red' if pnl_val >= 0 else 'blue'
            
            # calculated_pnl 컬럼에 색상 적용
            if 'calculated_pnl' in row.index:
                styles[row.index.get_loc('calculated_pnl')] = f'color: {color}'
            
            # calculated_return_rate 컬럼에 색상 적용
            if 'calculated_return_rate' in row.index:
                styles[row.index.get_loc('calculated_return_rate')] = f'color: {color}'
        
        return styles

    # 📌 스타일 적용된 DataFrame 생성
    df_display = df_transactions[display_cols].copy()
    styled_df = df_display.style.apply(highlight_pnl_transaction, axis=1)

    # 📌 테이블 표시 (styled DataFrame 사용)
    # ⚠️ st.data_editor는 style을 지원하지 않으므로, 두 가지 방법 중 선택:
    # 방법 1: 편집 기능 우선 (색상 없음)
    # 방법 2: 색상 우선 (편집 불가)
    
    # 📌 [해결책] 색상과 편집 기능을 모두 지원하기 위해 HTML/CSS 사용
    # 하지만 st.data_editor는 CSS를 직접 지원하지 않으므로, 
    # 편집 기능을 유지하면서 색상을 표시하려면 커스텀 컴포넌트가 필요합니다.
    
    # 📌 [임시 해결책] 편집 가능한 테이블 표시 (색상은 별도 안내)
    edited_df = st.data_editor(
        df_display,
        num_rows="dynamic", 
        height=calculated_height,
        width='stretch',
        column_config=column_config,
        key="transaction_editor"
    )
    
    # 📌 [2번 요청] 색상 표시를 위한 추가 정보
    st.caption("💡 손익금/손익률: 빨강(+), 파랑(-) / SELL 거래만 표시됨")

    # ---------------------------------------------------------
    # 📌 버튼 영역 (삭제 + 저장)
    # ---------------------------------------------------------
    col_info, col_delete, col_save = st.columns([2, 1, 1])
    
    with col_info:
        selected_count = edited_df['선택'].sum()
        if selected_count > 0:
            st.info(f"📌 {int(selected_count)}개 거래 선택됨")
    
    with col_delete:
        if st.button("🗑️ 선택 삭제", type="secondary", key='delete_transactions'):
            rows_to_delete = edited_df[edited_df['선택'] == True]
            
            if len(rows_to_delete) > 0:
                delete_indices = rows_to_delete.index
                ids_to_delete = df_transactions.loc[delete_indices, 'id'].dropna().tolist()
                
                success_count = 0
                for del_id in ids_to_delete:
                    if delete_data("transactions", int(del_id)):
                        success_count += 1
                
                st.success(f"✅ {success_count}개 거래가 삭제되었습니다.")
                st.rerun()
            else:
                st.warning("⚠️ 삭제할 거래를 선택해주세요.")
    
    with col_save:
        if st.button("💾 변경사항 저장", type="primary", key='save_transactions'):
            # '선택' 컬럼과 계산된 컬럼 제거
            save_df = edited_df.drop(columns=['선택', 'calculated_pnl', 'calculated_return_rate'], errors='ignore')
            
            # 📌 [핵심 수정] 원본 df의 id를 인덱스 기반으로 매칭
            # st.data_editor는 원본 DataFrame의 인덱스를 유지하므로,
            # edited_df의 인덱스를 사용하여 df_transactions의 id를 가져옵니다.
            if 'id' in df_transactions.columns:
                # 방법 1: 인덱스 기반 매칭 (안전)
                save_df['id'] = save_df.index.map(
                    lambda idx: df_transactions.loc[idx, 'id'] if idx in df_transactions.index else None
                )
            else:
                save_df['id'] = None
            
            # 📌 Display Name -> DB의 account_name으로 역변환
            display_to_name_map = {v: k for k, v in name_to_display_map.items()}
            save_df['account_name'] = save_df['account_display_name'].map(display_to_name_map)
            
            # 📌 자산명(한글) -> asset_id로 역변환
            name_to_asset_id = {v: k for k, v in asset_id_to_name.items()}
            save_df['asset_id'] = save_df['name_kr'].map(name_to_asset_id)
            
            # UI용 컬럼 제거
            save_df = save_df.drop(columns=['name_kr', 'account_display_name'], errors='ignore')
            
            # 📌 [추가] 숫자형 컬럼들의 NaN 처리 및 Inf 처리
            # (update_data에서 처리하지만 여기서 1차적으로 정리하면 더 안전함)
            cols_to_numeric = ['quantity', 'price', 'asset_id']
            for col in cols_to_numeric:
                if col in save_df.columns:
                    save_df[col] = pd.to_numeric(save_df[col], errors='coerce')
            
            # DB 저장
            if update_data("transactions", save_df):
                st.rerun()