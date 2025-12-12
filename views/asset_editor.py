#'자산 요약 편집' 또는 '자산 정보 관리' 기능을 담당합니다.
import streamlit as st
import pandas as pd
from utils.data_loader import update_data, fetch_current_prices

def show_asset_editor(df_assets_original, lookup_data):
    st.markdown("### 💼 자산 정보(Assets) 관리")
    st.caption("종목명, 티커, 유형 등 기초 자산 정보를 관리합니다. '현재가 업데이트' 버튼으로 라이브 가격을 조회할 수 있습니다.")
    
    # 📌 [1번 요청 반영] 현재가 업데이트 요청 상태를 세션에 저장
    if 'current_prices_fetched' not in st.session_state:
        st.session_state['current_prices_fetched'] = False # 초기값: 미실행

    df_assets_to_edit = df_assets_original.copy()
    
    # 📌 '현재가 업데이트' 버튼이 눌렸을 때만 fetch_current_prices 호출
    if st.session_state['current_prices_fetched']:
        st.info("현재가를 조회하여 테이블에 반영합니다.")
        df_assets_to_edit = fetch_current_prices(df_assets_to_edit)
    else:
        st.info("현재가는 저장된 값(혹은 Null)을 사용합니다. 최신 정보를 원하면 버튼을 눌러주세요.")

    # 룩업 데이터 (코드 -> 한글 맵)
    code_to_kr = lookup_data['codes']['code_map']

    # 📌 편집을 위해 코드 -> 한글로 변환
    if not df_assets_to_edit.empty:
        # DB 컬럼을 한글 컬럼으로 변환 (편집용)
        df_assets_to_edit['asset_type_kr'] = df_assets_to_edit['asset_type'].map(code_to_kr['asset_type']).fillna(df_assets_to_edit['asset_type'])
        df_assets_to_edit['currency_kr'] = df_assets_to_edit['currency'].map(code_to_kr['currency']).fillna(df_assets_to_edit['currency'])
        df_assets_to_edit['market_kr'] = df_assets_to_edit['market'].map(code_to_kr['market']).fillna(df_assets_to_edit['market'])
    else:
        # 빈 프레임 생성 시에도 한글 컬럼 포함
        df_assets_to_edit = pd.DataFrame(columns=list(df_assets_original.columns) + ['asset_type_kr', 'currency_kr', 'market_kr'])

    # 데이터 에디터 설정 (드롭다운 + 한글 적용)
    # 📌 [2-2번 요청 반영] column_config를 사용하여 드롭다운 설정
    column_config = {
        "id": None, # PK 숨김
        "name_kr": st.column_config.TextColumn("종목명 (한글)", required=True),
        "ticker": st.column_config.TextColumn("티커"),
        
        # 📌 [2-1 요청 반영] 현재가 (읽기 전용, yfinance로 업데이트됨)
        "current_price": st.column_config.NumberColumn("현재가", format="%.2f", disabled=True), 
        
        # 📌 한글 컬럼을 Selectbox로 표시 (DB 저장 시 update_data에서 코드로 역변환)
        "asset_type_kr": st.column_config.SelectboxColumn(
            "자산 유형",
            options=lookup_data['codes']['asset_types'], # 한글 옵션
            required=True
        ),
        "currency_kr": st.column_config.SelectboxColumn(
            "통화",
            options=lookup_data['codes']['currencies'], # 한글 옵션
            required=True
        ),
        "market_kr": st.column_config.SelectboxColumn(
            "시장",
            options=lookup_data['codes']['markets'] # 한글 옵션
        ),
        # 원본 DB 컬럼은 숨김
        "asset_type": None, 
        "currency": None,
        "market": None
    }
    
    display_cols = ['name_kr', 'ticker', 'asset_type_kr', 'currency_kr', 'market_kr', 'current_price']
    
    edited_df = st.data_editor(
        df_assets_to_edit[display_cols],
        num_rows="dynamic",
        width='stretch',
        column_config=column_config,
        key="asset_editor"
    )

    col1, col2 = st.columns([1, 4])
    with col1:
        # 📌 버튼 클릭 시 상태 변경 및 재실행
        if st.button("🔄 현재가 업데이트", key="update_price", width='stretch', type='secondary'):
            st.session_state['current_prices_fetched'] = True
            st.rerun() 
    with col2:
        if st.button("💾 자산 정보 저장", key="save_assets", type="primary", width='stretch'):
            # 저장 시에는 'fetch_prices_requested' 상태를 초기화하지 않음 (다시 로드될 때 이전 가격 사용)
            if update_data("assets", edited_df):
                st.rerun()