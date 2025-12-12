#'자산 요약 편집' 또는 '자산 정보 관리' 기능을 담당합니다.
import streamlit as st
from utils.data_loader import update_data

def show_asset_editor(df_assets):
    st.markdown("### 💼 자산 정보(Assets) 관리")
    st.caption("종목명(한글), 티커 등 기초 자산 정보를 관리합니다.")

    edited_df = st.data_editor(
        df_assets,
        num_rows="dynamic",
        width='stretch',
        key="asset_editor"
    )

    if st.button("💾 자산 정보 저장", key="save_assets"):
        if update_data("assets", edited_df):
            st.rerun()