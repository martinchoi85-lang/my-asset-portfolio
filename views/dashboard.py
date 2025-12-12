# 대시보드: 시각화 & 포맷팅)
# 핵심 변경점:
# 1)숫자 포맷팅: st.column_config를 사용하여 숫자가 문자로 깨지는 문제 해결.
# 2)모바일 최적화: width='stretch' 및 중요 컬럼 위주 표시.
# views/dashboard.py (수정 버전)
import streamlit as st
import pandas as pd
import altair as alt

def show_dashboard(asset_summary_df, usd_rate):
    st.markdown("### 📊 포트폴리오 대시보드")
    
    if asset_summary_df.empty:
        st.info("데이터가 없습니다. 거래 기록을 먼저 입력해주세요.")
        return

    # ---------------------------------------------------------
    # 1. KPI 지표 (필드명 수정 반영)
    # ---------------------------------------------------------
    total_val = asset_summary_df['total_valuation_amount'].sum()
    # [필드명 수정] total_invested_amount -> total_purchase_amount
    total_invest = asset_summary_df['total_purchase_amount'].sum() 
    total_pnl = total_val - total_invest
    total_pnl_rate = (total_pnl / total_invest * 100) if total_invest > 0 else 0

    col1, col2, col3 = st.columns(3)
    col1.metric("총 평가 금액", f"{total_val:,.0f} 원", delta=f"{total_pnl:,.0f} 원")
    col2.metric("총 수익률", f"{total_pnl_rate:.2f} %")
    col3.metric("환율 (USD)", f"{usd_rate:,.2f} 원")

    st.divider()

    # ---------------------------------------------------------
    # 2. 보유 자산 상세 테이블 (필드명 및 포맷팅 수정)
    # ---------------------------------------------------------
    st.subheader("📌 보유 종목 현황")
    
    column_config = {
        "name_kr": st.column_config.TextColumn("종목명"),
        "ticker": st.column_config.TextColumn("티커"),
        "total_quantity": st.column_config.NumberColumn("보유수량", format="%.2f"),
        "average_price": st.column_config.NumberColumn("평단가", format="%.0f"),
        "current_price": st.column_config.NumberColumn("현재가", format="%.2f"),
        "total_valuation_amount": st.column_config.NumberColumn("평가금액", format="%d 원"),
        # [필드명 수정]
        "total_purchase_amount": st.column_config.NumberColumn("매수금액", format="%d 원"),
        "unrealized_pnl": st.column_config.NumberColumn("평가손익", format="%d 원"),
        # [필드명 수정]
        "unrealized_return_rate": st.column_config.NumberColumn("수익률", format="%.2f %%"),
    }

    display_columns = [
        "name_kr", "ticker", "total_quantity", 
        "total_valuation_amount", "unrealized_return_rate" # 필드명 수정 반영
    ]
    
    final_cols = [c for c in display_columns if c in asset_summary_df.columns]

    st.dataframe(
        asset_summary_df[final_cols],
        column_config=column_config,
        use_container_width=True,
        hide_index=True
    )

    # ---------------------------------------------------------
    # 3. 자산 비중 차트 (asset_type 기준으로 변경)
    # ---------------------------------------------------------
    st.subheader("📈 자산 유형별 비중")
    
    # [차트 기준 변경] asset_summary_df는 종목별 현황이므로,
    # asset_type으로 그룹화하여 합산해야 합니다.
    if 'asset_type' in asset_summary_df.columns:
        # asset_type별로 total_valuation_amount를 합산합니다.
        grouped_df = asset_summary_df.groupby('asset_type', dropna=True).agg(
            total_valuation_amount=('total_valuation_amount', 'sum')
        ).reset_index()

        base = alt.Chart(grouped_df).encode(
            theta=alt.Theta("total_valuation_amount", stack=True),
            color=alt.Color("asset_type", legend=alt.Legend(orient="bottom", columns=3)),
            # 툴팁에 자산 유형과 금액을 표시
            tooltip=["asset_type", alt.Tooltip("total_valuation_amount", format=",.0f")]
        )
        pie = base.mark_arc(outerRadius=100)
        
        st.altair_chart(pie, use_container_width=True)
    else:
        st.warning("`asset_summary` 뷰에 `asset_type` 컬럼이 없어 차트를 그릴 수 없습니다.")