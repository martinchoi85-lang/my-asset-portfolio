# 대시보드: 시각화 & 포맷팅)
# 핵심 변경점:
# 1)숫자 포맷팅: st.column_config를 사용하여 숫자가 문자로 깨지는 문제 해결.
# 2)모바일 최적화: width='stretch' 및 중요 컬럼 위주 표시.
# views/dashboard.py (수정 버전)
import streamlit as st
import pandas as pd
import altair as alt

# 조건부 스타일링 함수 정의
def highlight_pnl(s):
    """평가손익과 수익률이 양수/음수에 따라 색상을 적용합니다."""
    # 평가손익 (unrealized_pnl)과 수익률 (unrealized_return_rate) 컬럼을 찾아 색상을 결정
    styles = [''] * len(s)
    
    # 양수(0 포함)는 빨간색 (주식 수익률 관행), 음수는 노란색(혹은 파란색 계열)을 사용합니다.
    # 사용자의 요청에 따라 양수(>=0)는 빨간색, 음수(<0)는 노란색으로 처리합니다.
    pnl_val = s.get('unrealized_pnl')
    rate_val = s.get('unrealized_return_rate')
    
    color = ''
    if pnl_val is not None:
        # 📌 [3번 요청 반영] 양수(>=0)는 빨간색, 음수(<0)는 파란색으로 처리
        color = 'red' if pnl_val >= 0 else 'blue'
    
    # 맵핑할 컬럼에만 스타일을 적용
    if 'unrealized_pnl' in s.index:
        styles[s.index.get_loc('unrealized_pnl')] = f'color: {color}'
    if 'unrealized_return_rate' in s.index:
        styles[s.index.get_loc('unrealized_return_rate')] = f'color: {color}'
        
    return styles


def show_dashboard(asset_summary_df, usd_rate, lookup_data):
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
    # 2. 보유 자산 상세 테이블 (필드명 및 포맷팅 수정, 접기/펼치기 및 스타일링 적용)
    # ---------------------------------------------------------
    st.subheader("📌 보유 종목 현황")
    # 📌 [1번 요청 반영] st.expander를 사용하여 테이블을 감싸고, 기본은 닫힌 상태로 둠
    # rows가 길면 스크롤이 되지만, expander를 통해 전체를 펼쳐 볼 수 있음.
    
    df = asset_summary_df.copy()

    column_config = {
        "name_kr": st.column_config.TextColumn("종목명"),
        "ticker": st.column_config.TextColumn("티커"),
        "total_quantity": st.column_config.NumberColumn("보유수량", format="%.2f"),
        "average_price": st.column_config.NumberColumn("평단가", format="%.0f"),
        "current_price": st.column_config.NumberColumn("현재가", format="%.2f"),
        "total_valuation_amount": st.column_config.NumberColumn("평가금액", format="%d 원"),
        "total_purchase_amount": st.column_config.NumberColumn("매수금액", format="%d 원"),
        "unrealized_pnl": st.column_config.NumberColumn("평가손익", format="%d 원"),
        "unrealized_return_rate": st.column_config.NumberColumn("수익률", format="%.2f %%"),
    }

    display_columns = [
        "name_kr", "ticker", "total_quantity", 
        "total_valuation_amount", "unrealized_pnl", "unrealized_return_rate"
    ]
    
    final_cols = [c for c in display_columns if c in asset_summary_df.columns]

# 📌 [2번 요청 반영] session_state를 이용한 테이블 펼치기/접기 상태 관리
    if 'dashboard_table_expanded' not in st.session_state:
        st.session_state['dashboard_table_expanded'] = True # 기본값: 펼침

    is_expanded = st.session_state['dashboard_table_expanded']

    # 조건부 스타일링 적용 (Read-Only 상태)
    # df.style을 적용하기 위해 컬럼을 먼저 선택하고, NaN 값 처리
    styled_df_data = df[final_cols].fillna(0)
    styled_df = styled_df_data.style.apply(highlight_pnl, axis=1)

    # 📌 [에러 수정 및 2번 요청 반영] 펼침/접힘 상태에 따라 height 인자를 다르게 전달
    if is_expanded:
        button_label = "테이블 접기 (스크롤 모드로 전환)"
        # 펼침 상태: height 인자를 완전히 생략하여 모든 행을 표시 (에러 방지)
        st.dataframe(
            styled_df, # 스타일링 적용된 DF
            column_config=column_config,
            width='stretch',
            hide_index=True
        )
    else:
        button_label = "테이블 펼치기 (전체 행 표시)"
        # 접힘 상태: height 인자에 픽셀 값을 전달하여 스크롤 가능하게 함
        st.dataframe(
            styled_df, # 스타일링 적용된 DF
            column_config=column_config,
            width='stretch',
            height=300, # 접힘: 스크롤 가능한 높이로 제한
            hide_index=True
        )
    
    # 📌 상태 토글 버튼
    if st.button(button_label, key='dashboard_table_toggle', type='secondary', width='stretch'):
        st.session_state['dashboard_table_expanded'] = not is_expanded
        st.rerun()

    # ---------------------------------------------------------
    # 3. 자산 비중 차트 (asset_type 기준으로 변경)
    # ---------------------------------------------------------
    st.subheader("📈 자산 유형별 비중")
    
    if 'asset_type' in asset_summary_df.columns:
        grouped_df = asset_summary_df.groupby('asset_type', dropna=True).agg(
            total_valuation_amount=('total_valuation_amount', 'sum')
        ).reset_index()

        # asset_type을 한글로 변환할 임시 컬럼 생성
        type_to_kr = lookup_data['codes']['code_map']['asset_type']
        grouped_df['asset_type_kr'] = grouped_df['asset_type'].map(type_to_kr).fillna(grouped_df['asset_type'])

        # 비중 계산 및 레이블 생성
        total = grouped_df['total_valuation_amount'].sum()
        grouped_df['percentage'] = (grouped_df['total_valuation_amount'] / total) * 100
        # 레이블 형식: 한글명 (XX.X%)
        grouped_df['label'] = grouped_df['asset_type_kr'] + ' (' + grouped_df['percentage'].round(1).astype(str) + '%)'

        base = alt.Chart(grouped_df).encode(
            theta=alt.Theta("total_valuation_amount", stack=True),
        )
        pie = base.mark_arc(outerRadius=100).encode(
            # 📌 [1-4 요청 반영] Color 인코딩에 한글 컬럼 사용
            color=alt.Color("asset_type_kr", title="자산 유형", legend=alt.Legend(orient="bottom", columns=3)),
            tooltip=["asset_type_kr", alt.Tooltip("total_valuation_amount", format=",.0f"), alt.Tooltip("percentage", format=".1f")]
        )
        
        # 📌 [1-5 요청 반영] 텍스트 레이어 추가 (파이 차트 위에 레이블 표시)
        text = base.mark_text(radius=120).encode(
            text=alt.Text("label"), # 계산된 한글 + 비중 레이블
            order=alt.Order("total_valuation_amount", sort="descending"), 
            color=alt.value("black") 
        )
        
        chart = pie + text # 차트 합치기
        st.altair_chart(chart, width='stretch')
    else:
        st.warning("`asset_summary` 뷰에 `asset_type` 컬럼이 없어 차트를 그릴 수 없습니다.")