import os
from dotenv import load_dotenv
from datetime import datetime, timezone
from supabase import create_client, Client
import pandas as pd
import streamlit as st
import yfinance as yf
from FinanceDataReader import data as fdr

# ----------------------------------------------------
# 1. Supabase 접속 정보 설정 및 연결
# ----------------------------------------------------
load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# @st.cache_resource: Streamlit에게 이 함수의 반환 값(Supabase 연결 객체)을 앱의 세션 전반에 걸쳐 캐시하고 재사용하도록 지시합니다. (앱 성능 향상)
@st.cache_resource 
def init_connection():
    """Supabase 연결을 초기화하고 캐시합니다."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        st.error("FATAL ERROR: Supabase URL 또는 Key가 설정되지 않았습니다. .env 파일을 확인하세요.")
        return None
    return create_client(SUPABASE_URL, SUPABASE_KEY)

supabase = init_connection()

# ----------------------------------------------------
# 2. 데이터 조회 함수 (뷰 활용)
# ----------------------------------------------------
# @st.cache_data(ttl=600): 이 함수의 결과를 캐시하고, 600초(10분) 후에 함수를 다시 실행하여 DB에서 데이터를 새로 가져오도록 합니다.
@st.cache_data(ttl=600) 
def fetch_data_from_view(view_name):
    """지정된 뷰(View)에서 데이터를 조회합니다."""
    if supabase is None:
        return pd.DataFrame()

    try:
        # Supabase Python SDK를 사용하여 지정된 뷰에서 모든 데이터를 조회합니다.
        response = supabase.from_(view_name).select("*").execute()
        return pd.DataFrame(response.data)
    except Exception as e:
        st.error(f"{view_name} 뷰 데이터 로드 오류: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=3600) # 환율은 1시간(3600초)마다 새로고침
def fetch_usd_exchange_rate():
    """FinanceDataReader를 사용해 USD/KRW 환율을 조회합니다."""
    try:
        # FinanceDataReader를 사용하여 'USD/KRW' (달러-원 환율)의 최근 20일 데이터를 가져옵니다.
        df = fdr.DataReader('USD/KRW')#, '20 days ago') 
        # 가장 최근 종가(Close)를 환율로 반환
        return df['Close'].iloc[-1]
    except Exception as e:
        # 환율 조회 실패 시, 임시 고정값을 사용하며 경고를 표시
        st.warning(f"🚨 환율 조회 실패 ({e}). 임시 환율 1,350원 사용")
        return 1350.0 # 안전을 위한 임시 고정값

# ----------------------------------------------------
# 3. Streamlit 대시보드 레이아웃
# ----------------------------------------------------
# 페이지 설정: wide 모드로 설정하여 화면을 넓게 사용하고, 페이지 제목을 설정합니다.
st.set_page_config(layout="wide", page_title="금융 자산 포트폴리오 대시보드")

st.title("💰 승엽민희 금융 자산 포트폴리오")
# 현재 시간을 표시하여 사용자에게 데이터 업데이트 시점을 알려줍니다.
st.caption(f"최종 앱 실행 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# ====================================================
# 데이터 로드
# ====================================================
asset_summary_df = fetch_data_from_view("asset_summary")
transaction_stats_df = fetch_data_from_view("transaction_stats")
# 이전에 생성한 최종 P&L 뷰를 사용합니다.
portfolio_pnl_history_df = fetch_data_from_view("portfolio_pnl_view") 

# 환율 조회 및 표시
usd_krw_rate = fetch_usd_exchange_rate()
st.caption(f"현재 적용 환율 (USD/KRW): ₩{usd_krw_rate:,.2f}") 

st.divider() # 가로 구분선

# ----------------------------------------------------
# 3. 총괄 현황 (Overall Summary) - KRW 통합 계산 포함
# ----------------------------------------------------
st.header("📊 포트폴리오 총괄 현황")

if not asset_summary_df.empty:
    
    # 통화 통합 계산을 위한 임시 DataFrame 생성
    combined_df = asset_summary_df.copy()

    # KRW 기준 평가 금액 및 매입 금액 계산 로직
    # 'currency' 컬럼이 'USD'인 경우에만 환율을 곱하여 원화(KRW)로 변환합니다.
    combined_df['krw_valuation'] = combined_df.apply(
        lambda row: pd.to_numeric(row['total_valuation_amount'], errors='coerce', downcast='float') * usd_krw_rate 
                    if row['currency'] == 'USD' else pd.to_numeric(row['total_valuation_amount'], errors='coerce', downcast='float'),
        axis=1
    ).fillna(0) # NaN 발생 시 0으로 처리

    combined_df['krw_purchase'] = combined_df.apply(
        lambda row: pd.to_numeric(row['total_purchase_amount'], errors='coerce', downcast='float') * usd_krw_rate 
                    if row['currency'] == 'USD' else pd.to_numeric(row['total_purchase_amount'], errors='coerce', downcast='float'),
        axis=1
    ).fillna(0) # NaN 발생 시 0으로 처리

    # 총괄 지표 합산
    total_valuation = combined_df['krw_valuation'].sum()
    total_purchase = combined_df['krw_purchase'].sum()
    unrealized_pnl = total_valuation - total_purchase
    overall_return_rate = (unrealized_pnl / total_purchase) * 100 if total_purchase > 0 else 0

    # st.columns(3): 화면을 세 개의 동일한 너비의 열로 나눕니다.
    col1, col2, col3, col4 = st.columns(4)

    # st.metric: 핵심 지표를 강조하여 표시합니다. delta는 이전 값과의 차이를 표시합니다.
    # PnL을 delta로 사용하여 시각적 강조 효과를 줍니다.
    col1.metric("총 포트폴리오 가치 (KRW 통합)", f"₩ {total_valuation:,.0f}")
    col2.metric("총 매입 원금 (KRW 통합)", f"₩ {total_purchase:,.0f}")
    col3.metric("총 평가 손익", f"₩ {unrealized_pnl:,.0f}", delta=f"₩ {unrealized_pnl:,.0f}", delta_color="normal")
    # 수익률은 delta 대신 value에만 표시
    col4.metric("현재 포트폴리오 수익률", f"{overall_return_rate:,.2f}%")
    
else:
    st.info("데이터를 불러올 수 없거나, 보유 종목 데이터가 비어 있습니다. `asset_summary` 뷰를 확인하세요.")

st.divider()

# ----------------------------------------------------
# 4. 포트폴리오 상세 현황 (asset_summary 뷰 활용)
# ----------------------------------------------------
st.header("보유 종목별 현황 (Asset Summary)")

if not asset_summary_df.empty:
    # st.dataframe: 데이터를 표 형태로 표시합니다.
    # column_config: Streamlit의 고급 데이터프레임 기능을 사용하여 컬럼 이름 변경 및 숫자 포맷팅을 적용합니다.
    st.dataframe(
        asset_summary_df,
        width='stretch',
        # 사용자 정의 컬럼 설정
        column_config={
            "total_quantity": st.column_config.NumberColumn("보유 수량", format="%d"),
            "current_valuation_price": st.column_config.NumberColumn("현재가", format="%.2f"),
            "total_purchase_amount": st.column_config.NumberColumn("총 매수 금액 (원화/달러)", format="%,.0f"),
            "total_valuation_amount": st.column_config.NumberColumn("총 평가 금액 (원화/달러)", format="%,.0f"),
            "average_purchase_price": st.column_config.NumberColumn("평균 매입 단가", format="%.2f"),
            "unrealized_pnl": st.column_config.NumberColumn("평가 손익 (원화/달러)", format="%,.0f"),
            "unrealized_return_rate": st.column_config.NumberColumn("수익률 (%)", format="%.2f%%"),
            "name_kr": "종목명",
            "ticker": "티커",
            "currency": "통화"
        }
    )
else:
    st.info("보유 종목 데이터를 불러올 수 없습니다.")

st.divider()

# ----------------------------------------------------
# 5. 거래 통계 및 역사적 P&L 차트
# ----------------------------------------------------

# --- A. 거래 통계 (Transaction Stats) ---
st.header("📈 거래 통계 (Transaction Stats)")
if not transaction_stats_df.empty:
    # 1. 누적 실현 손익 총합 메트릭 표시
    try:
        # 실현 손익을 숫자형으로 변환 후 합산
        total_realized_pnl = pd.to_numeric(transaction_stats_df['total_realized_pnl'], errors='coerce').sum()
        
        st.metric(
            label="✅ 누적 실현 손익 총합 (Total Realized P&L)", 
            value=f"₩ {total_realized_pnl:,.0f}", 
            delta_color="normal"
        )
    except Exception:
        st.warning("실현 손익 합계 계산 중 오류가 발생했습니다. 데이터 타입을 확인해주세요.")

    # 2. 종목별 거래 통계 테이블 표시
    st.subheader("종목별 실현 손익 현황")
    display_df = transaction_stats_df.copy()
    
    # 컬럼 이름 변경 및 포맷팅 설정
    display_df = display_df.rename(columns={
        'name_kr': '종목명',
        'ticker': '티커',
        'total_realized_pnl': '실현 손익 합계',
        'total_buy_amount': '총 매수 금액',
        'total_sell_amount': '총 매도 금액',
        'total_fees_taxes': '총 수수료/세금',
    })
    
    # .style.format: Pandas Style 기능을 사용하여 금액 컬럼을 통화 형식으로 포맷팅합니다.
    styled_df = display_df.style.format({
        '실현 손익 합계': "₩ {:,.0f}",
        '총 매수 금액': "₩ {:,.0f}",
        '총 매도 금액': "₩ {:,.0f}",
        '총 수수료/세금': "₩ {:,.0f}",
    })
    
    st.dataframe(styled_df, width='stretch', hide_index=True)
    
else:
    st.info("거래 통계(transaction_stats) 뷰에 데이터가 없습니다. 매매 기록을 확인하세요.")

st.divider()

# --- B. 포트폴리오 P&L 역사 (Historical PnL) ---
st.header("📅 포트폴리오 P&L 역사 (손익 기록)")

if not portfolio_pnl_history_df.empty:
    pnl_df = portfolio_pnl_history_df.copy()
    
    # 1. 데이터 클리닝 및 인덱스 설정
    try:
        # 날짜를 datetime으로 변환하고, 시계열 데이터 분석을 위해 날짜를 인덱스로 설정
        pnl_df['date'] = pd.to_datetime(pnl_df['date'], errors='coerce')
        pnl_df = pnl_df.set_index('date').sort_index()
        
        # 숫자 컬럼을 명시적으로 float으로 변환하여 차트 오류를 방지
        numeric_cols = ['cumulative_pnl', 'portfolio_return_rate', 'cumulative_valuation_amount', 'cumulative_contribution']
        for col in numeric_cols:
            pnl_df[col] = pd.to_numeric(pnl_df[col], errors='coerce').fillna(0) 
            
    except Exception as e:
        st.error(f"P&L 역사 데이터 처리 중 오류 발생: {e}. 데이터 형식을 확인하세요.")
        pnl_df = pd.DataFrame() 

    if not pnl_df.empty:
        
        # st.line_chart: Streamlit이 제공하는 간단한 라인 차트 API
        
        # 차트1: 누적 자산 및 원금 추이
        st.subheader("누적 자산 (총 평가 금액 vs. 총 매입 원금)")
        st.line_chart(
            pnl_df, 
            y=['cumulative_valuation_amount', 'cumulative_contribution'], 
            width='stretch',
            color=["#007bff", "#adb5bd"] # 계열별 색상 지정
        )
        
        # 차트2: 누적 평가 손익 (PnL)
        st.subheader("누적 평가 손익 추이 (Cumulative PnL)")
        st.line_chart(
            pnl_df, 
            y='cumulative_pnl', 
            width='stretch',
            color="#28a745" 
        )
        
        # 차트3: 포트폴리오 수익률 (%)
        st.subheader("포트폴리오 수익률 추이 (%)")
        st.line_chart(
            pnl_df, 
            y='portfolio_return_rate', 
            width='stretch',
            color="#ffc107" 
        )
        
    else:
        st.info("포트폴리오 P&L 역사(portfolio_pnl_view) 뷰에 데이터가 없습니다.")
else:
    st.warning("`portfolio_pnl_view` 데이터 로드에 문제가 발생했습니다.")