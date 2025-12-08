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
# 2. 데이터 조회 및 저장 함수
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

def save_changes_to_db(table_name, df, edited_rows):
    """
    st.data_editor에서 수정된 데이터를 Supabase DB에 업데이트합니다.
    (주의: 현재 코드에서는 row_id (고유 식별자)가 있다고 가정합니다.)
    """
    if not edited_rows:
        st.info("수정된 내용이 없습니다.")
        return

    # st.data_editor의 'edited_rows' 딕셔너리에는 수정된 행의 인덱스와 변경된 컬럼/값이 포함되어 있습니다.
    updates_to_send = []
    
    # 수정된 각 행에 대해 반복합니다. (key는 데이터프레임의 내부 인덱스입니다)
    for index, changes in edited_rows.items():
        # 데이터프레임의 해당 인덱스에 접근하여 고유 식별자(예: 'id' 컬럼)를 가져옵니다.
        # 이 'id' 컬럼이 Supabase 테이블의 Primary Key라고 가정합니다.
        row_id = df.loc[index, 'id'] 
        
        # 업데이트할 데이터에 고유 ID를 추가합니다.
        update_data = {"id": row_id}
        # 변경된 값들을 추가합니다.
        update_data.update(changes) 
        updates_to_send.append(update_data)

    try:
        # Supabase의 upsert(업데이트 또는 삽입) 기능을 사용하여 변경 사항을 적용합니다.
        # 'on_conflict'에 Primary Key(여기서는 'id')를 지정하여 업데이트를 수행하도록 합니다.
        response = supabase.from_(table_name).upsert(updates_to_send, on_conflict="id").execute()
        
        # 캐시된 데이터를 무효화하여 다음 로드 시 DB에서 최신 데이터를 가져오도록 합니다.
        st.cache_data.clear()
        
        st.success(f"✅ {len(updates_to_send)}개의 항목이 '{table_name}' 테이블에 성공적으로 업데이트되었습니다.")
        return True
    
    except Exception as e:
        st.error(f"❌ DB 업데이트 중 오류 발생: {e}")
        return False


# ----------------------------------------------------
# 3. Streamlit 대시보드 탭 함수
# ----------------------------------------------------

def dashboard_tab(asset_summary_df, transaction_stats_df, portfolio_pnl_history_df, usd_krw_rate):
    """
    기존의 포트폴리오 현황 및 차트를 표시하는 대시보드 탭입니다.
    """
    
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

        # st.columns(4): 화면을 네 개의 동일한 너비의 열로 나눕니다. (모바일에서 자동으로 세로로 쌓임)
        col1, col2, col3, col4 = st.columns(4)

        # st.metric: 핵심 지표를 강조하여 표시합니다.
        col1.metric("총 포트폴리오 가치 (KRW 통합)", f"₩ {total_valuation:,.0f}")
        col2.metric("총 매입 원금 (KRW 통합)", f"₩ {total_purchase:,.0f}")
        # 모바일에서도 잘 보이도록, 긍정적인 PnL은 녹색으로 강조
        col3.metric("총 평가 손익", f"₩ {unrealized_pnl:,.0f}", delta=f"₩ {unrealized_pnl:,.0f}", delta_color="normal")
        col4.metric("현재 포트폴리오 수익률", f"{overall_return_rate:,.2f}%")
        
    else:
        st.info("데이터를 불러올 수 없거나, 보유 종목 데이터가 비어 있습니다. `asset_summary` 뷰를 확인하세요.")

    st.divider() # 가로 구분선

    # ----------------------------------------------------
    # 4. 포트폴리오 상세 현황 (asset_summary 뷰 활용)
    # ----------------------------------------------------
    st.header("보유 종목별 현황 (Asset Summary)")

    if not asset_summary_df.empty:
        # st.dataframe: 데이터를 표 형태로 표시합니다.
        st.dataframe(
            asset_summary_df,
            # 'width='stretch'는 모바일에서 화면 너비에 꽉 차도록 유동적으로 크기를 조정합니다.
            width='stretch', 
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
        # (기존 거래 통계 및 테이블 코드 유지)
        try:
            total_realized_pnl = pd.to_numeric(transaction_stats_df['total_realized_pnl'], errors='coerce').sum()
            
            st.metric(
                label="✅ 누적 실현 손익 총합 (Total Realized P&L)", 
                value=f"₩ {total_realized_pnl:,.0f}", 
                delta_color="normal"
            )
        except Exception:
            st.warning("실현 손익 합계 계산 중 오류가 발생했습니다. 데이터 타입을 확인해주세요.")

        st.subheader("종목별 실현 손익 현황")
        display_df = transaction_stats_df.copy()
        
        display_df = display_df.rename(columns={
            'name_kr': '종목명',
            'ticker': '티커',
            'total_realized_pnl': '실현 손익 합계',
            'total_buy_amount': '총 매수 금액',
            'total_sell_amount': '총 매도 금액',
            'total_fees_taxes': '총 수수료/세금',
        })
        
        # st.dataframe 사용 시 .style.format 대신 column_config를 사용하는 것이 Streamlit 권장 사항입니다.
        # 기존 스타일링 코드를 유지하며, 모바일 최적화를 위해 width='stretch'를 적용합니다.
        st.dataframe(
            display_df, 
            width='stretch', 
            hide_index=True,
            column_config={
                '실현 손익 합계': st.column_config.NumberColumn("실현 손익 합계", format="₩ %,.0f"),
                '총 매수 금액': st.column_config.NumberColumn("총 매수 금액", format="₩ %,.0f"),
                '총 매도 금액': st.column_config.NumberColumn("총 매도 금액", format="₩ %,.0f"),
                '총 수수료/세금': st.column_config.NumberColumn("총 수수료/세금", format="₩ %,.0f"),
            }
        )
        
    else:
        st.info("거래 통계(transaction_stats) 뷰에 데이터가 없습니다. 매매 기록을 확인하세요.")

    st.divider()

    # --- B. 포트폴리오 P&L 역사 (Historical PnL) ---
    st.header("📅 포트폴리오 P&L 역사 (손익 기록)")

    if not portfolio_pnl_history_df.empty:
        pnl_df = portfolio_pnl_history_df.copy()
        
        # 1. 데이터 클리닝 및 인덱스 설정
        try:
            pnl_df['date'] = pd.to_datetime(pnl_df['date'], errors='coerce')
            pnl_df = pnl_df.set_index('date').sort_index()
            
            numeric_cols = ['cumulative_pnl', 'portfolio_return_rate', 'cumulative_valuation_amount', 'cumulative_contribution']
            for col in numeric_cols:
                pnl_df[col] = pd.to_numeric(pnl_df[col], errors='coerce').fillna(0) 
                
        except Exception as e:
            st.error(f"P&L 역사 데이터 처리 중 오류 발생: {e}. 데이터 형식을 확인하세요.")
            pnl_df = pd.DataFrame() 

        if not pnl_df.empty:
            
            # st.line_chart 사용 시 'width='stretch'를 통해 반응형을 유지합니다.
            
            # 차트1: 누적 자산 및 원금 추이
            st.subheader("누적 자산 (총 평가 금액 vs. 총 매입 원금)")
            st.line_chart(
                pnl_df, 
                y=['cumulative_valuation_amount', 'cumulative_contribution'], 
                width='stretch', # 모바일 반응형
                color=["#007bff", "#adb5bd"] 
            )
            
            # 차트2: 누적 평가 손익 (PnL)
            st.subheader("누적 평가 손익 추이 (Cumulative PnL)")
            st.line_chart(
                pnl_df, 
                y='cumulative_pnl', 
                width='stretch', # 모바일 반응형
                color="#28a745" 
            )
            
            # 차트3: 포트폴리오 수익률 (%)
            st.subheader("포트폴리오 수익률 추이 (%)")
            st.line_chart(
                pnl_df, 
                y='portfolio_return_rate', 
                width='stretch', # 모바일 반응형
                color="#ffc107" 
            )
            
        else:
            st.info("포트폴리오 P&L 역사(portfolio_pnl_view) 뷰에 데이터가 없습니다.")
    else:
        st.warning("`portfolio_pnl_view` 데이터 로드에 문제가 발생했습니다.")

# ----------------------------------------------------
# 4. 데이터 편집 및 업데이트 탭 함수
# ----------------------------------------------------

# @st.cache_data를 사용하지 않는 새로운 함수를 정의하여, 
# 이 함수를 호출할 때마다 DB에서 최신 '거래' 데이터를 가져오도록 합니다.
def fetch_transactions_data():
    """트랜잭션 테이블에서 모든 데이터를 가져옵니다. (id 컬럼 필수)"""
    if supabase is None:
        return pd.DataFrame()

    try:
        # 실제 데이터 쓰기/수정이 발생할 테이블 이름을 지정합니다.
        # (예: 'transactions' 테이블을 사용한다고 가정)
        response = supabase.from_("transactions").select("*").order("transaction_date", desc=True).execute()
        df = pd.DataFrame(response.data)
        
        # st.data_editor 사용을 위해 'id' 컬럼이 문자열이 아닌지 확인합니다.
        if 'id' in df.columns:
             df['id'] = pd.to_numeric(df['id'], errors='coerce')

        return df
    except Exception as e:
        st.error(f"거래 데이터 로드 오류: {e}")
        return pd.DataFrame()


def data_editor_tab():
    """
    Supabase DB 데이터를 표시하고 수정 후 저장하는 탭입니다.
    """
    st.header("📝 거래 기록 직접 편집 및 업데이트")

    # 1. 초기 데이터 로드 및 세션 상태 관리
    # st.session_state를 사용하여 앱의 상태를 유지합니다.
    # 'transactions_data'에 원본 데이터를 저장하여, 'Save' 버튼을 눌렀을 때만 업데이트 비교에 사용합니다.
    if 'transactions_data' not in st.session_state:
        st.session_state['transactions_data'] = fetch_transactions_data()

    transaction_df = st.session_state['transactions_data']

    if transaction_df.empty:
        st.warning("데이터베이스에서 'transactions' 테이블 데이터를 로드할 수 없습니다.")
        return

    st.caption(f"총 {len(transaction_df)}개의 거래 기록이 로드되었습니다. 아래에서 내용을 수정하세요.")

    # 2. st.data_editor를 사용하여 데이터 표시 및 수정 허용
    # key='data_editor_transactions'를 사용하여 이 위젯의 상태를 세션 상태와 분리합니다.
    # hide_index=True: Streamlit 데이터프레임 인덱스를 숨겨 모바일 공간을 절약합니다.
    edited_df = st.data_editor(
        transaction_df,
        key='data_editor_transactions',
        # 'id' 컬럼은 수정할 수 없도록 disable 처리 (Primary Key이므로)
        column_config={
            "id": st.column_config.NumberColumn("ID", disabled=True),
            "ticker": "티커",
            "transaction_date": st.column_config.DateColumn("거래일"),
            "transaction_type": st.column_config.SelectboxColumn("유형", options=["BUY", "SELL"]),
            "quantity": st.column_config.NumberColumn("수량", format="%d"),
            "price": st.column_config.NumberColumn("단가", format="%.2f"),
            "fee": st.column_config.NumberColumn("수수료", format="%.0f"),
            "currency": st.column_config.SelectboxColumn("통화", options=["KRW", "USD"]),
            # 여기에 필요한 다른 컬럼 설정 추가
        },
        height=500, # 모바일에서 스크롤 가능하도록 높이 설정
        use_container_width=True, # 모바일에서 너비 꽉 채우기
    )

    # 3. 변경 사항 저장 버튼
    # st.data_editor는 수정 사항을 'st.session_state[key]['edited_rows']'에 저장합니다.
    edited_rows = st.session_state['data_editor_transactions']['edited_rows']
    
    st.divider()
    
    # Save 버튼을 눌렀을 때만 DB 업데이트 로직을 실행합니다.
    if st.button("💾 Supabase DB에 변경 내용 저장", type="primary"):
        # save_changes_to_db 함수를 호출하여 DB에 업데이트를 시도합니다.
        if save_changes_to_db("transactions", transaction_df, edited_rows):
            # DB 업데이트 성공 후, 최신 데이터로 세션 상태를 갱신하고 앱을 새로고침합니다.
            st.session_state['transactions_data'] = fetch_transactions_data()
            st.rerun() # st.rerun()으로 앱을 새로고침하여 에디터에 최신 데이터가 반영되도록 합니다.
        
# ----------------------------------------------------
# 5. 메인 앱 실행 로직
# ----------------------------------------------------

# 페이지 설정: wide 모드, 모바일에서 사이드바를 기본적으로 'collapsed' (접힌 상태)로 설정하여 화면 공간을 확보합니다.
st.set_page_config(
    layout="wide", 
    page_title="금융 자산 포트폴리오 대시보드",
    initial_sidebar_state="collapsed" 
)

st.title("💰 승엽민희 금융 자산 포트폴리오")
st.caption(f"최종 앱 실행 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# ====================================================
# 데이터 로드 (모든 탭에서 공유)
# ====================================================
asset_summary_df = fetch_data_from_view("asset_summary")
transaction_stats_df = fetch_data_from_view("transaction_stats")
portfolio_pnl_history_df = fetch_data_from_view("portfolio_pnl_view") 

# 환율 조회 및 표시
usd_krw_rate = fetch_usd_exchange_rate()
st.caption(f"현재 적용 환율 (USD/KRW): ₩{usd_krw_rate:,.2f}") 

st.divider() # 가로 구분선

# ====================================================
# Streamlit Tabs (탭) 생성
# ====================================================

# st.tabs(): 탭 UI를 생성하고, 리스트의 각 항목에 해당하는 탭 컨테이너 객체를 반환합니다.
tab1, tab2 = st.tabs(["📈 포트폴리오 대시보드", "📝 DB 데이터 편집 (Transactions)"])

# 첫 번째 탭: 기존 대시보드 기능을 실행합니다.
with tab1:
    dashboard_tab(asset_summary_df, transaction_stats_df, portfolio_pnl_history_df, usd_krw_rate)

# 두 번째 탭: 데이터 편집 기능을 실행합니다.
with tab2:
    # Supabase 데이터 수정 로직이 포함된 새로운 함수를 호출합니다.
    data_editor_tab()