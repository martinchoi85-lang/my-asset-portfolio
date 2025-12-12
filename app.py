import os
from dotenv import load_dotenv
from datetime import datetime, timezone
from supabase import create_client, Client
import pandas as pd
import streamlit as st
import yfinance as yf
from FinanceDataReader import data as fdr
import altair as alt 

# ----------------------------------------------------
# 1. Supabase 접속 정보 설정 및 연결
# ----------------------------------------------------
load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# 📌 디버그 모드 설정 (True로 변경 시 디버깅 정보가 출력됨)
DEBUG_MODE = False 

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
@st.cache_data(ttl=600) 
def fetch_data_from_view(view_name):
    """지정된 뷰(View)에서 데이터를 조회합니다."""
    if supabase is None:
        return pd.DataFrame()

    try:
        response = supabase.from_(view_name).select("*").execute()
        return pd.DataFrame(response.data)
    except Exception as e:
        st.error(f"{view_name} 뷰 데이터 로드 오류: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600) 
def fetch_usd_exchange_rate():
    """USD/KRW 환율을 조회합니다."""
    try:
        df = fdr.DataReader('USD/KRW')
        return df['Close'].iloc[-1]
    except Exception as e:
        st.warning(f"🚨 환율 조회 실패 ({e}). 임시 환율 1,350원 사용")
        return 1350.0 

@st.cache_data(ttl=600) 
def fetch_editor_data():
    """트랜잭션 테이블, 에셋 테이블, 에셋 요약 뷰, 계좌 테이블 데이터를 가져옵니다."""
    if supabase is None:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    try:
        # 1. 거래 (transactions)
        transactions_res = supabase.from_("transactions").select("*").order("transaction_date", desc=True).execute()
        transactions_df = pd.DataFrame(transactions_res.data)

        # 2. 자산 (assets) (매핑용)
        assets_res = supabase.from_("assets").select("id, name_kr, asset_type").execute()
        assets_df = pd.DataFrame(assets_res.data)

        # 3. 자산 요약 (asset_summary) (편집용)
        asset_summary_res = supabase.from_("asset_summary").select("*").execute()
        asset_summary_df = pd.DataFrame(asset_summary_res.data)
        
        # 4. 계좌 (accounts) (매핑용)
        accounts_res = supabase.from_("accounts").select("id, name, brokerage").execute()
        accounts_df = pd.DataFrame(accounts_res.data)

        # 'id' 컬럼 타입 확인 및 변환
        for df in [transactions_df, assets_df]:
            if 'id' in df.columns:
                 df['id'] = pd.to_numeric(df['id'], errors='coerce')
        if 'asset_id' in asset_summary_df.columns:
             asset_summary_df['asset_id'] = pd.to_numeric(asset_summary_df['asset_id'], errors='coerce')

        return transactions_df, assets_df, asset_summary_df, accounts_df
    except Exception as e:
        st.error(f"편집 데이터 로드 오류: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


def save_changes_to_db(table_name, df, edited_rows, name_to_id_map=None, display_to_db_map=None, account_display_to_id_map=None):
    """st.data_editor에서 수정된 데이터를 Supabase DB에 업데이트합니다."""
    if not edited_rows:
        st.info("수정된 내용이 없습니다.")
        return False

    updates_to_send = []
    
    primary_keys = ['id'] if table_name == 'transactions' else ['asset_id', 'account_id']
    on_conflict_keys = ','.join([k for k in primary_keys if k in df.columns])

    for index, changes in edited_rows.items():
        update_data = {}
        is_valid = True
        
        # Primary Key 설정
        for key in primary_keys:
            if key in df.columns:
                pk_value = df.loc[index, key] 
                if pd.isna(pk_value) or pk_value is None:
                    st.warning(f"⚠️ 행 인덱스 {index}에 대한 Primary Key ({key})가 누락되었습니다. 업데이트를 건너뜀.")
                    is_valid = False
                    break
                update_data[key] = pk_value
        
        if not is_valid:
            continue
            
        update_data.update(changes) 

        # 양방향 매핑 처리 (이전 로직 복구)
        if name_to_id_map is not None and 'asset_name' in update_data:
            asset_name = update_data.pop('asset_name') 
            asset_id = name_to_id_map.get(asset_name) 
            if asset_id is not None:
                update_data['asset_id'] = asset_id
            else:
                st.warning(f"⚠️ 자산명 '{asset_name}'에 해당하는 ID를 찾을 수 없습니다. 해당 행은 건너뜀.")
                continue
            
        if display_to_db_map is not None and 'currency' in update_data:
            display_value = update_data['currency']
            db_value = display_to_db_map.get(display_value)
            if db_value:
                update_data['currency'] = db_value
        
        if account_display_to_id_map is not None and 'account_display' in update_data:
            account_display = update_data.pop('account_display')
            account_id = account_display_to_id_map.get(account_display)
            if account_id is not None:
                update_data['account_id'] = account_id
            else:
                st.warning(f"⚠️ 계좌 정보 '{account_display}'에 해당하는 ID를 찾을 수 없습니다. 해당 행은 건너뜜.")
                continue

        updates_to_send.append(update_data)
        
    if not updates_to_send:
        st.warning("유효한 업데이트 항목이 없습니다. Primary Key 또는 매핑을 확인하세요.")
        return False

    try:
        supabase.from_(table_name).upsert(updates_to_send, on_conflict=on_conflict_keys).execute()
        st.cache_data.clear()
        st.success(f"✅ {len(updates_to_send)}개의 항목이 '{table_name}' 테이블에 성공적으로 업데이트되었습니다.")
        return True
    
    except Exception as e:
        st.error(f"❌ DB 업데이트 중 오류 발생: {e}. Supabase Policy(RLS)와 컬럼 이름을 확인하세요.")
        return False


# ----------------------------------------------------
# 3. Streamlit 대시보드 탭 함수
# ----------------------------------------------------

def dashboard_tab(asset_summary_df, transaction_stats_df, portfolio_pnl_history_df, usd_krw_rate):
    """
    포트폴리오 현황 및 차트를 표시하는 대시보드 탭입니다.
    """
    
    # ----------------------------------------------------
    # 3. 총괄 현황 (Overall Summary)
    # ----------------------------------------------------
    st.header("📊 포트폴리오 총괄 현황")
    # ... (중략: 총괄 현황 계산 및 표시)

    if not asset_summary_df.empty:
        
        combined_df = asset_summary_df.copy()

        # KRW 통합 계산
        combined_df['krw_valuation'] = combined_df.apply(
            lambda row: pd.to_numeric(row['total_valuation_amount'], errors='coerce', downcast='float') * usd_krw_rate 
                        if row['currency'].lower() == 'usd' else pd.to_numeric(row['total_valuation_amount'], errors='coerce', downcast='float'),
            axis=1
        ).fillna(0)

        combined_df['krw_purchase'] = combined_df.apply(
            lambda row: pd.to_numeric(row['total_purchase_amount'], errors='coerce', downcast='float') * usd_krw_rate 
                        if row['currency'].lower() == 'usd' else pd.to_numeric(row['total_purchase_amount'], errors='coerce', downcast='float'),
            axis=1
        ).fillna(0)

        total_valuation = combined_df['krw_valuation'].sum()
        total_purchase = combined_df['krw_purchase'].sum()
        unrealized_pnl = total_valuation - total_purchase
        overall_return_rate = (unrealized_pnl / total_purchase) * 100 if total_purchase > 0 else 0

        col1, col2, col3, col4 = st.columns(4)

        col1.metric("총 포트폴리오 가치 (KRW 통합)", f"₩ {total_valuation:,.0f}")
        col2.metric("총 매입 원금 (KRW 통합)", f"₩ {total_purchase:,.0f}")
        col3.metric("총 평가 손익", f"₩ {unrealized_pnl:,.0f}", delta=f"₩ {unrealized_pnl:,.0f}", delta_color="normal")
        col4.metric("현재 포트폴리오 수익률", f"{overall_return_rate:,.2f}%")
        
    else:
        st.info("데이터를 불러올 수 없거나, 보유 종목 데이터가 비어 있습니다. `asset_summary` 뷰를 확인하세요.")

    st.divider() 

    # ----------------------------------------------------
    # 4. 포트폴리오 상세 현황 (asset_summary 뷰 활용)
    # ----------------------------------------------------
    st.header("보유 종목별 현황 (Asset Summary)")

    if not asset_summary_df.empty:
        st.dataframe(
            asset_summary_df,
            width='stretch', 
            column_config={
                # 📌 FIX: D3-Format 적용 (통화 기호 제거)
                "total_quantity": st.column_config.NumberColumn("보유 수량", format=",d"), 
                "current_valuation_price": st.column_config.NumberColumn("현재가", format=",.2f"), 
                "total_purchase_amount": st.column_config.NumberColumn("총 매수 금액 (원화/달러)", format=",.0f"), 
                "total_valuation_amount": st.column_config.NumberColumn("총 평가 금액 (원화/달러)", format=",.0f"), 
                "average_purchase_price": st.column_config.NumberColumn("평균 매입 단가", format=",.2f"), 
                "unrealized_pnl": st.column_config.NumberColumn("평가 손익 (원화/달러)", format=",.0f"), 
                "unrealized_return_rate": st.column_config.NumberColumn("수익률 (%)", format=",.2f%%"), 
                "name_kr": "종목명",
                "ticker": "티커",
                "currency": "통화",
                "asset_type": "자산 유형", # asset_type이 있다면 표시
            }
        )
    else:
        st.info("보유 종목 데이터를 불러올 수 없습니다.")

    st.divider()

    # ----------------------------------------------------
    # 5. 거래 통계 및 역사적 P&L 차트
    # ----------------------------------------------------

    st.header("📈 거래 통계 (Transaction Stats)")
    if not transaction_stats_df.empty:
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
        
        # 📌 FIX: Transaction Stats도 column_config로 포맷팅
        st.dataframe(
            display_df, 
            width='stretch', 
            hide_index=True,
            column_config={
                '실현 손익 합계': st.column_config.NumberColumn("실현 손익 합계", format="₩ ,d"), 
                '총 매수 금액': st.column_config.NumberColumn("총 매수 금액", format="₩ ,d"), 
                '총 매도 금액': st.column_config.NumberColumn("총 매도 금액", format="₩ ,d"), 
                '총 수수료/세금': st.column_config.NumberColumn("총 수수료/세금", format="₩ ,d"), 
            }
        )
        
    else:
        st.info("거래 통계(transaction_stats) 뷰에 데이터가 없습니다. 매매 기록을 확인하세요.")

    st.divider()

    # --- B. 포트폴리오 P&L 역사 (Historical PnL) ---
    st.header("📅 포트폴리오 자산 및 수익률 추이")

    # (중략: P&L 차트 Altair 구현)
    if not portfolio_pnl_history_df.empty:
        pnl_df = portfolio_pnl_history_df.copy()
        
        try:
            pnl_df['date'] = pd.to_datetime(pnl_df['date'], errors='coerce')
            pnl_df = pnl_df.set_index('date').sort_index().reset_index()
            
            numeric_cols = ['cumulative_pnl', 'portfolio_return_rate', 'cumulative_valuation_amount', 'cumulative_contribution']
            for col in numeric_cols:
                pnl_df[col] = pd.to_numeric(pnl_df[col], errors='coerce').fillna(0) 
                
        except Exception as e:
            st.error(f"P&L 역사 데이터 처리 중 오류 발생: {e}. 데이터 형식을 확인하세요.")
            pnl_df = pd.DataFrame() 

        if not pnl_df.empty:
            
            st.subheader("총 자산 변화 및 포트폴리오 수익률 추이 (이중 축)")
            
            # 1. Base Chart (X축 정의)
            base = alt.Chart(pnl_df).encode(
                x=alt.X('date:T', axis=alt.Axis(title='날짜')),
            )

            # 2. Left Axis: Valuation and Contribution (Lines) - Folded for Legend
            chart_left = base.transform_fold(
                ['cumulative_valuation_amount', 'cumulative_contribution'],
                as_=['Metric', 'Amount']
            ).mark_line(point=True).encode(
                y=alt.Y('Amount:Q', 
                        axis=alt.Axis(title='총 자산 (₩)', titleColor='#007bff', format='~s'), 
                        scale=alt.Scale(zero=False)), 
                color=alt.Color('Metric:N', 
                                scale=alt.Scale(domain=['cumulative_valuation_amount', 'cumulative_contribution'], range=['#007bff', '#adb5bd']),
                                legend=alt.Legend(title="자산 지표", labelExpr="datum.label == 'cumulative_valuation_amount' ? '총 평가 금액' : '총 매입 원금'")),
                tooltip=[alt.Tooltip('date:T', title='날짜'), 
                         alt.Tooltip('Amount:Q', title='금액', format=',.0f')]
            )

            # 3. Right Axis: Return Rate (Line) - Single Metric
            chart_right = alt.Chart(pnl_df).mark_line(point=True).encode(
                x='date:T',
                y=alt.Y('portfolio_return_rate:Q', 
                        axis=alt.Axis(title='수익률 (%)', titleColor='#ffc107', format='.2f', orient='right'), 
                        scale=alt.Scale(zero=False)),
                color=alt.value('#ffc107'), 
                tooltip=[alt.Tooltip('date:T', title='날짜'),
                         alt.Tooltip('portfolio_return_rate:Q', title='수익률', format='.2f')]
            )
            
            # 4. 차트 통합
            final_chart = alt.layer(chart_left, chart_right).resolve_scale(
                y='independent' # 두 Y축을 독립적으로 사용
            ).properties(
                title='총 자산 변화 및 포트폴리오 수익률 추이'
            ).interactive()

            st.altair_chart(final_chart, width='stretch') 
            
            st.markdown("### 🔍 차트 범례 설명")
            st.markdown("— <span style='color:#007bff; font-weight:bold'>총 평가 금액</span> / — <span style='color:#adb5bd; font-weight:bold'>총 매입 원금</span> (왼쪽 Y축)", unsafe_allow_html=True)
            st.markdown("— <span style='color:#ffc107; font-weight:bold'>수익률 (%)</span> (오른쪽 보조 Y축)", unsafe_allow_html=True)

        else:
            st.info("포트폴리오 P&L 역사(portfolio_pnl_view) 뷰에 데이터가 없습니다.")
    else:
        st.warning("`portfolio_pnl_view` 데이터 로드에 문제가 발생했습니다.")
        
    st.divider()

    # --- C. 자산 유형별 비중 파이 차트 ---
    st.header("📊 자산 유형별 포트폴리오 비중")

    if 'asset_type' in asset_summary_df.columns and not asset_summary_df.empty:
        
        asset_summary_df['total_valuation_amount_numeric'] = pd.to_numeric(
            asset_summary_df['total_valuation_amount'], errors='coerce'
        ).fillna(0)
        
        type_summary = asset_summary_df.groupby('asset_type').agg(
            total_value=('total_valuation_amount_numeric', 'sum')
        ).reset_index()
        
        total_sum = type_summary['total_value'].sum()
        
        if total_sum > 0:
            type_summary['percentage'] = (type_summary['total_value'] / total_sum) * 100
            
            base = alt.Chart(type_summary).encode(
                theta=alt.Theta("total_value", stack=True)
            ).properties(
                title="자산 유형별 총 평가 금액 비중"
            )

            pie = base.mark_arc(outerRadius=120, innerRadius=80).encode(
                color=alt.Color("asset_type", title="자산 유형"),
                order=alt.Order("total_value", sort="descending"),
                tooltip=["asset_type", 
                         alt.Tooltip("total_value", format=",.0f", title="총 평가 금액"), 
                         alt.Tooltip("percentage", format=".2f", title="비중 (%)")]
            )

            text = base.mark_text(radius=140).encode(
                text=alt.Text("percentage", format=".1f"),
                order=alt.Order("total_value", sort="descending"),
                color=alt.value("black")
            )
            
            st.altair_chart(pie + text, width='stretch')
        else:
            st.info("총 평가 금액이 0이어서 파이 차트를 그릴 수 없습니다.")
    else:
        st.warning("`asset_summary` 뷰에 **'asset_type' 컬럼**이 없거나 데이터가 비어있습니다. 자산 구성 차트를 표시하려면 **Supabase DB에서 `asset_summary` 뷰를 수정**하여 `assets` 테이블의 `asset_type`을 포함시켜야 합니다.")


# ----------------------------------------------------
# 4. 거래 기록 편집 및 업데이트 탭 함수 (transactions 테이블)
# ----------------------------------------------------
def data_editor_tab():
    """ transactions 테이블 데이터를 표시하고 수정 후 저장하는 탭입니다. """
    st.header("📝 거래 기록 직접 편집 및 업데이트 (transactions)")
    TABLE_NAME = "transactions"

    # 1. 초기 데이터 로드 및 세션 상태 관리
    # 📌 FIX: 데이터를 한 번에 로드하고 세션 상태에 저장하여 불필요한 DB 호출 방지
    if 'transactions_df' not in st.session_state or 'assets_df' not in st.session_state or 'accounts_df' not in st.session_state:
        transactions_df, assets_df, asset_summary_df, accounts_df = fetch_editor_data()
        st.session_state['transactions_df'] = transactions_df
        st.session_state['assets_df'] = assets_df
        st.session_state['asset_summary_df'] = asset_summary_df
        st.session_state['accounts_df'] = accounts_df
    
    transaction_df = st.session_state['transactions_df'].copy()
    assets_df = st.session_state['assets_df'].copy()

    if transaction_df.empty or assets_df.empty:
        st.warning("데이터베이스에서 'transactions' 또는 'assets' 테이블 데이터를 로드할 수 없습니다.")
        return

    # 🌟 자산 ID-이름 맵핑 준비
    id_to_name_map = assets_df.set_index('id')['name_kr'].to_dict()
    name_to_id_map = {v: k for k, v in id_to_name_map.items()}
    asset_name_options = list(name_to_id_map.keys())

    if 'asset_id' in transaction_df.columns:
        transaction_df['asset_name'] = transaction_df['asset_id'].map(id_to_name_map)
    else:
        st.error("transactions 테이블에 'asset_id' 컬럼이 없습니다.")
        return

    # 2. 데이터 타입 명시적 변환
    try:
        if 'id' in transaction_df.columns:
            transaction_df['id'] = pd.to_numeric(transaction_df['id'], errors='coerce').fillna(0).astype(int)
            
        numeric_cols = ['quantity', 'price', 'fee', 'commission', 'realized_pnl']
        for col in numeric_cols:
            if col in transaction_df.columns:
                transaction_df[col] = pd.to_numeric(transaction_df[col], errors='coerce').fillna(0)
                
        if 'transaction_date' in transaction_df.columns:
            transaction_df['transaction_date'] = pd.to_datetime(transaction_df['transaction_date'], errors='coerce').dt.date
    except Exception as e:
        st.error(f"거래 데이터 전처리 중 오류 발생: {e}")
        return
    
    st.caption(f"총 {len(transaction_df)}개의 거래 기록이 로드되었습니다. 아래에서 내용을 수정하세요.")

    # 3. st.data_editor를 사용하여 데이터 표시 및 수정 허용
    st.data_editor(
        transaction_df,
        key='data_editor_transactions',
        column_config={
            "asset_name": st.column_config.SelectboxColumn("자산명", options=asset_name_options, required=True), 
            "ticker": "티커",
            "transaction_date": st.column_config.DateColumn("거래일"),
            "transaction_type": st.column_config.SelectboxColumn("유형", options=["BUY", "SELL", "DIVIDEND"]), # 배당(DIVIDEND) 추가
            "trade_type": st.column_config.SelectboxColumn("매매 유형", options=["매수", "매도", "분할매수", "분할매도", "배당"], required=True), 
            "quantity": st.column_config.NumberColumn("수량", format=",d"), 
            "price": st.column_config.NumberColumn("단가", format=",.2f"), 
            "fee": st.column_config.NumberColumn("거래 수수료", format=",d"), 
            "commission": st.column_config.NumberColumn("기타 수수료", format=",d"), 
            "realized_pnl": st.column_config.NumberColumn("실현 손익", format=",d"), 
            "currency": st.column_config.SelectboxColumn("통화", options=["KRW", "USD"]),
            "memo": "메모", 
        },
        width='stretch', 
        column_order=[
            'id', 'asset_name', 'ticker', 'transaction_date', 'transaction_type', 'trade_type',
            'quantity', 'price', 'fee', 'commission', 'realized_pnl', 'currency', 'memo'
        ]
    )

    # 4. 변경 사항 저장 버튼
    # 📌 FIX: 세션 상태 안전 접근 로직 추가
    edited_data = st.session_state.get('data_editor_transactions', {})
    edited_rows = edited_data.get('edited_rows', {})
    st.divider()
    
    if st.button("💾 Supabase DB에 거래 기록 변경 내용 저장", type="primary"):
        if save_changes_to_db(TABLE_NAME, transaction_df, edited_rows, name_to_id_map):
            st.cache_data.clear() 
            if 'transactions_df' in st.session_state:
                # 변경 사항이 DB에 반영되었으므로 캐시된 데이터를 삭제하고 새로고침
                del st.session_state['transactions_df'] 
            st.rerun() 


# ----------------------------------------------------
# 5. 자산 요약 편집 및 업데이트 탭 함수 (asset_summary 뷰)
# ----------------------------------------------------

def asset_summary_editor_tab():
    """ asset_summary 뷰 데이터를 표시하고 수정 후 저장하는 탭입니다. """
    st.header("🏠 보유 자산 요약 편집 (asset_summary)")
    TABLE_NAME = "asset_summary" 
    
    # 1. 초기 데이터 로드 및 세션 상태 관리
    if 'asset_summary_df' not in st.session_state or 'assets_df' not in st.session_state or 'accounts_df' not in st.session_state:
        transactions_df, assets_df, asset_summary_df, accounts_df = fetch_editor_data()
        st.session_state['transactions_df'] = transactions_df
        st.session_state['assets_df'] = assets_df
        st.session_state['asset_summary_df'] = asset_summary_df
        st.session_state['accounts_df'] = accounts_df
    
    summary_df = st.session_state['asset_summary_df'].copy()
    assets_df = st.session_state['assets_df'].copy()
    accounts_df = st.session_state['accounts_df'].copy()

    if summary_df.empty or assets_df.empty or accounts_df.empty:
        st.warning("데이터베이스에서 필수 데이터 ('asset_summary', 'assets', 'accounts') 로드에 실패했습니다. DB 연결 또는 테이블의 데이터 유무를 확인하세요.")
        return

    # 🌟 복잡한 양방향 매핑 준비
    
    # 1) 자산 ID-이름 맵핑
    id_to_name_map = assets_df.set_index('id')['name_kr'].to_dict()
    name_to_id_map = {v: k for k, v in id_to_name_map.items()}
    asset_name_options = list(name_to_id_map.keys())
    
    # 2) 통화 표시-DB 값 맵핑 (원화->won, 달러->usd)
    db_to_display_currency = {"won": "원", "usd": "달러"}
    display_to_db_currency = {v: k for k, v in db_to_display_currency.items()}
    currency_display_options = list(db_to_display_currency.values())
    
    # 3) 계좌 ID-표시 문자열 맵핑 (증권사 - 계좌명)
    accounts_df['account_display'] = accounts_df['brokerage'] + " - " + accounts_df['name']
    id_to_account_display_map = accounts_df.set_index('id')['account_display'].to_dict()
    account_display_to_id_map = accounts_df.set_index('account_display')['id'].to_dict()
    account_display_options = list(account_display_to_id_map.keys())

    # 화면 표시를 위한 DataFrame 전처리
    if 'asset_id' in summary_df.columns:
        summary_df['asset_name'] = summary_df['asset_id'].map(id_to_name_map)
    
    if 'account_id' in summary_df.columns:
        summary_df['account_display'] = summary_df['account_id'].map(id_to_account_display_map)

    if 'currency' in summary_df.columns:
        summary_df['currency'] = summary_df['currency'].map(db_to_display_currency).fillna(summary_df['currency'])
    
    # 2. 데이터 타입 명시적 변환
    try:
        if 'asset_id' in summary_df.columns:
            summary_df['asset_id'] = pd.to_numeric(summary_df['asset_id'], errors='coerce').fillna(0).astype(int)
        
        numeric_cols = [
            'total_quantity', 'current_valuation_price', 'total_purchase_amount', 
            'total_valuation_amount', 'average_purchase_price', 'unrealized_pnl', 
            'unrealized_return_rate'
        ]
        for col in numeric_cols:
            if col in summary_df.columns:
                summary_df[col] = pd.to_numeric(summary_df[col], errors='coerce')
                
    except Exception as e:
        st.error(f"자산 요약 데이터 전처리 중 오류 발생: {e}")
        return
    
    st.caption(f"총 {len(summary_df)}개의 보유 기록이 로드되었습니다. 수정 가능한 항목은 '계좌 정보'와 '통화'입니다.")

    # 3. st.data_editor를 사용하여 데이터 표시 및 수정 허용
    st.data_editor(
        summary_df,
        key='data_editor_asset_summary',
        column_config={
            "asset_name": st.column_config.SelectboxColumn("자산명", options=asset_name_options, required=True, disabled=True), 
            "account_display": st.column_config.SelectboxColumn("계좌 정보 (증권사-계좌명)", options=account_display_options, required=True), 
            "ticker": st.column_config.TextColumn("티커", disabled=True), 
            "currency": st.column_config.SelectboxColumn("통화", options=currency_display_options, required=True), 
            
            # 📌 FIX: D3-Format 적용 및 수정 불가 처리
            "total_quantity": st.column_config.NumberColumn("총 수량", format=",d", disabled=True),
            "current_valuation_price": st.column_config.NumberColumn("현재가", format=",.2f", disabled=True),
            "total_purchase_amount": st.column_config.NumberColumn("총 매수 금액", format=",.0f", disabled=True),
            "total_valuation_amount": st.column_config.NumberColumn("총 평가 금액", format=",.0f", disabled=True),
            "average_purchase_price": st.column_config.NumberColumn("평균 단가", format=",.2f", disabled=True),
            "unrealized_pnl": st.column_config.NumberColumn("평가 손익", format=",.0f", disabled=True),
            "unrealized_return_rate": st.column_config.NumberColumn("수익률 (%)", format=",.2f%%", disabled=True),
        },
        width='stretch', 
        column_order=[
            'asset_id', 'account_id', 'asset_name', 'account_display', 'ticker', 'currency', 'total_quantity', 
            'current_valuation_price', 'total_purchase_amount', 'total_valuation_amount', 
            'average_purchase_price', 'unrealized_pnl', 'unrealized_return_rate'
        ]
    )

    # 4. 변경 사항 저장 버튼
    # 📌 FIX: 세션 상태 안전 접근 로직 추가
    edited_data = st.session_state.get('data_editor_asset_summary', {})
    edited_rows = edited_data.get('edited_rows', {})
    st.divider()
    
    if st.button("💾 Supabase DB에 자산 요약 변경 내용 저장", type="primary"):
        if save_changes_to_db(TABLE_NAME, summary_df, edited_rows, name_to_id_map, display_to_db_currency, account_display_to_id_map):
            st.cache_data.clear()
            if 'asset_summary_df' in st.session_state:
                del st.session_state['asset_summary_df'] 
            st.rerun() 


# ----------------------------------------------------
# 6. 메인 앱 실행 로직
# ----------------------------------------------------

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

usd_krw_rate = fetch_usd_exchange_rate()
st.caption(f"현재 적용 환율 (USD/KRW): ₩{usd_krw_rate:,.2f}") 

st.divider() 

# ====================================================
# Streamlit Tabs (탭) 생성 📌 FIX: 탭 구조 복구
# ====================================================

tab1, tab2, tab3 = st.tabs([
    "📈 포트폴리오 대시보드", 
    "📝 거래 기록 편집 (Transactions)", 
    "🏠 자산 요약 편집 (Asset Summary)" 
])

with tab1:
    dashboard_tab(asset_summary_df, transaction_stats_df, portfolio_pnl_history_df, usd_krw_rate)

with tab2:
    # 거래 기록 편집 탭 복구
    data_editor_tab()

with tab3:
    # 자산 요약 편집 탭 복구
    asset_summary_editor_tab()