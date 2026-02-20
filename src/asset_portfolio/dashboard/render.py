import pandas as pd
import altair as alt
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from datetime import date, timedelta
from asset_portfolio.backend.infra.supabase_client import get_supabase_client
from asset_portfolio.backend.services.portfolio_weight_service import (
    load_asset_weight_timeseries,
    build_asset_weight_df,
    load_latest_asset_weights
)
from asset_portfolio.backend.services.portfolio_service import (
    get_portfolio_return_series,
    # load_asset_contribution_data, 
    calculate_asset_contributions
)
from asset_portfolio.backend.services.benchmark_service import (
    # load_cash_benchmark_series,
    # merge_portfolio_and_benchmark, 
    # merge_portfolio_and_benchmark_ffill,
    load_sp500_benchmark_series,
    align_portfolio_to_benchmark_calendar
)
from asset_portfolio.backend.services.manual_cost_basis_service import attach_manual_cost_basis
from asset_portfolio.backend.services.transaction_service import (
    TransactionService,
    CreateTransactionRequest,
)
from asset_portfolio.backend.infra import query
from asset_portfolio.dashboard.data import load_assets_lookup
from asset_portfolio.backend.infra.query import fetch_all_pagination, load_asset_prices


@st.cache_data(ttl=600)
def load_portfolio_return_series_cached(user_id: str, account_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    """cached wrapper for get_portfolio_return_series"""
    return get_portfolio_return_series(user_id, account_id, start_date, end_date)


@st.cache_data(ttl=600)
def load_asset_grouping_summary(user_id: str, account_id: str) -> pd.DataFrame:
    """
    자산 분류 기준(자산 유형/기초자산 클래스)별 평가금액 합계를 가져옵니다.

    - 캐시를 사용해서 동일한 계좌/사용자 요청을 빠르게 처리합니다.
    - Supabase에서 원본 데이터를 가져오고, 파이썬에서 그룹 집계를 수행합니다.
    - asset_summary_live가 비어 있으면 daily_snapshots의 최신 날짜 데이터를 대체로 사용합니다.
    """
    supabase = get_supabase_client()

    # 기본 조회: asset_summary_live + assets 조인
    query_builder = (
        supabase.table("asset_summary_live")
        .select(
            "asset_id, account_id, total_valuation_amount, "
            "assets (asset_type, underlying_asset_class)"
        )
    )

    # 계좌 선택이 "전체"인지 여부에 따라 필터 조건이 달라짐
    if account_id and account_id != "__ALL__":
        query_builder = query_builder.eq("account_id", account_id)
    else:
        # 전체 계좌 조회 시, 로그인 사용자의 계좌 리스트를 가져와서 IN 조건으로 조회
        user_accounts = query.get_accounts(user_id)
        user_account_ids = [acc["id"] for acc in user_accounts]
        if not user_account_ids:
            return pd.DataFrame(
                columns=["asset_type", "underlying_asset_class", "total_valuation_amount"]
            )
        query_builder = query_builder.in_("account_id", user_account_ids)

    rows = query_builder.execute().data or []

    # ============================================
    # 1) 우선 asset_summary_live 기반 데이터 정규화
    # ============================================
    df = pd.json_normalize(rows, sep=".") if rows else pd.DataFrame()

    # 데이터 안전성: 숫자 변환 + 결측치 기본값 처리
    if not df.empty:
        df["total_valuation_amount"] = pd.to_numeric(
            df["total_valuation_amount"], errors="coerce"
        ).fillna(0)
        df["assets.asset_type"] = df["assets.asset_type"].fillna("미분류")
        df["assets.underlying_asset_class"] = df["assets.underlying_asset_class"].fillna("미분류")

        # 표준화된 컬럼명으로 정리
        df = df.rename(
            columns={
                "assets.asset_type": "asset_type",
                "assets.underlying_asset_class": "underlying_asset_class",
            }
        )

        return df[["asset_type", "underlying_asset_class", "total_valuation_amount"]]

    # ==========================================================
    # 2) asset_summary_live가 비어 있으면 최신 스냅샷으로 대체
    # ==========================================================
    latest_query = (
        supabase.table("daily_snapshots")
        .select("date")
        .order("date", desc=True)
        .limit(1)
    )
    if account_id and account_id != "__ALL__":
        latest_query = latest_query.eq("account_id", account_id)
    else:
        user_accounts = query.get_accounts(user_id)
        user_account_ids = [acc["id"] for acc in user_accounts]
        if not user_account_ids:
            return pd.DataFrame(
                columns=["asset_type", "underlying_asset_class", "total_valuation_amount"]
            )
        latest_query = latest_query.in_("account_id", user_account_ids)

    latest_row = latest_query.execute().data or []
    if not latest_row:
        return pd.DataFrame(
            columns=["asset_type", "underlying_asset_class", "total_valuation_amount"]
        )

    latest_date = latest_row[0]["date"]

    snapshot_query = (
        supabase.table("daily_snapshots")
        .select(
            "asset_id, account_id, valuation_amount, "
            "assets (asset_type, underlying_asset_class)"
        )
        .eq("date", latest_date)
    )
    if account_id and account_id != "__ALL__":
        snapshot_query = snapshot_query.eq("account_id", account_id)
    else:
        user_accounts = query.get_accounts(user_id)
        user_account_ids = [acc["id"] for acc in user_accounts]
        if not user_account_ids:
            return pd.DataFrame(
                columns=["asset_type", "underlying_asset_class", "total_valuation_amount"]
            )
        snapshot_query = snapshot_query.in_("account_id", user_account_ids)

    snapshot_rows = snapshot_query.execute().data or []
    if not snapshot_rows:
        return pd.DataFrame(
            columns=["asset_type", "underlying_asset_class", "total_valuation_amount"]
        )

    snapshot_df = pd.json_normalize(snapshot_rows, sep=".")
    snapshot_df["valuation_amount"] = pd.to_numeric(
        snapshot_df["valuation_amount"], errors="coerce"
    ).fillna(0)
    snapshot_df["assets.asset_type"] = snapshot_df["assets.asset_type"].fillna("미분류")
    snapshot_df["assets.underlying_asset_class"] = snapshot_df["assets.underlying_asset_class"].fillna("미분류")

    snapshot_df = snapshot_df.rename(
        columns={
            "assets.asset_type": "asset_type",
            "assets.underlying_asset_class": "underlying_asset_class",
            "valuation_amount": "total_valuation_amount",
        }
    )

    return snapshot_df[["asset_type", "underlying_asset_class", "total_valuation_amount"]]


def render_asset_grouping_pie_section(user_id: str, account_id: str):
    st.subheader("🧩 동적 그룹화 차트")

    if not account_id:
        st.info("계좌를 선택해주세요.")
        return

    group_options = {
        "자산 유형 (asset_type)": "asset_type",
        "기초자산 클래스 (underlying_asset_class)": "underlying_asset_class",
    }

    # 사용자가 어떤 기준으로 묶을지 선택하도록 제공
    selected_label = st.selectbox(
        "묶을 기준을 선택하세요.",
        list(group_options.keys()),
    )
    group_key = group_options[selected_label]

    # DB에서 데이터를 가져오고, 선택된 기준으로 그룹 집계
    raw_df = load_asset_grouping_summary(user_id=user_id, account_id=account_id)
    if raw_df.empty:
        st.info("표시할 자산 데이터가 없습니다.")
        return

    # 선택한 기준으로 평가금액 합계를 계산
    grouped_df = (
        raw_df.groupby(group_key, as_index=False)["total_valuation_amount"]
        .sum()
        .sort_values("total_valuation_amount", ascending=False)
    )

    # 한글 맵핑 정의
    type_map = {
        "fund": "펀드",
        "etf": "ETF",
        "tdf": "TDF",
        "cash": "현금(예수금)",
        "stock": "주식",
        "deposit": "예적금",
        "reits": "리츠",
    }
    class_map = {
        "Multi-Asset": "멀티에셋",
        "Real Asset": "대체자산",
        "Fixed Income": "채권",
        "Equity": "주식",
        "Other": "기타",
    }
    
    # 맵핑 적용 함수
    def _map_label(val):
        s = str(val).strip()
        if group_key == "asset_type":
            lower_s = s.lower()
            return type_map.get(lower_s, s)
        elif group_key == "underlying_asset_class":
            return class_map.get(s, s)
        return s

    # 시각화를 위한 파이 차트 (Plotly)
    grouped_df["display_label"] = grouped_df[group_key].apply(_map_label)
    
    fig = px.pie(
        grouped_df,
        names="display_label",
        values="total_valuation_amount",
        hole=0.35,
        title="분류 기준별 평가금액 비중",
    )
    fig.update_traces(textposition="inside", textinfo="percent+label")
    fig.update_layout(height=360, margin=dict(t=40, l=10, r=10, b=10))

    st.plotly_chart(fig, width='stretch')

    # 표 형태로도 확인할 수 있도록 데이터프레임 출력
    display_df = grouped_df.copy()
    display_df[group_key] = display_df["display_label"] # 맵핑된 한글 적용
    
    st.dataframe(
        display_df[[group_key, "total_valuation_amount"]].rename(
            columns={
                group_key: "분류 기준",
                "total_valuation_amount": "평가금액 합계",
            }
        ),
        width='stretch',
        hide_index=True,
        column_config={
            "평가금액 합계": st.column_config.NumberColumn(
                "평가금액 합계(₩)",
                format="%d",  # 소수점 제거
            )
        }
    )
    
    
def render_kpi_section(user_id: str, account_id: str, start_date: str, end_date: str):
    st.subheader("📈 Portfolio 전체 수익률")

    if not account_id:
        st.info("계좌를 선택해주세요.")
        return

    # =========================
    # 1) 포트폴리오 시계열 (Cached)
    # =========================
    @st.cache_data(ttl=600)
    def _load_portfolio_series(u_id, acc_id, s_date, e_date):
        return get_portfolio_return_series(u_id, acc_id, s_date, e_date)

    portfolio_df = _load_portfolio_series(user_id, account_id, start_date, end_date)

    if portfolio_df.empty:
        st.warning("조회된 데이터가 없습니다.")
        return

    # =========================
    # 4) KPI 요약 카드
    # =========================
    # portfolio_return이 NaN인 경우가 있을 수 있으니, 마지막 유효값 기준으로 계산
    pf_valid = portfolio_df.dropna(subset=["portfolio_return"]).copy()

    if not pf_valid.empty:
        last = pf_valid.sort_values("date").iloc[-1]
        total_val = float(last["valuation_amount"])
        total_buy = float(last["purchase_amount"])
        pnl = total_val - total_buy
        pnl_rate = (pnl / total_buy * 100) if total_buy > 0 else 0.0
        portfolio_return_pct = float(last["portfolio_return"]) * 100
    else:
        total_val = float(portfolio_df["valuation_amount"].dropna().iloc[-1]) if portfolio_df["valuation_amount"].notna().any() else 0.0
        total_buy = float(portfolio_df["purchase_amount"].dropna().iloc[-1]) if portfolio_df["purchase_amount"].notna().any() else 0.0
        pnl = total_val - total_buy
        pnl_rate = (pnl / total_buy * 100) if total_buy > 0 else 0.0
        portfolio_return_pct = 0.0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("평가금액", f"{total_val:,.0f} 원")
    c2.metric("투자원금", f"{total_buy:,.0f} 원")
    c3.metric("평가손익", f"{pnl:,.0f} 원", delta=f"{pnl_rate:.2f}%")
    c4.metric("누적 수익률", f"{portfolio_return_pct:.2f}%")


def render_period_performance_section(user_id: str, account_id: str, start_date: str, end_date: str):
    """
    기간별 성과 분석 (Cash Flow 고려)
    """
    from asset_portfolio.backend.services.portfolio_service import calculate_period_performance
    
    st.subheader(f"🗓️ 기간별 성과 ({start_date} ~ {end_date})")

    if not account_id:
        return

    # 데이터 계산
    res = calculate_period_performance(user_id, account_id, start_date, end_date)
    
    start_val = res["start_value"]
    end_val = res["end_value"]
    net_flow = res["net_flow"]
    inv_gain = res["investment_gain"]
    ret_rate = res["return_rate"] * 100

    # Layout
    c1, c2, c3, c4, c5 = st.columns(5)
    
    c1.metric(
        "기초 자산 (Start)", 
        f"{start_val:,.0f}",
        help="선택한 기간의 시작 시점 자산 총액"
    )
    
    c2.metric(
        "순입출금 (Net Flow)", 
        f"{net_flow:,.0f}",
        delta=None, # 입출금은 좋고 나쁨이 아님
        help="기간 내 (입금 - 출금) 총액"
    )
    
    # 투자 손익: 색상 표시 (Streamlit metric delta 활용)
    c3.metric(
        "투자 손익 (Gain)", 
        f"{inv_gain:,.0f}",
        delta=f"{inv_gain:,.0f}",
        help="순수 투자로 발생한 이익/손실 (기말 - 기초 - 순입출금)"
    )
    
    c4.metric(
        "기간 수익률 (Return)", 
        f"{ret_rate:.2f}%",
        delta=f"{ret_rate:.2f}%",
        help="기간 내 평균 자산 대비 수익률 (Modified Dietz 방식)"
    )
    
    c5.metric(
        "기말 자산 (End)", 
        f"{end_val:,.0f}",
        help="선택한 기간의 종료 시점 자산 총액"
    )
    
    st.caption("※ 기간 수익률은 입출금을 고려하여 계산된 '순수 투자 성과' 입니다. (단순 수익률과 다를 수 있음)")



def render_portfolio_trend_chart(user_id: str, account_id: str, start_date: str, end_date: str):
    st.subheader("📈 자산 추세 (Trend)")
    
    if not account_id:
        st.info("계좌를 선택해주세요.")
        return

    # 데이터 조회
    df = get_portfolio_return_series(user_id, account_id, start_date, end_date)
    
    if df.empty:
        st.info("표시할 데이터가 없습니다.")
        return

    # Plotly Line Chart
    fig = go.Figure()

    # 1) 평가금액 (Line)
    fig.add_trace(go.Scatter(
        x=df["date"], 
        y=df["valuation_amount"],
        mode='lines',
        name='총 평가금액',
        # stackgroup='one',  <-- 제거: Area 차트가 0부터 시작하게 강제하는 원인
        line=dict(width=2, color='rgba(55, 128, 191, 1.0)'),
    ))

    # 2) 투자원금 (Line)
    fig.add_trace(go.Scatter(
        x=df["date"], 
        y=df["purchase_amount"],
        mode='lines',
        name='투자원금 (Net Invested)',
        line=dict(width=2, color='rgba(255, 165, 0, 1.0)', dash='dot'), # 구분을 위해 dot or lighter color
    ))

    # Y축 범위 계산 (데이터의 min/max 기준)
    # 0을 포함하지 않고 변화량을 잘 보여주도록 설정
    all_values = pd.concat([df["valuation_amount"], df["purchase_amount"]])
    min_val = all_values.min()
    max_val = all_values.max()
    margin = (max_val - min_val) * 0.1 if max_val != min_val else max_val * 0.05
    
    fig.update_layout(
        height=350,
        margin=dict(t=10, l=10, r=10, b=10),
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="top", y=1.02, xanchor="left", x=0),
    )
    # rangemode="normal"은 기본적으로 데이터 범위에 맞춤 (0 강제 안함)
    fig.update_yaxes(
        title_text="금액 (KRW)", 
        tickformat=",", 
        range=[max(0, min_val - margin), max_val + margin]
    )

    st.plotly_chart(fig, width='stretch')



def render_benchmark_comparison_section(user_id: str, account_id: str, start_date: str, end_date: str):
    st.subheader("벤치마크(S&P500)와 수익률 비교")

    if not account_id:
        st.info("계좌를 선택해 주세요.")
        return

    # =========================
    # 1) 포트폴리오 수익률 (Cached)
    # =========================
    portfolio_df = load_portfolio_return_series_cached(user_id, account_id, start_date, end_date)

    if portfolio_df.empty:
        st.warning("조회 가능한 데이터가 없습니다.")
        return

    # =========================
    # 2) 벤치마크 수익률 (S&P 500)
    # =========================
    benchmark_start = start_date
    benchmark_end = end_date
    if benchmark_start is None or benchmark_end is None:
        portfolio_dates = pd.to_datetime(portfolio_df["date"], errors="coerce").dropna()
        if not portfolio_dates.empty:
            benchmark_start = portfolio_dates.min().date()
            benchmark_end = portfolio_dates.max().date()

    benchmark_df = pd.DataFrame()
    if benchmark_start is not None and benchmark_end is not None:
        benchmark_df = load_sp500_benchmark_series(
            start_date=benchmark_start,
            end_date=benchmark_end,
        )

    # =========================
    # 3) 벤치마크 캘린더에 맞춰 forward-fill
    # =========================
    if not benchmark_df.empty:
        portfolio_df = align_portfolio_to_benchmark_calendar(portfolio_df, benchmark_df)
    else:
        st.warning("벤치마크 데이터를 불러오지 못했습니다. (네트워크/API 이슈 가능)")

    # =========================
    # 4) 차트 데이터 준비
    # =========================
    chart_df = portfolio_df[["date", "portfolio_return"]].copy()
    chart_df["date"] = pd.to_datetime(chart_df["date"]).dt.date
    chart_df["portfolio_return_pct"] = chart_df["portfolio_return"] * 100

    if not benchmark_df.empty:
        b = benchmark_df.copy()
        b["date"] = pd.to_datetime(b["date"]).dt.date
        b["benchmark_return_pct"] = b["benchmark_return"] * 100
        chart_df = chart_df.merge(
            b[["date", "benchmark_return_pct"]],
            on="date",
            how="left",
        )

    # =========================
    # 5) 이중 Y축 라인 차트 (좌: 포트폴리오, 우: 벤치마크)
    # =========================
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(
        go.Scatter(
            x=chart_df["date"],
            y=chart_df["portfolio_return_pct"],
            name="포트폴리오 수익률(%)",
            mode="lines",
        ),
        secondary_y=False,
    )

    if "benchmark_return_pct" in chart_df.columns:
        fig.add_trace(
            go.Scatter(
                x=chart_df["date"],
                y=chart_df["benchmark_return_pct"],
                name="벤치마크(S&P500) 수익률(%)",
                mode="lines",
            ),
            secondary_y=True,
        )

    fig.update_layout(
        height=350,
        margin=dict(t=10, l=10, r=10, b=10),
        legend=dict(orientation="h", yanchor="top", y=1.02, xanchor="left", x=0),
    )
    fig.update_yaxes(title_text="포트폴리오 수익률(%)", secondary_y=False)
    fig.update_yaxes(title_text="벤치마크(S&P500) 수익률(%)", secondary_y=True)

    st.plotly_chart(fig, width='stretch')
    st.caption(
        "※ 우리 포트폴리오 수익률(%)은 선택한 기간의 포트폴리오 누적 수익률을 의미합니다. "
        "(기준일 대비 자산 가치가 어느 정도 증가/감소했는지를 비율로 표시)"
    )


def render_asset_return_section(
    user_id: str,
    account_id: str,
    start_date: str,
    end_date: str,
):
    st.subheader("📈 자산별 수익률 추이")

    # ============================
    # 1. daily_snapshots + assets JOIN 조회
    # ============================
    q = query.build_daily_snapshots_query(
        select_cols="""
            date,
            asset_id,
            valuation_amount,
            purchase_amount,
            assets (
                id,
                ticker,
                name_kr
            )
            """,
        start_date=start_date,
        end_date=end_date,
        user_id=user_id,
        account_id=account_id,
    )
    data = fetch_all_pagination(q)

    if not data:
        st.info("자산별 수익률 데이터가 없습니다.")
        return

    # ============================
    # 2. DataFrame 변환 및 정규화
    # ============================
    df = pd.json_normalize(
        data,
        sep="."
    )

    # 필수 컬럼 검증 (방어 코드)
    required_cols = {
        "date",
        "asset_id",
        "valuation_amount",
        "purchase_amount",
        "assets.ticker",
        "assets.name_kr",
    }

    missing = required_cols - set(df.columns)
    if missing:
        st.error(f"필수 컬럼 누락: {missing}")
        return

    df["date"] = pd.to_datetime(df["date"])

    # ============================
    # 3. 자산 선택 UI
    # ============================
    df["asset_label"] = (
        df["assets.ticker"] + " - " + df["assets.name_kr"]
    )

    asset_options = (
        df[["asset_id", "asset_label"]]
        .drop_duplicates()
        .sort_values("asset_label")
    )

    selected_asset_label = st.selectbox(
        "자산 선택",
        asset_options["asset_label"].tolist()
    )

    selected_asset_id = asset_options[
        asset_options["asset_label"] == selected_asset_label
    ]["asset_id"].iloc[0]

    # ============================
    # 4. 선택 자산 필터링
    # ============================
    asset_df = df[df["asset_id"] == selected_asset_id].copy()
    asset_df.sort_values("date", inplace=True)

    # ============================
    # 5. 누적 수익률 계산
    # (purchase_amount 기준)
    # ============================

    asset_df = asset_df[
        (asset_df["valuation_amount"] > 0)
        & (asset_df["purchase_amount"] > 0)
    ]

    asset_df["return_rate"] = (
        asset_df["valuation_amount"] / asset_df["purchase_amount"] - 1
    )

    # ============================
    # 6. 차트 출력
    # ============================
    # ============================
    # 6. 차트 출력 (Dual Axis: 수익률(L) vs 가격(R))
    # ============================
    asset_df["date"] = pd.to_datetime(asset_df["date"]).dt.date  # 시간 제거
    
    # 가격 데이터 조회
    price_rows = load_asset_prices(selected_asset_id, start_date, end_date)
    price_df = pd.DataFrame(price_rows)
    
    # 가격 데이터 전처리 & 병합
    if not price_df.empty:
        price_df["date"] = pd.to_datetime(price_df["price_date"]).dt.date
        price_df.rename(columns={"close_price": "price"}, inplace=True)
        # 필요한 컬럼만 남기고 병합
        combined_df = pd.merge(
            asset_df, 
            price_df[["date", "price", "currency"]], 
            on="date", 
            how="left"
        )
    else:
        combined_df = asset_df.copy()
        combined_df["price"] = None

    # Plotly Dual Axis Chart 생성
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # 1) 수익률 (Left Y)
    fig.add_trace(
        go.Scatter(
            x=combined_df["date"],
            y=combined_df["return_rate"] * 100, # % 단위
            name="수익률(%)",
            mode="lines",
            line=dict(color="#2962FF", width=2),
        ),
        secondary_y=False,
    )

    # 2) 자산 가격 (Right Y) - 데이터 있을 경우만
    if not price_df.empty:
        # 통화 정보 확인 (첫 행 기준)
        curr = price_df["currency"].iloc[0].upper() if "currency" in price_df.columns and price_df["currency"].iloc[0] else ""
        price_label = f"자산 가격({curr})" if curr else "자산 가격"
        
        fig.add_trace(
            go.Scatter(
                x=combined_df["date"],
                y=combined_df["price"],
                name=price_label,
                mode="lines",
                line=dict(color="#FF6D00", width=2, dash="dot"), # 점선 등 스타일 차별화
            ),
            secondary_y=True,
        )
        fig.update_yaxes(title_text=price_label, secondary_y=True, showgrid=False)

    # Layout 설정
    fig.update_layout(
        title=f"{selected_asset_label} 수익률 및 가격 추이",
        height=400,
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=10, r=10, t=50, b=10),
    )
    
    fig.update_yaxes(title_text="수익률(%)", secondary_y=False)
    
    # Streamlit에 표시
    st.plotly_chart(fig, width='stretch')

    # ============================
    # 7. 테이블 (확인용)
    # ============================
    with st.expander("📄 원본 데이터 확인"):
        st.dataframe(
            asset_df[
                [
                    "date",
                    "valuation_amount",
                    "purchase_amount",
                    "return_rate",
                ]
            ]
        )





def render_latest_snapshot_table(user_id: str, account_id: str):
    st.subheader("🧾 최신 스냅샷 테이블")

    if not account_id:
        st.info("계좌를 선택해주세요.")
        return

    supabase = get_supabase_client()

    latest_query = (
        supabase.table("daily_snapshots")
        .select("date")
        .order("date", desc=True)
        .limit(1)
    )
    if account_id != "__ALL__":
        latest_query = latest_query.eq("account_id", account_id)
    else:
        user_accounts = query.get_accounts(user_id)
        user_account_ids = [acc['id'] for acc in user_accounts]
        if not user_account_ids:
            st.info("daily_snapshots 데이터가 없습니다.")
            return
        latest_query = latest_query.in_("account_id", user_account_ids)


    latest_row = latest_query.execute().data or []

    if not latest_row:
        st.info("daily_snapshots 데이터가 없습니다.")
        return

    latest_date = latest_row[0]["date"]

    rows_query = (
        supabase.table("daily_snapshots")
        .select(
            "date, account_id, asset_id, quantity, purchase_price, valuation_price, "
            "valuation_amount, purchase_amount, currency, "
            "assets (name_kr, asset_type, price_source), accounts (name)"
        )
        .eq("date", latest_date)
    )
    if account_id != "__ALL__":
        rows_query = rows_query.eq("account_id", account_id)
    else:
        user_accounts = query.get_accounts(user_id)
        user_account_ids = [acc['id'] for acc in user_accounts]
        rows_query = rows_query.in_("account_id", user_account_ids)


    rows = rows_query.execute().data or []

    if not rows:
        st.info("최신 스냅샷 데이터를 불러오지 못했습니다.")
        return

    df = pd.json_normalize(rows, sep=".")

    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df = df[df["quantity"].fillna(0) != 0]
    if df.empty:
        st.info("최신 스냅샷에 수량이 0인 자산만 있습니다.")
        return

    df = attach_manual_cost_basis(df, user_id=user_id)

    df["purchase_amount"] = pd.to_numeric(df["purchase_amount"], errors="coerce")
    df["valuation_amount"] = pd.to_numeric(df["valuation_amount"], errors="coerce")
    if "manual_principal" in df.columns:
        df["manual_principal"] = pd.to_numeric(df["manual_principal"], errors="coerce")

    df["profit_base_amount"] = df["purchase_amount"]
    manual_mask = df["assets.price_source"].fillna("").str.lower().str.strip().eq("manual")
    df.loc[manual_mask, "profit_base_amount"] = df.loc[manual_mask, "manual_principal"]

    df["profit_amount"] = df["valuation_amount"] - df["profit_base_amount"]
    df["profit_rate"] = df.apply(
        lambda r: (r["profit_amount"] / r["profit_base_amount"] * 100)
        if float(r["profit_base_amount"] or 0) > 0
        else 0.0,
        axis=1,
    )

    currency_map = {
        "krw": "원화",
        "usd": "달러",
    }
    df["currency"] = df["currency"].apply(
        lambda x: currency_map.get(str(x).lower(), x) if x is not None else x
    )

    asset_type_map = {
        "cash": "예수금",
        "stock": "주식",
        "deposit": "예적금",
        "etf": "ETF",
        "fund": "펀드류",
        "tdf": "TDF",
    }
    df["assets.asset_type"] = df["assets.asset_type"].apply(
        lambda x: asset_type_map.get(str(x).lower(), x) if x is not None else x
    )

    df = df.rename(
        columns={
            "accounts.name": "계좌명",
            "assets.name_kr": "자산명",
            "quantity": "수량",
            "purchase_price": "매수단가",
            "valuation_price": "현재단가",
            "manual_principal": "원금(수동자산)",
            "valuation_amount": "평가금액",
            "profit_amount": "수익금액",
            "profit_rate": "수익률",
            "currency": "통화",
            "assets.asset_type": "자산 타입",
        }
    )

    columns = [
        "계좌명",
        "자산명",
        "수량",
        "매수단가",
        "현재단가",
        "원금(수동자산)",
        "평가금액",
        "수익금액",
        "수익률",
        "통화",
        "자산 타입",
    ]

    st.caption(f"기준일: {latest_date}")

    display_df = df[columns].copy()

    profit_amount_col = columns[7]
    profit_rate_col = columns[8]
    asset_name_col = columns[1]
    def _format_quantity(value):
        if pd.isna(value):
            return ""
        try:
            num = float(value)
        except (TypeError, ValueError):
            return value
        if num.is_integer():
            return f"{num:,.0f}"
        return f"{num:,.2f}"

    format_map = {
        columns[2]: _format_quantity,
        columns[3]: "{:,.2f}",
        columns[4]: "{:,.2f}",
        columns[5]: "{:,.0f}",
        columns[6]: "{:,.0f}",
        columns[7]: "{:,.0f}",
        profit_rate_col: "{:.2f}%",
    }

    for col in format_map:
        display_df[col] = pd.to_numeric(display_df[col], errors="coerce")

    profit_amount_idx = display_df.columns.get_loc(profit_amount_col)
    profit_rate_idx = display_df.columns.get_loc(profit_rate_col)
    asset_name_idx = display_df.columns.get_loc(asset_name_col)

    def _profit_color(row):
        rate = row[profit_rate_col]
        if pd.isna(rate):
            return [""] * len(row)
        if rate > 0:
            color = "color: red"
        elif rate < 0:
            color = "color: blue"
        else:
            color = ""
        styles = [""] * len(row)
        styles[asset_name_idx] = color
        styles[profit_amount_idx] = color
        styles[profit_rate_idx] = color
        return styles

    styled_df = display_df.style.format(format_map).apply(_profit_color, axis=1)

    st.dataframe(styled_df, width='stretch')


def render_account_selector(accounts: list):
    st.sidebar.subheader("🏦 계좌 선택")

    if not accounts:
        st.sidebar.warning("등록된 계좌가 없습니다.")
        return None

    # 사용자에게 보여줄 label → account_id 매핑
    options = {
        f"{a['brokerage']} | {a['name']}": a["id"]
        for a in accounts
    }

    # ✅ 전체 계좌 옵션 추가 (맨 위)
    options = {"전체 계좌 (ALL)": "__ALL__", **options}

    # 계좌 선택 UI
    selected_label = st.sidebar.selectbox(
        "조회할 계좌를 선택하세요",
        options=list(options.keys()),
        index=0,
        key="account_selector_label",
    )

    return options[selected_label]




def _get_min_snapshot_date(user_id: str, account_id: str):
    """
    daily_snapshots의 최소 날짜를 조회한다.
    - YTD 보정에 사용
    """
    supabase = get_supabase_client()
    q = (
        supabase.table("daily_snapshots")
        .select("date")
        .order("date", desc=False)
        .limit(1)
    )
    if account_id and account_id != "__ALL__":
        q = q.eq("account_id", account_id)
    else:
        # '전체'일 경우 user_id에 속한 모든 계좌를 대상으로 함
        from asset_portfolio.backend.infra import query
        user_accounts = query.get_accounts(user_id)
        user_account_ids = [acc['id'] for acc in user_accounts]
        if not user_account_ids:
            return None
        q = q.in_("account_id", user_account_ids)


    rows = q.execute().data or []
    if not rows:
        return None

    return pd.to_datetime(rows[0]["date"], errors="coerce").date()

def resolve_date_range(user_id: str, period: str, account_id: str):
    """
    기간 코드("오늘", "일주일", "한달", "3달(1분기)", "YTD(올해)", "ALL")를
    실제 조회용 start_date, end_date로 변환
    """
    end_date = date.today()

    if period == "오늘":
        start_date = end_date
    elif period == "일주일":
        start_date = end_date - timedelta(days=7)
    elif period == "한달":
        start_date = end_date - timedelta(days=30)
    elif period == "3달(1분기)":
        start_date = end_date - timedelta(days=90)
    elif period == "YTD(올해)":
        start_date = date(end_date.year, 1, 1)
    elif period == "ALL":
        start_date = None
        end_date = None
    else:
        raise ValueError(f"Unknown period: {period}")
    
    # YTD 구간이 비는 경우, 실제 데이터 시작일로 보정한다.
    note = None
    if period == "YTD(올해)":
        min_date = _get_min_snapshot_date(user_id, account_id)
        if min_date and start_date and min_date > start_date:
            start_date = min_date
            note = f"YTD 구간에 데이터가 없어 시작일을 {min_date}로 보정했습니다."

    return start_date, end_date, note


def render_period_selector(user_id: str, account_id: str):
    st.sidebar.subheader("📅 기간 선택")

    period = st.sidebar.radio(
        "조회 기간",
        options=["오늘", "일주일", "한달", "3달(1분기)", "YTD(올해)", "ALL"],
        index=1  # 기본값: "일주일"
    )

    start_date, end_date, note = resolve_date_range(user_id, period, account_id)
    if note:
        st.sidebar.caption(note)
    return start_date, end_date



def render_asset_weight_section(user_id: str, account_id: str, start_date: str, end_date: str):
    st.subheader("📊 자산 비중 변화")

    rows = load_asset_weight_timeseries(
        user_id=user_id,
        account_id=account_id,
        start_date=start_date,
        end_date=end_date,
    )
    
    df = build_asset_weight_df(rows)
    
    # 총액이 0인 날짜는 제거(의미 없는 구간 제거)
    # df는 build_asset_weight_df 결과(valuation_amount_krw, total_amount_krw가 있음)
    if "total_amount_krw" not in df.columns:
        st.warning("자산 비중 데이터에 total_amount_krw 컬럼이 없습니다.")
        return
        
    df = df[df["total_amount_krw"] > 0].copy()
    if df.empty:
        st.info("자산 비중 데이터가 없습니다. (평가금액 합계가 0인 날짜만 존재)")
        return

    # =========================
    # ✅ 안전 가드: asset_id가 없으면 pivot/집계 불가
    # =========================
    if "asset_id" not in df.columns:
        st.error("build_asset_weight_df() 결과에 asset_id가 없습니다. (pivot 안정성을 위해 필수)")
        with st.expander("🔎 디버깅: build_asset_weight_df() 결과 확인"):
            st.write("columns =", list(df.columns))
            st.dataframe(df.head(50))
        return

    # =========================
    # ✅ ALL 모드: (date, asset_id) 기준으로 유일화 + weight 재계산
    # =========================
    if account_id == "__ALL__":
        # valuation_amount가 있어야 전체 평가금액 합산 가능
        if "valuation_amount" not in df.columns:
            st.error("ALL 모드 합산을 위해 valuation_amount 컬럼이 필요합니다.")
            with st.expander("🔎 디버깅: df 확인"):
                st.write("columns =", list(df.columns))
                st.dataframe(df.head(50))
            return

        df["valuation_amount"] = pd.to_numeric(df["valuation_amount"], errors="coerce").fillna(0.0)
        df["asset_id"] = pd.to_numeric(df["asset_id"], errors="coerce")
        df = df.dropna(subset=["asset_id"])
        df["asset_id"] = df["asset_id"].astype(int)

        # ✅ (date, asset_id)로 합산
        df_agg = (
            df.groupby(["date", "asset_id"], as_index=False)
              .agg(
                  valuation_amount=("valuation_amount", "sum"),
                  asset_name=("asset_name", "first"),
              )
        )

        # ✅ date_total 계산
        df_agg["date_total"] = df_agg.groupby("date")["valuation_amount"].transform("sum")

        # ✅ 0 division 방지
        df_agg["weight"] = 0.0
        mask = df_agg["date_total"] > 0
        df_agg.loc[mask, "weight"] = df_agg.loc[mask, "valuation_amount"] / df_agg.loc[mask, "date_total"]

        df = df_agg[["date", "asset_id", "asset_name", "weight"]].copy()

    # =========================
    # ✅ 시각화 개선 (Plotly Area Chart + Top N + Others)
    # =========================
    
    # 어떤 경로에서 오든 weight 컬럼을 안전하게 선택
    weight_col = None
    for c in ["weight_krw", "weight", "weight_pct", "weight_krw_pct"]:
        if c in df.columns:
            weight_col = c
            break

    if weight_col is None:
        st.error(f"자산 비중 컬럼이 없습니다. df.columns={list(df.columns)}")
        return

    # 날짜 시간 제거
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df.dropna(subset=["date"])

    # --- Pagination (Rank Range) ---
    # 1. 자산별 평균 비중 계산 및 정렬
    avg_weights = df.groupby("asset_name")[weight_col].mean().sort_values(ascending=False)
    all_sorted_assets = avg_weights.index.tolist()
    total_assets = len(all_sorted_assets)
    
    # 2. 페이지네이션 UI
    PAGE_SIZE = 10
    options = []
    import math
    num_pages = math.ceil(total_assets / PAGE_SIZE) if total_assets > 0 else 1
    
    for i in range(num_pages):
        start = i * PAGE_SIZE + 1
        end = min((i + 1) * PAGE_SIZE, total_assets)
        options.append(f"Top {start}~{end}위")
        
    selected_page = st.selectbox("순위 구간 선택", options, index=0)
    
    # 3. 선택된 구간의 자산 필터링
    page_idx = options.index(selected_page)
    start_idx = page_idx * PAGE_SIZE
    end_idx = start_idx + PAGE_SIZE
    
    target_assets = all_sorted_assets[start_idx:end_idx]
    
    df_filtered = df[df["asset_name"].isin(target_assets)].copy()
    
    # 4. 정렬 (범례 순서 보장)
    df_filtered["asset_name"] = pd.Categorical(df_filtered["asset_name"], categories=target_assets, ordered=True)
    df_filtered = df_filtered.sort_values(["date", "asset_name"])

    # 5. 퍼센트 변환
    df_filtered["weight_pct"] = df_filtered[weight_col] * 100

    fig = px.area(
        df_filtered,
        x="date",
        y="weight_pct",
        color="asset_name",
        title=f"자산 비중 변화 ({selected_page})",
        labels={"weight_pct": "비중(%)", "asset_name": "자산명", "date": "날짜"},
        groupnorm=None
    )
    
    fig.update_layout(
        height=400,
        margin=dict(t=40, l=10, r=10, b=10),
        hovermode="x unified",
        yaxis_title="비중(%)"
    )
    # y축 범위: Top 1~10이면 0~100 고정, 그 외에는 데이터에 맞게 자동 (작은 비중도 잘 보이게)
    if page_idx == 0:
        fig.update_yaxes(range=[0, 100])
    
    st.plotly_chart(fig, width='stretch')

    st.caption(
        "※ 내 전체 자산(KRW 환산 기준)에서 각 자산이 차지하는 비율(%)이 시간에 따라 어떻게 변했는지를 보여줍니다."
        "특정 자산 가격이 급등하거나, 추가 매수를 했을 때 비중 영역이 커지는 것을 볼 수 있습니다. (리밸런싱 참고용)"
    )


    with st.expander("📄 디버깅: weight 원본"):
        st.dataframe(df_filtered.sort_values(["date", weight_col], ascending=[True, False]).head(200))


def render_asset_contribution_section(
    user_id: str,
    account_id: str,
    start_date: str,
    end_date: str,
):
    st.subheader("🧩 자산별 수익률 기여도")

    if not account_id:
        st.info("계좌를 선택해주세요.")
        return

    snapshots = query.load_asset_contribution_data(
        user_id, account_id, start_date, end_date
    )

    df = calculate_asset_contributions(snapshots)

    if df.empty:
        st.warning("기여도 데이터를 계산할 수 없습니다.")
        return

    # 자산명 join
    assets = load_assets_lookup()
    df = df.merge(assets, on="asset_id", how="left")

    st.dataframe(
        df.sort_values("date", ascending=False),
        height=350,
        width='stretch'
    )

    st.caption("※ 전일 포트폴리오 대비 기여도 (%)")


def render_asset_contribution_stacked_area(
    user_id: str,
    account_id: str,
    start_date: str,
    end_date: str,
):
    st.subheader("🧩 자산별 누적 기여도 (Stacked Area)")

    if not account_id:
        st.info("계좌를 선택해주세요.")
        return

    snapshots = query.load_asset_contribution_data(user_id, account_id, start_date, end_date)
    df = calculate_asset_contributions(snapshots)

    if df.empty:
        st.warning("기여도 데이터를 계산할 수 없습니다.")
        return

    # 자산명 조인
    assets = load_assets_lookup()
    df = df.merge(assets[["asset_id", "name_kr"]], on="asset_id", how="left")
    df["name_kr"] = df["name_kr"].fillna(df["asset_id"].astype(str))

    # =========================
    # 누적 기여도 계산
    # =========================
    df = df.sort_values(["asset_id", "date"])
    df["cum_contribution"] = df.groupby("asset_id")["contribution"].cumsum()
    df["cum_contribution_pct"] = df["cum_contribution"] * 100

    # 너무 많은 자산이면 상위 N개만 (UX 보호)
    top_n = st.slider("표시할 자산 개수(상위 누적 기여도 기준)", 5, 30, 6)

    latest_cum = (
        df.groupby(["asset_id", "name_kr"], as_index=False)["cum_contribution"]
        .last()
        .sort_values("cum_contribution", ascending=False)
    )
    top_assets = set(latest_cum.head(top_n)["asset_id"].tolist())
    df_plot = df[df["asset_id"].isin(top_assets)].copy()
    
    if df_plot.empty:
        st.warning("누적 기여도 차트에 표시할 데이터가 없습니다. (필터링 결과 empty)")
        return
    
    df_plot["date"] = pd.to_datetime(df_plot["date"])  # ✅ datetime 유지
    
    # =========================
    # Altair stacked area
    # =========================
    chart = (
        alt.Chart(df_plot)
        .mark_area()
        .encode(
            # 2번 방법: axis format을 날짜만 나오도록 강제
            x=alt.X("date:T", title="Date", axis=alt.Axis(format="%Y-%m-%d")),
            # 문자열 날짜는 O(Ordinal)로 처리 → 시간(12 PM) 표시가 사라짐
            # 날짜를 “시간 데이터”가 아니라 “범주(ordered)”로 처리(단점: 기간이 길면 틱이 너무 많아질 수 있습니다.)
            # x=alt.X("date:O", title="Date"),
            # x=alt.X("date:T", title="Date"),
            y=alt.Y("cum_contribution_pct:Q", stack="zero", title="누적 기여도(%)"),
            color=alt.Color("name_kr:N", title="자산"),
            tooltip=[
                alt.Tooltip("date:T", title="Date"),
                alt.Tooltip("name_kr:N", title="자산"),
                alt.Tooltip("cum_contribution_pct:Q", title="누적기여도(%)", format=".2f"),
            ],
        )
        .properties(height=350)
    )

    st.altair_chart(chart, width='stretch')

    with st.expander("📄 누적 기여도 원본"):
        st.dataframe(
            df_plot[["date", "asset_id", "name_kr", "contribution_pct", "cum_contribution_pct"]]
            .sort_values(["date", "cum_contribution_pct"], ascending=[True, False])
        )



def render_portfolio_treemap(
    user_id: str,
    account_id: str,
    start_date: str,
    end_date: str,
):
    st.subheader("🗺️ Portfolio Treemap")

    if not account_id:
        st.info("계좌를 선택해주세요.")
        return

    mode = st.radio("Treemap 모드", ["현재 비중(평가금액)", "기간 누적 기여도"], index=0, horizontal=True)

    # ✅ Plotly 표시용 한글 라벨 (hover, legend 등에 반영)
    LABELS = {
        "valuation_amount": "평가금액",
        "name_kr": "자산명",
        "asset_type": "자산유형",
        "market": "시장",
        "cum_pct": "누적 기여도(%)",
        "abs_cum": "누적 기여도(절대)",
    }

    assets = load_assets_lookup()

    if mode == "현재 비중(평가금액)":
        # df_w는 최소 컬럼: ['asset_id','valuation_amount','name_kr','asset_type','market'] 를 가지도록 준비
        df_w = load_latest_asset_weights(user_id, account_id, start_date, end_date)
        if df_w.empty:
            st.warning("해당 기간에 daily_snapshots 데이터가 없습니다.")
            return

        df_w = df_w.merge(assets[["asset_id", "name_kr", "asset_type", "market"]], on="asset_id", how="left")
        df_w["name_kr"] = df_w["name_kr"].fillna(df_w["asset_id"].astype(str))

        leaf_count = int(df_w["asset_id"].nunique())  # ✅ 말단 개수 근사

        # ✅ 말단이 적으면 더 크게, 많으면 덜 크게(숫자를 하드코딩하지만 "데이터에 따라 자동 변화" = adaptive)
        # - 최소/최대만 정해두면 사용자 입장에서는 "자동"으로 느껴집니다.
        base = 22
        fontSizeByLeaf = max(12, min(base, int(28 - leaf_count * 0.6)))

        # ✅ KRW 환산이 있으면 그 값을 사용
        value_col = "valuation_amount_krw" if "valuation_amount_krw" in df_w.columns else "valuation_amount"

        if df_w.empty or df_w[value_col].sum() <= 0:
            st.warning("표시할 평가금액 데이터가 없습니다. (스냅샷 생성/수동입력 여부를 확인하세요)")
            return
        
        fig = px.treemap(
            df_w,
            path=["market", "asset_type", "name_kr"],
            values=value_col,
            # ✅ 자산유형별로 색을 다르게 주면 시각적으로 훨씬 구분이 잘 됩니다.
            color="asset_type",
            # ✅ 여러 색을 제공하는 팔레트(원하는 것으로 바꿔도 됨)
            color_discrete_sequence=px.colors.qualitative.Alphabet,
            labels=LABELS,
            hover_data={
                "valuation_amount": ":,.0f",
                "market": True,
                "asset_type": True,
                "name_kr": True,
            }
        )
        fig.update_layout(height=550)
        fig.update_layout(margin=dict(t=20, l=10, r=10, b=10))
        fig.update_traces(
            hovertemplate="<b>%{label}</b><br>평가금액=%{value:,.0f}<extra></extra>"
        )
        fig.update_traces(textfont_size=fontSizeByLeaf)

        st.plotly_chart(fig, width='stretch')
        st.caption("※ 마지막 스냅샷 날짜 기준 평가금액 Treemap")        

        with st.expander("📄 데이터 원본"):
            st.dataframe(df_w.sort_values(["date"], ascending=[True]))

    else:
        # 기간 누적 기여도
        snapshots = query.load_asset_contribution_data(user_id, account_id, start_date, end_date)
        df_c = calculate_asset_contributions(snapshots)
        if df_c.empty:
            st.warning("기여도 데이터를 계산할 수 없습니다.")
            return

        df_c = df_c.sort_values(["asset_id", "date"])
        df_c["cum_contribution"] = df_c.groupby("asset_id")["contribution"].cumsum()

        latest = (
            df_c.groupby("asset_id", as_index=False)["cum_contribution"]
            .last()
        )

        latest = latest.merge(assets[["asset_id", "name_kr", "asset_type", "market"]], on="asset_id", how="left")
        latest["name_kr"] = latest["name_kr"].fillna(latest["asset_id"].astype(str))

        latest["abs_cum"] = latest["cum_contribution"].abs()
        latest["cum_pct"] = latest["cum_contribution"] * 100

        leaf_count = int(latest["asset_id"].nunique())
        base = 22
        fontSizeByLeaf = max(12, min(base, int(28 - leaf_count * 0.6)))


        fig = px.treemap(
            latest,
            path=["market", "asset_type", "name_kr"],
            values="abs_cum",
            color="cum_pct",
            color_continuous_scale=px.colors.diverging.RdYlGn,
            labels=LABELS,
        )
        fig.update_layout(height=550)
        fig.update_layout(margin=dict(t=20, l=10, r=10, b=10))
        fig.update_traces(
            hovertemplate="<b>%{label}</b><br>누적기여도=%{value:,.0f}<extra></extra>"
        )
        fig.update_traces(textfont_size=fontSizeByLeaf)
        st.plotly_chart(fig, width='stretch')
        st.caption("※ 기간 누적 기여도 Treemap (면적=절대값, 색=방향/크기)")


def render_asset_contribution_section_full(
    user_id: str,
    account_id: str,
    start_date: str,
    end_date: str,
):
    st.subheader("🧩 자산별 수익률 기여도 요약")

    if not account_id:
        st.info("계좌를 선택해주세요.")
        return

    # =========================
    # 1) 데이터 로드 + 기여도 계산
    # =========================
    snapshots = query.load_asset_contribution_data(user_id, account_id, start_date, end_date)
    df = calculate_asset_contributions(snapshots)

    if df.empty:
        st.warning("기여도 데이터를 계산할 수 없습니다.")
        return

    assets = load_assets_lookup()
    df = df.merge(
        assets[["asset_id", "name_kr", "asset_type", "market"]],
        on="asset_id",
        how="left",
    )
    df["name_kr"] = df["name_kr"].fillna(df["asset_id"].astype(str))

    # =========================
    # 2) 누적 기여도 계산 (자산별)
    # =========================
    df = df.sort_values(["asset_id", "date"])
    df["cum_contribution"] = df.groupby("asset_id")["contribution"].cumsum()
    df["cum_contribution_pct"] = df["cum_contribution"] * 100

    # 최신 날짜 기준 누적 기여도 스냅샷
    latest = (
        df.groupby(["asset_id", "name_kr", "asset_type", "market"], as_index=False)
        .last()[["asset_id", "name_kr", "asset_type", "market", "cum_contribution", "cum_contribution_pct"]]
        .sort_values("cum_contribution", ascending=False)
    )

    # =========================
    # 3) 요약 카드 (Top 3 / Bottom 3)
    # =========================
    st.markdown("#### 📌 이번 기간 ‘성과 만든 자산’ / ‘성과 까먹은 자산’")

    top_n = 3
    top = latest.head(top_n).copy()
    bottom = latest.tail(top_n).sort_values("cum_contribution").copy()

    colL, colR = st.columns(2)

    with colL:
        st.markdown("**상위 기여 Top 3**")
        if top.empty:
            st.info("Top 기여 자산이 없습니다.")
        else:
            for i, (_, r) in enumerate(top.iterrows(), start=1):
                st.metric(
                    label=f"{i}. {r['name_kr']}",
                    value=f"{r['cum_contribution_pct']:.2f}%",
                )

    with colR:
        st.markdown("**하위 기여 Bottom 3**")
        if bottom.empty:
            st.info("Bottom 기여 자산이 없습니다.")
        else:
            for i, (_, r) in enumerate(bottom.iterrows(), start=1):
                st.metric(
                    label=f"{i}. {r['name_kr']}",
                    value=f"{r['cum_contribution_pct']:.2f}%",
                )

    st.caption("※ 누적 기여도는 ‘전일 포트폴리오 평가금액 대비 일간 기여도’를 누적한 값입니다.")

    st.divider()

    # =========================
    # 4) Stacked Area (누적 기여도)
    # =========================
    render_asset_contribution_stacked_area(user_id, account_id, start_date, end_date)

    # =========================
    # 5) 디버깅/검증용 테이블
    # =========================
    with st.expander("📄 기여도 계산 결과(자산별 누적) 확인"):
        st.dataframe(
            latest.rename(columns={
                "cum_contribution_pct": "누적기여도(%)",
                "name_kr": "자산명",
                "market": "시장",
                "asset_type": "유형",
            })[
                ["자산명", "시장", "유형", "누적기여도(%)"]
            ],
            height=400,
            width='stretch'
        )


def render_transactions_table_section(user_id: str, account_id: str, start_date: str, end_date: str):
    st.subheader("거래 내역")

    supabase = get_supabase_client()
    q = (
        supabase.table("transactions")
        .select("""
            id,
            account_id,
            asset_id,
            transaction_date,
            trade_type,
            quantity,
            price,
            fee,
            tax,
            memo,
            assets ( ticker, name_kr, currency ),
            accounts ( name, brokerage, old_owner, type )
        """)
        .order("transaction_date", desc=True)
    )

    if start_date is not None:
        q = q.gte("transaction_date", start_date)
    if end_date is not None:
        q = q.lte("transaction_date", end_date)

    if account_id and account_id != "__ALL__":
        q = q.eq("account_id", account_id)
    else:
        user_accounts = query.get_accounts(user_id)
        user_account_ids = [acc['id'] for acc in user_accounts]
        if not user_account_ids:
            st.info("선택한 기간에 거래 내역이 없습니다.")
            return
        q = q.in_("account_id", user_account_ids)

    response = q.execute()
    rows = response.data or []

    if not rows:
        st.info("선택한 기간에 거래 내역이 없습니다.")
        return

    df = pd.DataFrame(rows)
    df_raw = df.copy()

    # 수정/삭제 UI용 라벨 계산 (표시용 컬럼은 원본과 분리)
    if "accounts" not in df_raw.columns:
        df_raw["accounts"] = None
    if "assets" not in df_raw.columns:
        df_raw["assets"] = None

    trade_type_kr_map = {
        "BUY": "매수",
        "SELL": "매도",
        "DEPOSIT": "입금",
        "WITHDRAW": "출금",
    }
    df_raw["transaction_date"] = pd.to_datetime(df_raw["transaction_date"]).dt.date
    df_raw["trade_type_kr"] = df_raw["trade_type"].map(trade_type_kr_map).fillna(df_raw["trade_type"])
    df_raw["asset_label"] = df_raw["assets"].apply(
        lambda x: f"{(x or {}).get('ticker', '')} | {(x or {}).get('name_kr', '')}".strip(" |")
    )
    df_raw["account_label"] = df_raw["accounts"].apply(
        lambda x: f"{(x or {}).get('brokerage', '')} | {(x or {}).get('name', '')} ({(x or {}).get('owner', '')})".strip(" |")
    )

    # accounts 컬럼이 dict(JSON)로 내려오면 name만 추출해 표시
    if "accounts" in df.columns:
        df["account_name"] = df["accounts"].apply(
            lambda x: (x or {}).get("name")
        )
        df = df.drop(columns=["accounts"], errors="ignore")

    # assets dict에서 표시용 컬럼 추출
    df["ticker"] = df["assets"].apply(lambda x: (x or {}).get("ticker"))
    df["asset_name"] = df["assets"].apply(lambda x: (x or {}).get("name_kr"))
    df["asset_currency"] = df["assets"].apply(lambda x: (x or {}).get("currency"))

    currency_map = {
        "krw": "원",
        "usd": "달러",
    }
    df["asset_currency"] = df["asset_currency"].apply(
        lambda x: currency_map.get(str(x).lower(), x) if x is not None else x
    )

    # id/내부키/원본 dict 컬럼 숨기기
    df = df.drop(columns=["id", "account_id", "asset_id", "assets"], errors="ignore")

    # =========================
    # 컬럼명 표시용 매핑
    # =========================
    COL_KR = {
        "transaction_date": "거래일",
        "trade_type": "거래구분",
        "ticker": "티커",
        "asset_name": "자산명",
        "asset_currency": "통화",
        "quantity": "수량/금액",
        "price": "가격",
        "fee": "수수료",
        "tax": "세금",
        "memo": "메모",
        "account_name": "계좌",
    }
    TRADE_TYPE_KR = {
        "BUY": "매수",
        "SELL": "매도",
        "DEPOSIT": "입금",
        "WITHDRAW": "출금",
    }

    df["trade_type"] = df["trade_type"].map(TRADE_TYPE_KR).fillna(df["trade_type"])
    df["transaction_date"] = pd.to_datetime(df["transaction_date"]).dt.date

    df_display = df.rename(columns=COL_KR)

    display_order = [
        "거래일", "거래구분", "티커", "자산명", "통화",
        "수량/금액", "가격", "수수료", "세금", "계좌", "메모"
    ]

    cols = [c for c in display_order if c in df_display.columns] + [c for c in df_display.columns if c not in display_order]
    df_display = df_display[cols]

    st.dataframe(df_display, width="stretch")

    with st.expander("✏️ 거래 수정/삭제"):
        tx_rows = df_raw.sort_values("transaction_date", ascending=False).to_dict("records")
        if not tx_rows:
            st.info("수정/삭제할 거래가 없습니다.")
            return

        tx_label_map = {
            r["id"]: f"{r['transaction_date']} | {r['asset_label']} | {r['trade_type_kr']} | qty={r['quantity']} | price={r['price']} | id={r['id']}"
            for r in tx_rows
        }

        selected_tx_id = st.selectbox(
            "수정/삭제할 거래 선택",
            options=[r["id"] for r in tx_rows],
            format_func=lambda tid: tx_label_map.get(tid, str(tid)),
        )

        selected = next(r for r in tx_rows if r["id"] == selected_tx_id)

        st.caption(f"계좌: {selected.get('account_label', '')}")
        st.caption(f"자산: {selected.get('asset_label', '')}")

        trade_type_options = ["BUY", "SELL", "DEPOSIT", "WITHDRAW"]
        trade_type_labels = {
            "BUY": "매수",
            "SELL": "매도",
            "DEPOSIT": "입금",
            "WITHDRAW": "출금",
        }
        trade_type = st.selectbox(
            "거래 구분",
            options=trade_type_options,
            index=trade_type_options.index(selected["trade_type"]),
            format_func=lambda v: trade_type_labels.get(v, v),
        )

        tx_date = st.date_input("거래일", value=selected["transaction_date"])
        quantity = st.number_input("수량/금액", min_value=0.0, value=float(selected["quantity"] or 0.0), step=1.0)

        if trade_type in {"DEPOSIT", "WITHDRAW"}:
            price = 1.0
            st.number_input("가격", min_value=0.0, value=1.0, step=1.0, disabled=True)
        else:
            price = st.number_input("가격", min_value=0.0, value=float(selected["price"] or 0.0), step=1.0)

        fee = st.number_input("수수료", min_value=0.0, value=float(selected.get("fee") or 0.0), step=1.0)
        tax = st.number_input("세금", min_value=0.0, value=float(selected.get("tax") or 0.0), step=1.0)
        memo = st.text_input("메모", value=selected.get("memo") or "")

        auto_cash = st.checkbox("BUY/SELL 자동 CASH 거래도 함께 조정", value=True)

        col_u, col_d = st.columns(2)
        with col_u:
            update_clicked = st.button("거래 수정 반영", type="primary")
        with col_d:
            delete_clicked = st.button("거래 삭제", type="secondary")

        if update_clicked:
            try:
                req = CreateTransactionRequest(
                    account_id=str(selected["account_id"]),
                    asset_id=int(selected["asset_id"]),
                    transaction_date=tx_date,
                    trade_type=str(trade_type),
                    quantity=float(quantity),
                    price=float(price),
                    fee=float(fee),
                    tax=float(tax),
                    memo=memo if memo else None,
                )
                with st.spinner("거래 수정 및 스냅샷 리빌드 중..."):
                    result = TransactionService.update_transaction_and_rebuild(
                        int(selected_tx_id),
                        req,
                        auto_cash=auto_cash,
                    )
                st.success(
                    f"수정 완료. (리빌드: {result['rebuilt_start_date']} ~ {result['rebuilt_end_date']})"
                )
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"수정 실패: {e}")

        if delete_clicked:
            try:
                with st.spinner("거래 삭제 및 스냅샷 리빌드 중..."):
                    result = TransactionService.delete_transaction_and_rebuild(
                        int(selected_tx_id),
                        auto_cash=auto_cash,
                    )
                st.success(
                    f"삭제 완료. (리빌드: {result['rebuilt_start_date']} ~ {result['rebuilt_end_date']})"
                )
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"삭제 실패: {e}")


def render_asset_transaction_history(user_id: str, account_id: str):
    """
    보유 중인 자산을 선택하여 해당 자산의 전체 거래 내역을 조회합니다.
    
    특징:
    - 현재 보유 중인 자산(quantity > 0)만 드롭다운에 표시
    - 선택한 자산의 전체 거래 내역을 날짜 역순으로 표시
    - 거래 메모를 포함하여 매수 이유 등을 한눈에 파악 가능
    """
    st.subheader("📝 자산별 거래 내역")
    
    # ================================
    # 1. 보유 중인 자산 조회 (quantity > 0)
    # ================================
    supabase = get_supabase_client()
    
    # 최신 daily_snapshots에서 보유 중인 자산 조회
    q_snapshots = (
        supabase.table("daily_snapshots")
        .select("""
            date,
            asset_id,
            quantity,
            assets ( ticker, name_kr, currency )
        """)
    )
    
    if account_id and account_id != "__ALL__":
        q_snapshots = q_snapshots.eq("account_id", account_id)
    else:
        # '전체'일 경우 user_id에 속한 모든 계좌
        user_accounts = query.get_accounts(user_id)
        user_account_ids = [acc['id'] for acc in user_accounts]
        if not user_account_ids:
            st.info("등록된 계좌가 없습니다.")
            return
        q_snapshots = q_snapshots.in_("account_id", user_account_ids)
    
    # 최신 날짜 기준으로 필터링
    q_snapshots = q_snapshots.order("date", desc=True)
    
    snapshot_rows = q_snapshots.execute().data or []
    
    if not snapshot_rows:
        st.info("스냅샷 데이터가 없습니다. 먼저 스냅샷을 생성해주세요.")
        return
    
    # 최신 날짜 데이터만 필터링 (같은 자산이 여러 날짜에 있을 수 있음)
    df_snapshots = pd.DataFrame(snapshot_rows)
    
    # 날짜별로 그룹화하여 최신 날짜만 선택
    if "date" not in df_snapshots.columns:
        # date 컬럼이 없으면 전체 사용 (이미 order by date desc로 정렬됨)
        df_latest = df_snapshots.copy()  # SettingWithCopyWarning 방지
    else:
        latest_date = pd.to_datetime(df_snapshots["date"]).max()
        df_latest = df_snapshots[pd.to_datetime(df_snapshots["date"]) == latest_date].copy()  # SettingWithCopyWarning 방지
    
    # asset_id별로 quantity 합계 계산 (같은 자산이 여러 계좌에 있을 수 있음)
    df_latest.loc[:, "quantity"] = pd.to_numeric(df_latest["quantity"], errors="coerce").fillna(0)
    df_asset_qty = (
        df_latest.groupby("asset_id", as_index=False)
        .agg({"quantity": "sum", "assets": "first"})
    )
    
    # 보유 중인 자산만 필터링 (quantity > 0)
    df_holding = df_asset_qty[df_asset_qty["quantity"] > 0].copy()
    
    if df_holding.empty:
        st.info("현재 보유 중인 자산이 없습니다.")
        return
    
    # assets 정보 추출
    df_holding["ticker"] = df_holding["assets"].apply(lambda x: (x or {}).get("ticker", ""))
    df_holding["name_kr"] = df_holding["assets"].apply(lambda x: (x or {}).get("name_kr", ""))
    df_holding["currency"] = df_holding["assets"].apply(lambda x: (x or {}).get("currency", ""))
    
    # 드롭다운 표시용 라벨 생성: "티커 | 자산명 (통화)"
    df_holding["display_label"] = df_holding.apply(
        lambda row: f"{row['ticker']} | {row['name_kr']} ({row['currency']}) - 보유: {row['quantity']:.2f}",
        axis=1
    )
    
    # asset_id를 키로 하는 딕셔너리 생성
    asset_options = df_holding.set_index("asset_id")["display_label"].to_dict()
    
    if not asset_options:
        st.info("보유 중인 자산이 없습니다.")
        return
    
    # ================================
    # 2. 자산 선택 드롭다운
    # ================================
    st.markdown("#### 🔍 자산 선택")
    
    # 자산 정렬: 보유 수량 기준 내림차순
    sorted_asset_ids = df_holding.sort_values("quantity", ascending=False)["asset_id"].tolist()
    
    selected_asset_id = st.selectbox(
        "조회할 자산을 선택하세요",
        options=sorted_asset_ids,
        format_func=lambda aid: asset_options.get(aid, str(aid)),
        key="asset_transaction_history_selector"
    )
    
    if not selected_asset_id:
        return
    
    # ================================
    # 3. 선택한 자산의 거래 내역 조회
    # ================================
    st.markdown("#### 📊 거래 내역")
    
    q_transactions = (
        supabase.table("transactions")
        .select("""
            id,
            transaction_date,
            trade_type,
            quantity,
            price,
            fee,
            tax,
            memo,
            accounts (name, brokerage, old_owner)
        """)
        .eq("asset_id", int(selected_asset_id))
        .order("transaction_date", desc=True)
    )
    
    # 계좌 필터링
    if account_id and account_id != "__ALL__":
        q_transactions = q_transactions.eq("account_id", account_id)
    else:
        user_accounts = query.get_accounts(user_id)
        user_account_ids = [acc['id'] for acc in user_accounts]
        if user_account_ids:
            q_transactions = q_transactions.in_("account_id", user_account_ids)
    
    tx_response = q_transactions.execute()
    tx_rows = tx_response.data or []
    
    if not tx_rows:
        st.info("해당 자산의 거래 내역이 없습니다.")
        return
    
    # ================================
    # 4. 거래 내역 테이블 구성
    # ================================
    df_tx = pd.DataFrame(tx_rows)
    
    # 날짜 변환
    df_tx["transaction_date"] = pd.to_datetime(df_tx["transaction_date"]).dt.date
    
    # quantity를 숫자 타입으로 명시적 변환
    df_tx["quantity"] = pd.to_numeric(df_tx["quantity"], errors="coerce").fillna(0)
    
    # ================================
    # ✅ 통계 정보 계산 (한글화 전에 먼저 계산)
    # ================================
    # BUY와 INIT(초기입고)을 합산하여 총 매수로 취급
    total_buy = df_tx[df_tx["trade_type"].isin(["BUY", "INIT"])]["quantity"].sum()
    total_sell = df_tx[df_tx["trade_type"] == "SELL"]["quantity"].sum() if "SELL" in df_tx["trade_type"].values else 0
    
    # 거래 타입 한글화 (통계 계산 후에 수행)
    trade_type_kr_map = {
        "BUY": "매수",
        "SELL": "매도",
        "DEPOSIT": "입금",
        "WITHDRAW": "출금",
        "INIT": "초기입고",
    }
    df_tx["trade_type_kr"] = df_tx["trade_type"].map(trade_type_kr_map).fillna(df_tx["trade_type"])
    
    # 계좌 정보 추출
    if "accounts" in df_tx.columns:
        df_tx["account_label"] = df_tx["accounts"].apply(
            lambda x: f"{(x or {}).get('brokerage', '')} | {(x or {}).get('name', '')}".strip(" |") if x else ""
        )
        df_tx = df_tx.drop(columns=["accounts"], errors="ignore")
    
    # 표시용 컬럼 선택 및 순서 지정
    display_columns = {
        "transaction_date": "거래일",
        "trade_type_kr": "거래구분",
        "quantity": "수량/금액",
        "price": "단가",
        "fee": "수수료",
        "tax": "세금",
        "memo": "메모",
        "account_label": "계좌",
    }
    
    # 존재하는 컬럼만 선택
    df_display = df_tx[[col for col in display_columns.keys() if col in df_tx.columns]].copy()
    
    # 컬럼명 한글화
    df_display = df_display.rename(columns=display_columns)
    
    # ================================
    # 통계 정보 표시
    # ================================
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("총 매수 수량", f"{total_buy:,.2f}")
    with col2:
        st.metric("총 매도 수량", f"{total_sell:,.2f}")
    with col3:
        st.metric("순 보유 수량", f"{total_buy - total_sell:,.2f}")
    
    # 디버깅용 정보 (문제 해결 시 제거 가능)
    # # with st.expander("🔍 통계 계산 디버깅 정보"):
    # #     st.write(f"**총 거래 건수**: {len(df_tx)}")
    # #     st.write(f"**trade_type 고유값**: {df_tx['trade_type'].unique().tolist()}")
    # #     st.write(f"**BUY 거래 건수**: {(df_tx['trade_type'] == 'BUY').sum()}")
    # #     st.write(f"**SELL 거래 건수**: {(df_tx['trade_type'] == 'SELL').sum()}")
    # #     st.write(f"**총 매수 수량**: {total_buy:,.2f}")
    # #     st.write(f"**총 매도 수량**: {total_sell:,.2f}")
    # #     st.dataframe(df_tx[["transaction_date", "trade_type", "quantity"]].head(10))
    # st.divider()
    
    # 거래 내역 테이블 표시
    st.dataframe(df_display, width="stretch", height=400)
    
    st.caption(
        "※ 이 자산에 대한 모든 거래 내역입니다. "
        "메모를 통해 각 매수/매도의 이유를 확인할 수 있습니다."
    )


