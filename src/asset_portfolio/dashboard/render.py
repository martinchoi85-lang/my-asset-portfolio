import pandas as pd
import streamlit as st
from datetime import date, timedelta
from asset_portfolio.backend.infra.supabase_client import get_supabase_client
from asset_portfolio.backend.services.portfolio_weight_service import (
    load_asset_weight_timeseries,
    build_asset_weight_df,
)
from asset_portfolio.backend.services.portfolio_service import (
    get_portfolio_return_series
)
from asset_portfolio.backend.services.benchmark_service import (
    load_cash_benchmark_series,
    merge_portfolio_and_benchmark,
    load_sp500_benchmark_series
)

def render_portfolio_return_section(account_id, start_date, end_date):
    st.subheader("📈 Portfolio 전체 수익률")

    if not account_id:
        st.info("계좌를 선택해주세요.")
        return

    # ============================
    # 데이터 로드
    # ============================
    df = get_portfolio_return_series(
        account_id=account_id,
        start_date=start_date,
        end_date=end_date,
    )

    if df.empty:
        st.warning("조회된 데이터가 없습니다.")
        return

    # ============================
    # 기본 전처리
    # ============================
    df = df.sort_values("date")
    latest = df.iloc[-1]

    total_purchase = latest["purchase_amount"]
    total_valuation = latest["valuation_amount"]
    portfolio_return = latest["portfolio_return"]

    # ============================
    # 계좌 정보 요약
    # ============================
    st.caption(f"선택된 계좌 ID: {account_id}")

    # ============================
    # KPI 카드 영역
    # ============================
    col1, col2, col3 = st.columns(3)

    col1.metric(
        label="총 투자금",
        value=f"{int(total_purchase):,} 원",
    )

    col2.metric(
        label="현재 평가금액",
        value=f"{int(total_valuation):,} 원",
        delta=f"{int(total_valuation - total_purchase):,} 원",
    )

    col3.metric(
        label="누적 수익률",
        value=f"{portfolio_return * 100:.2f} %",
    )

    st.divider()

    # ============================
    # 누적 수익률 차트
    # ============================
    df["portfolio_return_pct"] = df["portfolio_return"] * 100

    st.line_chart(
        df.set_index("date")["portfolio_return_pct"],
        height=350,
    )

    st.caption("※ 누적 수익률 기준 (%)")

    # ============================
    # 디버깅 / 확인용 데이터
    # ============================
    with st.expander("📄 원본 데이터 확인"):
        st.dataframe(
            df[
                [
                    "date",
                    "purchase_amount",
                    "valuation_amount",
                    "portfolio_return_pct",
                ]
            ]
        )


    # ============================
    # Benchmark 데이터 로드
    # ============================
    benchmark_df = load_cash_benchmark_series(start_date, end_date)

    merged_df = merge_portfolio_and_benchmark(
        portfolio_df=df,
        benchmark_df=benchmark_df,
    )

    st.subheader("📊 Portfolio vs Benchmark")

    st.line_chart(
        merged_df
            .set_index("date")[
                ["portfolio_return_pct", "benchmark_return_pct"]
            ],
        height=350,
    )

    st.caption("※ Portfolio vs 현금 기준 수익률 비교 (%)")


    # ============================
    # Benchmark S&P500 데이터 로드
    # ============================
    benchmark_df = load_sp500_benchmark_series(
        start_date=start_date,
        end_date=end_date,
    )

    merged_df = merge_portfolio_and_benchmark(
        portfolio_df=df,
        benchmark_df=benchmark_df,
    )

    st.subheader("📊 Portfolio vs S&P 500")

    if merged_df.empty:
        st.info("Benchmark 데이터를 불러올 수 없습니다.")
    else:
        st.line_chart(
            merged_df
                .set_index("date")[
                    ["portfolio_return_pct", "benchmark_return_pct"]
                ],
            height=350,
        )

        st.caption("※ Portfolio vs S&P 500 누적 수익률 비교 (%)")




    
def render_asset_return_section(
    account_id: str,
    start_date: str,
    end_date: str,
):
    st.subheader("📈 자산별 수익률 추이")

    supabase = get_supabase_client()

    # ============================
    # 1. daily_snapshots + assets JOIN 조회
    # ============================
    response = (
        supabase.table("daily_snapshots")
        .select(
            """
            date,
            asset_id,
            valuation_amount,
            purchase_amount,
            assets (
                id,
                ticker,
                name_kr
            )
            """
        )
        .eq("account_id", account_id)
        .gte("date", start_date)
        .lte("date", end_date)
        .order("date")
        .execute()
    )

    data = response.data or []

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
    st.line_chart(
        asset_df.set_index("date")["return_rate"],
        height=300
    )

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


def load_accounts():
    supabase = get_supabase_client()

    response = (
        supabase.table("accounts")
        .select("id, name, brokerage, owner, type")
        .order("brokerage")
        .execute()
    )

    return response.data or []


def render_account_selector():
    st.sidebar.subheader("🏦 계좌 선택")

    accounts = load_accounts()

    if not accounts:
        st.sidebar.warning("등록된 계좌가 없습니다.")
        return None

    # 사용자에게 보여줄 label → account_id 매핑
    options = {
        f"{a['brokerage']} | {a['name']} ({a['owner']})": a["id"]
        for a in accounts
    }

    labels = list(options.keys())

    # session_state에 기본값 설정
    if "selected_account_label" not in st.session_state:
        st.session_state.selected_account_label = labels[0]

    # 계좌 선택 UI
    selected_label = st.sidebar.selectbox(
        "조회할 계좌를 선택하세요",
        options=labels,
        index=labels.index(st.session_state.selected_account_label)
    )

    # 선택 결과를 session_state에 반영
    st.session_state.selected_account_label = selected_label
    st.session_state.account_id = options[selected_label]

    return st.session_state.account_id


def resolve_date_range(period: str):
    """
    기간 코드(1M, 3M, YTD, ALL)를
    실제 조회용 start_date, end_date로 변환
    """
    end_date = date.today()

    if period == "1M":
        start_date = end_date - timedelta(days=30)
    elif period == "3M":
        start_date = end_date - timedelta(days=90)
    elif period == "YTD":
        start_date = date(end_date.year, 1, 1)
    elif period == "ALL":
        start_date = None
    else:
        raise ValueError(f"Unknown period: {period}")

    return start_date, end_date


def render_period_selector():
    st.sidebar.subheader("📅 기간 선택")

    period = st.sidebar.radio(
        "조회 기간",
        options=["1M", "3M", "YTD", "ALL"],
        index=1  # 기본값: 3M
    )

    return resolve_date_range(period)


def render_asset_weight_section(account_id, start_date, end_date):
    st.subheader("📊 자산 비중 변화")

    rows = load_asset_weight_timeseries(
        account_id, start_date, end_date
    )

    df = build_asset_weight_df(rows)

    if df.empty:
        st.info("비중 데이터가 없습니다.")
        return

    pivot = (
        df.pivot(
            index="date",
            columns="asset_name",
            values="weight"
        )
        .fillna(0)
    )

    st.area_chart(pivot)





def render_asset_weight_section(account_id, start_date, end_date):
    st.subheader("📊 자산 비중 변화")

    rows = load_asset_weight_timeseries(
        account_id=account_id,
        start_date=start_date,
        end_date=end_date,
    )

    df = build_asset_weight_df(rows)

    if df.empty:
        st.info("자산 비중 데이터가 없습니다.")
        return

    pivot = (
        df.pivot(
            index="date",
            columns="asset_name",
            values="weight"
        )
        .fillna(0)
    )

    st.area_chart(pivot)
