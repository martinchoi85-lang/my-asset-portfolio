import pandas as pd
import altair as alt
import streamlit as st
import plotly.express as px
from datetime import date, timedelta
from asset_portfolio.backend.infra.supabase_client import get_supabase_client
from asset_portfolio.backend.services.portfolio_weight_service import (
    load_asset_weight_timeseries,
    build_asset_weight_df,
    load_latest_asset_weights
)
from asset_portfolio.backend.services.portfolio_service import (
    get_portfolio_return_series,
    load_asset_contribution_data, 
    calculate_asset_contributions
)
from asset_portfolio.backend.services.benchmark_service import (
    load_cash_benchmark_series,
    merge_portfolio_and_benchmark, 
    merge_portfolio_and_benchmark_ffill,
    load_sp500_benchmark_series,
    align_portfolio_to_benchmark_calendar
)
from asset_portfolio.dashboard.data import (
    load_asset_contribution_data,
    load_assets_lookup,
    build_daily_snapshots_query
)


def render_portfolio_return_section(account_id: str, start_date: str, end_date: str):
    st.subheader("📈 Portfolio 전체 수익률")

    if not account_id:
        st.info("계좌를 선택해주세요.")
        return

    # =========================
    # 1) 포트폴리오 시계열
    # =========================
    portfolio_df = get_portfolio_return_series(account_id, start_date, end_date)

    if portfolio_df.empty:
        st.warning("조회된 데이터가 없습니다.")
        return

    # =========================
    # 2) 벤치마크 시계열 (S&P 500)
    #    - 반환: date, benchmark_return (0~1)
    # =========================
    benchmark_df = load_sp500_benchmark_series(start_date=start_date, end_date=end_date)

    # =========================
    # 3) forward-fill 정렬 (벤치마크 캘린더 기준)
    # =========================
    if not benchmark_df.empty:
        portfolio_df = align_portfolio_to_benchmark_calendar(portfolio_df, benchmark_df)

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

    st.divider()

    # =========================
    # 5) 차트 데이터 구성 (포트폴리오 vs 벤치마크)
    # =========================
    chart_df = portfolio_df[["date", "portfolio_return"]].copy()
    chart_df["portfolio_return_pct"] = chart_df["portfolio_return"] * 100

    if not benchmark_df.empty:
        b = benchmark_df.copy()
        b["date"] = pd.to_datetime(b["date"])
        b["benchmark_return_pct"] = b["benchmark_return"] * 100

        chart_df = chart_df.merge(
            b[["date", "benchmark_return_pct"]],
            on="date",
            how="left",
        )

    # =========================
    # 6) 라인 차트
    # =========================
    st.line_chart(
        chart_df.set_index("date")[
            [c for c in ["portfolio_return_pct", "benchmark_return_pct"] if c in chart_df.columns]
        ],
        height=350,
    )

    with st.expander("📄 원본 데이터 확인"):
        st.dataframe(chart_df)

    st.caption("※ 누적 수익률 기준(%) / 벤치마크 날짜 기준으로 portfolio를 forward-fill 적용")

    
def render_asset_return_section(
    account_id: str,
    start_date: str,
    end_date: str,
):
    st.subheader("📈 자산별 수익률 추이")

    # ============================
    # 1. daily_snapshots + assets JOIN 조회
    # ============================
    q = build_daily_snapshots_query(
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
        account_id=account_id,
    )
    data = q.execute().data or []

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

    # ✅ 전체 계좌 옵션 추가 (맨 위)
    options = {"전체 계좌 (ALL)": "__ALL__", **options}

    # session_state에 기본값 설정
    # if "selected_account_label" not in st.session_state:
    #     st.session_state.selected_account_label = labels[0]

    # 계좌 선택 UI
    selected_label = st.sidebar.selectbox(
        "조회할 계좌를 선택하세요",
        options=list(options.keys()),
        index=0,
        key="account_selector_label",
    )

    # 선택 결과를 session_state에 반영
    # st.session_state.selected_account_label = selected_label
    # st.session_state.account_id = options[selected_label]

    # return st.session_state.account_id
    return options[selected_label]


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


import pandas as pd
import streamlit as st

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
    # ✅ pivot은 asset_id로 (name_kr 변경/중복 대비)
    # =========================
    pivot = (
        df.pivot_table(
            index="date",
            columns="asset_id",
            values="weight",
            aggfunc="sum",     # 혹시 남아있을 중복도 방어
        )
        .fillna(0)
        .sort_index()
    )

    # =========================
    # ✅ 표시용 라벨 매핑 (asset_id -> asset_name)
    # =========================
    id_to_label = (
        df[["asset_id", "asset_name"]]
        .drop_duplicates()
        .set_index("asset_id")["asset_name"]
        .to_dict()
    )

    pivot_display = pivot.rename(columns=lambda aid: id_to_label.get(aid, f"asset_id={aid}"))

    st.area_chart(pivot_display, height=350)

    with st.expander("📄 디버깅: weight 원본"):
        st.dataframe(df.sort_values(["date", "weight"], ascending=[True, False]).head(200))


def render_asset_contribution_section(
    account_id: str,
    start_date: str,
    end_date: str,
):
    st.subheader("🧩 자산별 수익률 기여도")

    if not account_id:
        st.info("계좌를 선택해주세요.")
        return

    snapshots = load_asset_contribution_data(
        account_id, start_date, end_date
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
    )

    st.caption("※ 전일 포트폴리오 대비 기여도 (%)")


def render_asset_contribution_stacked_area(
    account_id: str,
    start_date: str,
    end_date: str,
):
    st.subheader("🧩 자산별 누적 기여도 (Stacked Area)")

    if not account_id:
        st.info("계좌를 선택해주세요.")
        return

    snapshots = load_asset_contribution_data(account_id, start_date, end_date)
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
    top_n = st.slider("표시할 자산 개수(상위 누적 기여도 기준)", 5, 30, 12)

    latest_cum = (
        df.groupby(["asset_id", "name_kr"], as_index=False)["cum_contribution"]
        .last()
        .sort_values("cum_contribution", ascending=False)
    )
    top_assets = set(latest_cum.head(top_n)["asset_id"].tolist())
    df_plot = df[df["asset_id"].isin(top_assets)].copy()

    # =========================
    # Altair stacked area
    # =========================
    chart = (
        alt.Chart(df_plot)
        .mark_area()
        .encode(
            x=alt.X("date:T", title="Date"),
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
    account_id: str,
    start_date: str,
    end_date: str,
):
    st.subheader("🗺️ Portfolio Treemap")

    if not account_id:
        st.info("계좌를 선택해주세요.")
        return

    mode = st.radio(
        "Treemap 기준",
        ["현재 비중(평가금액)", "기간 누적 기여도"],
        horizontal=True,
    )

    assets = load_assets_lookup()

    if mode == "현재 비중(평가금액)":
        df_w = load_latest_asset_weights(account_id, start_date, end_date)
        if df_w.empty:
            st.warning("해당 기간에 daily_snapshots 데이터가 없습니다.")
            return

        df_w = df_w.merge(assets[["asset_id", "name_kr", "asset_type", "market"]], on="asset_id", how="left")
        df_w["name_kr"] = df_w["name_kr"].fillna(df_w["asset_id"].astype(str))

        fig = px.treemap(
            df_w,
            path=["market", "asset_type", "name_kr"],
            values="valuation_amount",
            # ✅ 자산유형별로 색을 다르게 주면 시각적으로 훨씬 구분이 잘 됩니다.
            color="asset_type",
            # ✅ 여러 색을 제공하는 팔레트(원하는 것으로 바꿔도 됨)
            color_discrete_sequence=px.colors.qualitative.Set3,
        )
        fig.update_layout(height=550)
        st.plotly_chart(fig, width='stretch')
        st.caption("※ 마지막 스냅샷 날짜 기준 평가금액 Treemap")

    else:
        # 기간 누적 기여도
        snapshots = load_asset_contribution_data(account_id, start_date, end_date)
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

        # treemap values는 음수를 허용하지 않음 → 절대값(면적) + 색으로 방향 표시
        latest["abs_cum"] = latest["cum_contribution"].abs()
        latest["cum_pct"] = latest["cum_contribution"] * 100

        fig = px.treemap(
            latest,
            path=["market", "asset_type", "name_kr"],
            values="abs_cum",
            color="cum_pct",
            # ✅ 성과 방향(+) / (-)이 색으로 명확하게 보이는 컬러맵
            color_continuous_scale=px.colors.diverging.RdYlGn,
        )
        fig.update_layout(height=550)
        st.plotly_chart(fig, width='stretch')
        st.caption("※ 기간 누적 기여도 Treemap (면적=절대값, 색=방향/크기)")


def render_asset_contribution_section_full(
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
    snapshots = load_asset_contribution_data(account_id, start_date, end_date)
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

    # 보기 좋게 문자열 생성
    def _fmt_row(r):
        return f"{r['name_kr']} ({r['cum_contribution_pct']:.2f}%)"

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
    st.markdown("#### 📈 자산별 누적 기여도 (Stacked Area)")

    # 자산이 너무 많으면 UX가 죽는다 → 상위 N개만 보여주자
    max_assets = st.slider("표시할 자산 개수(상위 누적 기여도)", 5, 30, 12)

    top_assets = set(latest.head(max_assets)["asset_id"].tolist())
    df_plot = df[df["asset_id"].isin(top_assets)].copy()

    chart = (
        alt.Chart(df_plot)
        .mark_area()
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("cum_contribution_pct:Q", stack="zero", title="누적 기여도(%)"),
            color=alt.Color("name_kr:N", title="자산"),
            tooltip=[
                alt.Tooltip("date:T", title="Date"),
                alt.Tooltip("name_kr:N", title="자산"),
                alt.Tooltip("cum_contribution_pct:Q", title="누적기여도(%)", format=".2f"),
            ],
        )
        .properties(height=400)
    )

    st.altair_chart(chart, width='stretch')

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
        )


def render_transactions_table_section(account_id: str, start_date: str, end_date: str):
    st.subheader("🧾 Transactions")

    supabase = get_supabase_client()
    query = (
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
            assets ( ticker, name_kr, currency ),
            accounts ( name, brokerage, owner, type )
        """)
        .gte("transaction_date", f"{start_date}T00:00:00")
        .lte("transaction_date", f"{end_date}T23:59:59")
        .order("transaction_date", desc=True)
    )

    # ✅ ALL이 아닌 경우에만 계좌 필터 적용
    if account_id and account_id != "__ALL__":
        query = query.eq("account_id", account_id)

    response = query.execute()
    rows = response.data or []

    if not rows:
        st.info("선택한 기간에 거래 내역이 없습니다.")
        return

    df = pd.DataFrame(rows)

    # ✅ accounts 컬럼이 dict(JSON)로 들어오면, name만 뽑아서 표시하기
    if "accounts" in df.columns:
        df["account_name"] = df["accounts"].apply(
            lambda x: (x or {}).get("name")  # accounts가 None일 수 있으니 방어
        )
        # 원본 accounts dict 컬럼은 화면에서 숨김
        df = df.drop(columns=["accounts"], errors="ignore")

    # ✅ id 컬럼 숨기기(transactions의 PK를 화면에 굳이 보여줄 필요가 없으면 drop)
    df = df.drop(columns=["id"], errors="ignore")

    # (선택) 보기 좋게 컬럼 순서 재정렬
    preferred_cols = [
        "transaction_date", "trade_type", "ticker", "name_kr",
        "quantity", "price", "fee", "tax", "asset_currency",
        "account_name", "memo"
    ]    
    cols = [c for c in preferred_cols if c in df.columns] + [c for c in df.columns if c not in preferred_cols]
    df = df[cols]

    # join된 dict 펼치기(간단)
    df["ticker"] = df["assets"].apply(lambda x: (x or {}).get("ticker"))
    df["name_kr"] = df["assets"].apply(lambda x: (x or {}).get("name_kr"))
    df["asset_currency"] = df["assets"].apply(lambda x: (x or {}).get("currency"))
    df = df.drop(columns=["assets"], errors="ignore")

    st.dataframe(df, width='stretch')
