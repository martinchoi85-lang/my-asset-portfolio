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
from asset_portfolio.dashboard.data import load_assets_lookup
from asset_portfolio.backend.infra.query import (
    build_daily_snapshots_query,
    load_asset_contribution_data
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

    # ✅ datetime → date로 변환해서 시간 표시를 제거합니다.
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

    st.caption("※ portfolio_return_pct는 선택한 기간의 포트폴리오 수익률(%)을 의미합니다. (기준일 대비 자산 가치가 얼마나 증가/감소했는지를 비율로 표시)")

    
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
    asset_df["date"] = pd.to_datetime(asset_df["date"]).dt.date  # 시간 제거
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

    # 계좌 선택 UI
    selected_label = st.sidebar.selectbox(
        "조회할 계좌를 선택하세요",
        options=list(options.keys()),
        index=0,
        key="account_selector_label",
    )

    return options[selected_label]


def resolve_date_range(period: str):
    """
    기간 코드(1M, 3M, YTD, ALL)를
    실제 조회용 start_date, end_date로 변환
    """
    end_date = date.today()

    if period == "오늘":
        start_date = end_date
    elif period == "3일":
        start_date = end_date - timedelta(days=3)
    elif period == "1M":
        start_date = end_date - timedelta(days=30)
    elif period == "3M":
        start_date = end_date - timedelta(days=90)
    elif period == "YTD":
        start_date = date(end_date.year, 1, 1)
    elif period == "ALL":
        start_date = None
    else:
        raise ValueError(f"Unknown period: {period}")
    
    
    # 디버깅(차후 제거)
    print("start_date, end_date>>>>>>>>>>", start_date, end_date)
    

    return start_date, end_date


def render_period_selector():
    st.sidebar.subheader("📅 기간 선택")

    period = st.sidebar.radio(
        "조회 기간",
        options=["오늘", "3일", "1M", "3M", "YTD", "ALL"],
        index=0  # 기본값: 3M
    )

    return resolve_date_range(period)



def render_asset_weight_section(account_id, start_date, end_date):
    st.subheader("📊 자산 비중 변화")

    rows = load_asset_weight_timeseries(
        account_id=account_id,
        start_date=start_date,
        end_date=end_date,
    )
    
    df = build_asset_weight_df(rows)
    
    # 총액이 0인 날짜는 제거(의미 없는 구간 제거)
    # df는 build_asset_weight_df 결과(valuation_amount_krw, total_amount_krw가 있음)
    df = df[df["total_amount_krw"] > 0].copy()
    if df.empty:
        st.info("자산 비중 데이터가 없습니다. (평가금액 합계가 0인 날짜만 존재)")
        return
    
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
    weight_col = None
    for c in ["weight", "weight_krw", "weight_pct"]:
        if c in df.columns:
            weight_col = c
            break

    if weight_col is None:
        st.error(f"자산 비중 컬럼이 없습니다. df.columns={list(df.columns)}")
        return
    
    df["date"] = pd.to_datetime(df["date"]).dt.date  # 시간 제거
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

    # ✅ Plotly 표시용 한글 라벨 (hover, legend 등에 반영)
    LABELS = {
        "valuation_amount": "평가금액",
        "name_kr": "자산명",
        "asset_type": "자산유형",
        "market": "시장",
        "cum_pct": "누적 기여도(%)",
        "abs_cum": "누적 기여도(절대)",
    }

    if mode == "현재 비중(평가금액)":
        # df_w는 최소 컬럼: ['asset_id','valuation_amount','name_kr','asset_type','market'] 를 가지도록 준비
        df_w = load_latest_asset_weights(account_id, start_date, end_date)
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
            # color_discrete_sequence=px.colors.qualitative.Set3,  # 최초 팔레트
            color_discrete_sequence=px.colors.qualitative.Alphabet,  # 색 종류 많은 팔레트
            # color_continuous_scale=px.colors.diverging.RdYlGn,
            # color_discrete_sequence=px.colors.diverging.RdYlGn,   # 값에 따른 그레디언트 팔레트
            # ✅ Plotly가 자동으로 보여주는 필드명을 한글로 바꿉니다.
            labels=LABELS,
            # ✅ hover에 보여줄 값을 명시적으로 통제할 수 있습니다.
            hover_data={
                "valuation_amount": ":,.0f",
                "market": True,
                "asset_type": True,
                "name_kr": True,
            }
        )
        fig.update_layout(height=550)
        fig.update_layout(margin=dict(t=20, l=10, r=10, b=10))

        # ✅ hovertemplate을 완전히 덮어써서 Plotly 기본 필드(labels/parent/id)를 표시하지 않게 함
        # - <extra></extra>를 넣으면 오른쪽 회색 박스도 제거됩니다.
        fig.update_traces(
            hovertemplate="<b>%{label}</b><br>평가금액=%{value:,.0f}<extra></extra>"
        )
        # ✅ 전체 폰트(타이틀/범례 등) 기본 크기
        # fig.update_layout(font=dict(size=16))

        # ✅ 트리맵 라벨 텍스트 크기(블록 내부)
        fig.update_traces(textfont_size=fontSizeByLeaf)

        st.plotly_chart(fig, width='stretch')
        st.caption("※ 마지막 스냅샷 날짜 기준 평가금액 Treemap")        

        with st.expander("📄 데이터 원본"):
            st.dataframe(df_w.sort_values(["date"], ascending=[True]))

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

        leaf_count = int(latest["asset_id"].nunique())  # ✅ 말단 개수 근사

        # ✅ 말단이 적으면 더 크게, 많으면 덜 크게(숫자를 하드코딩하지만 "데이터에 따라 자동 변화" = adaptive)
        # - 최소/최대만 정해두면 사용자 입장에서는 "자동"으로 느껴집니다.
        base = 22
        fontSizeByLeaf = max(12, min(base, int(28 - leaf_count * 0.6)))


        fig = px.treemap(
            latest,
            path=["market", "asset_type", "name_kr"],
            values="abs_cum",
            color="cum_pct",
            # ✅ 성과 방향(+) / (-)이 색으로 명확하게 보이는 컬러맵
            color_continuous_scale=px.colors.diverging.RdYlGn,
            labels=LABELS,
        )
        fig.update_layout(height=550)
        fig.update_layout(margin=dict(t=20, l=10, r=10, b=10))

        # ✅ 타일 간 여백 축소
        # fig.update_traces(tiling=dict(pad=2))  

        # 추가로, legend가 세로 공간을 잡아먹는다면(특히 범주형 color):
        # fig.update_layout(showlegend=False)  # 필요하면 켬/끔

        fig.update_traces(
            hovertemplate="<b>%{label}</b><br>누적기여도=%{value:,.0f}<extra></extra>"
        )
        # ✅ 트리맵 라벨 텍스트 크기(블록 내부)
        fig.update_traces(textfont_size=fontSizeByLeaf)
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
    # df_plot["date"] = pd.to_datetime(df_plot["date"]).dt.strftime("%Y-%m-%d")
    df_plot["date"] = pd.to_datetime(df_plot["date"])  # ✅ datetime 유지

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
        # .gte("transaction_date", f"{start_date}T00:00:00")
        # .lte("transaction_date", f"{end_date}T23:59:59")
        .order("transaction_date", desc=True)
    )

    # ✅ ALL이 아닌 경우에만 계좌 필터 적용
    if account_id and account_id != "__ALL__":
        query = query.eq("account_id", account_id)

    if start_date is not None:
        query = query.gte("transaction_date", start_date)

    if end_date is not None:
        query = query.lte("transaction_date", end_date)

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

    # =========================
    # ✅ 컬럼명을 한글로 표시하기 위한 맵핑 테이블
    # - 실제 df에 존재하는 컬럼만 rename됩니다.
    # =========================
    COL_KR = {
        "transaction_date": "거래일",
        "trade_type": "거래구분",
        "ticker": "티커",
        "name_kr": "종목명",
        "asset_currency": "통화",
        "quantity": "수량/금액",
        "price": "단가",
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
    df["asset_currency"] = df["assets"].apply(lambda x: (x or {}).get("currency"))

    # ✅ 표시는 한글이지만, 내부 로직/코드는 영문 컬럼을 계속 써도 됩니다.
    df_display = df.rename(columns=COL_KR)

    # (선택) 표시 컬럼 순서를 한글 기준으로 정렬하고 싶으면:
    display_order = [
        "거래일", "거래구분", "티커", "종목명", "통화",
        "수량/금액", "단가", "수수료", "세금", "계좌", "메모"
    ]

    cols = [c for c in display_order if c in df_display.columns] + [c for c in df_display.columns if c not in display_order]
    df_display = df_display[cols]

    # join된 dict 펼치기(간단)
    # df["ticker"] = df["assets"].apply(lambda x: (x or {}).get("ticker"))
    # df["name_kr"] = df["assets"].apply(lambda x: (x or {}).get("name_kr"))
    # df["asset_currency"] = df["assets"].apply(lambda x: (x or {}).get("currency"))
    # df = df.drop(columns=["assets"], errors="ignore")

    # st.dataframe(df, width='stretch')
    st.dataframe(df_display, width='stretch')
