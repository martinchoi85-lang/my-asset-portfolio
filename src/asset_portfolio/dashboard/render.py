import pandas as pd
import streamlit as st
from asset_portfolio.backend.infra.supabase_client import get_supabase_client


def render_asset_return_section(account_id: str):
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


def render_portfolio_return_section(account_id: str):
    st.subheader("📊 포트폴리오 전체 수익률")

    supabase = get_supabase_client()

    # ============================
    # 1. daily_snapshots 조회
    # ============================
    response = (
        supabase.table("daily_snapshots")
        .select(
            """
            date,
            valuation_amount,
            purchase_amount
            """
        )
        .eq("account_id", account_id)
        .order("date")
        .execute()
    )

    data = response.data or []

    if not data:
        st.info("포트폴리오 수익률 데이터가 없습니다.")
        return

    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"])

    # ============================
    # 2. 날짜별 포트폴리오 합산
    # ============================
    portfolio_df = (
        df.groupby("date", as_index=False)
        .agg(
            valuation_amount=("valuation_amount", "sum"),
            purchase_amount=("purchase_amount", "sum"),
        )
        .sort_values("date")
    )

    # ============================
    # 3. 누적 수익률 계산
    # ============================
    portfolio_df["return_rate"] = (
        portfolio_df["valuation_amount"]
        / portfolio_df["purchase_amount"]
        - 1
    )

    # ============================
    # 4. 차트 출력
    # ============================
    st.line_chart(
        portfolio_df.set_index("date")["return_rate"],
        height=300
    )

    # ============================
    # 5. 요약 지표
    # ============================
    latest = portfolio_df.iloc[-1]

    col1, col2, col3 = st.columns(3)
    col1.metric(
        "총 투자금",
        f"{latest.purchase_amount:,.0f}"
    )
    col2.metric(
        "현재 평가금",
        f"{latest.valuation_amount:,.0f}"
    )
    col3.metric(
        "누적 수익률",
        f"{latest.return_rate:.2%}"
    )

    # ============================
    # 6. 데이터 확인
    # ============================
    with st.expander("📄 일별 포트폴리오 데이터"):
        st.dataframe(portfolio_df)





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



