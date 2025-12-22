# src/asset_portfolio/dashboard/app.py

import streamlit as st
import pandas as pd

from asset_portfolio.backend.infra.supabase_client import get_supabase_client
from asset_portfolio.backend.services.portfolio_aggregator import (
    calculate_portfolio_return_series
)
from asset_portfolio.backend.services.portfolio_calculator import (
    calculate_asset_return_series_from_snapshots
)
from asset_portfolio.dashboard.render import (
    render_asset_return_section, render_portfolio_return_section, render_account_selector
)






st.set_page_config(
    page_title="Asset Portfolio Dashboard",
    layout="wide"
)

st.title("📊 포트폴리오 수익률 대시보드")

# =========================
# Supabase 연결
# =========================
supabase = get_supabase_client()

# =========================
# 사용자 입력
# =========================
# account_id = st.text_input("Account ID", placeholder="계좌 UUID 입력")


account_id = render_account_selector()

if not account_id:
    st.stop()
    
start_date = st.date_input("시작 날짜")
end_date = st.date_input("종료 날짜")




# =========================
# daily_snapshots 조회
# =========================
response = (
    supabase
    .table("daily_snapshots")
    .select(
        "date, asset_id, purchase_amount, valuation_amount"
    )
    .eq("account_id", account_id)
    .gte("date", start_date.isoformat())
    .lte("date", end_date.isoformat())
    .order("date")
    .execute()
)



# =========================
# 디버깅 정보 출력 (향후 삭제)
# =========================
# st.subheader("🛠️ DEBUG: daily_snapshots raw data")

# st.write("입력된 account_id:", account_id)
# st.write("조회 시작일:", start_date)
# st.write("조회 종료일:", end_date)

# st.write("조회된 row 수:", len(response.data))

# if len(response.data) > 0:
#     st.dataframe(pd.DataFrame(response.data))
# else:
#     st.warning("❌ daily_snapshots에서 조회된 데이터가 없습니다.")
# =========================
# 디버깅 정보 출력 끝
# =========================


if not response.data:
    st.warning("조회된 데이터가 없습니다.")
    st.stop()

df = pd.DataFrame(response.data)


# =========================
# DataFrame 변환 이후 확인 (향후 삭제)
# =========================
# st.subheader("🛠️ DEBUG: DataFrame 상태")

# st.write("컬럼 목록:", df.columns.tolist())
# st.write("row 수:", len(df))

# st.dataframe(df)
# =========================
# 디버깅 정보 출력 끝
# =========================



# =========================
# 날짜 기준 집계 (자산 합산)
# =========================
portfolio_daily = (
    df.groupby("date", as_index=False)
    .agg(
        purchase_amount=("purchase_amount", "sum"),
        valuation_amount=("valuation_amount", "sum"),
    )
)

# =========================
# 포트폴리오 수익률 계산
# =========================
portfolio_series = calculate_portfolio_return_series(
    portfolio_daily.to_dict(orient="records")
)

result_df = pd.DataFrame(portfolio_series)

# =========================
# 요약 지표
# =========================
latest = result_df.iloc[-1]

col1, col2, col3 = st.columns(3)

col1.metric(
    "총 매입금액",
    f"{latest['purchase_amount']:,.0f}"
)

col2.metric(
    "총 평가금액",
    f"{latest['valuation_amount']:,.0f}"
)

col3.metric(
    "누적 수익률",
    f"{latest['cumulative_return'] * 100:.2f}%"
)

st.divider()

# =========================
# 수익률 차트
# =========================
st.subheader("📈 포트폴리오 누적 수익률 추이")

chart_df = result_df.copy()
chart_df["cumulative_return_pct"] = chart_df["cumulative_return"] * 100
chart_df = chart_df.set_index("date")

st.line_chart(chart_df["cumulative_return_pct"])

# =========================
# 원본 데이터 테이블 (디버그용)
# =========================
# with st.expander("📄 원본 계산 데이터 보기"):
#     st.dataframe(result_df)


st.divider()
st.subheader("📊 자산별 누적 수익률")

# =========================
# 자산 목록 추출
# =========================
asset_columns = ["asset_id"]
if "asset_name" in df.columns:
    asset_columns.append("asset_name")

assets = (
    df[asset_columns]
    .drop_duplicates()
    .sort_values("asset_id")
)

assets["label"] = assets.apply(
    lambda x: f"{x['asset_name']} ({x['asset_id']})"
    if "asset_name" in x else f"Asset {x['asset_id']}",
    axis=1
)

selected_label = st.selectbox(
    "자산 선택",
    assets["label"].tolist()
)

selected_asset_id = int(
    assets.loc[assets["label"] == selected_label, "asset_id"].iloc[0]
)

# =========================
# 선택 자산 snapshot 필터
# =========================
asset_df = df[df["asset_id"] == selected_asset_id]

asset_snapshots = (
    asset_df[["date", "purchase_amount", "valuation_amount"]]
    .sort_values("date")
    .to_dict(orient="records")
)



# =========================
# 자산 디버깅 정보 출력
# =========================
# st.subheader("🛠️ DEBUG: Asset list")

# if "asset_id" not in df.columns:
#     st.error("❌ asset_id 컬럼이 없습니다.")
# else:
#     st.write(
#         df[["asset_id"]]
#         .drop_duplicates()
#         .sort_values("asset_id")
#     )
# =========================
# 자산 디버깅 정보 출력 끝
# =========================





# =========================
# 자산 수익률 계산
# =========================
asset_result = calculate_asset_return_series_from_snapshots(
    asset_snapshots
)

asset_result_df = pd.DataFrame(asset_result)

# =========================
# 요약 지표
# =========================
latest_asset = asset_result_df.iloc[-1]

col1, col2, col3 = st.columns(3)

col1.metric(
    "매입금액",
    f"{latest_asset['purchase_amount']:,.0f}"
)

col2.metric(
    "평가금액",
    f"{latest_asset['valuation_amount']:,.0f}"
)

col3.metric(
    "누적 수익률",
    f"{latest_asset['cumulative_return'] * 100:.2f}%"
)

# =========================
# 자산 수익률 차트
# =========================
chart_df = asset_result_df.copy()
chart_df["cumulative_return_pct"] = chart_df["cumulative_return"] * 100
chart_df = chart_df.set_index("date")

st.line_chart(chart_df["cumulative_return_pct"])

# =========================
# 디버그 테이블
# =========================
# with st.expander("📄 자산별 계산 결과 보기"):
#     st.dataframe(asset_result_df)


render_portfolio_return_section(account_id)
render_asset_return_section(account_id)
