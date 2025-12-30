from __future__ import annotations
import pandas as pd
from typing import Dict, List, Optional
from asset_portfolio.backend.infra.query import build_daily_snapshots_query


def load_asset_weight_timeseries(
    account_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> List[Dict]:
    """
    자산 비중 시계열을 만들기 위한 원천 데이터 로드.
    - daily_snapshots에서 date, asset_id, valuation_amount를 가져오고
    - account_id가 __ALL__이면 계좌 필터를 걸지 않음
    - 결과는 "row list"로 반환 (이후 pandas에서 집계/계산)
    """

    # ✅ 조인으로 자산명을 같이 받되, 계산/집계는 pandas에서 확실히 한다.
    query = build_daily_snapshots_query(
        select_cols="date, asset_id, valuation_amount, assets(name_kr)",
        start_date=start_date,
        end_date=end_date,
        account_id=account_id,
    )

    response = query.order("date").execute()
    return response.data or []


def build_asset_weight_df(rows: List[Dict]) -> pd.DataFrame:
    """
    ALL/단일 계좌 모두 안정적으로 동작하는 자산 비중 DF 생성.
    반환 DF 스키마:
      - date (datetime64[ns])
      - asset_id (int)
      - asset_name (str)
      - valuation_amount (float)
      - total_amount (float)     # date별 전체 평가금액
      - weight (float)           # valuation_amount / total_amount

    중요:
    - pivot 에러 방지를 위해 (date, asset_id) 기준으로 유일하게 만들어둔다.
    """

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # =========================
    # 1) assets(name_kr) 조인 결과 펼치기
    # =========================
    # row 예시:
    # {
    #   "date": "2025-12-16",
    #   "asset_id": 1,
    #   "valuation_amount": "12345",
    #   "assets": {"name_kr": "KODEX 200"}
    # }
    df["asset_name"] = df["assets"].apply(
        lambda x: x.get("name_kr") if isinstance(x, dict) else None
    )
    df.drop(columns=["assets"], inplace=True, errors="ignore")

    # =========================
    # 2) 타입 정리
    # =========================
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["valuation_amount"] = pd.to_numeric(df["valuation_amount"], errors="coerce").fillna(0.0)

    # 혹시 asset_id가 문자열로 들어오는 경우 대비
    df["asset_id"] = pd.to_numeric(df["asset_id"], errors="coerce")

    # 필수 값 누락 제거
    df = df.dropna(subset=["date", "asset_id"])

    # =========================
    # 3) ✅ 핵심: (date, asset_id)로 집계해서 유일화
    #    - 단일 계좌도 안전하게 처리
    #    - ALL 계좌는 반드시 필요
    # =========================
    # name_kr는 asset_id별로 동일하므로, 집계 시 first를 사용
    df_agg = (
        df.groupby(["date", "asset_id"], as_index=False)
          .agg(
              valuation_amount=("valuation_amount", "sum"),
              asset_name=("asset_name", "first"),
          )
    )

    # =========================
    # 4) 날짜별 총 평가금액 및 weight 계산
    # =========================
    df_agg["total_amount"] = df_agg.groupby("date")["valuation_amount"].transform("sum")

    # 0 division 방지
    df_agg["weight"] = df_agg.apply(
        lambda r: (r["valuation_amount"] / r["total_amount"]) if r["total_amount"] > 0 else 0.0,
        axis=1
    )

    # 보기 좋게 정렬
    df_agg = df_agg.sort_values(["date", "valuation_amount"], ascending=[True, False])

    return df_agg


def load_latest_asset_weights(account_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    기간 내 마지막 날짜의 자산별 valuation_amount를 가져와 treemap용 weight로 사용
    """
    query = build_daily_snapshots_query(
        select_cols="date, asset_id, valuation_amount",
        start_date=start_date,
        end_date=end_date,
        account_id=account_id,
    )

    rows = query.execute().data or []
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df["valuation_amount"] = df["valuation_amount"].astype(float)

    last_date = df["date"].max()
    df_last = df[df["date"] == last_date].copy()

    return df_last[["date", "asset_id", "valuation_amount"]]
