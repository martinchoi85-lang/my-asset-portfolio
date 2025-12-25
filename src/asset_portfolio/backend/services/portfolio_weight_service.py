import pandas as pd
from typing import List, Dict
from asset_portfolio.backend.infra.supabase_client import get_supabase_client


def load_asset_weight_timeseries(
    account_id: str,
    start_date=None,
    end_date=None,
) -> List[Dict]:
    """
    계좌 기준 자산 비중 시계열 데이터 로드(Service 계층의 전형적인 역할)
    - Supabase 접근
    - account_id, date filter
    - 데이터 조회 책임

    :param rows: Supabase 조회 결과 리스트
    :return: 자산 비중 시계열 데이터 리스트
    """
    supabase = get_supabase_client()

    query = (
        supabase.table("daily_snapshots")
        .select("date, asset_id, valuation_amount, assets(name_kr)")
        .eq("account_id", account_id)
    )

    if start_date:
        query = query.gte("date", start_date)

    if end_date:
        query = query.lte("date", end_date)

    response = query.order("date").execute()
    return response.data or []


def build_asset_weight_df(rows: List[Dict]) -> pd.DataFrame:
    """
    자산 비중 계산용 DataFrame 생성
    (Calculator에 가깝지만, 수익률 계산과는 성격이 다르므로 분리하는 것이 좋음)
    - Pandas 변환
    - 계산은 하지만 “UI와 무관”
    
    :param rows: Supabase 조회 결과 리스트
    :return: 자산 비중 DataFrame
    """
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # 자산명 정리
    df["asset_name"] = df["assets"].apply(lambda x: x["name_kr"])
    df.drop(columns=["assets"], inplace=True)

    # 날짜별 전체 평가금액
    total = (
        df.groupby("date")["valuation_amount"]
        .sum()
        .rename("total_amount")
        .reset_index()
    )

    df = df.merge(total, on="date")

    # 비중 계산
    df["weight"] = df["valuation_amount"] / df["total_amount"]

    return df
