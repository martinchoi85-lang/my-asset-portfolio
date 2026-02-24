# src/asset_portfolio/backend/services/portfolio_service.py
import pandas as pd
from typing import List, Dict
from asset_portfolio.backend.infra.query import build_daily_snapshots_query, fetch_all_pagination
from asset_portfolio.backend.services.portfolio_calculator import (
    calculate_asset_return_series_from_snapshots, calculate_portfolio_return_series_from_snapshots,
)
from asset_portfolio.backend.services.data_contracts import (
    normalize_snapshot_df,
    normalize_contribution_df,
    CONTRIBUTION_COLUMNS,
)
from asset_portfolio.backend.infra import query
from datetime import datetime, date

"""
portfolio_service.py

[역할]
- Supabase에서 daily_snapshots 조회
- 조회 결과를 calculator에 전달
- '서비스 계층'으로서 orchestration만 담당
"""

def get_asset_return_series(
    user_id: str,
    asset_id: int,
    account_id: str,
    start_date: str,
    end_date: str,
) -> List[Dict]:
    """
    특정 자산 + 계좌의 기간별 수익률 시계열 조회

    [흐름]
    1. daily_snapshots 조회
    2. calculator로 전달
    3. 계산 결과 반환
    """
    query = build_daily_snapshots_query(
        select_cols="date, purchase_amount, valuation_amount",
        start_date=start_date,
        end_date=end_date,
        user_id=user_id,
        account_id=account_id,
    )

    snapshots = fetch_all_pagination(query)

    # calculator는 DB를 모른다
    return calculate_asset_return_series_from_snapshots(snapshots)



def load_portfolio_daily_snapshots(
    user_id: str,
    account_id: str,
    start_date: str,
    end_date: str,
):
    """
    daily_snapshots에서
    특정 계좌의 포트폴리오 단위 데이터를 date 기준으로 집계
    """
    query = build_daily_snapshots_query(
        select_cols="date, purchase_amount, valuation_amount",
        start_date=start_date,
        end_date=end_date,
        user_id=user_id,
        account_id=account_id,
    )

    snapshots = fetch_all_pagination(query)
    
    # =========================
    # date 기준으로 합산
    # =========================
    daily_map = {}

    for r in snapshots:
        d = r["date"]
        if d not in daily_map:
            daily_map[d] = {
                "date": d,
                "valuation_amount": 0,
                "purchase_amount": 0,
            }

        daily_map[d]["valuation_amount"] += float(r["valuation_amount"] or 0)
        daily_map[d]["purchase_amount"] += float(r["purchase_amount"] or 0)

    result = list(daily_map.values())
    result.sort(key=lambda x: x["date"])
    return result


def load_portfolio_daily_snapshots_krw(
    user_id: str,
    account_id: str,
    start_date: str,
    end_date: str,
    usd_krw: float = 1.0,
) -> list:
    """
    daily_snapshots를 date 기준으로 집계하되,
    USD 자산은 usd_krw 환율을 적용해 KRW로 환산한 후 합산한다.

    기존 load_portfolio_daily_snapshots()와 인터페이스 동일.
    트렌드 차트, 기간 성과 등 KRW 기준 시계열이 필요한 컴포넌트에서 사용.
    """
    q = build_daily_snapshots_query(
        # currency 컬럼을 포함해 조회
        select_cols="date, purchase_amount, valuation_amount, currency",
        start_date=start_date,
        end_date=end_date,
        user_id=user_id,
        account_id=account_id,
    )
    snapshots = fetch_all_pagination(q)

    daily_map: dict = {}
    for r in snapshots:
        d = r["date"]
        val = float(r.get("valuation_amount") or 0)
        buy = float(r.get("purchase_amount") or 0)
        ccy = str(r.get("currency") or "KRW").strip().upper()

        # USD 행은 KRW로 환산
        if ccy == "USD":
            val *= usd_krw
            buy *= usd_krw

        if d not in daily_map:
            daily_map[d] = {"date": d, "valuation_amount": 0.0, "purchase_amount": 0.0}
        daily_map[d]["valuation_amount"] += val
        daily_map[d]["purchase_amount"] += buy

    result = list(daily_map.values())
    result.sort(key=lambda x: x["date"])
    return result


def get_portfolio_return_series(user_id: str, account_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Streamlit / API에서 사용하는 최종 함수
    """
    snapshots = load_portfolio_daily_snapshots(user_id, account_id, start_date, end_date)
    return calculate_portfolio_return_series_from_snapshots(snapshots)


def calculate_asset_contributions(
    snapshots: List[Dict],
) -> pd.DataFrame:
    """
    daily_snapshots 기반 자산별 수익률 기여도 계산

    반환:
    date | asset_id | contribution | contribution_pct
    """

    if not snapshots:
        return pd.DataFrame(columns=CONTRIBUTION_COLUMNS)

    df = normalize_snapshot_df(pd.DataFrame(snapshots))
    if df.empty:
        return pd.DataFrame(columns=CONTRIBUTION_COLUMNS)

    df = (
        df.groupby(["date", "asset_id"], as_index=False)[["valuation_amount", "purchase_amount"]]
        .sum()
    )

    # =========================
    # date, asset_id 기준 정렬
    # =========================
    df = df.sort_values(["asset_id", "date"])

    # =========================
    # 자산별 평가금액 변화
    # =========================
    df["prev_valuation"] = df.groupby("asset_id")["valuation_amount"].shift(1)
    df["delta_valuation"] = df["valuation_amount"] - df["prev_valuation"]

    # =========================
    # 포트폴리오 전일 총 평가금액
    # =========================
    portfolio_prev = (
        df.groupby("date")["prev_valuation"]
        .sum()
        .rename("portfolio_prev_valuation")
        .reset_index()
    )

    df = df.merge(portfolio_prev, on="date", how="left")

    # =========================
    # 기여도 계산
    # =========================
    
    # 기여도 "inf%" 표시 방어 로직: inf / NaN 제거
    # 1) 자산 전일값 없는 행 제거 (첫날)
    # 2) 포트폴리오 전일 총액이 0/NaN이면 제거
    df = df.dropna(subset=["prev_valuation", "portfolio_prev_valuation"])
    df = df[df["portfolio_prev_valuation"] > 0]

    df["contribution"] = df["delta_valuation"] / df["portfolio_prev_valuation"]

    df = df.dropna(subset=["contribution"])

    df["contribution_pct"] = df["contribution"] * 100

    return normalize_contribution_df(
        df[
            [
                "date",
                "asset_id",
                "contribution",
                "contribution_pct",
            ]
        ]
    )


def calculate_period_performance(
    user_id: str,
    account_id: str,
    start_date: str,
    end_date: str,
) -> Dict[str, float]:
    """
    기간별 성과 분석 (Cash Flow 고려)
    
    Returns:
        {
            "start_value": float,
            "end_value": float,
            "net_flow": float,     # 입금 - 출금
            "investment_gain": float, # 기말 - 기초 - 순입출금
            "return_rate": float,   # Modified Dietz or Simple Return
        }
    """
    
    # 0. Data Fetching
    snapshots = load_portfolio_daily_snapshots(user_id, account_id, start_date, end_date)
    if not snapshots:
        return {
            "start_value": 0.0,
            "end_value": 0.0,
            "net_flow": 0.0,
            "investment_gain": 0.0,
            "return_rate": 0.0,
        }
    
    # 1. Start / End Value
    # snapshots는 날짜 오름차순 정렬되어 있음
    start_row = snapshots[0]
    end_row = snapshots[-1]
    
    start_val = float(start_row["valuation_amount"] or 0)
    end_val = float(end_row["valuation_amount"] or 0)
    
    # 2. Cash Flow
    # start_date < date <= end_date 범위의 입출금 합산
    s_date_obj = None
    if isinstance(start_row["date"], str):
        s_date_obj = datetime.strptime(start_row["date"], "%Y-%m-%d").date()
    else:
        s_date_obj = start_row["date"]
        
    e_date_obj = None
    if isinstance(end_row["date"], str):
        e_date_obj = datetime.strptime(end_row["date"], "%Y-%m-%d").date()
    else:
        e_date_obj = end_row["date"]

    
    raw_flows = query.get_period_cash_flow(user_id, account_id, start_date, end_date)
    
    net_flow = 0.0
    
    # 입출금 내역이 기간 내에 포함되는지 확인
    # - start_date의 스냅샷은 해당일 EOD 기준이므로, start_date 당일 입출금은 이미 반영됨 => 제외
    # - end_date의 스냅샷은 해당일 EOD 기준이므로, end_date 당일 입출금은 반영됨 => 포함
    for row in raw_flows:
        td_str = row["transaction_date"]
        if "T" in td_str:
            td = datetime.fromisoformat(td_str).date()
        else:
            td = datetime.strptime(td_str[:10], "%Y-%m-%d").date()
            
        if td <= s_date_obj:
            continue
        if td > e_date_obj:
            continue
            
        q = float(row["quantity"] or 0)
        t_type = row["trade_type"]
        
        if t_type == "DEPOSIT":
            net_flow += q
        elif t_type == "WITHDRAW":
            net_flow -= q
            
    # 3. Investment Gain & Return
    investment_gain = end_val - start_val - net_flow
    
    # Modified Dietz (약식): Gain / (Start + Flow/2)
    denominator = start_val + (net_flow / 2.0)
    
    if denominator == 0:
        return_rate = 0.0
    else:
        return_rate = investment_gain / denominator

    return {
        "start_value": start_val,
        "end_value": end_val,
        "net_flow": net_flow,
        "investment_gain": investment_gain,
        "return_rate": return_rate,
    }

