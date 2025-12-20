import pandas as pd
from datetime import date
from collections import defaultdict
from src.asset_portfolio.backend.services.portfolio_service import calculate_asset_summary
from src.asset_portfolio.backend.services.portfolio_calculator import apply_transactions
from src.asset_portfolio.backend.infra.supabase_client import get_supabase_client


def generate_daily_snapshots(snapshot_date: date):
    """
    특정 날짜 기준으로 daily_snapshots 테이블을 생성/재생성한다.

    - snapshot_date 이전의 모든 거래를 기준으로 계산
    - 부분 매도, 전량 매도 모두 반영
    """

    supabase = get_supabase_client()

    # =========================
    # 1. 거래 내역 조회
    # =========================
    transactions = (
        supabase
        .table("transactions")
        .select("*")
        .lte("transaction_date", snapshot_date.isoformat())
        .order("transaction_date")
        .execute()
        .data
    )

    if not transactions:
        return 0

    # =========================
    # 2. 자산별 / 계좌별 거래 묶기
    # =========================
    grouped = defaultdict(list)

    for tx in transactions:
        key = (tx["asset_id"], tx["account_id"])
        grouped[key].append(tx)

    snapshots = []

    # =========================
    # 3. 각 자산-계좌 조합별 계산
    # =========================
    for (asset_id, account_id), tx_list in grouped.items():

        # 거래 내역을 계산기에 전달
        result = apply_transactions(tx_list)

        # 전량 매도된 경우 snapshot 생성하지 않음
        if result["quantity"] <= 0:
            continue

        # =========================
        # 현재가 조회 (assets 테이블 기준)
        # =========================
        asset = (
            supabase
            .table("assets")
            .select("current_price, currency")
            .eq("id", asset_id)
            .single()
            .execute()
            .data
        )

        current_price = float(asset["current_price"])
        currency = asset["currency"]

        # =========================
        # snapshot 레코드 생성
        # =========================
        snapshot = {
            "date": snapshot_date.isoformat(),
            "asset_id": asset_id,
            "account_id": account_id,
            "quantity": result["quantity"],
            "purchase_price": result["average_price"],
            "purchase_amount": result["remaining_cost"],
            "valuation_price": current_price,
            "valuation_amount": result["quantity"] * current_price,
            "currency": currency,
        }

        snapshots.append(snapshot)

    # =========================
    # 4. 기존 snapshot 삭제 후 재삽입
    # =========================
    supabase.table("daily_snapshots") \
        .delete() \
        .eq("date", snapshot_date.isoformat()) \
        .execute()

    if snapshots:
        supabase.table("daily_snapshots").insert(snapshots).execute()

    return len(snapshots)

