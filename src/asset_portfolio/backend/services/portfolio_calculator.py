# src/asset_portfolio/backend/services/portfolio_calculator.py
import pandas as pd
from collections import defaultdict
from datetime import date, timedelta
from asset_portfolio.backend.infra.supabase_client import get_supabase_client
from typing import List, Dict
"""
portfolio_calculator.py

[역할]
- 거래 + 가격 + 배당을 이용해 "하루 단위 상태(snapshot)" 계산
- DB, Supabase, pandas와 완전히 분리된
- '순수 계산 로직'만 담당하는 모듈

[설계 원칙]
- 입력이 같으면 출력은 항상 같다 (Pure Function)
- 외부 상태에 의존하지 않는다
- 테스트 가능성이 최우선
"""


def calculate_portfolio_state_at_date(account_id: str, target_date: date):
    """
    특정 계좌(account_id)에 대해
    target_date 종료 시점 기준 포트폴리오 상태를 계산한다.

    반환 형식:
    [
        {
            asset_id,
            quantity,
            purchase_price,
            purchase_amount,
            valuation_price,
            valuation_amount,
            currency
        }
    ]
    """

    supabase = get_supabase_client()

    # =========================
    # 1. 거래 내역 로드
    # =========================
    transactions = (
        supabase.table("transactions")
        .select(
            "asset_id, trade_type, quantity, price, transaction_date"
        )
        .eq("account_id", account_id)
        .lte("transaction_date", target_date.isoformat())
        .order("transaction_date")
        .execute()
        .data
    )

    if not transactions:
        return []

    # =========================
    # 2. 자산별 누적 상태 계산
    # =========================
    portfolio = defaultdict(lambda: {
        "quantity": 0.0,
        "purchase_amount": 0.0,
        "purchase_price": 0.0,
    })

    for tx in transactions:
        asset_id = tx["asset_id"]
        qty = float(tx["quantity"])
        price = float(tx["price"])
        trade_type = tx["trade_type"]

        asset = portfolio[asset_id]

        # -------------------------
        # 매수
        # -------------------------
        if trade_type == "BUY":
            new_qty = asset["quantity"] + qty
            new_purchase_amount = asset["purchase_amount"] + (qty * price)

            asset["quantity"] = new_qty
            asset["purchase_amount"] = new_purchase_amount
            asset["purchase_price"] = (
                new_purchase_amount / new_qty if new_qty > 0 else 0
            )

        # -------------------------
        # 매도 (부분 매도 포함)
        # -------------------------
        elif trade_type == "SELL":
            asset["quantity"] -= qty

            # 평균단가는 유지
            asset["purchase_amount"] = (
                asset["purchase_price"] * asset["quantity"]
            )

            # 전량 매도 시 정리
            if asset["quantity"] <= 0:
                asset["quantity"] = 0
                asset["purchase_amount"] = 0
                asset["purchase_price"] = 0

        # -------------------------
        # 배당 / 분배금
        # -------------------------
        elif trade_type == "DIVIDEND":
            # 원금 불변, 평가금만 증가
            asset["purchase_amount"] += price

        # -------------------------
        # 초기 잔고 반영 (INIT)
        # -------------------------
        elif trade_type == "INIT":
            asset["quantity"] += qty
            asset["purchase_amount"] += qty * price
            asset["purchase_price"] = price

    # =========================
    # 3. 현재가 로드
    # =========================
    asset_ids = list(portfolio.keys())

    prices = (
        supabase.table("assets")
        .select("id, current_price, currency")
        .in_("id", asset_ids)
        .execute()
        .data
    )

    price_map = {
        a["id"]: {
            "price": float(a["current_price"] or 0),
            "currency": a["currency"]
        }
        for a in prices
    }

    # =========================
    # 4. 최종 결과 구성
    # =========================
    result = []

    for asset_id, asset in portfolio.items():
        if asset["quantity"] <= 0:
            continue

        current_price = price_map.get(asset_id, {}).get("price", 0)
        currency = price_map.get(asset_id, {}).get("currency")

        valuation_amount = asset["quantity"] * current_price

        result.append({
            "asset_id": asset_id,
            "quantity": asset["quantity"],
            "purchase_price": asset["purchase_price"],
            "purchase_amount": asset["purchase_amount"],
            "valuation_price": current_price,
            "valuation_amount": valuation_amount,
            "currency": currency,
        })

    return result



def calculate_asset_return_series_from_snapshots(
    snapshots: List[Dict]
) -> List[Dict]:
    """
    daily_snapshots 데이터를 기반으로
    개별 자산의 누적 수익률 시계열을 계산한다.

    [입력]
    snapshots: 날짜 오름차순으로 정렬된 list[dict]
        예:
        {
            "date": "2025-01-02",
            "purchase_amount": 1000,
            "valuation_amount": 1100
        }

    [출력]
    list[dict]:
        각 날짜별 누적 수익률 포함
        {
            "date": "2025-01-02",
            "purchase_amount": 1000,
            "valuation_amount": 1100,
            "return_rate": 0.10
        }

    ⚠️ 주의
    - 이 함수는 DB 접근을 절대 하지 않는다
    - 스냅샷 생성 로직과도 분리되어 있다
    """

    # =========================
    # 방어 로직: 빈 데이터
    # =========================
    if not snapshots:
        return []

    result = []

    # =========================
    # 기준 매입금액 (누적 기준점)
    # =========================
    # - 일반적으로 첫 날의 purchase_amount를 기준으로 삼는다
    # - 현금성 자산, 단일 보유 자산 모두 동일한 규칙 적용
    base_purchase_amount = snapshots[0]["purchase_amount"]

    # 0으로 나누는 상황 방지 (이론적으로는 없어야 함)
    if base_purchase_amount == 0:
        base_purchase_amount = 1

    # =========================
    # 날짜별 누적 수익률 계산
    # =========================
    for snap in snapshots:
        purchase_amount = snap["purchase_amount"]
        valuation_amount = snap["valuation_amount"]

        # 누적 수익률 계산
        # (평가금액 - 기준 매입금액) / 기준 매입금액
        cumulative_return = (
            valuation_amount - base_purchase_amount
        ) / base_purchase_amount

        result.append({
            "date": snap["date"],
            "purchase_amount": purchase_amount,
            "valuation_amount": valuation_amount,
            "cumulative_return": cumulative_return
        })

    return result


def apply_transactions(transactions):
    """
    거래 내역을 순서대로 처리하여
    - 잔여 수량
    - 평균 매입가
    - 누적 실현 손익
    을 계산한다.
    """

    total_quantity = 0.0
    total_cost = 0.0
    realized_pnl = 0.0

    for tx in transactions:
        qty = float(tx["quantity"])
        price = float(tx["price"])

        # =========================
        # 매수 처리
        # =========================
        if tx["type"] == "BUY":
            total_cost += qty * price
            total_quantity += qty

        # =========================
        # 매도 처리 (부분 매도 포함)
        # =========================
        elif tx["type"] == "SELL":
            if total_quantity <= 0:
                raise ValueError("보유 수량 없이 매도할 수 없습니다.")

            avg_cost = total_cost / total_quantity  # 평균 매입가 계산
            sell_cost = qty * avg_cost  # 매도한 수량의 원가
            sell_value = qty * price    # 매도한 수량의 매출액

            realized_pnl += sell_value - sell_cost  # 실현 손익 계산

            total_quantity -= qty    # 잔여 수량 차감
            total_cost -= sell_cost  # 잔여 원가 차감

    avg_price = total_cost / total_quantity if total_quantity > 0 else 0.0

    return {
        "quantity": total_quantity,
        "average_price": avg_price,
        "realized_pnl": realized_pnl,
        "remaining_cost": total_cost,
    }



def calculate_daily_snapshots_for_asset(
    asset_id: int,
    account_id: str,
    start_date: date,
    end_date: date,
):
    """
    특정 자산에 대해 일별 snapshot 데이터를 계산한다.
    (DB 저장 X, 계산 결과만 반환)

    반환값:
    [
      {
        date,
        asset_id,
        account_id,
        quantity,
        valuation_price,
        purchase_price,
        valuation_amount,
        purchase_amount,
        currency
      },
      ...
    ]
    """

    supabase = get_supabase_client()

    # =========================
    # 1. 거래 내역 조회
    # =========================
    tx_resp = (
        supabase.table("transactions")
        .select("*")
        .eq("asset_id", asset_id)
        .eq("account_id", account_id)
        .order("transaction_date")
        .execute()
    )

    transactions = tx_resp.data or []

    if not transactions:
        return []

    # =========================
    # 2. 자산 기본 정보
    # =========================
    asset_resp = (
        supabase.table("assets")
        .select("currency")
        .eq("id", asset_id)
        .single()
        .execute()
    )

    currency = asset_resp.data["currency"]

    # =========================
    # 3. 날짜 루프
    # =========================
    snapshots = []

    current_qty = 0
    total_purchase_amount = 0

    current_date = start_date
    tx_idx = 0

    while current_date <= end_date:
        # -------------------------
        # 해당 날짜까지의 거래 반영
        # -------------------------
        while (
            tx_idx < len(transactions)
            and transactions[tx_idx]["transaction_date"] <= str(current_date)
        ):
            tx = transactions[tx_idx]

            qty = tx["quantity"]
            price = tx["price"]

            if tx["trade_type"] in ("BUY", "INIT"):
                current_qty += qty
                total_purchase_amount += qty * price

            elif tx["trade_type"] == "SELL":
                avg_price = (
                    total_purchase_amount / current_qty
                    if current_qty > 0 else 0
                )
                total_purchase_amount -= avg_price * qty
                current_qty -= qty

            elif tx["trade_type"] == "DIVIDEND":
                # 배당은 평가금액 증가로 반영
                total_purchase_amount += 0

            tx_idx += 1

        # -------------------------
        # 현재가 조회 (임시: 평균매입가)
        # 👉 향후 크롤링/가격 테이블로 대체
        # -------------------------
        valuation_price = (
            total_purchase_amount / current_qty
            if current_qty > 0 else 0
        )

        valuation_amount = current_qty * valuation_price

        snapshots.append({
            "date": current_date,
            "asset_id": asset_id,
            "account_id": account_id,
            "quantity": current_qty,
            "valuation_price": valuation_price,
            "purchase_price": (
                total_purchase_amount / current_qty
                if current_qty > 0 else 0
            ),
            "valuation_amount": valuation_amount,
            "purchase_amount": total_purchase_amount,
            "currency": currency,
        })

        current_date += timedelta(days=1)

    return snapshots



def calculate_portfolio_return_series_from_snapshots(
    snapshots: List[Dict],
) -> pd.DataFrame:
    """
    daily_snapshots 데이터를 기반으로
    포트폴리오 전체 누적 수익률 시계열을 계산한다.

    snapshots 예시:
    [
        {
            "date": "2025-01-01",
            "valuation_amount": 100000,
            "purchase_amount": 95000,
        },
        ...
    ]
    """

    if not snapshots:
        return pd.DataFrame()

    df = pd.DataFrame(snapshots)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    # =========================
    # 누적 수익률 계산 전 0 이하 snapshot은 필터링
    # =========================
    df = df[
        (df["valuation_amount"] > 0)
        & (df["purchase_amount"] > 0)
    ]

    df["portfolio_return"] = (
        df["valuation_amount"] / df["purchase_amount"] - 1
    )

    return df[["date", "portfolio_return", "valuation_amount", "purchase_amount"]]