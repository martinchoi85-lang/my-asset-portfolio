# src/asset_portfolio/backend/services/portfolio_calculator.py
import pandas as pd
from collections import defaultdict
from datetime import datetime, date, timedelta, timezone
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


def _to_date(v) -> date:
    """
    ✅ Supabase에서 오는 timestamp(with tz) / 문자열 / datetime을 date로 정규화
    - 문자열 비교로 날짜를 비교하면 오동작할 수 있으므로 반드시 date로 바꿉니다.
    """
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, str):
        s = v.strip().replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(s).date()
        except ValueError:
            # 'YYYY-MM-DD ...' 형태면 앞 10자리만으로 date 파싱
            return datetime.fromisoformat(s[:10]).date()
    raise TypeError(f"Unsupported date type: {type(v)}")


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

        # -------------------------
        # 현금 입금 (예수금)
        # -------------------------
        elif trade_type == "DEPOSIT":
            # 현금: 단가 1, quantity는 금액(잔고)
            new_qty = asset["quantity"] + qty
            new_purchase_amount = asset["purchase_amount"] + (qty * price)  # price=1

            asset["quantity"] = new_qty
            asset["purchase_amount"] = new_purchase_amount
            asset["purchase_price"] = (new_purchase_amount / new_qty) if new_qty > 0 else 0

        # -------------------------
        # 현금 출금
        # -------------------------
        elif trade_type == "WITHDRAW":
            asset["quantity"] -= qty
            asset["purchase_amount"] = asset["purchase_price"] * asset["quantity"]

            if asset["quantity"] <= 0:
                asset["quantity"] = 0
                asset["purchase_amount"] = 0
                asset["purchase_price"] = 0

    # =========================
    # 3. 현재가 로드
    # =========================
    asset_ids = list(portfolio.keys())

    prices = (
        supabase.table("assets")
        .select("id, current_price, currency, asset_type")
        .in_("id", asset_ids)
        .execute()
        .data
    )

    price_map = {
    a["id"]: {
        "price": float(a["current_price"] or 0),
        "currency": (a.get("currency") or "").lower().strip(),
        "asset_type": (a.get("asset_type") or "").lower().strip(),
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
        
        # ✅ cash면 1로 강제
        if price_map.get(asset_id, {}).get("asset_type") == "cash":
            current_price = 1.0
            
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
) -> pd.DataFrame:
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
    반환 컬럼:
      - date (datetime64)
      - valuation_amount (float)
      - purchase_amount (float)
      - portfolio_return (float)   # 0.10 = +10%

    ⚠️ 주의
    - 이 함수는 DB 접근을 절대 하지 않는다
    - 스냅샷 생성 로직과도 분리되어 있다
    """

    # =========================
    # 방어 로직: 빈 데이터
    # =========================
    if not snapshots:
        return pd.DataFrame()

    df = pd.DataFrame(snapshots)

    # =========================
    # 기준 매입금액 (누적 기준점)
    # =========================
    # - 일반적으로 첫 날의 purchase_amount를 기준으로 삼는다
    # - 현금성 자산, 단일 보유 자산 모두 동일한 규칙 적용
    # base_purchase_amount = snapshots[0]["purchase_amount"]

    # # 0으로 나누는 상황 방지 (이론적으로는 없어야 함)
    # if base_purchase_amount == 0:
    #     base_purchase_amount = 1

    # # =========================
    # # 날짜별 누적 수익률 계산
    # # =========================
    # for snap in snapshots:
    #     purchase_amount = snap["purchase_amount"]
    #     valuation_amount = snap["valuation_amount"]

    #     # 누적 수익률 계산
    #     # (평가금액 - 기준 매입금액) / 기준 매입금액
    #     cumulative_return = (
    #         valuation_amount - base_purchase_amount
    #     ) / base_purchase_amount

    #     result.append({
    #         "date": snap["date"],
    #         "purchase_amount": purchase_amount,
    #         "valuation_amount": valuation_amount,
    #         "cumulative_return": cumulative_return
    #     })
    # 
    # return result

    base_purchase_amount = df.iloc[0]["purchase_amount"]

    # 0으로 나누는 상황 방지 (이론적으로는 없어야 함)
    if base_purchase_amount == 0:
        base_purchase_amount = 1

    # df[cumulative_return = (
    #         valuation_amount - base_purchase_amount
    #     ) / base_purchase_amount

    df["date"] = pd.to_datetime(df["date"])
    df["valuation_amount"] = df["valuation_amount"].astype(float)
    df["purchase_amount"] = df["purchase_amount"].astype(float)

    df = df.sort_values("date")

    # ✅ purchase_amount가 0이면 수익률은 계산 불가 → NaN 처리
    df["portfolio_return"] = (df["valuation_amount"] / df["purchase_amount"]) - 1.0
    df.loc[df["purchase_amount"] <= 0, "portfolio_return"] = pd.NA

    return df[["date", "valuation_amount", "purchase_amount", "portfolio_return"]]

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


def calculate_daily_snapshots_for_asset(asset_id: int, account_id: str, start_date: date, end_date: date):
    """
    특정 자산에 대해 일별 snapshot 데이터를 계산한다.
    (DB 저장 X, 계산 결과만 반환)

    ✅ 반영 정책
    - BUY/INIT: 수량 증가, 원가 증가
    - SELL: 평균단가로 원가 감소, 수량 감소
    - DEPOSIT: (현금 자산) 수량(=잔고금액) 증가, 원가 증가 (price=1)
    - WITHDRAW: (현금 자산) 수량(=잔고금액) 감소, 원가 감소 (price=1)
    - 전량 매도/출금 후에도 quantity=0 row는 유지(스냅샷 연속성)
    - 현금 자산은 valuation_price=1 고정, valuation_amount=quantity
    """
    supabase = get_supabase_client()

    # =========================
    # 1) 거래 조회(기존과 동일)
    # =========================
    tx_resp = (
        supabase.table("transactions")
        .select("trade_type, quantity, price, transaction_date")
        .eq("asset_id", asset_id)
        .eq("account_id", account_id)
        .order("transaction_date")
        .execute()
    )
    transactions = tx_resp.data or []
    if not transactions:
        return []

    # =========================
    # 2) 자산 메타 조회: currency, asset_type, current_price
    # ✅ valuation_price를 current_price로 쓰기 위해 current_price를 반드시 가져옵니다.
    # =========================
    asset_resp = (
        supabase.table("assets")
        .select("currency, asset_type, current_price")
        .eq("id", asset_id)
        .single()
        .execute()
    )
    asset_row = asset_resp.data or {}
    currency = asset_row.get("currency")
    asset_type = (asset_row.get("asset_type") or "").lower()

    # ✅ 현금 자산은 평가단가 1 고정
    is_cash = (asset_type in ["cash", "deposit", "fund"])

    # ✅ 비현금 자산은 assets.current_price를 평가단가로 사용 (V1.1)
    # - 주의: 이 값은 과거 날짜에도 동일하게 적용됩니다. (가격 히스토리 테이블 도입 전까지의 한계)
    current_price = float(asset_row.get("current_price") or 0)

    # =========================
    # 3) 누적 상태 변수(기존과 동일)
    # =========================
    current_qty = 0.0
    total_purchase_amount = 0.0

    snapshots = []
    current_date = start_date
    tx_idx = 0

    while current_date <= end_date:
        # -------------------------
        # (A) 오늘까지의 거래 반영
        # -------------------------
        while tx_idx < len(transactions) and _to_date(transactions[tx_idx]["transaction_date"]) <= current_date:
            tx = transactions[tx_idx]
            trade_type = tx["trade_type"]
            qty = float(tx["quantity"])
            price = float(tx["price"])

            # ✅ BUY/INIT: 수량 증가, 원가 증가
            if trade_type in ("BUY", "INIT"):
                current_qty += qty
                total_purchase_amount += qty * price

            # ✅ SELL: 평균단가 기준 원가 차감, 수량 감소
            elif trade_type == "SELL":
                avg_price = (total_purchase_amount / current_qty) if current_qty > 0 else 0.0
                total_purchase_amount -= avg_price * qty
                current_qty -= qty
                # ✅ 전량 매도 시 0으로 정리(0-row는 유지)
                if current_qty <= 0:
                    current_qty = 0.0
                    total_purchase_amount = 0.0

            # ✅ 현금 입금/출금: cash 자산에서만 허용, price=1
            elif trade_type == "DEPOSIT":
                if not is_cash:
                    raise ValueError("DEPOSIT은 cash 자산에서만 허용됩니다. assets.asset_type='cash' 확인")
                current_qty += qty
                total_purchase_amount += qty * price  # price=1 → +qty

            elif trade_type == "WITHDRAW":
                if not is_cash:
                    raise ValueError("WITHDRAW는 cash 자산에서만 허용됩니다. assets.asset_type='cash' 확인")
                current_qty -= qty
                total_purchase_amount -= qty * price  # price=1 → -qty
                if current_qty <= 0:
                    current_qty = 0.0
                    total_purchase_amount = 0.0

            # ✅ DIVIDEND는 V1에서 별도 editor로 분리 (여기서는 영향 없음)
            elif trade_type == "DIVIDEND":
                pass

            tx_idx += 1
            
        # =========================
        # 평균매입단가(purchase_price) 계산
        # =========================
        # ✅ 보유수량이 0이면 평균단가를 0으로 둡니다.
        purchase_price = 0.0
        if current_qty > 0:
            purchase_price = total_purchase_amount / current_qty
            
        # -------------------------
        # (B) 평가단가(valuation_price) 결정
        # -------------------------
        if is_cash:
            # ✅ CASH는 단가 1 고정 (잔고를 quantity로 들고 가므로)
            valuation_price = 1.0
        else:
            # ✅ 비현금 자산: current_price를 우선 사용
            # ✅ 중요: 업데이트 실패로 current_price가 0/None이면,
            #          valuation_price가 0이 되어 차트에서 자산이 빠질 수 있으므로 fallback 필요
            if current_price is not None and float(current_price) > 0:
                valuation_price = float(current_price)
            else:
                # ✅ fallback: 평균매입단가로 평가(최소한 0이 되지 않게)
                valuation_price = float(purchase_price)
        
        # -------------------------
        # (C) 금액/단가 계산
        # -------------------------
        valuation_amount = float(current_qty) * float(valuation_price)
        urchase_amount = float(total_purchase_amount)

        # ✅ 요구사항: quantity=0이어도 row를 유지 (차트에서는 0이 사실상 보이지 않도록 필터링 가능)
        snapshots.append({
            "date": current_date,
            "asset_id": asset_id,
            "account_id": account_id,
            "quantity": current_qty,
            "valuation_price": valuation_price,
            "purchase_price": purchase_price,
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
    daily_snapshots 데이터를 기반으로 포트폴리오 전체 누적 수익률 시계열을 계산한다.
    - purchase_amount/valuation_amount 조건으로 행을 통째로 삭제하지 않는다.
    - purchase_amount가 0이면 해당 일자의 return은 NaN으로 두고,
      이후 구간에서 forward-fill로 시계열을 유지한다.
    """
    if not snapshots:
        return pd.DataFrame()

    df = pd.DataFrame(snapshots)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    # ✅ 숫자형 변환/결측 방어
    df["valuation_amount"] = pd.to_numeric(df["valuation_amount"], errors="coerce")
    df["purchase_amount"] = pd.to_numeric(df["purchase_amount"], errors="coerce")

    # ✅ purchase_amount가 0이거나 NaN이면 return 계산이 불가하므로 NaN 처리
    df["portfolio_return"] = None
    mask = (df["purchase_amount"] > 0) & (df["valuation_amount"] >= 0)
    df.loc[mask, "portfolio_return"] = df.loc[mask, "valuation_amount"] / df.loc[mask, "purchase_amount"] - 1

    # ✅ 시계열 유지: return이 비는 구간은 이전 값으로 채움(원하면 0으로 채워도 됨)
    df["portfolio_return"] = pd.to_numeric(df["portfolio_return"], errors="coerce").ffill().fillna(0.0)

    return df[["date", "portfolio_return", "valuation_amount", "purchase_amount"]]
