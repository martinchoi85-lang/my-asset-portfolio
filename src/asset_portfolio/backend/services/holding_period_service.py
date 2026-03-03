from datetime import date, datetime
from typing import List, Dict, Any
import pandas as pd
from collections import defaultdict

from asset_portfolio.backend.infra.supabase_client import get_supabase_client

def _to_date(v: Any) -> date:
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, str):
        v = v.strip().replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(v).date()
        except ValueError:
            return datetime.fromisoformat(v[:10]).date()
    raise TypeError(f"Unsupported date type: {type(v)}")

def calculate_holding_periods(user_id: str, account_id: str, reference_date: date = None) -> pd.DataFrame:
    """
    TWR/FIFO 기반으로 자산별 보유 기간(Holding Period)을 계산합니다.
    (현금 자산은 제외)
    """
    if reference_date is None:
        reference_date = date.today()

    supabase = get_supabase_client()

    # 1. 대상 계좌 확인
    if not account_id or account_id == "__ALL__":
        # 전체 계좌 조회 시
        acc_resp = supabase.table("accounts").select("id").eq("user_id", user_id).execute()
        account_ids = [acc["id"] for acc in acc_resp.data or []]
    else:
        account_ids = [account_id]

    if not account_ids:
        return pd.DataFrame()

    # 2. 거래 내역 조회 (수량 계산을 위해 시간순 정렬)
    # 필요한 컬럼만 추출, assets 테이블 조인하여 자산 타입 등 가져옴
    tx_resp = (
        supabase.table("transactions")
        .select(
            "id, asset_id, trade_type, quantity, price, transaction_date, "
            "assets (ticker, name_kr, asset_type)"
        )
        .in_("account_id", account_ids)
        .order("transaction_date")
        .execute()
    )
    transactions = tx_resp.data or []

    if not transactions:
        return pd.DataFrame()

    # 3. 자산별 로트(Lot) 관리
    # asset_id -> [{"date": date, "quantity": float, "price": float}]
    lots_by_asset = defaultdict(list)
    asset_info = {}

    for tx in transactions:
        asset_id = tx["asset_id"]
        trade_type = tx["trade_type"]
        qty = float(tx["quantity"])
        price = float(tx["price"])
        tx_date = _to_date(tx["transaction_date"])
        
        # 미래의 거래는 제외 (reference_date 기준)
        if tx_date > reference_date:
            continue

        a_info = tx.get("assets") or {}
        if isinstance(a_info, list):
            # SDK 이슈로 list가 반환되는 경우 첫 번째 요소 사용
            a_info = a_info[0] if a_info else {}
            
        a_type = str(a_info.get("asset_type") or "").lower()
        
        # 현금 자산은 보유기간 분석에서 제외
        if a_type == "cash":
            continue
            
        if asset_id not in asset_info:
            asset_info[asset_id] = {
                "ticker": a_info.get("ticker", ""),
                "name_kr": a_info.get("name_kr", "")
            }

        # 매수/입고
        if trade_type in ("BUY", "INIT"):
            lots_by_asset[asset_id].append({
                "date": tx_date,
                "quantity": qty,
                "price": price
            })
        
        # 매도/출고 (FIFO 로직)
        elif trade_type == "SELL":
            remaining_to_sell = qty
            asset_lots = lots_by_asset[asset_id]
            
            while remaining_to_sell > 1e-6 and asset_lots:
                oldest_lot = asset_lots[0]
                if oldest_lot["quantity"] <= remaining_to_sell + 1e-6:
                    # 이 로트 전량 소진
                    remaining_to_sell -= oldest_lot["quantity"]
                    asset_lots.pop(0)
                else:
                    # 이 로트 일부 소진
                    oldest_lot["quantity"] -= remaining_to_sell
                    remaining_to_sell = 0

    # 4. 자산별 보유기간 수치 계산
    results = []
    
    for asset_id, lots in lots_by_asset.items():
        if not lots:
            continue
            
        total_qty = sum(lot["quantity"] for lot in lots)
        if total_qty <= 1e-6:
            continue
            
        first_buy_date = lots[0]["date"]
        
        # 가중 평균 보유일수 계산
        # 각 로트별 (현재일 - 매수일) * 수량 의 합 / 총 수량
        total_weighted_days = 0.0
        
        for lot in lots:
            days_held = (reference_date - lot["date"]).days
            # 0일보다 작을 수 없음
            days_held = max(0, days_held)
            total_weighted_days += days_held * lot["quantity"]
            
        avg_holding_days = total_weighted_days / total_qty
        
        # 장기(365일 이상) / 단기 수량 비중 계산
        long_term_qty = sum(lot["quantity"] for lot in lots if (reference_date - lot["date"]).days >= 365)
        short_term_qty = total_qty - long_term_qty
        
        info = asset_info[asset_id]
        results.append({
            "asset_id": asset_id,
            "ticker": info["ticker"],
            "name_kr": info["name_kr"],
            "remaining_quantity": total_qty,
            "first_buy_date": first_buy_date,
            "avg_holding_days": avg_holding_days,
            "long_term_qty": long_term_qty,
            "short_term_qty": short_term_qty,
            "long_term_ratio": long_term_qty / total_qty if total_qty > 0 else 0.0,
            "short_term_ratio": short_term_qty / total_qty if total_qty > 0 else 0.0
        })

    return pd.DataFrame(results)
