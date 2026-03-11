import os
from datetime import date
from asset_portfolio.backend.infra.supabase_client import get_supabase_client
from asset_portfolio.backend.services.portfolio_calculator import _load_asset_price_history, _to_date
from datetime import timedelta

asset_id = 7
account_id = "09d704b6-a7b2-4aaa-bba3-3601308f6144"
start_date = date(2026, 3, 11)
end_date = date(2026, 3, 11)

supabase = get_supabase_client()
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

is_cash = asset_type == "cash"
current_price = float(asset_row.get("current_price") or 0)

price_history = _load_asset_price_history(asset_id, start_date, end_date)
price_idx = 0
last_price = None

print("asset_row:", asset_row)
print("asset_type:", asset_type)
print("is_cash:", is_cash)
print("current_price:", current_price)
print("price_history:", price_history)

tx_resp = (
    supabase.table("transactions")
    .select("trade_type, quantity, price, transaction_date")
    .eq("asset_id", asset_id)
    .eq("account_id", account_id)
    .order("transaction_date")
    .execute()
)
transactions = tx_resp.data or []
current_qty = 0.0
total_purchase_amount = 0.0
current_date = start_date
tx_idx = 0

while current_date <= end_date:
    processed_tx_on_date = False
    while tx_idx < len(transactions) and _to_date(transactions[tx_idx]["transaction_date"]) <= current_date:
        tx = transactions[tx_idx]
        processed_tx_on_date = True
        trade_type = tx["trade_type"]
        qty = float(tx["quantity"])
        price = float(tx["price"])
        if trade_type in ("BUY", "INIT"):
            current_qty += qty
            total_purchase_amount += qty * price
        elif trade_type == "SELL":
            avg_price = (total_purchase_amount / current_qty) if current_qty > 0 else 0.0
            total_purchase_amount -= avg_price * qty
            current_qty -= qty
            if current_qty <= 0:
                current_qty = 0.0
                total_purchase_amount = 0.0
        tx_idx += 1
        
    if tx_idx >= len(transactions) and current_qty <= 0 and not processed_tx_on_date:
        break

    purchase_price = 0.0
    if current_qty > 0:
        purchase_price = total_purchase_amount / current_qty

    if not is_cash and price_history:
        while price_idx < len(price_history) and price_history[price_idx][0] <= current_date:
            last_price = price_history[price_idx][1]
            price_idx += 1
            
    if is_cash:
        valuation_price = 1.0
        print("branch A (is_cash)")
    else:
        if last_price is not None and float(last_price) > 0:
            valuation_price = float(last_price)
            print("branch B (last_price)", last_price)
        elif current_price is not None and float(current_price) > 0:
            valuation_price = float(current_price)
            print("branch C (current_price)", current_price)
        else:
            valuation_price = float(purchase_price)
            print("branch D (purchase_price)", purchase_price)

    print("valuation_price calculated:", valuation_price)
    current_date += timedelta(days=1)
