import pandas as pd
import streamlit as st
from supabase import create_client
# from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os
from datetime import datetime

# =========================
# 환경 설정
# =========================
load_dotenv()

@st.cache_resource
def init_connection():
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    return create_client(url, key)

supabase = init_connection()

INIT_DATE = datetime(2026, 1, 2)

# =========================
# CSV 로드
# =========================
df = pd.read_csv("init_assets.csv")
df = df.fillna("")

# =========================
# Helper: account upsert
# =========================
def get_or_create_account(row):
    brokerage = row["증권사"]
    account_type = row["계좌"]
    owner = row["owner"]

    resp = (
        supabase.table("accounts")
        .select("id")
        .eq("brokerage", brokerage)
        .eq("type", account_type)
        .eq("owner", owner)
        .execute()
    )

    if resp.data:
        return resp.data[0]["id"]

    payload = {
        "name": f"{brokerage}_{account_type}_{owner}",
        "brokerage": brokerage,
        "type": account_type,
        "owner": owner
    }

    inserted = supabase.table("accounts").insert(payload).execute()
    return inserted.data[0]["id"]



# =========================
# Helper: asset upsert
# =========================
def get_or_create_asset(row):
    ticker = row["티커(코드)"] if row["티커(코드)"] else f"AUTO_{row['종목명']}"

    resp = supabase.table("assets") \
        .select("id") \
        .eq("ticker", ticker) \
        .execute()

    if resp.data:
        return resp.data[0]["id"]

    insert_resp = supabase.table("assets").insert({
        "ticker": ticker,
        "name_kr": row["종목명"],
        "asset_type": row["vehicle_type"],
        "currency": "krw" if row["economic_exposure_region"] == "korea" else "usd",
        "market": row["economic_exposure_region"],
        "current_price": float(row["현재가"] or 0),
        "underlying_asset_class": row["underlying_asset_class"],
        "economic_exposure_region": row["economic_exposure_region"],
        "asset_nature": row["asset_nature"],
        "vehicle_type": row["vehicle_type"],
        "fx_exposure_type": row["fx_exposure_type"],
        "return_driver": row["return_driver"],
        "strategy_type": row["strategy_type"],
        "lookthrough_available": row["lookthrough_available"] == True
    }).execute()

    return insert_resp.data[0]["id"]


# =========================
# Helper: Insert initial transactions
# =========================    
INIT_DATE = "2026-01-02T00:00:00"

def insert_init_transaction(asset_id, account_id, row):
    qty = float(row["잔고수량"] or 0)
    price = float(row["평균매입가"] or 0)

    if qty == 0 and float(row["매입 금액"] or 0) > 0:
        qty = 1
        price = float(row["매입 금액"])

    supabase.table("transactions").insert({
        "transaction_date": INIT_DATE,
        "asset_id": asset_id,
        "account_id": account_id,
        "trade_type": "INIT",
        "quantity": qty,
        "price": price,
        "fee": float(row["수수료"] or 0),
        "tax": float(row["세금"] or 0),
        "memo": "초기 포트폴리오 반영"
    }).execute()


# =========================
# Helper: TDF segments
# =========================
def insert_segments(asset_id, row):
    if row.get("lookthrough_available") != True:
        return

    seg_classes = row.get("segment_asset_class")
    seg_weights = row.get("segment_weight")

    if not seg_classes or not seg_weights:
        return

    classes = [c.strip() for c in seg_classes.split(",") if c.strip()]
    weights = [w.strip() for w in seg_weights.split(",") if w.strip()]

    if len(classes) != len(weights):
        return

    payload = []

    for c, w in zip(classes, weights):
        try:
            weight_value = float(
                w.replace("Equity_", "")
                 .replace("Fixed_", "")
                 .replace("%", "")
            )
        except ValueError:
            continue

        payload.append({
            "asset_id": asset_id,
            "segment_asset_class": c,
            "weight": weight_value
        })

    if payload:
        supabase.table("asset_segments").insert(payload).execute()



# =========================
# Main Loop
# =========================
# =========================
# Main Loop (Supabase Version)
# =========================

INIT_DATE = "2026-01-02T00:00:00"

for _, row in df.iterrows():
    # 1. Account
    account_id = get_or_create_account(row)

    # 2. Asset
    asset_id = get_or_create_asset(row)

    # 3. TDF / Look-through Segments
    insert_segments(asset_id, row)

    # 4. INIT 거래 수량 / 단가 계산
    quantity = float(row["잔고수량"] or 0)
    price = float(row["평균매입가"] or 0)

    # 예수금 / 펀드형 자산 처리
    if quantity == 0 and float(row["매입 금액"] or 0) > 0:
        quantity = 1
        price = float(row["매입 금액"])

    # 5. INIT Transaction Insert
    supabase.table("transactions").insert({
        "transaction_date": INIT_DATE,
        "asset_id": asset_id,
        "account_id": account_id,
        "trade_type": "INIT",
        "quantity": quantity,
        "price": price,
        "fee": float(row["수수료"] or 0),
        "tax": float(row["세금"] or 0),
        "memo": "초기 포트폴리오 반영"
    }).execute()

print("초기 포트폴리오 적재 완료")