from __future__ import annotations

import pandas as pd
from datetime import datetime, timezone

from asset_portfolio.backend.infra.supabase_client import get_supabase_client


CSV_PATH = "snapshot_260102.csv"   # ✅ 파일 경로에 맞게 수정
TX_DATE = datetime(2026, 1, 2, tzinfo=timezone.utc)  # ✅ INIT 기준일(오늘)


def main():
    supabase = get_supabase_client()

    df = pd.read_csv(CSV_PATH)

    # =========================
    # 1) 한글 컬럼명을 내부 표준명으로 매핑
    # =========================
    df = df.rename(columns={
        "현재가": "valuation_price",
        "평균매입가": "purchase_price",
        "평가 금액": "valuation_amount",
        "매입 금액": "purchase_amount",
    })

    # 숫자형 안전 변환
    for c in ["quantity", "valuation_price", "purchase_price", "valuation_amount", "purchase_amount"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    # =========================
    # 2) assets 테이블에서 ticker -> asset_id 조회 맵 만들기
    # - ticker는 unique 제약이 있다고 하셨으니 가장 안정적인 키입니다.
    # =========================
    assets_rows = supabase.table("assets").select("id, ticker, asset_type, currency").execute().data or []
    assets_map = {str(r["ticker"]): r for r in assets_rows}

    # =========================
    # 3) transactions insert payload 생성
    # 정책:
    # - 일반 자산: trade_type="INIT", quantity, price=평균매입가(purchase_price)
    # - CASH(예수금 등): 가격은 1로 두고, quantity=금액 으로 DEPOSIT로 넣는 편이 운영상 안전
    #   (CASH를 INIT로 넣어도 되지만, 이후 로직이 DEPOSIT/WITHDRAW 기반이면 혼란이 생길 수 있음)
    # =========================
    payload = []

    missing = []
    for _, r in df.iterrows():
        ticker = str(r["ticker"])
        account_id = str(r["account_id"])

        asset_row = assets_map.get(ticker)
        if not asset_row:
            missing.append(ticker)
            continue

        asset_id = int(asset_row["id"])
        asset_type = (asset_row.get("asset_type") or "").lower().strip()

        qty = float(r["quantity"])
        avg_price = float(r["purchase_price"])

        # ✅ CASH는 거래 타입을 DEPOSIT로 넣는 것을 권장
        if asset_type == "cash":
            # 평가금액(또는 매입금액)이 "잔고" 성격이라면 quantity로 사용
            # 우선순위: valuation_amount > purchase_amount > quantity
            cash_amt = float(r["valuation_amount"] or 0.0)
            if cash_amt <= 0:
                cash_amt = float(r["purchase_amount"] or 0.0)
            if cash_amt <= 0:
                cash_amt = qty

            payload.append({
                "transaction_date": TX_DATE.isoformat(),
                "asset_id": asset_id,
                "account_id": account_id,
                "trade_type": "DEPOSIT",
                "quantity": cash_amt,
                "price": 1.0,
                "fee": 0.0,
                "tax": 0.0,
                "memo": "초기 포트폴리오 반영(CASH)",
            })
        else:
            payload.append({
                "transaction_date": TX_DATE.isoformat(),
                "asset_id": asset_id,
                "account_id": account_id,
                "trade_type": "INIT",
                "quantity": qty,
                "price": avg_price,   # ✅ 평균매입가를 INIT 단가로 저장
                "fee": 0.0,
                "tax": 0.0,
                "memo": "초기 포트폴리오 반영",
            })

    if missing:
        # ✅ assets에 없는 ticker가 있으면 먼저 assets에 추가해야 함
        uniq = sorted(set(missing))
        raise RuntimeError(f"assets 테이블에 없는 ticker가 있습니다. 먼저 assets에 추가하세요: {uniq[:20]}{'...' if len(uniq)>20 else ''}")

    # =========================
    # 4) transactions 일괄 insert
    # - Supabase는 한번에 너무 큰 payload를 보내면 실패할 수 있으니 chunk 권장
    # =========================
    print(f"rows to insert: {len(payload)}")

    chunk_size = 200
    for i in range(0, len(payload), chunk_size):
        chunk = payload[i:i+chunk_size]
        supabase.table("transactions").insert(chunk).execute()

    print("DONE: inserted init/deposit transactions")


if __name__ == "__main__":
    main()
