from datetime import date
from asset_portfolio.backend.services.daily_snapshot_generator import (
    generate_daily_snapshots
)

# =========================
# 실제 account_id 입력
# =========================
accunt_ids = [
    "0dfa7606-ba14-47f0-afd4-e7d414ace467",
    "3a56b6c8-a994-4bb3-ba79-341961480fce",
    "3e84e330-6dc4-41de-93f9-b63ca744c127",
    "4746f6fb-b1d6-4606-b5ca-673d985301f6",
    "95eafbff-2397-4f5f-b916-57ce91f85fe8",
    "c835a7a6-56cd-450e-b438-4fcb06f46d8d",
    "e2f314a4-1347-49d3-8630-012bdbbdd921",
    "ff068fee-f3c5-4941-9a4d-e15d1d4157d4"
]
# ACCOUNT_ID = "여기에_실제_account_uuid"

if __name__ == "__main__":
    for ACCOUNT_ID in accunt_ids:
        generate_daily_snapshots(
            account_id=ACCOUNT_ID,
            start_date=date(2025, 12, 15),   # 거래 시작일 기준
            end_date=date.today(),
        )
