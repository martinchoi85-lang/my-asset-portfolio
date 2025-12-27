from datetime import date, timedelta
from asset_portfolio.backend.services.portfolio_calculator import (
    calculate_daily_snapshots_for_asset
)
from asset_portfolio.backend.infra.supabase_client import get_supabase_client


from datetime import date
from asset_portfolio.backend.infra.supabase_client import get_supabase_client
from asset_portfolio.backend.services.portfolio_calculator import (
    calculate_daily_snapshots_for_asset
)


def generate_daily_snapshots(account_id: str, start_date: date, end_date: date):
    """
    특정 account에 대해
    거래가 존재하는 모든 자산의 daily snapshot을 생성한다.
    """

    supabase = get_supabase_client()

    # =========================
    # 1. 거래가 존재하는 asset_id 목록 조회
    # =========================
    tx_resp = (
        supabase.table("transactions")
        .select("asset_id")
        .eq("account_id", account_id)
        .execute()
    )

    asset_ids = sorted(
        {row["asset_id"] for row in (tx_resp.data or [])}
    )

    if not asset_ids:
        print(f"[INFO] account_id={account_id} 에 대한 거래 내역이 없습니다.")
        return

    # =========================
    # 2. 자산별 snapshot 생성
    # =========================
    for asset_id in asset_ids:
        snapshots = calculate_daily_snapshots_for_asset(
            asset_id=asset_id,
            account_id=account_id,
            start_date=start_date,
            end_date=end_date,
        )

        if not snapshots:
            continue

        # -------------------------
        # 3. DB insert
        # -------------------------

        # 🔽 날짜 타입을 문자열로 변환 (JSON 직렬화 대응)
        for row in snapshots:
            if isinstance(row.get("date"), (date,)):
                row["date"] = row["date"].isoformat()

        supabase.table("daily_snapshots").upsert(
            snapshots,
            on_conflict="date,asset_id, account_id",
        ).execute()

        print(
            f"[OK] asset_id={asset_id}, "
            f"{len(snapshots)} rows inserted"
        )
