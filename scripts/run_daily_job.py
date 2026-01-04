from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path
import traceback

# ✅ 프로젝트 루트를 PYTHONPATH에 추가 (Task Scheduler 환경에서 import 실패 방지)
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from asset_portfolio.backend.infra.supabase_client import get_supabase_client
from asset_portfolio.backend.services.price_updater_service import PriceUpdaterService
from asset_portfolio.backend.services.daily_snapshot_generator import generate_daily_snapshots


def _get_all_account_ids() -> list[str]:
    supabase = get_supabase_client()
    rows = supabase.table("accounts").select("id").execute().data or []
    return [r["id"] for r in rows if r.get("id")]


def _get_all_asset_ids_for_price_update() -> list[int]:
    """
    ✅ 가격 업데이트 대상 자산만 선정
    - cash / 수동평가 자산은 제외 (운영 안정성)
    - 필요 시 asset_type 리스트를 확장하세요.
    """
    supabase = get_supabase_client()
    rows = (
        supabase.table("assets")
        .select("id, asset_type")
        .execute()
        .data or []
    )

    excluded = {"cash", "manual", "deposit", "bond"}  # ✅ 필요 시 확장
    ids: list[int] = []
    for r in rows:
        at = (r.get("asset_type") or "").lower().strip()
        if at in excluded:
            continue
        ids.append(int(r["id"]))
    return ids


def main():
    # =========================
    # 1) Price update
    # =========================
    asset_ids = _get_all_asset_ids_for_price_update()
    print(f"[JOB] price update target assets: {len(asset_ids)}")

    results = PriceUpdaterService.update_many(asset_ids)
    ok = sum(1 for r in results if r.ok)
    failed = sum(1 for r in results if not r.ok)
    print(f"[JOB] price update result: ok={ok}, failed={failed}")

    # 실패 샘플 몇 개만 출력 (로그 폭주 방지)
    for r in results:
        if not r.ok:
            print(f"[FAILED] asset_id={r.asset_id}, ticker={r.ticker}, reason={r.reason}")
            break

    # =========================
    # 2) Snapshot generation (today까지)
    # =========================
    account_ids = _get_all_account_ids()
    print(f"[JOB] snapshot target accounts: {len(account_ids)}")

    # ✅ 운영 정책: 과거 시작일은 “각 계좌의 첫 거래일”을 쓰는 것이 좋지만
    # 지금은 최소 운영 목적이므로 “오늘~오늘”로도 가능.
    # 다만 시계열 차트를 위해 최소 30일을 권장합니다.
    # 아래는 안전하게 최근 60일로 생성하는 예시입니다.
    # 필요하면 30일로 줄이세요.
    from datetime import timedelta
    start_date = date.today() - timedelta(days=60)
    end_date = date.today()

    total_pairs = 0
    for acc_id in account_ids:
        try:
            generate_daily_snapshots(
                account_id=str(acc_id),
                start_date=start_date,
                end_date=end_date,
            )
            total_pairs += 1
            print(f"[JOB] snapshots generated: account_id={acc_id}, range={start_date}~{end_date}")
        except Exception:
            print(f"[ERROR] snapshot generation failed: account_id={acc_id}")
            print(traceback.format_exc())

    print(f"[JOB] done. processed accounts={total_pairs}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("[FATAL] job failed")
        print(traceback.format_exc())
        raise
