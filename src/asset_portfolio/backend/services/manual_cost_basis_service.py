from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

from asset_portfolio.backend.infra.supabase_client import get_supabase_client


def _build_cost_basis_map(rows: List[Dict]) -> Dict[Tuple[str, int], Dict[str, float]]:
    """
    manual_asset_cost_basis_current 테이블 결과를
    (account_id, asset_id) → {cost_basis_amount, currency, as_of_date} 형태로 변환한다.

    ✅ 초보자를 위한 설명
    - SQL에서 불러온 rows는 list[dict] 형태다.
    - 조인/매칭을 빠르게 하려면 key 기반 dict로 바꾸는 것이 유리하다.
    - key는 (계좌, 자산) 조합이므로 tuple을 사용한다.
    """
    cost_basis_map: Dict[Tuple[str, int], Dict[str, float]] = {}
    for row in rows:
        account_id = row.get("account_id")
        asset_id = row.get("asset_id")
        if not account_id or asset_id is None:
            # 데이터가 불완전하면 스킵
            continue
        key = (account_id, int(asset_id))
        cost_basis_map[key] = {
            "cost_basis_amount": float(row.get("cost_basis_amount") or 0),
            "currency": row.get("currency"),
            "as_of_date": row.get("as_of_date"),
        }
    return cost_basis_map


def fetch_cost_basis_current(
    account_ids: Iterable[str],
    asset_ids: Iterable[int],
) -> Dict[Tuple[str, int], Dict[str, float]]:
    """
    manual_asset_cost_basis_current를 조회해 (account_id, asset_id) → 원금 정보를 반환한다.

    ✅ 초보자를 위한 설명
    - Supabase 쿼리는 "조건 필터"를 걸어야 성능이 좋다.
    - account_ids/asset_ids가 비어 있으면 바로 빈 dict를 반환한다.
    """
    account_ids = [aid for aid in account_ids if aid]
    asset_ids = [int(aid) for aid in asset_ids if aid is not None]

    if not account_ids or not asset_ids:
        return {}

    supabase = get_supabase_client()

    query = (
        supabase.table("manual_asset_cost_basis_current")
        .select("account_id, asset_id, cost_basis_amount, currency, as_of_date")
        .in_("account_id", account_ids)
        .in_("asset_id", asset_ids)
    )
    rows = query.execute().data or []
    return _build_cost_basis_map(rows)


def attach_manual_cost_basis(
    df: pd.DataFrame,
    *,
    account_id_col: str = "account_id",
    asset_id_col: str = "asset_id",
    price_source_col: str = "assets.price_source",
) -> pd.DataFrame:
    """
    스냅샷 DataFrame에 수동 자산 원금(cost basis)을 붙인다.

    ✅ 초보자를 위한 설명
    - manual 자산 여부(price_source == 'manual')를 먼저 판단한다.
    - manual 자산만 원금을 붙이고, 다른 자산은 NaN으로 둔다.
    """
    if df.empty:
        df["manual_principal"] = pd.NA
        return df

    # manual 자산 여부 판단(소문자 표준화)
    price_source = df.get(price_source_col)
    if price_source is None:
        df["manual_principal"] = pd.NA
        return df

    price_source_norm = price_source.fillna("").astype(str).str.lower().str.strip()
    is_manual = price_source_norm.eq("manual")

    manual_df = df.loc[is_manual, [account_id_col, asset_id_col]].dropna()
    if manual_df.empty:
        df["manual_principal"] = pd.NA
        return df

    cost_basis_map = fetch_cost_basis_current(
        account_ids=manual_df[account_id_col].unique().tolist(),
        asset_ids=manual_df[asset_id_col].unique().tolist(),
    )

    def _lookup_principal(row) -> Optional[float]:
        key = (row.get(account_id_col), int(row.get(asset_id_col)))
        if key not in cost_basis_map:
            return None
        return cost_basis_map[key]["cost_basis_amount"]

    df["manual_principal"] = pd.NA
    df.loc[is_manual, "manual_principal"] = df.loc[is_manual].apply(_lookup_principal, axis=1)
    return df


def record_cost_basis_events(events: List[Dict]) -> None:
    """
    manual_asset_cost_basis_events에 이벤트를 기록하고
    manual_asset_cost_basis_current를 함께 갱신한다.
    """
    if not events:
        return

    supabase = get_supabase_client()

    # 이벤트를 먼저 저장한다.
    supabase.table("manual_asset_cost_basis_events").insert(events).execute()

    # (account_id, asset_id) 기준으로 delta를 합산한다.
    delta_map: Dict[Tuple[str, int], Dict[str, object]] = {}
    for ev in events:
        account_id = ev.get("account_id")
        asset_id = ev.get("asset_id")
        if not account_id or asset_id is None:
            continue
        key = (str(account_id), int(asset_id))
        cur = delta_map.setdefault(key, {
            "delta": 0.0,
            "currency": ev.get("currency"),
            "as_of_date": ev.get("event_date"),
        })
        cur["delta"] = float(cur["delta"] or 0) + float(ev.get("delta_amount") or 0)
        # 가장 최근 날짜로 갱신
        ev_date = ev.get("event_date")
        if ev_date:
            cur["as_of_date"] = ev_date

    if not delta_map:
        return

    # 기존 current를 불러와 delta를 더한다.
    account_ids = [k[0] for k in delta_map.keys()]
    asset_ids = [k[1] for k in delta_map.keys()]
    current_map = fetch_cost_basis_current(account_ids=account_ids, asset_ids=asset_ids)

    upsert_rows = []
    for key, info in delta_map.items():
        existing = current_map.get(key, {})
        current_amount = float(existing.get("cost_basis_amount") or 0)
        new_amount = current_amount + float(info.get("delta") or 0)
        if new_amount < 0:
            raise ValueError("원금이 음수로 내려갈 수 없습니다.")

        currency = info.get("currency") or existing.get("currency") or ""
        as_of_date = info.get("as_of_date") or existing.get("as_of_date")

        upsert_rows.append({
            "account_id": key[0],
            "asset_id": key[1],
            "currency": currency,
            "cost_basis_amount": new_amount,
            "as_of_date": as_of_date,
        })

    supabase.table("manual_asset_cost_basis_current").upsert(
        upsert_rows,
        on_conflict="account_id,asset_id",
    ).execute()
