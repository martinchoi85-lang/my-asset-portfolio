from __future__ import annotations

from typing import Iterable

import pandas as pd


SNAPSHOT_COLUMNS = [
    "date",
    "asset_id",
    "valuation_amount",
    "purchase_amount",
]

WEIGHT_COLUMNS = [
    "date",
    "asset_id",
    "asset_name",
    "currency",
    "valuation_amount",
    "valuation_amount_krw",
    "total_amount_krw",
    "weight",
    "weight_krw",
]

LATEST_WEIGHT_COLUMNS = [
    "date",
    "asset_id",
    "valuation_amount",
    "currency",
    "valuation_amount_krw",
]

CONTRIBUTION_COLUMNS = [
    "date",
    "asset_id",
    "contribution",
    "contribution_pct",
]

BENCHMARK_COLUMNS = [
    "date",
    "benchmark_return",
]


def _ensure_columns(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    for col in columns:
        if col not in df.columns:
            df[col] = pd.NA
    return df


def _ensure_unique_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()].copy()
    return df


def _to_date_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.date


def normalize_snapshot_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    daily_snapshots 기반 스냅샷 DataFrame 정규화.
    - date는 date 타입으로 변환
    - asset_id는 정수형(Int64)으로 변환
    - valuation_amount/purchase_amount는 float으로 변환
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=SNAPSHOT_COLUMNS)

    out = _ensure_unique_columns(df.copy())
    out = _ensure_columns(out, SNAPSHOT_COLUMNS)

    out["date"] = _to_date_series(out["date"])
    out["asset_id"] = pd.to_numeric(out["asset_id"], errors="coerce").astype("Int64")
    out["valuation_amount"] = pd.to_numeric(out["valuation_amount"], errors="coerce")
    out["purchase_amount"] = pd.to_numeric(out["purchase_amount"], errors="coerce")

    return out


def normalize_weight_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    자산 비중 DataFrame 정규화.
    - date는 date 타입
    - weight (0~1) 컬럼을 표준으로 유지
    - weight_krw는 호환성을 위해 동치 컬럼으로 유지
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=WEIGHT_COLUMNS)

    out = _ensure_unique_columns(df.copy())
    out = _ensure_columns(out, WEIGHT_COLUMNS)

    out["date"] = _to_date_series(out["date"])
    out["asset_id"] = pd.to_numeric(out["asset_id"], errors="coerce").astype("Int64")
    out["asset_name"] = out["asset_name"].astype("string")
    out["currency"] = (
        out["currency"]
        .fillna("")
        .astype(str)
        .str.lower()
        .str.strip()
    )

    for col in ["valuation_amount", "valuation_amount_krw", "total_amount_krw", "weight", "weight_krw"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    if out["weight"].isna().all() and "weight_krw" in out.columns:
        out["weight"] = out["weight_krw"]
    if out["weight_krw"].isna().all() and "weight" in out.columns:
        out["weight_krw"] = out["weight"]

    return out


def normalize_latest_weight_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=LATEST_WEIGHT_COLUMNS)

    out = _ensure_unique_columns(df.copy())
    out = _ensure_columns(out, LATEST_WEIGHT_COLUMNS)

    out["date"] = _to_date_series(out["date"])
    out["asset_id"] = pd.to_numeric(out["asset_id"], errors="coerce").astype("Int64")
    out["valuation_amount"] = pd.to_numeric(out["valuation_amount"], errors="coerce")
    out["valuation_amount_krw"] = pd.to_numeric(out["valuation_amount_krw"], errors="coerce")
    out["currency"] = (
        out["currency"]
        .fillna("")
        .astype(str)
        .str.lower()
        .str.strip()
    )

    return out


def normalize_contribution_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=CONTRIBUTION_COLUMNS)

    out = _ensure_unique_columns(df.copy())
    out = _ensure_columns(out, CONTRIBUTION_COLUMNS)

    out["date"] = _to_date_series(out["date"])
    out["asset_id"] = pd.to_numeric(out["asset_id"], errors="coerce").astype("Int64")
    out["contribution"] = pd.to_numeric(out["contribution"], errors="coerce")
    out["contribution_pct"] = pd.to_numeric(out["contribution_pct"], errors="coerce")

    if out["contribution_pct"].isna().all() and out["contribution"].notna().any():
        out["contribution_pct"] = out["contribution"] * 100

    return out


def normalize_benchmark_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=BENCHMARK_COLUMNS)

    out = _ensure_unique_columns(df.copy())
    out = _ensure_columns(out, BENCHMARK_COLUMNS)
    out["date"] = _to_date_series(out["date"])
    out["benchmark_return"] = pd.to_numeric(out["benchmark_return"], errors="coerce")

    return out.dropna(subset=["date"])
