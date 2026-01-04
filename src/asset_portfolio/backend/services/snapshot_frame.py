# src/asset_portfolio/backend/services/snapshot_frame.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd


NUMERIC_COLS = [
    "quantity",
    "valuation_price",
    "purchase_price",
    "valuation_amount",
    "purchase_amount",
]


def _flatten_rows(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Supabase(PostgREST) 응답 rows는 join/select에 따라 중첩 dict가 섞일 수 있음.
    json_normalize로 평탄화해 컬럼명을 안정적으로 확보한다.
    """
    if not rows:
        return pd.DataFrame()

    df = pd.json_normalize(rows, sep=".")
    return df


def _to_yyyy_mm_dd(v: Any) -> Optional[str]:
    if v is None:
        return None
    # pandas Timestamp/date/datetime/str 모두 수용
    try:
        ts = pd.to_datetime(v)
        return ts.strftime("%Y-%m-%d")
    except Exception:
        s = str(v)
        # 이미 YYYY-MM-DD면 그대로
        if len(s) >= 10 and s[4] == "-" and s[7] == "-":
            return s[:10]
        return s


def _strict_numeric(df: pd.DataFrame, col: str) -> pd.Series:
    """
    숫자 변환은 '조용히 0으로 덮지 않는다'.
    - 변환 불가 -> NaN 유지
    - 콤마 등 로케일 포맷 제거
    """
    if col not in df.columns:
        # 없는 컬럼은 NaN 시리즈로
        return pd.Series([pd.NA] * len(df), index=df.index, dtype="Float64")

    s = df[col]

    # dict/list 등 비정상 타입이 섞이면 문자열로 바꾸면 'nan'이 되기 쉬우므로 먼저 걸러냄
    # (이 경우 아래 검증에서 즉시 실패하도록 유도)
    if s.map(lambda x: isinstance(x, (dict, list))).any():
        return pd.to_numeric(pd.Series([pd.NA] * len(df), index=df.index), errors="coerce")

    # 문자열 정리: 콤마 제거, 공백 제거
    if s.dtype == "object":
        s = s.astype(str).str.replace(",", "", regex=False).str.strip()

        # "None", "nan" 같은 문자열 정리
        s = s.replace({"None": "", "nan": "", "NaN": ""})

    return pd.to_numeric(s, errors="coerce")


def to_snapshot_df(
    rows: List[Dict[str, Any]],
    *,
    required_cols: Optional[List[str]] = None,
    rename_map: Optional[Dict[str, str]] = None,
    min_non_null_ratio: float = 0.90,
    debug_sample: int = 3,
) -> pd.DataFrame:
    """
    daily_snapshots rows -> 정규화된 DataFrame

    핵심 정책
    1) 중첩 구조 평탄화(json_normalize)
    2) date는 YYYY-MM-DD 문자열로 정규화
    3) numeric 컬럼은 엄격 변환(실패 시 NaN)
    4) numeric 변환 성공률이 낮으면 즉시 예외(=0 덮어씌움 방지)
    """
    df = _flatten_rows(rows)

    if df.empty:
        return df

    # rename (예: assets.name_kr -> asset_name)
    if rename_map:
        df = df.rename(columns=rename_map)

    # required columns check (존재 자체가 문제인 경우 즉시 드러나야 함)
    if required_cols:
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            sample_keys = list(rows[0].keys()) if rows else []
            raise KeyError(
                f"[snapshot_frame] Missing required cols: {missing}. "
                f"Top-level keys(sample)={sample_keys}, df.columns(sample)={list(df.columns)[:30]}"
            )

    # date normalize
    if "date" in df.columns:
        df["date"] = df["date"].map(_to_yyyy_mm_dd)

    # numeric normalize + validation
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = _strict_numeric(df, col).astype("Float64")

            non_null_ratio = float(df[col].notna().mean())
            if non_null_ratio < min_non_null_ratio:
                # 변환 실패를 숨기지 않고 즉시 알려준다.
                head = df[[c for c in ["date", "asset_id", "account_id", col] if c in df.columns]].head(debug_sample)
                raise ValueError(
                    f"[snapshot_frame] Numeric coercion suspicious for '{col}'. "
                    f"non_null_ratio={non_null_ratio:.2%} < {min_non_null_ratio:.2%}. "
                    f"head=\n{head.to_string(index=False)}"
                )

    return df
