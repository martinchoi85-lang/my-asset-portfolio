from __future__ import annotations
import pandas as pd
from typing import Dict, List, Optional
from asset_portfolio.backend.infra.query import build_daily_snapshots_query, fetch_all_pagination
from asset_portfolio.backend.services.fx_service import FxService
from asset_portfolio.backend.services.data_contracts import (
    normalize_weight_df,
    normalize_latest_weight_df,
)


def load_asset_weight_timeseries(
    user_id: str,
    account_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> List[Dict]:
    """
    자산 비중 시계열 원천 데이터 로드
    - currency까지 가져와서 '기준통화(KRW) 환산'이 가능하도록 한다.
    """
    query = build_daily_snapshots_query(
        # ✅ assets(currency, name_kr)까지 같이 가져오기
        select_cols="date, asset_id, valuation_amount, assets(name_kr, currency)",
        start_date=start_date,
        end_date=end_date,
        user_id=user_id,
        account_id=account_id,
    )
    rows = fetch_all_pagination(query.order("date"))
    return rows


def build_asset_weight_df(rows: List[Dict]) -> pd.DataFrame:
    """
    ✅ ALL/단일 계좌 모두 안전한 비중 DF 생성 + USD 환산 반영

    반환 DF 주요 컬럼:
      - date
      - asset_id
      - asset_name
      - currency
      - valuation_amount (원통화)
      - valuation_amount_krw (환산)
      - total_amount_krw
      - weight_krw
    """
    df = pd.DataFrame(rows)
    if df.empty:
        return normalize_weight_df(df)

    # =========================
    # 1) assets 조인 결과 펼치기
    # =========================
    df["asset_name"] = df["assets"].apply(lambda x: x.get("name_kr") if isinstance(x, dict) else None)
    df["currency"] = df["assets"].apply(lambda x: (x.get("currency") or "").lower().strip() if isinstance(x, dict) else "")
    df.drop(columns=["assets"], inplace=True, errors="ignore")

    # =========================
    # 2) 타입 정리
    # =========================
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    # df["valuation_amount"] = pd.to_numeric(df["valuation_amount"], errors="coerce").fillna(0.0)
    df["asset_id"] = pd.to_numeric(df["asset_id"], errors="coerce")
    df = df.dropna(subset=["date", "asset_id"])

    # =========================
    # 3) (date, asset_id) 유일화
    # =========================
    df_agg = (
        df.groupby(["date", "asset_id"], as_index=False)
          .agg(
              valuation_amount=("valuation_amount", "sum"),
              asset_name=("asset_name", "first"),
              currency=("currency", "first"),
          )
    )

    # =========================
    # 4) ✅ USD 환산
    # - 합산/비중은 KRW 기준으로 계산해야 Treemap 등에서 정상 비중이 나온다.
    # =========================
    fx = FxService.fetch_usdkrw()
    usdkrw = float(fx.rate)

    def _to_krw(row) -> float:
        # ✅ currency가 'usd'면 환율 곱
        if (row.get("currency") or "") == "usd":
            return float(row["valuation_amount"]) * usdkrw
        return float(row["valuation_amount"])

    df_agg["valuation_amount_krw"] = df_agg.apply(_to_krw, axis=1)

    # =========================
    # 5) 날짜별 총액 및 비중(KRW 기준)
    # =========================
    df_agg["total_amount_krw"] = df_agg.groupby("date")["valuation_amount_krw"].transform("sum")
    df_agg["weight"] = df_agg.apply(
        lambda r: (r["valuation_amount_krw"] / r["total_amount_krw"]) if r["total_amount_krw"] > 0 else 0.0,
        axis=1
    )
    df_agg["weight_krw"] = df_agg["weight"]

    df_agg = df_agg.sort_values(["date", "valuation_amount_krw"], ascending=[True, False])
    return normalize_weight_df(df_agg)


def _safe_float_series(s: pd.Series, col_name: str) -> pd.Series:
    """
    ✅ Supabase 응답에서 numeric이 str/Decimal/None 등으로 섞여 들어와도 안전하게 float로 변환
    - 변환 실패는 NaN으로 두고, 호출부에서 dropna/에러 처리
    """
    def _to_float(x):
        if x is None:
            return None
        # Decimal/숫자/문자열 모두 float() 시도
        try:
            return float(x)
        except Exception:
            return None

    out = s.apply(_to_float)
    return pd.to_numeric(out, errors="coerce")


def load_latest_asset_weights(user_id: str, account_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Treemap용 최신 비중 데이터
    정책: (중요) '자산별 최신 1행'이 아니라 '최신 날짜(기준일) 1일치 스냅샷'을 사용한다.
    """
    query = build_daily_snapshots_query(
        select_cols="date, asset_id, valuation_amount, assets(currency)",
        start_date=start_date,
        end_date=end_date,
        user_id=user_id,
        account_id=account_id,
    )

    rows = fetch_all_pagination(query)
    if not rows:
        return normalize_latest_weight_df(pd.DataFrame())

    df = pd.DataFrame(rows)
    if df.empty:
        return normalize_latest_weight_df(df)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["asset_id"] = pd.to_numeric(df["asset_id"], errors="coerce")
    df["valuation_amount"] = _safe_float_series(df["valuation_amount"], "valuation_amount")

    df["currency"] = df["assets"].apply(
        lambda x: (x.get("currency") or "").lower().strip() if isinstance(x, dict) else ""
    )
    df.drop(columns=["assets"], inplace=True, errors="ignore")

    df = df.dropna(subset=["date", "asset_id", "valuation_amount"])
    if df.empty:
        return normalize_latest_weight_df(pd.DataFrame())

    # ✅ 최신 날짜 기준(포트폴리오 기준일)
    latest_date = df["date"].max()
    df = df[df["date"] == latest_date].copy()

    # ✅ 보유분만(0은 제외) + (account_id=ALL이면 중복 합산 방지 차원에서 groupby)
    df = df[df["valuation_amount"] > 0].copy()
    if df.empty:
        return normalize_latest_weight_df(pd.DataFrame())

    df = (
        df.groupby(["date", "asset_id", "currency"], as_index=False)["valuation_amount"]
        .sum()
    )

    # ✅ USD 환산
    fx = FxService.fetch_usdkrw()
    usdkrw = float(fx.rate)

    df["valuation_amount_krw"] = df["valuation_amount"]
    is_usd = df["currency"].fillna("").eq("usd")
    df.loc[is_usd, "valuation_amount_krw"] = df.loc[is_usd, "valuation_amount_krw"] * usdkrw

    return normalize_latest_weight_df(
        df[["date", "asset_id", "valuation_amount", "currency", "valuation_amount_krw"]].copy()
    )
