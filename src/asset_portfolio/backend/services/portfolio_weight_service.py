from __future__ import annotations
import pandas as pd
from typing import Dict, List, Optional
from asset_portfolio.backend.infra.query import build_daily_snapshots_query
from asset_portfolio.backend.services.fx_service import FxService


def load_asset_weight_timeseries(
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
        account_id=account_id,
    )
    response = query.order("date").execute()
    return response.data or []


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
        return df

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
    df_agg["weight_krw"] = df_agg.apply(
        lambda r: (r["valuation_amount_krw"] / r["total_amount_krw"]) if r["total_amount_krw"] > 0 else 0.0,
        axis=1
    )

    df_agg = df_agg.sort_values(["date", "valuation_amount_krw"], ascending=[True, False])
    return df_agg


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


def load_latest_asset_weights(account_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    ✅ Treemap용 최신 비중 데이터 + USD 환산 포함
    - (중요) valuation_amount 변환 실패를 0으로 덮지 않는다(=원인 은닉 방지)
    """
    query = build_daily_snapshots_query(
        select_cols="date, asset_id, valuation_amount, assets(currency)",
        start_date=start_date,
        end_date=end_date,
        account_id=account_id,
    )

    rows = query.execute().data or []
    if not rows:
        return pd.DataFrame()

    # ✅ 1) 원본 rows 샘플 확인(문제 추적용)
    # 필요 시 잠깐 켜서 확인 후 제거하세요.
    # import streamlit as st
    # st.write("rows[0] keys:", list(rows[0].keys()))
    # st.write("rows[0] sample:", rows[0])

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame()

    # ✅ 2) 필수 컬럼 존재 확인(없으면 바로 원인)
    required = {"date", "asset_id", "valuation_amount"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"daily_snapshots query result missing columns: {missing}. "
                           f"Got columns={list(df.columns)}")

    # ✅ 3) 타입 정리
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["asset_id"] = pd.to_numeric(df["asset_id"], errors="coerce")

    # ✅ 핵심: 조용히 0으로 덮지 않고 안전 변환 후 dropna
    df["valuation_amount"] = _safe_float_series(df["valuation_amount"], "valuation_amount")

    # ✅ assets(currency) 펼치기
    df["currency"] = df["assets"].apply(
        lambda x: (x.get("currency") or "").lower().strip() if isinstance(x, dict) else ""
    )
    df.drop(columns=["assets"], inplace=True, errors="ignore")

    # ✅ 4) 유효 행만 남김
    df = df.dropna(subset=["date", "asset_id", "valuation_amount"])
    if df.empty:
        raise RuntimeError(
            "valuation_amount 변환 결과가 전부 NaN입니다. "
            "rows[0]를 출력해서 valuation_amount 형태(문자열/딕트/누락)를 확인하세요."
        )

    # ✅ 5) asset_id별 최신 1행
    df = df.sort_values(["asset_id", "date"])
    df_latest = df.groupby("asset_id", as_index=False).tail(1).copy()

    # ✅ 6) USD 환산(벡터화)
    fx = FxService.fetch_usdkrw()
    usdkrw = float(fx.rate)

    is_usd = df_latest["currency"].fillna("").eq("usd")
    df_latest["valuation_amount_krw"] = df_latest["valuation_amount"]
    df_latest.loc[is_usd, "valuation_amount_krw"] = df_latest.loc[is_usd, "valuation_amount_krw"] * usdkrw

    return df_latest[["date", "asset_id", "valuation_amount", "currency", "valuation_amount_krw"]].copy()
