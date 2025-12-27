import pandas as pd
import yfinance as yf

def load_cash_benchmark_series(start_date, end_date):
    """
    현금 기준 benchmark
    → 항상 수익률 0%
    """
    dates = pd.date_range(start=start_date, end=end_date)

    return pd.DataFrame({
        "date": dates,
        "benchmark_return": [0.0] * len(dates),
    })


def merge_portfolio_and_benchmark(
    portfolio_df: pd.DataFrame,
    benchmark_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Portfolio 수익률과 Benchmark 수익률 병합
    """
    if portfolio_df.empty or benchmark_df.empty:
        return pd.DataFrame()

    df = portfolio_df.merge(
        benchmark_df,
        on="date",
        how="inner",
    )

    df["portfolio_return_pct"] = df["portfolio_return"] * 100
    df["benchmark_return_pct"] = df["benchmark_return"] * 100

    return df


def _normalize_yf_download_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    yfinance.download 결과가
    - 단일 컬럼 DataFrame 이거나
    - MultiIndex 컬럼 DataFrame 일 수 있으므로
    이를 단일 레벨 컬럼으로 정리한다.

    또한 date 컬럼을 표준화한다.
    """
    if df is None or df.empty:
        return pd.DataFrame()

    # 1) MultiIndex 컬럼이면 1차원으로 평탄화
    # 예: ('Adj Close', '^GSPC') -> 'Adj Close'
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    # 2) 인덱스가 DatetimeIndex면 컬럼으로 빼서 date 표준화
    if isinstance(df.index, pd.DatetimeIndex):
        df = df.reset_index()

    # 3) 날짜 컬럼명 표준화: Date 또는 index 등의 변형 대응
    if "Date" in df.columns:
        df["date"] = pd.to_datetime(df["Date"])
    elif "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    else:
        # 마지막 방어: 첫 컬럼을 날짜로 간주
        df["date"] = pd.to_datetime(df.iloc[:, 0], errors="coerce")

    return df


def load_sp500_benchmark_series(start_date: str, end_date: str) -> pd.DataFrame:
    """
    S&P 500 (^GSPC) 누적 수익률 시계열 생성.

    - yfinance 결과가 MultiIndex일 수 있음
    - Adj Close 컬럼이 없을 수 있음
    - 그런 경우 Close를 fallback으로 사용
    - return: [date, benchmark_return] DataFrame
    """
    ticker = "^GSPC"

    # ✅ 안정성 향상을 위해 auto_adjust=True 권장
    # auto_adjust=True이면 보통 'Close' 자체가 조정 종가에 가까워져서
    # 'Adj Close'가 아예 없을 수 있습니다.
    raw = yf.download(
        ticker,
        start=start_date,
        end=end_date,
        progress=False,
        auto_adjust=True,
        actions=False,
    )

    df = _normalize_yf_download_df(raw)
    if df.empty:
        return pd.DataFrame()

    # ✅ 가격 컬럼 선택 우선순위
    # 1) Adj Close (있는 경우)
    # 2) Close
    price_col = None
    if "Adj Close" in df.columns:
        price_col = "Adj Close"
    elif "Close" in df.columns:
        price_col = "Close"

    if price_col is None:
        # 여기까지 오면 yfinance 응답 포맷이 예상과 다르므로
        # 디버깅에 도움이 되도록 컬럼을 보여줄 수 있게 예외 메시지 구성
        raise KeyError(
            f"S&P 500 benchmark price column not found. Available columns: {list(df.columns)}"
        )

    # 결측 제거 + 정렬
    df = df[["date", price_col]].dropna().sort_values("date")

    if df.empty:
        return pd.DataFrame()

    # 기준 가격 (첫 유효 가격)
    base_price = float(df.iloc[0][price_col])
    if base_price <= 0:
        return pd.DataFrame()

    df["benchmark_return"] = df[price_col].astype(float) / base_price - 1.0

    return df[["date", "benchmark_return"]]


def align_portfolio_to_benchmark_dates(
    portfolio_df: pd.DataFrame,
    benchmark_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Portfolio의 date를 Benchmark의 거래일(date)에 맞춰 정렬한다.
    - benchmark_df의 date를 기준 인덱스로 삼는다.
    - portfolio_df는 benchmark 날짜에 대해 forward-fill 한다.

    전제:
    - portfolio_df: columns = [date, portfolio_return, purchase_amount, valuation_amount, ...]
    - benchmark_df: columns = [date, benchmark_return]
    """
    if portfolio_df is None or portfolio_df.empty:
        return pd.DataFrame()
    if benchmark_df is None or benchmark_df.empty:
        return pd.DataFrame()

    p = portfolio_df.copy()
    b = benchmark_df.copy()

    # =========================
    # date 타입 표준화
    # =========================
    p["date"] = pd.to_datetime(p["date"])
    b["date"] = pd.to_datetime(b["date"])

    # 정렬
    p = p.sort_values("date")
    b = b.sort_values("date")

    # =========================
    # benchmark 날짜를 기준 캘린더로 사용
    # =========================
    b_index = b.set_index("date").index

    # portfolio를 date index로 만들고, benchmark 날짜로 reindex
    p_indexed = p.set_index("date")

    # ✅ forward-fill: benchmark 날짜에 해당하는 값이 없으면 직전 portfolio 값을 사용
    p_aligned = p_indexed.reindex(b_index, method="ffill")

    # reindex 후 date 컬럼 복원
    p_aligned = p_aligned.reset_index().rename(columns={"index": "date"})

    return p_aligned


def merge_portfolio_and_benchmark_ffill(
    portfolio_df: pd.DataFrame,
    benchmark_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Benchmark 거래일(date)을 기준으로:
    - portfolio를 forward-fill로 정합
    - benchmark와 merge
    - 차트용 % 컬럼 생성
    """
    if portfolio_df is None or portfolio_df.empty:
        return pd.DataFrame()
    if benchmark_df is None or benchmark_df.empty:
        return pd.DataFrame()

    p_aligned = align_portfolio_to_benchmark_dates(portfolio_df, benchmark_df)
    if p_aligned.empty:
        return pd.DataFrame()

    b = benchmark_df.copy()
    b["date"] = pd.to_datetime(b["date"])

    # =========================
    # merge (date가 benchmark 거래일만 남음)
    # =========================
    df = p_aligned.merge(b, on="date", how="inner")

    # =========================
    # 차트 표시용 %
    # =========================
    if "portfolio_return" in df.columns:
        df["portfolio_return_pct"] = df["portfolio_return"] * 100
    if "benchmark_return" in df.columns:
        df["benchmark_return_pct"] = df["benchmark_return"] * 100

    return df


def align_portfolio_to_benchmark_calendar(
    portfolio_df: pd.DataFrame,
    benchmark_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    벤치마크 날짜 캘린더를 기준으로 포트폴리오 시계열을 reindex 후 forward-fill

    - portfolio_df: date, valuation_amount, purchase_amount, portfolio_return
    - benchmark_df: date, benchmark_return
    """
    if portfolio_df.empty or benchmark_df.empty:
        return portfolio_df

    p = portfolio_df.copy()
    b = benchmark_df.copy()

    p["date"] = pd.to_datetime(p["date"])
    b["date"] = pd.to_datetime(b["date"])

    p = p.sort_values("date").set_index("date")
    b = b.sort_values("date").set_index("date")

    # ✅ 벤치마크 캘린더로 reindex
    p = p.reindex(b.index)

    # ✅ forward-fill: 평가금액/매입금액/수익률 모두 ffill (휴장일 대응)
    # - 첫 값이 NaN이면 ffill로도 안 채워지므로 남는다(정상)
    p[["valuation_amount", "purchase_amount", "portfolio_return"]] = (
        p[["valuation_amount", "purchase_amount", "portfolio_return"]].ffill()
    )

    p = p.reset_index().rename(columns={"index": "date"})
    return p
