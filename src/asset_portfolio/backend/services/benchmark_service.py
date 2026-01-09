import pandas as pd
import yfinance as yf
from pandas.tseries.offsets import BDay
from asset_portfolio.backend.services.data_contracts import normalize_benchmark_df


def load_cash_benchmark_series(start_date, end_date):
    """
    현금 기준 benchmark
    → 항상 수익률 0%
    """
    dates = pd.date_range(start=start_date, end=end_date)

    return normalize_benchmark_df(pd.DataFrame({
        "date": dates,
        "benchmark_return": [0.0] * len(dates),
    }))


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

    df = df.loc[:, ~df.columns.duplicated()].copy()

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
    - Adj Close 컬럼이 없을 수 있음(그런 경우 Close를 fallback으로 사용)
    - yfinance의 end는 'exclusive' 이므로 end_date를 포함하려면 +1 day가 필요.
      또한 start==end가 들어오면 최소 1일 구간으로 보정한다.
    - return: [date, benchmark_return] DataFrame
    """
    ticker = "^GSPC"

    # 1) 입력 정규화
    s = pd.to_datetime(start_date).normalize()
    e = pd.to_datetime(end_date).normalize()

    # 2) start >= end 방어 (가끔 end_date가 "오늘"로 들어오는데 캘린더 정렬에서 동일해질 수 있음)
    if e <= s:
        e = s  # 같은 날이라면, 아래에서 end_exclusive로 +1 day 해줌

    # yfinance: end는 미포함(exclusive)
    end_exclusive = e + pd.Timedelta(days=1)

    def _download(_s, _end_excl):
        # ✅ 안정성 향상을 위해 auto_adjust=True 권장
        # auto_adjust=True이면 보통 'Close' 자체가 조정 종가에 가까워져서
        # 'Adj Close'가 아예 없을 수 있습니다.
        raw = yf.download(
            ticker,
            start=_s,
            end=_end_excl,
            progress=False,
            auto_adjust=True,
            actions=False,
            threads=False,
        )
        df = _normalize_yf_download_df(raw)
        return df

    # 3) 1차 시도
    try:
        df = _download(s, end_exclusive)
    except Exception:
        df = pd.DataFrame()

    # 4) 실패/빈 DF면: 전 영업일로 end를 당겨 재시도
    if df.empty:
        e2 = (e - BDay(1)).normalize()
        # start가 end보다 커지면 최소 하루 확보
        if e2 < s:
            s2 = e2
        else:
            s2 = s
        end_excl2 = e2 + pd.Timedelta(days=1)

        try:
            df = _download(s2, end_excl2)
        except Exception:
            return normalize_benchmark_df(pd.DataFrame())

    if df.empty:
        return normalize_benchmark_df(pd.DataFrame())

    # 5) 누적 수익률 산출 (Close 기준)
    df = df.sort_index()

    price_col = None
    for col in ["Adj Close", "Close", "Adj_Close", "adj close", "adjclose"]:
        if col in df.columns:
            price_col = col
            break

    if price_col is None:
        return normalize_benchmark_df(pd.DataFrame())

    df["benchmark_return"] = (df[price_col] / df[price_col].iloc[0]) - 1.0

    out = df.reset_index()

    # normalize가 이미 date를 만들었다면 index는 제거
    if "date" in out.columns and "index" in out.columns:
        out = out.drop(columns=["index"])

    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date

    # ✅ 날짜 컬럼을 1개로 확정 (중복 방지)
    # yfinance/normalize에 따라 'Date', 'date', 또는 index가 섞일 수 있으므로 우선순위로 선택
    date_col = None
    for c in ["Date", "date"]:
        if c in out.columns:
            date_col = c
            break

    if date_col is None:
        # 보통 reset_index 후 첫 컬럼이 날짜(index)임
        date_col = out.columns[0]

    # date 컬럼을 단일 Series로 만들어 'date'라는 이름으로 고정
    out["date"] = pd.to_datetime(out[date_col], errors="coerce").dt.date

    # 혹시 기존에 date/Date가 여러 개면 제거(중복 방지)
    cols_to_drop = [c for c in ["Date", "date"] if c in out.columns and c != "date"]
    # 위에서 out["date"]를 만들었기 때문에 기존 date_col이 "date"였다면 중복이 생길 수 있어 제거
    for c in cols_to_drop:
        if c != "date":
            out = out.drop(columns=[c], errors="ignore")

    out = out.dropna(subset=["date"])

    return normalize_benchmark_df(out[["date", "benchmark_return"]])


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
