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


def load_sp500_benchmark_series(start_date: str, end_date: str) -> pd.DataFrame:
    """
    S&P 500 (^GSPC) 누적 수익률 시계열 생성
    """
    ticker = "^GSPC"

    df = yf.download(
        ticker,
        start=start_date,
        end=end_date,
        progress=False,
    )

    if df.empty:
        return pd.DataFrame()

    df = df.reset_index()
    df["date"] = pd.to_datetime(df["Date"])

    # 기준 가격 (첫 날)
    base_price = df.iloc[0]["Adj Close"]

    # 누적 수익률 계산
    df["benchmark_return"] = df["Adj Close"] / base_price - 1

    return df[["date", "benchmark_return"]]
