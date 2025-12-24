import pandas as pd
from typing import Tuple, Optional


def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure datetime parsing and sorting.

    Expects columns: date, open, high, low, close, ticker
    """
    if df.empty:
        return df
    if "date" not in df.columns:
        return df
    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])  # drop rows where date couldn't be parsed
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)
    return df


def aggregate_monthly_ohlc(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate daily data to monthly OHLC per ticker.

    Monthly logic:
      - open: first trading day's open in the month
      - close: last trading day's close in the month
      - high: max high in the month
      - low: min low in the month

    Returns a DataFrame with columns: [ticker, date, open, high, low, close]
    where `date` is the month-end timestamp.
    """
    if df.empty:
        return df

    df = prepare_dataframe(df)

    # Group by ticker + calendar month-end using Grouper (avoids deprecated GroupBy.resample behavior)
    monthly = (
        df.groupby(["ticker", pd.Grouper(key="date", freq="ME")])
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
        )
        .reset_index()
        .rename(columns={"date": "date"})
    )
    return monthly


def compute_target_month_range(monthly_df: pd.DataFrame, periods: int = 24) -> Tuple[pd.DatetimeIndex, Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    """Compute a global target month-end DateTimeIndex spanning exactly `periods` months.

    If more than `periods` months exist, take the last `periods` months.
    If fewer than `periods`, extend forward from the first available month.
    Returns (date_index, start_ts, end_ts).
    """
    if monthly_df.empty:
        return pd.DatetimeIndex([]), None, None

    month_ends = monthly_df["date"].dropna().sort_values().unique()
    if len(month_ends) == 0:
        return pd.DatetimeIndex([]), None, None

    if len(month_ends) >= periods:
        start_ts = pd.to_datetime(month_ends[-periods])
        end_ts = pd.to_datetime(month_ends[-1])
        date_index = pd.date_range(start=start_ts, end=end_ts, freq="ME")
    else:
        start_ts = pd.to_datetime(month_ends[0])
        date_index = pd.date_range(start=start_ts, periods=periods, freq="ME")
        end_ts = date_index[-1]

    return pd.DatetimeIndex(date_index), start_ts, end_ts


def align_each_ticker_to_range(monthly_df: pd.DataFrame, date_index: pd.DatetimeIndex) -> pd.DataFrame:
    """Align each ticker's monthly rows to the provided month-end index.

    Missing months will be filled with NaNs for OHLC, maintaining ticker label.
    """
    if monthly_df.empty:
        return monthly_df

    out = []
    for ticker, g in monthly_df.groupby("ticker", sort=False):
        g = g.set_index("date").reindex(date_index)
        g["ticker"] = ticker
        g.index.name = "date"
        out.append(g.reset_index())

    aligned = pd.concat(out, ignore_index=True)
    return aligned
