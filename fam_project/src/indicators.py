import pandas as pd


def add_moving_averages(monthly_df: pd.DataFrame) -> pd.DataFrame:
    """Add SMA(10/20) and EMA(10/20) computed on monthly closing prices per ticker.

    SMA: simple rolling mean of 'close'
    EMA: exponential weighted mean (adjust=False) of 'close'
    """
    if monthly_df.empty:
        return monthly_df

    out = monthly_df.sort_values(["ticker", "date"])  

    g = out.groupby("ticker", sort=False)
    
    out["SMA_10"] = (
        g["close"].rolling(window=10, min_periods=10).mean().reset_index(level=0, drop=True)
    )
    out["SMA_20"] = (
        g["close"].rolling(window=20, min_periods=20).mean().reset_index(level=0, drop=True)
    )

    out["EMA_10"] = g["close"].transform(lambda s: s.ewm(span=10, adjust=False, min_periods=1).mean())
    out["EMA_20"] = g["close"].transform(lambda s: s.ewm(span=20, adjust=False, min_periods=1).mean())

    return out
