import os
import pandas as pd


EXPECTED_SYMBOLS = [
    "AAPL","AMD","AMZN","AVGO","CSCO","MSFT","NFLX","PEP","TMUS","TSLA"
]


def validate_symbols(df: pd.DataFrame) -> None:
    """Ensure input contains only expected symbols; warn if missing any."""
    present = sorted(df["ticker"].dropna().unique().tolist())
    missing = [s for s in EXPECTED_SYMBOLS if s not in present]
    extra = [s for s in present if s not in EXPECTED_SYMBOLS]
    if missing:
        print(f"[WARN] Missing symbols: {missing}")
    if extra:
        print(f"[WARN] Unexpected symbols found: {extra}")


def write_symbol_csvs(df: pd.DataFrame, outdir: str) -> None:
    """Write one CSV per symbol following naming: result_{SYMBOL}.csv

    The DataFrame should include columns: [date, ticker, open, high, low, close, SMA_10, SMA_20, EMA_10, EMA_20]
    """
    os.makedirs(outdir, exist_ok=True)

    required_cols = {"date","ticker","open","high","low","close","SMA_10","SMA_20","EMA_10","EMA_20"}
    missing_cols = required_cols - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing expected columns: {sorted(missing_cols)}")

    for symbol, g in df.groupby("ticker", sort=False):
        out_path = os.path.join(outdir, f"result_{symbol}.csv")
        g = g.sort_values("date")
        if len(g) != 24:
            print(f"[WARN] {symbol}: expected 24 rows, found {len(g)}. Writing available rows.")
        g.to_csv(out_path, index=False)
        print(f"[OK] Wrote {out_path} ({len(g)} rows)")
