import argparse
import os
import pandas as pd

from src.aggregations import aggregate_monthly_ohlc, compute_target_month_range, align_each_ticker_to_range
from src.indicators import add_moving_averages
from src.io_utils import write_symbol_csvs, validate_symbols


def run(input_csv: str, outdir: str) -> None:
    if not os.path.isfile(input_csv):
        raise FileNotFoundError(f"Input file not found: {input_csv}")

    print(f"[INFO] Loading: {input_csv}")
    df = pd.read_csv(input_csv)

    # Basic validation
    expected_cols = {"date","volume","open","high","low","close","adjclose","ticker"}
    missing_cols = expected_cols - set(df.columns)
    if missing_cols:
        raise ValueError(f"Input missing columns: {sorted(missing_cols)}")

    validate_symbols(df)

    # 1) Monthly OHLC aggregation
    monthly = aggregate_monthly_ohlc(df)
    if monthly.empty:
        raise ValueError("No data after aggregation.")

    # 2) Align to a common 24-month range across dataset
    date_index, start_ts, end_ts = compute_target_month_range(monthly, periods=24)
    start_str = start_ts.date() if isinstance(start_ts, pd.Timestamp) else "N/A"
    end_str = end_ts.date() if isinstance(end_ts, pd.Timestamp) else "N/A"
    print(f"[INFO] Target month range: {start_str} -> {end_str} ({len(date_index)} months)")
    aligned = align_each_ticker_to_range(monthly, date_index)

    # 3) Technical indicators on monthly 'close'
    with_indicators = add_moving_averages(aligned)

    # 4) Write per-symbol CSVs
    write_symbol_csvs(with_indicators, outdir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Resample daily stock data to monthly and compute SMA/EMA.")
    parser.add_argument("--input", required=True, help="Path to input CSV (daily prices)")
    parser.add_argument("--outdir", default="results", help="Output directory for per-symbol CSVs")
    args = parser.parse_args()

    run(args.input, args.outdir)
