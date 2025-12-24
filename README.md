# Monthly Stock Resampling with SMA & EMA — Assignment
**Author:** Rohit

This project processes a 2-year daily stock dataset for 10 tickers and generates **monthly OHLC aggregates** along with **SMA-10, SMA-20, EMA-10, and EMA-20** indicators.  
Each ticker is exported as a **separate CSV file**.

---

## Input
- Columns: `date, volume, open, high, low, close, adjclose, ticker`
- Tickers: `AAPL, AMD, AMZN, AVGO, CSCO, MSFT, NFLX, PEP, TMUS, TSLA`

---

## Monthly Aggregation Logic
- **Open** → first trading day’s `open`
- **Close** → last trading day’s `close`
- **High** → max `high` in the month
- **Low** → min `low` in the month

---

## Indicators
- **SMA(N)** → simple moving average on monthly `close`
- **EMA(N)** → exponential moving average using  
  `α = 2 / (N + 1)` with Pandas `ewm(adjust=False)`

Indicators are computed **per-ticker on the monthly close series**.

---

## Output
- One file per ticker → `result_<TICKER>.csv`
- 24-month aligned timeline (missing months kept as NaN)



---

## Project Structure
- `src/aggregations.py` — monthly OHLC aggregation  
- `src/indicators.py` — SMA / EMA calculations  
- `src/io_utils.py` — validation + CSV writing  
- `main.py` — pipeline runner  
- `results/` — generated outputs

---

## How to Run
```bash
pip install -r requirements.txt
python main.py --input <input_file.csv> --outdir results
