from __future__ import annotations
import pandas as pd
from src.paths import DATA_PROC

BASE = 1000.0

def main():
    prices = pd.read_csv(DATA_PROC / "btc_usd_daily.csv", parse_dates=["date"])
    prices = prices.sort_values("date").reset_index(drop=True)
    source = prices.get("source", pd.Series(["unknown"]*len(prices))).iloc[0]
    first = float(prices["price"].iloc[0])
    prices["index_level"] = BASE * (prices["price"] / first)
    prices["divisor"] = first / BASE
    prices["notes"] = f"BTC price index (base=1000); pricing source={source}"
    out = prices[["date","index_level","divisor","notes"]].copy()
    out["date"] = out["date"].dt.date
    out_path = DATA_PROC / "btc_price_index.csv"
    out.to_csv(out_path, index=False)
    print(f"Wrote {out_path} rows={len(out)}")

if __name__ == "__main__":
    main()
