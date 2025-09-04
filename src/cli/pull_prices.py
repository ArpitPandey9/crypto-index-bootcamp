from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
from src.data.coingecko import fetch_daily_close_coin
from src.data.yahoo import fetch_daily_close_yahoo

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--coin_id", required=True, choices=["bitcoin","ethereum"])
    ap.add_argument("--days", default="max")  # we'll try max, fallback if needed
    ap.add_argument("--out_dir", default="data/raw")
    args = ap.parse_args()

    # First try CoinGecko
    try:
        df = fetch_daily_close_coin(args.coin_id, days=args.days, cache_dir=Path(args.out_dir) / "coingecko")
        provider = "coingecko"
    except Exception as e:
        print(f"[warn] CoinGecko failed or limited: {e}")
        print("[info] Falling back to Yahoo Finance (full history, free)")
        df = fetch_daily_close_yahoo(args.coin_id)
        provider = "yahoo"

    out = Path(args.out_dir) / provider / f"{args.coin_id}_usd_daily.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)
    print(f"Wrote {out} with {len(df)} rows using provider={provider}")

if __name__ == "__main__":
    main()
