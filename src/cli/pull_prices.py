from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
from src.data.coingecko import fetch_daily_close_coin
from src.data.yahoo import fetch_daily_close_yahoo

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--coin_id", required=True, choices=["bitcoin","ethereum"])
    ap.add_argument("--days", default="max")
    ap.add_argument("--out_dir", default="data/raw")
    args = ap.parse_args()

    df = None
    provider = None

    # 1) CoinGecko (max → 365 fallback internally)
    try:
        df = fetch_daily_close_coin(args.coin_id, days=args.days, cache_dir=Path(args.out_dir) / "coingecko")
        provider = "coingecko"
    except Exception as e1:
        print(f"[warn] CoinGecko failed or limited: {e1}")
        # 2) Yahoo
        try:
            from src.data.yahoo import fetch_daily_close_yahoo
            df = fetch_daily_close_yahoo(args.coin_id)
            provider = "yahoo"
        except Exception as e2:
            print(f"[warn] Yahoo fallback failed: {e2}")
            # 3) CCXT
            from src.data.ccxt_provider import fetch_daily_close_ccxt
            print("[info] Falling back to CCXT (Kraken→Binance)")
            df = fetch_daily_close_ccxt(args.coin_id)
            provider = "ccxt"

    out = Path(args.out_dir) / provider / f"{args.coin_id}_usd_daily.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)
    print(f"Wrote {out} with {len(df)} rows using provider={provider}")

