from __future__ import annotations
import argparse
from pathlib import Path

from src.data.coingecko import fetch_daily_close_coin

# Optional imports only when used
def _try_yahoo(coin_id: str):
    from src.data.yahoo import fetch_daily_close_yahoo
    return fetch_daily_close_yahoo(coin_id)

def _try_ccxt(coin_id: str):
    from src.data.ccxt_provider import fetch_daily_close_ccxt
    return fetch_daily_close_ccxt(coin_id)

def main():
    ap = argparse.ArgumentParser(description="Pull daily close data and save parquet.")
    ap.add_argument("--coin_id", required=True, choices=["bitcoin", "ethereum"])
    ap.add_argument("--days", default="max", help="CoinGecko days parameter; 'max' will auto-fallback to 365 on free tier.")
    ap.add_argument("--out_dir", default="data/raw", help="Root output dir (provider subfolder auto-added).")
    ap.add_argument("--provider", choices=["auto","coingecko","yahoo","ccxt"], default="auto",
                    help="Force a specific provider for debugging, else try auto fallback chain.")
    args = ap.parse_args()

    coin = args.coin_id
    out_root = Path(args.out_dir)
    df = None
    provider = None

    print(f"[info] Starting pull for coin={coin} provider={args.provider} days={args.days}")

    try:
        if args.provider in ("auto", "coingecko"):
            print("[info] Trying CoinGecko…")
            df = fetch_daily_close_coin(coin_id=coin, days=args.days, cache_dir=out_root / "coingecko")
            provider = "coingecko"
            print("[info] CoinGecko success.")
        if df is None and args.provider in ("auto", "yahoo"):
            print("[warn] CoinGecko unavailable. Trying Yahoo…")
            df = _try_yahoo(coin)
            provider = "yahoo"
            print("[info] Yahoo success.")
        if df is None and args.provider in ("auto", "ccxt"):
            print("[warn] Yahoo unavailable. Trying CCXT…")
            df = _try_ccxt(coin)
            provider = "ccxt"
            print("[info] CCXT success.")
    except Exception as e:
        # If we forced a provider, do not try others—fail loudly
        if args.provider != "auto":
            raise
        # If auto mode failed at some stage, try successive fallbacks
        if provider is None:
            print(f"[warn] CoinGecko failed: {e}")
            try:
                print("[info] Falling back to Yahoo…")
                df = _try_yahoo(coin)
                provider = "yahoo"
                print("[info] Yahoo success.")
            except Exception as e2:
                print(f"[warn] Yahoo failed: {e2}")
                print("[info] Falling back to CCXT…")
                df = _try_ccxt(coin)
                provider = "ccxt"
                print("[info] CCXT success.")

    if df is None or df.empty:
        raise RuntimeError("All providers returned empty data frame.")

    out_path = out_root / provider / f"{coin}_usd_daily.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)
    print(f"[ok] Wrote {out_path} with {len(df)} rows using provider={provider}")

if __name__ == "__main__":
    main()
