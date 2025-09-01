from __future__ import annotations
from src.coingecko import get_market_chart_daily
from src.paths import DATA_PROC

def main():
    df, source = get_market_chart_daily(coin_id="bitcoin", vs="usd", days=365*10)
    out = df[["date","price","market_cap","total_volume"]].copy()
    out["source"] = source
    DATA_PROC.mkdir(parents=True, exist_ok=True)
    out_path = DATA_PROC / "btc_usd_daily.csv"
    out.to_csv(out_path, index=False)
    print(f"Wrote {out_path} rows={len(out)} source={source}")

if __name__ == "__main__":
    main()
    
