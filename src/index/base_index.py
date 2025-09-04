from __future__ import annotations
from pathlib import Path
import pandas as pd

def _resolve_price_path(coin: str) -> Path:
    for provider in ("coingecko","yahoo","ccxt"):
        p = Path("data/raw") / provider / f"{coin}_usd_daily.parquet"
        if p.exists():
            return p
    raise FileNotFoundError(f"No price parquet found for {coin}")

def make_base_index_for_coin(coin: str, csv_out: str | Path, base_level: float = 1000.0) -> None:
    parquet_in = _resolve_price_path(coin)
    df = pd.read_parquet(parquet_in).copy().sort_values("date").reset_index(drop=True)
    p0 = float(df.loc[0, "close"])
    df["index_level"] = (df["close"] / p0) * base_level
    df["divisor"] = p0 / base_level
    df["notes"] = "daily close; provider=coingecko/yahoo/ccxt; base normalized"
    out = df[["date","index_level","divisor","notes"]]
    Path(csv_out).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(csv_out, index=False)
