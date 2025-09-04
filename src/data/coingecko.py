from __future__ import annotations
from pathlib import Path
from typing import Dict, Any
import time
import pandas as pd

from src.utils.http import get_with_backoff
from src.utils.cache import read_json_cache, write_json_cache, is_fresh

BASE = "https://api.coingecko.com/api/v3"

def _build_df_from_payload(payload: Dict[str, Any]) -> pd.DataFrame:
    prices = payload.get("prices", [])
    vols   = payload.get("total_volumes", [])
    df_p = pd.DataFrame(prices, columns=["ms", "close"])
    df_v = pd.DataFrame(vols,   columns=["ms", "volume"])
    df = df_p.merge(df_v, on="ms", how="left")
    df["timestamp_utc"] = pd.to_datetime(df["ms"], unit="ms", utc=True)
    df["date"] = df["timestamp_utc"].dt.tz_convert("UTC").dt.date.astype("datetime64[ns]")
    df = df.drop(columns=["ms"]).sort_values("timestamp_utc").reset_index(drop=True)
    return df[["timestamp_utc","date","close","volume"]]

def fetch_daily_close_coin(coin_id: str, vs_currency: str = "usd", days: str = "max",
                           cache_dir: str | Path = "data/raw/coingecko",
                           ttl_seconds: int = 24*3600) -> pd.DataFrame:
    """
    Returns DataFrame with: ['timestamp_utc','date','close','volume'] (daily)
    On free tier, CoinGecko limits to last 365 days. If days='max', we
    gracefully retry with days='365' instead of crashing.
    """
    cache_path = Path(cache_dir) / f"{coin_id}_{vs_currency}_{days}_market_chart.json"
    meta = read_json_cache(cache_path)
    if meta and is_fresh(meta, ttl_seconds):
        payload = meta
    else:
        params = {"vs_currency": vs_currency, "days": days, "interval": "daily"}
        status, payload = get_with_backoff(f"{BASE}/coins/{coin_id}/market_chart", params)

        # Handle 365d limit (error_code=10012) by retrying with 365
        if status == 401 and isinstance(payload, dict):
            err = payload.get("error", {}).get("status", {})
            if err.get("error_code") == 10012 and days == "max":
                params["days"] = "365"
                status, payload = get_with_backoff(f"{BASE}/coins/{coin_id}/market_chart", params)

        if status != 200:
            raise RuntimeError(f"CoinGecko error {status}: {payload}")

        payload["_fetched_at"] = time.time()
        write_json_cache(cache_path, payload)

    return _build_df_from_payload(payload)
