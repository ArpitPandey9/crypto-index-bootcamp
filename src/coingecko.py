
from __future__ import annotations
import time, json, hashlib, pathlib
from datetime import datetime, timezone, timedelta
import requests
import pandas as pd

CACHE_DIR = pathlib.Path("data/data_raw")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "accept": "application/json",
    "User-Agent": "crypto-index-bootcamp/1.0"
}

def _hash(s: str) -> str:
    import hashlib
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:12]

def _get_json_with_retry(url: str, params: dict | None = None, headers: dict | None = None,
                         retries: int = 6, backoff: float = 1.0):
    last = None
    for _ in range(retries):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=30)
            last = r
            if r.status_code == 200:
                return r.json()
            time.sleep(backoff)
            backoff = min(backoff * 2, 16.0)
        except requests.RequestException:
            time.sleep(backoff)
            backoff = min(backoff * 2, 16.0)
    code = getattr(last, "status_code", "N/A")
    raise RuntimeError(f"HTTP {code}")

# -------- CoinGecko (primary) --------
CG_BASE = "https://api.coingecko.com/api/v3"

def _coingecko_daily(coin_id: str, vs: str, days: int) -> pd.DataFrame:
    params = {"vs_currency": vs, "days": days, "interval": "daily"}
    url = f"{CG_BASE}/coins/{coin_id}/market_chart"
    cache_key = _hash(url + json.dumps(params, sort_keys=True))
    cache_path = CACHE_DIR / f"coingecko_{coin_id}_{vs}_{days}_{cache_key}.json"
    if cache_path.exists():
        raw = json.loads(cache_path.read_text(encoding="utf-8"))
    else:
        raw = _get_json_with_retry(url, params=params, headers=HEADERS)
        cache_path.write_text(json.dumps(raw), encoding="utf-8")

    def to_df(field: str, col: str) -> pd.DataFrame:
        arr = raw.get(field, [])
        df = pd.DataFrame(arr, columns=["ms", col])
        df["date"] = pd.to_datetime(df["ms"], unit="ms", utc=True).dt.tz_convert("UTC").dt.normalize().dt.date
        return df[["date", col]]

    p = to_df("prices", "price")
    m = to_df("market_caps", "market_cap")
    v = to_df("total_volumes", "total_volume")
    out = p.merge(m, on="date").merge(v, on="date")
    return out.sort_values("date").drop_duplicates("date").reset_index(drop=True)

# -------- CoinCap (fallback #1) --------
CC_BASE = "https://api.coincap.io/v2"

def _coincap_btc_daily(days: int) -> pd.DataFrame:
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    params = {
        "interval": "d1",
        "start": int(start.timestamp() * 1000),
        "end": int(end.timestamp() * 1000),
    }
    url = f"{CC_BASE}/assets/bitcoin/history"
    raw = _get_json_with_retry(url, params=params, headers=HEADERS)
    data = raw.get("data", [])
    if not data:
        raise RuntimeError("CoinCap returned no data")

    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["time"], unit="ms", utc=True).dt.tz_convert("UTC").dt.normalize().dt.date
    df["price"] = pd.to_numeric(df["priceUsd"], errors="coerce")
    df["market_cap"] = pd.to_numeric(df.get("marketCapUsd", pd.NA), errors="coerce")
    df["total_volume"] = pd.to_numeric(df.get("volumeUsd", pd.NA), errors="coerce")

    out = df[["date", "price", "market_cap", "total_volume"]].dropna(subset=["price"])
    return out.sort_values("date").drop_duplicates("date").reset_index(drop=True)

# -------- Binance klines (fallback #2) --------
# BTCUSDT daily closes; USDT ~ USD
BN_BASE = "https://api.binance.com/api/v3"

def _binance_btcusdt_daily(days: int) -> pd.DataFrame:
    limit = 1000  # max per request
    end_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_ts = end_ts - days * 24 * 60 * 60 * 1000
    frames = []
    cur = start_ts
    while cur < end_ts:
        params = {"symbol": "BTCUSDT", "interval": "1d", "startTime": cur, "endTime": end_ts, "limit": limit}
        url = f"{BN_BASE}/klines"
        arr = _get_json_with_retry(url, params=params, headers=HEADERS)
        if not arr:
            break
        df = pd.DataFrame(arr, columns=[
            "open_time","open","high","low","close","volume","close_time","quote_asset_volume",
            "number_of_trades","taker_buy_base_asset_volume","taker_buy_quote_asset_volume","ignore"
        ])
        df["date"] = pd.to_datetime(df["close_time"], unit="ms", utc=True).dt.tz_convert("UTC").dt.normalize().dt.date
        df["price"] = pd.to_numeric(df["close"], errors="coerce")
        df["market_cap"] = pd.NA
        df["total_volume"] = pd.to_numeric(df["quote_asset_volume"], errors="coerce")
        frames.append(df[["date","price","market_cap","total_volume"]])
        last_close = int(df["close_time"].iloc[-1])
        cur = last_close + 1
        if len(df) < limit:
            break
        time.sleep(0.2)
    if not frames:
        raise RuntimeError("Binance returned no data")
    out = pd.concat(frames, ignore_index=True)
    return out.sort_values("date").drop_duplicates("date").reset_index(drop=True)

def get_market_chart_daily(coin_id: str = "bitcoin", vs: str = "usd", days: int = 365*10):
    """
    Returns (DataFrame, source_str)
    columns: date, price, market_cap, total_volume
    """
    try:
        return _coingecko_daily(coin_id=coin_id, vs=vs, days=days), "coingecko"
    except Exception as e1:
        print(f"[info] CoinGecko failed ({e1}); trying CoinCap…")
    try:
        if coin_id.lower() == "bitcoin" and vs.lower() == "usd":
            return _coincap_btc_daily(days=days), "coincap"
    except Exception as e2:
        print(f"[info] CoinCap failed ({e2}); trying Binance BTCUSDT…")
    return _binance_btcusdt_daily(days=days), "binance"
