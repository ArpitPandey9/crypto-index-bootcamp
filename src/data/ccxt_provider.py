from __future__ import annotations
from datetime import datetime, timezone
from typing import List, Tuple
import time
import math
import pandas as pd
import ccxt

# Preferred exchanges & symbols (USD first, then USDT)
_SYMBOLS = {
    "bitcoin": [("kraken", "BTC/USD"), ("binance", "BTC/USDT")],
    "ethereum": [("kraken", "ETH/USD"), ("binance", "ETH/USDT")],
}

def _fetch_all_ohlcv(ex, symbol: str, timeframe: str = "1d") -> pd.DataFrame:
    # paginate in chunks (limit ~1000)
    since = None
    limit = 1000
    rows: List[List[float]] = []
    while True:
        data = ex.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=limit)
        if not data:
            break
        rows.extend(data)
        # advance 'since' by last candle time + 1ms
        last_ts = data[-1][0]
        since = last_ts + 1
        # safety sleep to respect rate limits
        time.sleep(ex.rateLimit / 1000.0)
        # stop if we aren't making progress
        if len(data) < limit:
            break
    if not rows:
        raise RuntimeError(f"No OHLCV from {ex.id} {symbol}")
    # ccxt returns [ms, open, high, low, close, volume]
    df = pd.DataFrame(rows, columns=["ms","open","high","low","close","volume"])
    df["timestamp_utc"] = pd.to_datetime(df["ms"], unit="ms", utc=True)
    df["date"] = df["timestamp_utc"].dt.tz_convert(timezone.utc).dt.date.astype("datetime64[ns]")
    df = df[["timestamp_utc","date","close","volume"]].sort_values("timestamp_utc").reset_index(drop=True)
    return df

def fetch_daily_close_ccxt(coin_id: str) -> pd.DataFrame:
    pairs = _SYMBOLS.get(coin_id)
    if not pairs:
        raise ValueError(f"No CCXT mapping for {coin_id}")
    errors: List[Tuple[str,str]] = []
    for ex_id, symbol in pairs:
        try:
            ex = getattr(ccxt, ex_id)({"enableRateLimit": True})
            # load markets to validate symbol
            ex.load_markets()
            if symbol not in ex.markets:
                errors.append((ex_id, f"symbol {symbol} missing"))
                continue
            return _fetch_all_ohlcv(ex, symbol, timeframe="1d")
        except Exception as e:
            errors.append((ex_id, repr(e)))
            continue
    raise RuntimeError(f"CCXT failed for {coin_id}: {errors}")
