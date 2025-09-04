from __future__ import annotations
import time
from typing import Optional
from datetime import timezone
import pandas as pd
import yfinance as yf

_YAHOO_SYMBOL = {
    "bitcoin": "BTC-USD",
    "ethereum": "ETH-USD",
}

def _dl(sym: str, tries: int = 3) -> pd.DataFrame:
    last_err: Optional[Exception] = None
    for _ in range(tries):
        try:
            df = yf.download(sym, period="max", interval="1d", auto_adjust=False, actions=False, progress=False, threads=False)
            if df is not None and not df.empty:
                return df
        except Exception as e:
            last_err = e
        time.sleep(1.0)
    raise RuntimeError(f"Yahoo returned empty history for {sym}; last_err={last_err}")

def fetch_daily_close_yahoo(coin_id: str) -> pd.DataFrame:
    sym = _YAHOO_SYMBOL.get(coin_id)
    if not sym:
        raise ValueError(f"No Yahoo symbol mapping for {coin_id}")
    hist = _dl(sym).reset_index()
    hist.rename(columns={"Date":"timestamp_utc","Close":"close","Volume":"volume"}, inplace=True)
    if hist["timestamp_utc"].dt.tz is None:
        hist["timestamp_utc"] = hist["timestamp_utc"].dt.tz_localize(timezone.utc)
    else:
        hist["timestamp_utc"] = hist["timestamp_utc"].dt.tz_convert(timezone.utc)
    hist["date"] = hist["timestamp_utc"].dt.date.astype("datetime64[ns]")
    out = hist[["timestamp_utc","date","close","volume"]].sort_values("timestamp_utc").reset_index(drop=True)
    return out
