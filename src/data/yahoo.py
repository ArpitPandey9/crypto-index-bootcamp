from __future__ import annotations
from datetime import datetime, timezone
import pandas as pd
import yfinance as yf

# Map simple ids to Yahoo symbols
_YAHOO_SYMBOL = {
    "bitcoin": "BTC-USD",
    "ethereum": "ETH-USD",
}

def fetch_daily_close_yahoo(coin_id: str) -> pd.DataFrame:
    sym = _YAHOO_SYMBOL.get(coin_id)
    if not sym:
        raise ValueError(f"No Yahoo symbol mapping for {coin_id}")
    tkr = yf.Ticker(sym)
    hist = tkr.history(period="max", interval="1d", auto_adjust=False)
    if hist.empty:
        raise RuntimeError(f"Yahoo returned empty history for {sym}")

    hist = hist.reset_index()  # 'Date' column
    hist.rename(columns={"Date":"timestamp_utc","Close":"close","Volume":"volume"}, inplace=True)
    # Ensure tz-aware UTC
    if hist["timestamp_utc"].dt.tz is None:
        hist["timestamp_utc"] = hist["timestamp_utc"].dt.tz_localize(timezone.utc)
    else:
        hist["timestamp_utc"] = hist["timestamp_utc"].dt.tz_convert(timezone.utc)
    hist["date"] = hist["timestamp_utc"].dt.date.astype("datetime64[ns]")
    out = hist[["timestamp_utc","date","close","volume"]].sort_values("timestamp_utc").reset_index(drop=True)
    # Yahoo sometimes includes last partial day; we keep it—our factsheet uses daily diffs.
    return out
