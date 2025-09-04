from __future__ import annotations
import pandas as pd
from pathlib import Path

ASSETS = ["bitcoin","ethereum"]

def _find_parquet(coin: str) -> Path:
    for provider in ("coingecko","yahoo","ccxt"):
        p = Path("data/raw") / provider / f"{coin}_usd_daily.parquet"
        if p.exists():
            return p
    raise AssertionError(f"Missing parquet for {coin} in coingecko/ yahoo/ or ccxt/")

def _load(coin: str) -> pd.DataFrame:
    p = _find_parquet(coin)
    return pd.read_parquet(p)

def test_schema_and_tz():
    for c in ASSETS:
        df = _load(c)
        assert list(df.columns) == ["timestamp_utc","date","close","volume"]
        from pandas.core.dtypes.dtypes import DatetimeTZDtype
        assert isinstance(df["timestamp_utc"].dtype, DatetimeTZDtype)
        assert pd.api.types.is_datetime64_ns_dtype(df["date"])
        assert df["timestamp_utc"].is_monotonic_increasing
        assert df["close"].notna().all()
        assert df["close"].min() > 0

def test_calendar_alignment():
    for c in ASSETS:
        df = _load(c)
        d2 = df["timestamp_utc"].dt.tz_convert("UTC").dt.date.astype("datetime64[ns]")
        assert (d2 == df["date"]).all()
