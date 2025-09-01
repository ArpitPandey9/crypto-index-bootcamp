# Factsheet — BTC Price Index (Prototype)
- **Method**: Base=1000 on first available daily close.
- **Universe**: BTC only (prototype).
- **Pricing Source**: Recorded in prices file as 'source' (coingecko/coincap/binance).
- **Rebalance Cadence**: N/A today; future indices: daily or monthly at 00:00 UTC.
- **Disruption Policy**: Retry with backoff; else carry last level with note 'stale-carry'.
- **Metrics (to fill later)**: CAGR, Ann. Vol, Max DD, Tracking Error, Turnover.
- **Costs**: Not modeled today; to be added.
- **Governance Notes**: Versioned loader, cached raw JSON, deterministic CSVs, UTC dates.
