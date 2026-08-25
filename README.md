# Digital Assets Index Lab

[![CI](https://img.shields.io/github/actions/workflow/status/ArpitPandey9/digital-assets-index-lab/ci.yml?label=CI)](https://github.com/ArpitPandey9/digital-assets-index-lab/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.12-blue)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

Reproducible digital-asset research for public market-data ingestion, index construction, performance analysis, and data-quality testing.

This repository is a research and engineering project. It is not a market-prediction system or a production index product.

## Current scope

- BTC and ETH daily-close ingestion
- CoinGecko, Yahoo, and CCXT provider paths
- automatic provider fallback in the price-pull CLI
- Parquet-based price storage
- BTC close-only base-1000 index construction
- basic performance and factsheet metrics
- pytest-based data and schema checks

## Environment

Verified with Python 3.12 in WSL/Linux.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## Run the tests

```bash
python -m pytest -q
```

## Pull market data

Example BTC pull using the implemented provider chain:

```bash
python -m src.cli.pull_prices --coin_id bitcoin --days 365 --provider auto --out_dir data/raw
```

The CLI supports `bitcoin` and `ethereum` and can use CoinGecko, Yahoo, or CCXT according to the selected provider mode and runtime availability.

## Build the BTC baseline index

```bash
python -m src.cli.make_btc_index
```

Output:

```text
data/processed/indexes/btc_close_base1000.csv
```

## Generate the Markdown factsheet

```bash
python -m src.cli.make_factsheet_md
```

Output:

```text
docs/factsheet_btc_close_base1000.md
```

The current factsheet reports CAGR, annualized volatility, and maximum drawdown.

## Repository structure

```text
src/
  cli/       command-line entry points
  data/      market-data providers
  index/     index construction
  reports/   reporting metrics
  utils/     supporting utilities

tests/       pytest checks
data/        raw and processed research data artifacts
docs/        methodology and factsheet material
scripts/     helper scripts
figures/     generated visual material
reports/     research outputs
```

## Limitations

Public-data availability and coverage depend on the providers available at runtime. This repository should not be interpreted as an official index methodology, investment recommendation, production financial system, or representation of any employer or client.

## License

MIT.
