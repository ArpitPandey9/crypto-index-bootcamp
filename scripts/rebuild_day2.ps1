# Rebuild Day 2 pipeline end-to-end (prices -> parquet -> tests -> index CSV -> factsheet)
# Requires venv: .\.venv\Scripts\Activate.ps1

python -m src.cli.pull_prices --coin_id bitcoin  --days max
python -m src.cli.pull_prices --coin_id ethereum --days max
pytest -q
python -m src.cli.make_btc_index
python -m src.cli.make_factsheet_md
