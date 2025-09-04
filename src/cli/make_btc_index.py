from __future__ import annotations
from src.index.base_index import make_base_index_for_coin

if __name__ == "__main__":
    make_base_index_for_coin(
        coin="bitcoin",
        csv_out="data/processed/indexes/btc_close_base1000.csv",
        base_level=1000.0
    )
    print("Wrote data/processed/indexes/btc_close_base1000.csv")
