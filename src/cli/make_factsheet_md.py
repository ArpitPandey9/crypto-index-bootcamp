from __future__ import annotations
from pathlib import Path
from src.reports.metrics import perf_metrics_from_index

if __name__ == "__main__":
    csv_in = "data/processed/indexes/btc_close_base1000.csv"
    out_md = Path("docs") / "factsheet_btc_close_base1000.md"
    m = perf_metrics_from_index(csv_in)
    md = f"""# BTC Close-only Baseline (Base=1000)
- **CAGR:** {m['CAGR']:.2%}
- **Annualized Vol:** {m['AnnVol']:.2%}
- **Max Drawdown:** {m['MaxDD']:.2%}

**Data:** Free sources (CoinGecko ≤365d, Yahoo regional, CCXT OHLCV).  
**Method:** Normalize first close to 1000; record divisor.  
**Notes:** Day-2 scaffolding factsheet for later projects.
"""
    out_md.write_text(md, encoding="utf-8")
    print(f"Wrote {out_md}")
