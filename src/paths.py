from __future__ import annotations
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent
DATA_RAW = ROOT / "data" / "data_raw"
DATA_PROC = ROOT / "data" / "data_proc"
REPORTS = ROOT / "reports"
for p in (DATA_RAW, DATA_PROC, REPORTS):
    p.mkdir(parents=True, exist_ok=True)
