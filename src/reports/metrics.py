from __future__ import annotations
import pandas as pd
import numpy as np

def perf_metrics_from_index(csv_path: str) -> dict:
    df = pd.read_csv(csv_path, parse_dates=["date"]).sort_values("date")
    lv = df["index_level"].values.astype(float)
    if len(lv) < 2:
        return {"CAGR":0.0, "AnnVol":0.0, "MaxDD":0.0}
    rets = np.diff(lv) / lv[:-1]
    ann_factor = 365
    cagr = (lv[-1] / lv[0]) ** (ann_factor/len(rets)) - 1
    vol = np.std(rets, ddof=1) * np.sqrt(ann_factor)
    running_max = np.maximum.accumulate(lv)
    dd = (lv / running_max) - 1.0
    maxdd = float(dd.min())
    return {"CAGR":float(cagr), "AnnVol":float(vol), "MaxDD":maxdd}
