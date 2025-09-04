from __future__ import annotations
import json, time
from pathlib import Path
from typing import Any, Dict

def read_json_cache(path: str | Path) -> Dict[str, Any] | None:
    p = Path(path)
    if not p.exists(): return None
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)

def write_json_cache(path: str | Path, payload: Dict[str, Any]) -> None:
    p = Path(path); p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f)
    tmp.replace(p)

def is_fresh(metadata: Dict[str, Any], ttl_seconds: int) -> bool:
    ts = metadata.get("_fetched_at", 0)
    return (time.time() - ts) < ttl_seconds
