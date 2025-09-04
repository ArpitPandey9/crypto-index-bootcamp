from __future__ import annotations
import time, requests
from typing import Dict, Any, Tuple

def get_with_backoff(url: str, params: Dict[str, Any], max_retries: int = 5) -> Tuple[int, Any]:
    delay = 1.0
    for i in range(max_retries):
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 200:
            return (200, r.json())
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(delay); delay = min(delay * 2, 8)
            continue
        return (r.status_code, r.text)
    return (429, {"error": "rate_limited"})
