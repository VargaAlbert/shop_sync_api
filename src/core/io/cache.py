# src/core/io/cache.py
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class CacheSettings:
    cache_dir: Path
    ttl_sec: int
    enabled: bool
    debug: bool = False

def get_or_fetch_bytes(
    *,
    supplier_name: str,
    filename: str,
    cache: CacheSettings,
    fetch_fn,
) -> bytes:
    p = cache.cache_dir / supplier_name / filename
    p.parent.mkdir(parents=True, exist_ok=True)

    # ÚJ: globális force refresh ENV-ből (prefetch erre fog támaszkodni)
    force_refresh = os.getenv("CACHE_FORCE_REFRESH", "0") == "1"

    if not force_refresh and cache.enabled and p.exists():
        age_ok = (time.time() - p.stat().st_mtime) <= cache.ttl_sec
        if age_ok:
            if cache.debug:
                print(f"[{supplier_name}] CACHE HIT -> {p.name}")
            return p.read_bytes()

    if cache.debug:
        print(f"[{supplier_name}] CACHE {'FORCE' if force_refresh else 'MISS'} -> downloading.")

    data = fetch_fn()

    if cache.enabled:
        p.write_bytes(data)

    return data