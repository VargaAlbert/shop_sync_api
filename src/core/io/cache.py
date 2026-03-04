from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional


@dataclass(frozen=True)
class CacheSettings:
    cache_dir: Path
    ttl_sec: int
    enabled: bool
    debug: bool = False


def _now_ts() -> float:
    return time.time()


def _is_fresh(path: Path, ttl_sec: int) -> bool:
    if not path.exists():
        return False
    age = _now_ts() - path.stat().st_mtime
    return age <= ttl_sec


def get_or_fetch_bytes(
    *,
    supplier_name: str,
    filename: str,
    cache: CacheSettings,
    fetch_fn,
) -> bytes:
    p = cache.cache_dir / supplier_name / filename
    p.parent.mkdir(parents=True, exist_ok=True)

    if cache.enabled and p.exists():
        age_ok = (time.time() - p.stat().st_mtime) <= cache.ttl_sec
        if age_ok:
            if cache.debug:
                print(f"[{supplier_name}] CACHE HIT → {p.name}")
            return p.read_bytes()

    if cache.debug:
        print(f"[{supplier_name}] CACHE MISS → downloading...")

    data = fetch_fn()

    if cache.enabled:
        p.write_bytes(data)

    return data