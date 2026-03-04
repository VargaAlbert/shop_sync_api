from __future__ import annotations

import os
import datetime as dt
from pathlib import Path

from src.core.io.cache import CacheSettings, get_or_fetch_bytes
from src.core.io.http import download_bytes
from src.core.io.supplier_files import load_supplier_json


def ingest() -> bytes:
    cfg = load_supplier_json("natura")

    cache = CacheSettings(
        cache_dir=Path(cfg.cache_dir),
        ttl_sec=cfg.cache_ttl_sec,
        enabled=cfg.cache_enabled,
        debug=os.getenv("DEBUG_CACHE", "1") == "1",
    )

    filename = "latest.csv"

    return get_or_fetch_bytes(
        supplier_name=cfg.name,
        filename=filename,
        cache=cache,
        fetch_fn=lambda: download_bytes(
            cfg.url,
            timeout_sec=cfg.timeout_sec,
            headers=cfg.headers,
        ),
    )