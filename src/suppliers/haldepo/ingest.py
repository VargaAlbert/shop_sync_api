from __future__ import annotations

import os
from pathlib import Path

import requests
from dotenv import load_dotenv

from src.core.io.cache import CacheSettings, get_or_fetch_bytes
from src.core.io.supplier_files import load_supplier_json


def _download_haldepo_csv(url: str, *, timeout_sec: int = 120) -> bytes:
    user = (os.getenv("HALDEPO_USER") or "").strip()
    password = (os.getenv("HALDEPO_PASS") or "").strip()
    if not user or not password:
        raise RuntimeError("HALDEPO_USER / HALDEPO_PASS nincs beállítva a .env-ben")

    r = requests.get(
        url,
        auth=(user, password),
        headers={"User-Agent": "shop-sync/1.0"},
        timeout=timeout_sec,
    )
    r.raise_for_status()
    return r.content


def ingest() -> bytes:
    load_dotenv(override=False)

    cfg = load_supplier_json("haldepo")

    cache = CacheSettings(
        cache_dir=Path(cfg.cache_dir),
        ttl_sec=cfg.cache_ttl_sec,
        enabled=cfg.cache_enabled,
        debug=os.getenv("DEBUG_CACHE", "1") == "1",
    )

    # stabil név, hogy a TTL tényleg működjön
    filename = "latest.csv"

    return get_or_fetch_bytes(
        supplier_name=cfg.name,
        filename=filename,
        cache=cache,
        fetch_fn=lambda: _download_haldepo_csv(cfg.url, timeout_sec=cfg.timeout_sec),
    )