from __future__ import annotations

import os
from pathlib import Path

import requests
from dotenv import load_dotenv

from src.core.io.cache import CacheSettings, get_or_fetch_bytes
from src.core.io.supplier_files import load_supplier_json


def _build_auth_xml() -> bytes:
    user = (os.getenv("CARPZOOM_USER") or "").strip()
    key = (os.getenv("CARPZOOM_KEY") or "").strip()

    if not user or not key:
        raise RuntimeError("CARPZOOM_USER / CARPZOOM_KEY nincs beállítva")

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<xml>
  <auth>
    <user>{user}</user>
    <key>{key}</key>
  </auth>
</xml>
"""
    return xml.encode("utf-8")


def _download_carpzoom_xml(url: str, *, timeout_sec: int = 120) -> bytes:
    body = _build_auth_xml()

    r = requests.post(
        url,
        data=body,
        headers={
            "Content-Type": "text/xml; charset=utf-8",
            "Accept": "application/xml",
        },
        timeout=timeout_sec,
    )

    r.raise_for_status()
    return r.content


def ingest() -> bytes:
    load_dotenv(override=False)

    cfg = load_supplier_json("carpzoom")

    cache = CacheSettings(
        cache_dir=Path(cfg.cache_dir),
        ttl_sec=cfg.cache_ttl_sec,
        enabled=cfg.cache_enabled,
        debug=os.getenv("DEBUG_CACHE", "1") == "1",
    )

    filename = "latest.xml"  # stabil név → TTL működik

    return get_or_fetch_bytes(
        supplier_name=cfg.name,
        filename=filename,
        cache=cache,
        fetch_fn=lambda: _download_carpzoom_xml(cfg.url, timeout_sec=cfg.timeout_sec),
    )