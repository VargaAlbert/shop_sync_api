from __future__ import annotations
import os
import requests

from src.ingest.downloaders.registry import register_downloader


@register_downloader("haldepo")
def download_haldepo(url: str, timeout_sec: int = 120) -> bytes:
    user = os.getenv("HALDEPO_USER", "")
    password = os.getenv("HALDEPO_PASS", "")
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