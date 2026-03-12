from __future__ import annotations

import os

import src.suppliers  # side-effect: supplier regisztráció
from src.core.registry import get_supplier, list_suppliers
from src.utils.log import setup_logging


def prefetch_all_sources(*, skip: set[str] | None = None) -> None:
    """
    Napi 1x futtatandó "letöltő" job.
    - Minden supplier ingest lefut (cache FORCE refresh)
    - Skip: pl. {"natura"} mert lokális master, mindig a friss csv-t olvassa
    """
    log = setup_logging()

    skip = {s.lower() for s in (skip or set())}

    # csak erre az egy futásra force-oljuk a letöltést
    old = os.environ.get("CACHE_FORCE_REFRESH")
    os.environ["CACHE_FORCE_REFRESH"] = "1"

    try:
        for name in list_suppliers():
            if name.lower() in skip:
                log.info("PREFETCH skip: %s", name)
                continue

            try:
                log.info("PREFETCH start: %s", name)
                sup = get_supplier(name)
                _ = sup.ingest()  # letöltés/cache frissítés itt történik
                log.info("PREFETCH ok: %s", name)
            except Exception as e:
                log.exception("PREFETCH failed: %s error=%s", name, e)
    finally:
        # visszaállítás
        if old is None:
            os.environ.pop("CACHE_FORCE_REFRESH", None)
        else:
            os.environ["CACHE_FORCE_REFRESH"] = old