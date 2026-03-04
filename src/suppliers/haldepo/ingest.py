from __future__ import annotations

import json


def ingest() -> bytes:
    """
    Kompatibilitás:
    ingest_one_supplier_csv("haldepo") már kezeli:
    - auth downloader (HALDEPO_USER/PASS)
    - cache
    - CSV parse
    :contentReference[oaicite:7]{index=7} :contentReference[oaicite:8]{index=8}
    """
    from src.ingest.suppliers_csv import ingest_one_supplier_csv

    rows = ingest_one_supplier_csv("haldepo")
    return json.dumps(rows, ensure_ascii=False).encode("utf-8")