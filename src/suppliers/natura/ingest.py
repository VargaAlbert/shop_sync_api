from __future__ import annotations

import json


def ingest() -> bytes:
    """
    Jelenlegi kompatibilitás:
    - a meglévő ingest réteget használjuk (download + cache + parse CSV -> rows)
    - itt bytes-ként adjuk tovább JSON-ba csomagolva (hogy a pipeline interface egységes legyen)
    """
    from src.ingest.suppliers_csv import ingest_one_supplier_csv  # meglévő :contentReference[oaicite:4]{index=4}

    rows = ingest_one_supplier_csv("natura")
    return json.dumps(rows, ensure_ascii=False).encode("utf-8")