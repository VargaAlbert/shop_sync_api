from __future__ import annotations
from typing import Dict
import time

def build_product_sku_map(
    client,
    *,
    limit: int = 200,
    sleep_s: float = 0.2,
    max_pages: int = 2000,   # vészfék
) -> Dict[str, str]:
    page = 0
    out: Dict[str, str] = {}

    while True:
        t0 = time.time()
        data = client.get_product_extend_page(page=page, limit=limit, full=True)

        page_count = int(data.get("pageCount") or 0)
        items = data.get("items", []) or []

        for it in items:
            sku = (it.get("sku") or "").strip()
            pid = it.get("id")
            if sku and pid:
                out[sku] = pid

        dt = time.time() - t0
        print(f"[SKU_MAP] page={page+1}/{page_count} items={len(items)} map_size={len(out)} dt={dt:.2f}s")

        page += 1

        # STOP feltételek
        if page_count and page >= page_count:
            break
        if page >= max_pages:
            raise RuntimeError(f"Vészfék: max_pages elérve ({max_pages}). Valószínű pageCount/oldalazás gond van.")

        time.sleep(sleep_s)

    return out