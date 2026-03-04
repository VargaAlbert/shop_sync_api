from __future__ import annotations

from typing import Any, Dict, List

from src.core.model import Product
from src.core.match_key import normalize_match_key
from src.core.mapping import get_float, get_str, load_mapping

def normalize(raw_rows: List[Dict[str, Any]]) -> List[Product]:
    """
    Haldepó raw rows -> egységes Product lista.
    A mapping.json kulcsok a meglévő haldepo mappingból jönnek. :contentReference[oaicite:10]{index=10}
    """
    m = load_mapping("haldepo")
    out: List[Product] = []

    for r in raw_rows:
        model = get_str(r, m, "model")
        mk = normalize_match_key(model)
        if not mk:
            continue

        img0 = get_str(r, m, "mainPicture")
        img1 = get_str(r, m, "image1")
        img2 = get_str(r, m, "image2")
        image_urls = [u for u in (img0, img1, img2) if u]

        p: Product = {
            "supplier": "haldepo",
            "sku": model,  # Haldepó oldalon nincs Natura-szerű SKU: a modelt tesszük ide, hogy legyen stabil azonosító
            "model": model or None,
            "gtin": (get_str(r, m, "gtin") or None),
            "match_key": mk or None,

            "name_hu": None,
            "description_hu": (get_str(r, m, "productDescriptions") or None),
            "image_urls": image_urls,

            "gross_price": get_float(r, m, "gross_price"),
            "wholesale_price": get_float(r, m, "wholesale_price"),

            "manufacturer_name": (get_str(r, m, "manufacturer_name") or None),
            "category": None,

            "raw": r,
        }

        out.append(p)

    return out