from __future__ import annotations

from typing import Any, Dict, List

from src.core.model import Product
from src.core.match_key import normalize_match_key
from src.core.mapping import get_float, get_str, load_mapping


def normalize(raw_rows: List[Dict[str, Any]]) -> List[Product]:
    """
    Natura raw rows -> egységes Product lista.
    A mapping.json kulcsok a meglévő natura mappingból jönnek. :contentReference[oaicite:6]{index=6}
    """
    m = load_mapping("natura")
    out: List[Product] = []

    for r in raw_rows:
        sku = get_str(r, m, "sku")
        if not sku:
            continue

        model = get_str(r, m, "model")
        mk = normalize_match_key(model or sku)

        p: Product = {
            "supplier": "natura",
            "sku": sku,
            "model": model or None,
            "gtin": (get_str(r, m, "gtin") or None),
            "match_key": mk or None,

            "name_hu": (get_str(r, m, "name") or None),
            "description_hu": None,  # Natura-nál jelen mappingban nincs leírás mező
            "image_urls": [],

            "gross_price": get_float(r, m, "gross_price"),
            "wholesale_price": get_float(r, m, "wholesale_price"),

            "manufacturer_name": (get_str(r, m, "manufacturer_name") or None),
            "category": (get_str(r, m, "csoport1_name") or None),

            "raw": r,
        }

        out.append(p)

    return out