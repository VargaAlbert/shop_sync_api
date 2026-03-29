from __future__ import annotations

from typing import Any, Dict, List

from src.core.model import Product
from src.core.match_key import normalize_match_key


def _to_float(v) -> float | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    s = s.replace(" ", "").replace("\u00a0", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def _build_image_urls(r: Dict[str, Any]) -> List[str]:
    urls: List[str] = []
    main_img = (r.get("termek_kep") or "").strip()
    if main_img:
        urls.append(main_img)

    for u in (r.get("termek_tovabbi_kepek") or []):
        u = str(u).strip()
        if u:
            urls.append(u)

    return list(dict.fromkeys(urls))


def normalize(raw_rows: List[Dict[str, Any]]) -> List[Product]:
    out: List[Product] = []

    for r in raw_rows:
        model = (r.get("termek_cikkszam") or "").strip()
        mk = normalize_match_key(model)
        if not mk:
            continue

        image_urls = _build_image_urls(r)

        price_wholesale = _to_float(r.get("termek_ar"))
        price_gross = _to_float(r.get("termek_kisker_ar"))

        p: Product = {
            "supplier": "carpzoom",
            "sku": model,           # mint Haldepó: stabil azonosító
            "model": model or None,
            "gtin": (r.get("termek_vonalkod") or None),
            "match_key": mk or None,

            "name_hu": (r.get("termek_nev") or None),
            "description_hu": (r.get("termek_leiras") or None),
            "image_urls": image_urls,

            # carpzoom: wholesale már bruttó
            "gross_price": price_gross,
            "wholesale_price": price_wholesale,

            "manufacturer_name": None,
            "category": (r.get("termek_kategoria") or None),

            "raw": r,
        }

        out.append(p)

    return out
