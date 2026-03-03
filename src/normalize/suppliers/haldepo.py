from __future__ import annotations

from typing import Any, Dict, List

from src.normalize.supplier_generic import register_normalizer
from src.normalize.mapping import load_mapping, get_float, get_str
from src.merge.match_key import normalize_match_key


@register_normalizer("haldepo")
def normalize_haldepo_rows(raw_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Haldepó CSV -> normalizált "enricher" rekordok.

    Kötelező kimenet:
      - match_key (Cikkszám normalizált)
    Ajánlott kimenet:
      - description_hu (Termékleírás)
      - image_urls (Termékkép 1-3)
      - manufacturer, gtin
      - gross_price, wholesale_price (későbbi felhasználásra)
      - raw (debug)
    """
    m = load_mapping("haldepo")

    out: List[Dict[str, Any]] = []
    for r in raw_rows:
        model = get_str(r, m, "model")
        match_key = normalize_match_key(model)

        # Ha nincs kulcs, nem tudjuk joinolni -> skip
        if not match_key:
            continue

        # 3 kép összegyűjtése
        img0 = get_str(r, m, "mainPicture")
        img1 = get_str(r, m, "image1")
        img2 = get_str(r, m, "image2")
        image_urls = [u for u in (img0, img1, img2) if u]

        item: Dict[str, Any] = {
            "kind": "enricher",
            "source": "haldepo",

            "match_key": match_key,
            "model_raw": model,

            "model": model,
            "modelNumber": model,  # opcionális, ha ezt preferálod máshol

            # belső egységes mezők (ezeket fogja a merge/payload használni)
            "description_hu": get_str(r, m, "productDescriptions"),
            "manufacturer": get_str(r, m, "manufacturer_name"),
            "gtin": get_str(r, m, "gtin"),
            "image_urls": image_urls,

            # árak (nem biztos hogy kell, de normalizáljuk)
            "gross_price": get_float(r, m, "gross_price"),
            "wholesale_price": get_float(r, m, "wholesale_price"),

            "raw": r,  # debug
        }

        out.append(item)

    return out