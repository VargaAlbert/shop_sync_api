from __future__ import annotations
from typing import Dict, Any

from .base import copy_if_empty


def apply(master: Dict[str, Any], enricher: Dict[str, Any]) -> Dict[str, Any]:
    """
    Natura master + Haldepó enricher szabály.
    """

    out = dict(master)

    # Leírás (csak ha Natura üres)
    copy_if_empty(out, enricher, "description_hu")

    # GTIN (csak ha Natura üres)
    copy_if_empty(out, enricher, "gtin")

    # Gyártó
    if enricher.get("manufacturer") and not out.get("manufacturer_name"):
        out["manufacturer_name"] = enricher["manufacturer"]

    # -------------------------------------------------
    # 🔥 KÉP: mindig Haldepó az igazság
    # -------------------------------------------------
    if enricher.get("image_urls"):
        out["image_urls"] = enricher["image_urls"]

    out["_enriched_by"] = "haldepo"

    return out