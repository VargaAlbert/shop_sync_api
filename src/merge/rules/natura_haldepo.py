#/merge/rules/natura_haldepo.py
from __future__ import annotations
from typing import Dict, Any

from .base import copy_if_empty


def apply(master: Dict[str, Any], enricher: Dict[str, Any]) -> Dict[str, Any]:
    """
    Natura master + Haldepó enricher szabály.
    """
    out = dict(master)

    # Leírás
    copy_if_empty(out, enricher, "description_hu")

    # Képek
    copy_if_empty(out, enricher, "image_urls")

    # EAN
    copy_if_empty(out, enricher, "gtin")

    # Gyártó (Haldepó -> manufacturer, Natura -> manufacturer_name)
    if enricher.get("manufacturer") and not out.get("manufacturer_name"):
        out["manufacturer_name"] = enricher["manufacturer"]

    out["_enriched_by"] = "haldepo"

    return out