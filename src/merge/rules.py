from __future__ import annotations
from typing import Any, Dict

ENRICH_ALLOWED_FIELDS = {
    "description_hu",
    "image_urls",
    #"manufacturer",
    #"gtin",
    # ha később kell:
    # "gross_price", "wholesale_price",
}

def apply_enricher(master: Dict[str, Any], enricher: Dict[str, Any]) -> Dict[str, Any]:
    """
    Master (Natura) + Enricher (Haldepó) összeolvasztás.
    Enricher csak az engedélyezett mezőket adhatja hozzá,
    és alapból csak akkor, ha a masterben üres.
    """
    out = dict(master)

    # description
    if enricher.get("description_hu") and not out.get("description_hu"):
        out["description_hu"] = enricher["description_hu"]

    # images
    if enricher.get("image_urls") and not out.get("image_urls"):
        out["image_urls"] = enricher["image_urls"]

    # manufacturer / gtin (csak ha üres)
    if enricher.get("manufacturer") and not out.get("manufacturer"):
        out["manufacturer"] = enricher["manufacturer"]
    if enricher.get("gtin") and not out.get("gtin"):
        out["gtin"] = enricher["gtin"]

    if enricher:
        out["_enriched_by"] = enricher.get("source", "enricher")

    return out