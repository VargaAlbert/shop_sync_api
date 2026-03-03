# src/merge/rules/enrich_haldepo.py
from __future__ import annotations

from typing import Any, Dict, Optional, Sequence

from src.merge.engine import EnrichPlugin, build_default_index

Product = Dict[str, Any]


def _copy_if_empty(dst: Product, src: Product, dst_key: str, src_key: str) -> None:
    dv = dst.get(dst_key)
    if dv is None or (isinstance(dv, str) and not dv.strip()):
        sv = src.get(src_key)
        if sv is not None:
            dst[dst_key] = sv


def _ensure_image_urls(dst: Product, src: Product) -> None:
    # Haldepó: image_url or image_urls -> master.image_urls
    urls = None
    if isinstance(src.get("image_urls"), list) and src["image_urls"]:
        urls = [str(x).strip() for x in src["image_urls"] if str(x).strip()]
    else:
        u = str(src.get("image_url") or src.get("image") or "").strip()
        if u:
            urls = [u]

    if urls:
        dst["image_urls"] = urls


class HaldepoEnrichPlugin(EnrichPlugin):
    name = "haldepo"

    def build_indexes(self, supplier_rows: Sequence[Product], *, master_keys: Sequence[str]) -> Any:
        return build_default_index(supplier_rows, master_keys=master_keys)

    def find_supplier_row(self, index: Any, master: Product) -> Optional[Product]:
        # index_enricher_by_key usually returns a callable or dict-like structure depending on your implementation.
        # In your dump it's used like: hit = index(master)
        try:
            return index(master)  # type: ignore[misc]
        except TypeError:
            # if index is dict-like and you have a match_key in master:
            mk = (master.get("match_key") or "").strip()
            if mk:
                return index.get(mk)  # type: ignore[union-attr]
            return None

    def apply(self, master: Product, supplier_row: Product) -> Product:
        p = dict(master)

        # --- text fields
        _copy_if_empty(p, supplier_row, "name_hu", "name_hu")
        _copy_if_empty(p, supplier_row, "name_hu", "name")
        _copy_if_empty(p, supplier_row, "description_hu", "description_hu")
        _copy_if_empty(p, supplier_row, "description_hu", "description")

        # --- gtin/ean
        _copy_if_empty(p, supplier_row, "gtin", "gtin")
        _copy_if_empty(p, supplier_row, "gtin", "ean")

        # --- images: Haldepó legyen erős forrás
        _ensure_image_urls(p, supplier_row)

        return p