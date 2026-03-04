from __future__ import annotations

from typing import Any, Dict, Optional, Sequence

from src.core.enrich_engine import EnrichPlugin, build_default_index  # meglévő enrich engine interface :contentReference[oaicite:11]{index=11}

ProductRow = Dict[str, Any]


def _copy_if_empty(dst: ProductRow, src: ProductRow, dst_key: str, src_key: str) -> None:
    dv = dst.get(dst_key)
    if dv is None or (isinstance(dv, str) and not dv.strip()):
        sv = src.get(src_key)
        if sv is not None and (not isinstance(sv, str) or sv.strip()):
            dst[dst_key] = sv


def _ensure_image_urls(dst: ProductRow, src: ProductRow) -> None:
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
    """
    Haldepó -> Natura enrich:
    - leírás
    - gtin/ean
    - képek (erős forrás)
    """
    name = "haldepo"
    priority = 10

    def build_indexes(self, supplier_rows: Sequence[ProductRow], *, master_keys: Sequence[str]) -> Any:
        return build_default_index(supplier_rows, master_keys=master_keys)

    def find_supplier_row(self, index: Any, master: ProductRow) -> Optional[ProductRow]:
        # A te jelenlegi index_enricher_by_key implementációd callable-ként van használva. :contentReference[oaicite:12]{index=12}
        try:
            return index(master)  # type: ignore[misc]
        except TypeError:
            mk = (master.get("match_key") or "").strip()
            if mk:
                return index.get(mk)  # type: ignore[union-attr]
            return None

    def apply(self, master: ProductRow, supplier_row: ProductRow) -> ProductRow:
        p = dict(master)

        _copy_if_empty(p, supplier_row, "description_hu", "description_hu")
        _copy_if_empty(p, supplier_row, "description_hu", "description")

        _copy_if_empty(p, supplier_row, "gtin", "gtin")
        _copy_if_empty(p, supplier_row, "gtin", "ean")

        _ensure_image_urls(p, supplier_row)
        return p