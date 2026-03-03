# src/merge/engine.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from src.merge.merge_products import build_master_keys, index_enricher_by_key

Product = Dict[str, Any]


@dataclass(frozen=True)
class EnrichResult:
    products: List[Product]
    stats: Dict[str, Any]


class EnrichPlugin:
    """
    Enrich plugin interface:
    - name: supplier name (e.g. "haldepo")
    - build_indexes: return an index that can resolve a master product -> supplier row
    - apply: merge supplier row into master product (in-place or return new)
    """
    name: str

    def build_indexes(
        self,
        supplier_rows: Sequence[Product],
        *,
        master_keys: Sequence[str],
    ) -> Any:
        raise NotImplementedError

    def find_supplier_row(self, index: Any, master: Product) -> Optional[Product]:
        raise NotImplementedError

    def apply(self, master: Product, supplier_row: Product) -> Product:
        raise NotImplementedError


def enrich_products(
    *,
    master_products: Sequence[Product],
    supplier_data: Mapping[str, Sequence[Product]],
    plugins: Sequence[EnrichPlugin],
) -> EnrichResult:
    """
    master_products: Natura master normalized list
    supplier_data: { "haldepo": [rows...], ... }
    plugins: in priority order (last wins if you design it that way)
    """
    master_keys = build_master_keys(master_products)

    # build plugin indexes once
    indexes: Dict[str, Any] = {}
    for plg in plugins:
        rows = supplier_data.get(plg.name) or []
        indexes[plg.name] = plg.build_indexes(rows, master_keys=master_keys)

    out: List[Product] = []
    stats = {
        "master_count": len(master_products),
        "plugins": [p.name for p in plugins],
        "enriched_counts": {p.name: 0 for p in plugins},
        "enriched_any": 0,
    }

    for m in master_products:
        p = dict(m)  # do not mutate original
        enriched_by: Optional[str] = None

        for plg in plugins:
            idx = indexes[plg.name]
            srow = plg.find_supplier_row(idx, p)
            if not srow:
                continue

            p = plg.apply(p, srow)
            enriched_by = plg.name
            stats["enriched_counts"][plg.name] += 1

        if enriched_by:
            p["_enriched_by"] = enriched_by
            stats["enriched_any"] += 1

        out.append(p)

    return EnrichResult(products=out, stats=stats)


# Small helper you may reuse if you want simple indexing everywhere:
def build_default_index(
    rows: Sequence[Product],
    *,
    master_keys: Sequence[str],
) -> Any:
    return index_enricher_by_key(rows, list(master_keys))