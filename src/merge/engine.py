from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

Product = Dict[str, Any]


# -------------------------------------------------
# Minimal helpers (self-contained)
# -------------------------------------------------
def build_master_keys(master_products: Sequence[Product]) -> List[str]:
    """
    Megnézi a master mintán, hogy mely kulcsok alapján szoktunk tudni párosítani.
    Nálad jellemzően: match_key / model / gtin/ean.
    """
    # preferált sorrend
    candidates = ["match_key", "model", "modelNumber", "gtin", "ean", "sku"]
    present: List[str] = []
    for k in candidates:
        for p in master_products[:200]:  # elég mintát nézni
            v = p.get(k)
            if v is None:
                continue
            if isinstance(v, str) and v.strip():
                present.append(k)
                break
            if not isinstance(v, str):
                present.append(k)
                break
    # duplikáció kiszűrés, sorrend megtartás
    seen = set()
    out = []
    for k in present:
        if k not in seen:
            seen.add(k)
            out.append(k)
    return out


def _normalize_str(x: Any) -> str:
    return str(x or "").strip().lower()


def index_enricher_by_key(rows: Sequence[Product], master_keys: Sequence[str]) -> Any:
    """
    Egyszerű index: minden supplier row-hoz képezünk kulcsot a master_keys szerint.
    Visszaad egy callable-t: index(master_product) -> supplier_row|None
    """
    lookup: Dict[Tuple[str, str], Product] = {}

    for r in rows:
        for k in master_keys:
            v = _normalize_str(r.get(k))
            if v:
                lookup[(k, v)] = r

    def find(master: Product) -> Optional[Product]:
        for k in master_keys:
            v = _normalize_str(master.get(k))
            if not v:
                continue
            hit = lookup.get((k, v))
            if hit:
                return hit
        return None

    return find


# -------------------------------------------------
# Engine
# -------------------------------------------------
@dataclass(frozen=True)
class EnrichResult:
    products: List[Product]
    stats: Dict[str, Any]


class EnrichPlugin:
    name: str
    priority: int = 0

    def build_indexes(self, supplier_rows: Sequence[Product], *, master_keys: Sequence[str]) -> Any:
        raise NotImplementedError

    def find_supplier_row(self, index: Any, master: Product) -> Optional[Product]:
        raise NotImplementedError

    def apply(self, master: Product, supplier_row: Product) -> Product:
        raise NotImplementedError


def build_default_index(rows: Sequence[Product], *, master_keys: Sequence[str]) -> Any:
    return index_enricher_by_key(list(rows), list(master_keys))


def enrich_products(
    *,
    master_products: Sequence[Product],
    supplier_data: Mapping[str, Sequence[Product]],
    plugins: Sequence[Any],
) -> EnrichResult:
    master_keys = build_master_keys(list(master_products))

    indexes: Dict[str, Any] = {}
    for plg in plugins:
        sname = getattr(plg, "name", "")
        rows = supplier_data.get(sname) or []
        indexes[sname] = plg.build_indexes(rows, master_keys=master_keys)

    out: List[Product] = []
    stats: Dict[str, Any] = {
        "master_count": len(master_products),
        "plugins": [getattr(p, "name", str(p)) for p in plugins],
        "enriched_counts": {getattr(p, "name", str(p)): 0 for p in plugins},
        "enriched_any": 0,
        "master_keys": master_keys,
    }

    for m in master_products:
        p = dict(m)
        enriched_by: Optional[str] = None

        for plg in plugins:
            sname = getattr(plg, "name", "")
            idx = indexes.get(sname)
            row = plg.find_supplier_row(idx, p)
            if not row:
                continue
            p = plg.apply(p, row)
            enriched_by = sname
            stats["enriched_counts"][sname] = stats["enriched_counts"].get(sname, 0) + 1

        if enriched_by:
            p["_enriched_by"] = enriched_by
            stats["enriched_any"] += 1

        out.append(p)

    return EnrichResult(products=out, stats=stats)