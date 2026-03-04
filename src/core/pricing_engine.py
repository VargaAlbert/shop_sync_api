from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Protocol, Sequence, Tuple

from src.core.model import Product


class PricingPlugin(Protocol):
    """
    Pricing plugin interface.
    - name: supplier kulcs (pl. "haldepo")
    - priority: növekvő sorrendben fut (kicsi->nagy)
    """
    name: str
    priority: int

    def build_indexes(self, supplier_products: Sequence[Product]) -> Dict[str, Any]: ...
    def apply(self, *, master: Product, merged: Product, indexes: Dict[str, Any]) -> Product: ...


@dataclass(frozen=True)
class PricingResult:
    products: List[Product]
    stats: Dict[str, Any]


def apply_pricing(
    *,
    master: Sequence[Product],
    merged: Sequence[Product],
    plugins: Sequence[PricingPlugin],
    indexes_by_plugin: Dict[str, Dict[str, Any]],
) -> PricingResult:
    """
    plugins: priority szerint rendezve.
    indexes_by_plugin: { plugin.name: indexes }
    """
    out: List[Product] = []
    stats: Dict[str, Any] = {
        "plugins": [p.name for p in plugins],
        "applied_counts": {p.name: 0 for p in plugins},
    }

    for p_master, p_merged in zip(master, merged):
        p = dict(p_merged)
        for plg in plugins:
            idx = indexes_by_plugin.get(plg.name, {})
            p2 = plg.apply(master=p_master, merged=p, indexes=idx)
            if p2 is not p:
                stats["applied_counts"][plg.name] += 1
            p = p2
        out.append(p)

    return PricingResult(products=out, stats=stats)