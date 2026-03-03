from __future__ import annotations
from typing import List

from src.merge.rules.enrich_haldepo import HaldepoEnrichPlugin


def get_all_enrich_plugins() -> List[object]:
    plugins: List[object] = [
        HaldepoEnrichPlugin(),
        # ide jön majd: XyzEnrichPlugin(), AbcEnrichPlugin(), ...
    ]
    # priority szerint futnak (kicsitől nagy felé), ha van priority attribútum
    return sorted(plugins, key=lambda p: int(getattr(p, "priority", 0)))