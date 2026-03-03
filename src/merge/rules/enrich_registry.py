from __future__ import annotations
from typing import Dict, List

from src.merge.rules.enrich_plugins import EnrichPlugin
from src.merge.rules.enrich_haldepo import HaldepoEnrichPlugin


def get_all_enrich_plugins() -> List[EnrichPlugin]:
    plugins: List[EnrichPlugin] = [
        HaldepoEnrichPlugin(),
        # ide jön majd: XyzEnrichPlugin(), AbcEnrichPlugin(), ...
    ]
    # priority szerint futnak (kicsitől nagy felé)
    return sorted(plugins, key=lambda p: int(getattr(p, "priority", 0)))