from __future__ import annotations

from dataclasses import dataclass
from typing import List

from src.core.model import Product


@dataclass
class CarpZoomSupplier:
    name: str = "carpzoom"

    def ingest(self) -> bytes:
        from .ingest import ingest
        return ingest()

    def parse(self, raw_bytes: bytes) -> list[dict]:
        from .parse import parse
        return parse(raw_bytes)

    def normalize(self, rows: list[dict]) -> List[Product]:
        from .normalize import normalize
        return normalize(rows)

    def enrich_plugin(self):
        from .enrich import CarpZoomEnrichPlugin
        return CarpZoomEnrichPlugin()

    def pricing_plugin(self):
        from .pricing import CarpZoomWholesalePricingPlugin
        return CarpZoomWholesalePricingPlugin()