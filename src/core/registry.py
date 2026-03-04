from __future__ import annotations

from typing import Dict, Optional, Protocol

from src.core.model import Product


class SupplierModule(Protocol):
    """
    Supplier modul interface.
    A pipeline NEM ismerhet supplier-specifikus részleteket.
    """
    name: str

    def ingest(self) -> bytes: ...
    def parse(self, raw_bytes: bytes) -> list[dict]: ...
    def normalize(self, rows: list[dict]) -> list[Product]: ...

    def enrich_plugin(self): ...    # EnrichPlugin vagy None
    def pricing_plugin(self): ...   # PricingPlugin vagy None


_SUPPLIERS: Dict[str, SupplierModule] = {}


def register_supplier(mod: SupplierModule) -> SupplierModule:
    key = (getattr(mod, "name", "") or "").strip().lower()
    if not key:
        raise ValueError("SupplierModule.name is empty")
    _SUPPLIERS[key] = mod
    return mod


def get_supplier(name: str) -> SupplierModule:
    key = (name or "").strip().lower()
    if key not in _SUPPLIERS:
        raise KeyError(f"Unknown supplier: {name}. Known: {sorted(_SUPPLIERS.keys())}")
    return _SUPPLIERS[key]


def list_suppliers() -> list[str]:
    return sorted(_SUPPLIERS.keys())