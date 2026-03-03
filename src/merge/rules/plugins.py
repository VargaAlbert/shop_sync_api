from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Optional, Protocol


@dataclass(frozen=True)
class WholesaleResult:
    gross_price: Optional[float]
    wholesale_price: Optional[float]
    supplier: str


class WholesalePlugin(Protocol):
    name: str

    def build_indexes(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        ...

    def apply(self, *, master: dict[str, Any], indexes: dict[str, Any]) -> WholesaleResult:
        ...