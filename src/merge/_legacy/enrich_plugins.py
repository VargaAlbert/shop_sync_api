from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Optional, Protocol


@dataclass(frozen=True)
class EnrichResult:
    merged: Dict[str, Any]
    enriched_by: Optional[str]  # pl. "haldepo" vagy None (ha nem történt enrich)


class EnrichPlugin(Protocol):
    name: str
    priority: int  # nagyobb = később fusson (felülírhat)

    def supplier_name(self) -> str:
        """Melyik supplier rows-t kell betölteni hozzá (ingest/normalize)."""
        ...

    def build_indexes(self, rows: list[dict[str, Any]], master_keys: set[str]) -> dict[str, Any]:
        ...

    def apply(self, *, master: dict[str, Any], indexes: dict[str, Any]) -> EnrichResult:
        ...

    def build_enrich_update_payload(self, merged: dict[str, Any], *, language_id: str) -> dict[str, Any]:
        ...