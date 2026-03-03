from __future__ import annotations
from typing import Any, Dict

from src.merge.rules.enrich_plugins import EnrichResult
from src.merge.merge_products import merge_master_with_enricher
from src.merge.rules.natura_haldepo import apply as haldepo_apply_rule

class HaldepoEnrichPlugin:
    name = "haldepo"
    priority = 100

    def supplier_name(self) -> str:
        return "haldepo"

    def build_indexes(self, rows: list[dict[str, Any]], master_keys: set[str]) -> dict[str, Any]:
        # ⚠️ Itt majd a main-ben inkább index_enricher_by_key-t fogunk használni,
        # de ha mégis itt hagyod, akkor ezt ne használd.
        raise NotImplementedError("Use index_enricher_by_key in preview_runner")

    def apply(self, *, master: dict, indexes: dict) -> EnrichResult:
        # ✅ indexes: a index_enricher_by_key(...) által visszaadott haldepo_by_key dict
        merged_list = merge_master_with_enricher([master], indexes, apply_rule=haldepo_apply_rule)
        merged = merged_list[0]

        enriched_by = (merged.get("_enriched_by") or "").strip()
        return EnrichResult(merged=merged, enriched_by=enriched_by or None)

    def build_enrich_update_payload(self, merged: dict, *, language_id: str) -> dict:
        from src.shoprenter.payloads_natura import build_payload
        return build_payload("ENRICH_UPDATE", merged, language_id=language_id)