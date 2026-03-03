from __future__ import annotations
from typing import Any, Dict

from src.merge.rules.plugins import WholesaleResult
from src.utils.numbers import net_to_gross_rounded_5


class HaldepoWholesalePlugin:
    name = "haldepo"

    def build_indexes(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        by_model: dict[str, dict[str, Any]] = {}
        for r in rows:
            model = (r.get("modelNumber") or r.get("model") or "").strip()
            if model:
                by_model[model] = r
        return {"by_model": by_model}

    def apply(self, *, master: dict[str, Any], indexes: dict[str, Any]) -> WholesaleResult:
        sku = str(master.get("sku", "")).strip()
        model = (master.get("model") or "").strip() or sku

        # Natura alap
        gp = master.get("gross_price")
        wp = master.get("wholesale_price")

        h = indexes.get("by_model", {}).get(model)
        if not h:
            return WholesaleResult(
                gross_price=float(gp) if gp is not None else None,
                wholesale_price=float(wp) if wp is not None else None,
                supplier="natura",
            )

        # Haldepó felülír
        if h.get("gross_price") is not None:
            gp = h["gross_price"]
        if h.get("wholesale_price") is not None:
            wp = h["wholesale_price"]

        # Haldepó wholesale nálad nettó -> bruttó + 5-re felfelé
        wp_out = None if wp is None else float(net_to_gross_rounded_5(wp))

        return WholesaleResult(
            gross_price=float(gp) if gp is not None else None,
            wholesale_price=wp_out,
            supplier="haldepo",
        )