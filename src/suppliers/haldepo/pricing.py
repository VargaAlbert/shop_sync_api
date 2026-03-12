from __future__ import annotations

from typing import Any, Dict, Sequence

from src.core.model import Product
from src.utils.numbers import net_to_gross_rounded_5


class HaldepoWholesalePricingPlugin:
    name = "haldepo"
    priority = 10

    def build_indexes(self, supplier_products: Sequence[Product]) -> Dict[str, Any]:
        by_model: Dict[str, Product] = {}
        for p in supplier_products:
            model = (p.get("model") or "").strip()
            if model:
                by_model[model] = p
        return {"by_model": by_model}

    def apply(self, *, master: Product, merged: Product, indexes: Dict[str, Any]) -> Product:
        sku = str(master.get("sku", "")).strip()
        model = (master.get("model") or "").strip() or sku

        h: Product | None = indexes.get("by_model", {}).get(model)
        if not h:
            # nincs haldepo match -> nincs változás
            return merged

        changed = False
        p = dict(merged)

        # gross felülírás csak ha valóban ad értéket és eltér
        hg = h.get("gross_price")
        if hg is not None:
            hg_f = float(hg)
            if p.get("gross_price") != hg_f:
                p["gross_price"] = hg_f
                changed = True

        # wholesale: haldepo nettó -> bruttó (27%) -> 5-re felfelé
        hw = h.get("wholesale_price")
        if hw is not None:
            hw_gross_rounded = float(net_to_gross_rounded_5(hw))
            if p.get("wholesale_price") != hw_gross_rounded:
                p["wholesale_price"] = hw_gross_rounded
                changed = True
        """
        if changed:
            p["_priced_by"] = "haldepo"
            return p

        # volt match, de nem módosított semmit
        return merged
        """
        # ha van match, mindig jelöljük forrásként
        p["_priced_by"] = "haldepo"

        return p