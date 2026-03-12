from __future__ import annotations

from typing import Any, Dict, Sequence

from src.core.model import Product


class CarpZoomWholesalePricingPlugin:
    name = "carpzoom"
    priority = 20  # Haldepó 10 -> CarpZoom fusson utána (vagy tedd 5-re, ha CarpZoom legyen erősebb)

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

        cz: Product | None = indexes.get("by_model", {}).get(model)
        if not cz:
            # nincs carpzoom match -> nincs változás
            return merged

        changed = False
        p = dict(merged)

        # gross_price: csak ha ad értéket és eltér
        cz_g = cz.get("gross_price")
        if cz_g is not None:
            cz_gf = float(cz_g)
            if p.get("gross_price") != cz_gf:
                p["gross_price"] = cz_gf
                changed = True

        """
        # wholesale_price: CarpZoom esetén már BRUTTÓ (NINCS konverzió)
        cz_w = cz.get("wholesale_price")
        if cz_w is not None:
            cz_wf = float(cz_w)
            if p.get("wholesale_price") != cz_wf:
                p["wholesale_price"] = cz_wf
                changed = True

        if changed:
            p["_priced_by"] = "carpzoom"
            return p
        """
        # volt match, de nem módosított semmit
        return merged