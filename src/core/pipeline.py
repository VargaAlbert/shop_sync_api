from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from src.core.model import Product
from src.core.registry import get_supplier, list_suppliers

from src.core.enrich_engine import enrich_products  # meglévő enrich engine
from src.core.pricing_engine import PricingPlugin, PricingResult, apply_pricing


def _bootstrap() -> None:
    # .env betöltés projekt gyökérből (Windows-on is stabil)
    try:
        from dotenv import load_dotenv
        from pathlib import Path

        root = Path(__file__).resolve().parents[2]  # .../src/core/pipeline.py -> projekt root
        load_dotenv(root / ".env", override=True)
    except Exception as e:
        # ha nincs python-dotenv, nem állunk meg
        pass

    import src.suppliers  # noqa: F401


@dataclass(frozen=True)
class PipelineResult:
    master: List[Product]
    merged: List[Product]
    stats: Dict[str, Any]


def load_supplier_products(name: str) -> List[Product]:
    sup = get_supplier(name)
    raw_bytes = sup.ingest()
    rows = sup.parse(raw_bytes)
    return sup.normalize(rows)


def run_pipeline(
    *,
    master_supplier: str,
    enable_enrich: bool = True,
    enable_pricing: bool = True,
) -> PipelineResult:
    _bootstrap()

    master = load_supplier_products(master_supplier)

    # -------------------------
    # ENRICH (plugin alapon)
    # -------------------------
    enrich_plugins: list[Any] = []
    supplier_cache: Dict[str, List[Product]] = {}

    if enable_enrich:
        for sname in list_suppliers():
            plg = get_supplier(sname).enrich_plugin()
            if plg:
                enrich_plugins.append(plg)

        enrich_plugins = sorted(enrich_plugins, key=lambda p: int(getattr(p, "priority", 0)))

        if enrich_plugins:
            for plg in enrich_plugins:
                sname = getattr(plg, "name", "")
                if sname and sname not in supplier_cache:
                    supplier_cache[sname] = load_supplier_products(sname)

            enrich_res = enrich_products(
                master_products=master,
                supplier_data=supplier_cache,
                plugins=enrich_plugins,
            )
            merged = [dict(p) for p in enrich_res.products]
            enrich_stats = enrich_res.stats
        else:
            merged = [dict(p) for p in master]
            enrich_stats = {"enabled": True, "plugins": [], "enriched_any": 0}
    else:
        merged = [dict(p) for p in master]
        enrich_stats = {"enabled": False}

    # -------------------------
    # PRICING (plugin alapon)
    # -------------------------
    pricing_stats: Dict[str, Any] = {"enabled": False}
    if enable_pricing:
        pricing_plugins: list[PricingPlugin] = []
        for sname in list_suppliers():
            plg = get_supplier(sname).pricing_plugin()
            if plg:
                pricing_plugins.append(plg)

        pricing_plugins = sorted(pricing_plugins, key=lambda p: int(getattr(p, "priority", 0)))

        if pricing_plugins:
            indexes_by_plugin: Dict[str, Dict[str, Any]] = {}
            for plg in pricing_plugins:
                sname = plg.name
                if sname not in supplier_cache:
                    supplier_cache[sname] = load_supplier_products(sname)
                indexes_by_plugin[sname] = plg.build_indexes(supplier_cache[sname])

            pr: PricingResult = apply_pricing(
                master=master,
                merged=merged,
                plugins=pricing_plugins,
                indexes_by_plugin=indexes_by_plugin,
            )
            merged = pr.products
            pricing_stats = {"enabled": True, **pr.stats}
        else:
            pricing_stats = {"enabled": True, "plugins": []}

    return PipelineResult(
        master=master,
        merged=merged,
        stats={
            "master_supplier": master_supplier,
            "master_count": len(master),
            "enrich": enrich_stats,
            "pricing": pricing_stats,
        },
    )