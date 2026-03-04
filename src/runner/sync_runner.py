from __future__ import annotations

import argparse
import os
from typing import Any, Dict, List

from src.core.pipeline import run_pipeline
from src.payloads.shoprenter import (
    build_product_extend_from_product,
    build_update_payload_from_full,
)

# a te meglévő shoprenter sync belépőd:
from src.shoprenter.sync import sync_products  # <-- ha nálad máshogy hívják, ezt igazítsd


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default=os.getenv("MASTER_SUPPLIER", "natura"))
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-enrich", action="store_true")
    ap.add_argument("--no-pricing", action="store_true")
    ap.add_argument("--limit", type=int, default=0, help="0 = no limit")
    args = ap.parse_args()

    res = run_pipeline(
        master_supplier=args.master,
        enable_enrich=not args.no_enrich,
        enable_pricing=not args.no_pricing,
    )

    products = res.merged
    if args.limit and args.limit > 0:
        products = products[: args.limit]

    # 1) payload full (create) + update payload
    payload_full: List[Dict[str, Any]] = []
    payload_update: List[Dict[str, Any]] = []
    for p in products:
        full = build_product_extend_from_product(p)
        payload_full.append(full)
        payload_update.append(build_update_payload_from_full(full))

    if args.dry_run:
        print("[DRY-RUN] pipeline ok, payloads built.")
        print(f"products={len(products)}")
        print(res.stats)
        return

    # 2) Shoprenter sync (upsert/batch)
    # Itt a te meglévő logikád döntse el:
    # - mi megy create-ra
    # - mi megy update-re
    # - milyen endpointok vannak
    #
    # A lényeg: ide már CSAK payloadokat adunk be.
    sync_products(
        payload_full=payload_full,
        payload_update=payload_update,
        stats=res.stats,
    )

    print("[OK] sync completed.")


if __name__ == "__main__":
    main()