"""
src/runner/preview_runner.py
===========================

🎯 Cél
------
Supplier-centrikus (új) pipeline alapján Shoprenter kompatibilis
request preview payloadokat generál debug/ellenőrzési célra.

A script NEM hív Shoprenter API-t, csak JSONL és CSV fájlokat ír.

Támogatott logikai rétegek (preview):
  1) MASTER_CREATE   (POST /productExtend)        -> merged termékből (enrich + pricing után)
  2) MASTER_UPDATE   (PUT  /productExtend/{id})   -> master termékből
  3) ENRICH_UPDATE   (PUT  /productExtend/{id})   -> csak ha _enriched_by van
  4) DELETE_PREVIEW  (DELETE /productExtend/{id}) -> placeholder ID-val
  5) SKU_MAP_PREVIEW (GET /productExtend?page=...) -> sku_map építéshez sablon
  + WHOLESALE CSV export (pricing plugin után, merged termékből)

------------------------------------------------------------
📦 Kimenetek (--write esetén)
------------------------------------------------------------

data/debug/payload/

  master_create_all.jsonl
  master_update_all.jsonl
  enrich_update_all.jsonl
  delete_all.jsonl
  sku_map_preview_all.jsonl
  wholesale_price_update_all.csv
  errors.jsonl

------------------------------------------------------------
⚙️ .env változók
------------------------------------------------------------

SHOPRENTER_API_URL=
    Pl.: https://your-shop.api.myshoprenter.hu

MASTER_SUPPLIER=
    Pl.: natura (default)

TEST_PRODUCT_SKU=
    Opcionális default SKU szűrés.

HALDEPO_USER / HALDEPO_PASS
    Haldepó ingesthez (ha pluginek miatt betöltődik)

------------------------------------------------------------
🚀 Futtatási példák
------------------------------------------------------------

1) Alap preview (csak konzol summary):
    python -m src.runner.preview_runner

2) Fájlba írással:
    python -m src.runner.preview_runner --write

3) Egy konkrét SKU tesztelése:
    python -m src.runner.preview_runner --sku 2954 --write

4) Limitált elemszám:
    python -m src.runner.preview_runner --limit 50 --write

5) Csendes mód:
    python -m src.runner.preview_runner --write --quiet

6) SKU_MAP preview oldalszám:
    python -m src.runner.preview_runner --write --sku-map-pages 3

7) DELETE preview ID override:
    python -m src.runner.preview_runner --write --delete-id 123456
"""

from __future__ import annotations

import os
import argparse
from pathlib import Path
from typing import Dict, Any, List

from dotenv import load_dotenv

from src.core.pipeline import run_pipeline
from src.payloads.shoprenter import build_payload, DEFAULT_LANGUAGE_ID

from src.utils.export_debug import (
    reset_file,
    append_jsonl,
    init_csv,
    append_csv_row,
)

load_dotenv()

DEBUG_DIR = Path("data/debug/payload")
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

TEST_PRODUCT_SKU = (os.getenv("TEST_PRODUCT_SKU") or "").strip()


def safe_base_url() -> str:
    return (os.getenv("SHOPRENTER_API_URL", "") or "").rstrip("/")


def build_delete_request(*, base_url: str, product_extend_id: str, sku: str) -> Dict[str, Any]:
    return {
        "sku": sku,
        "method": "DELETE",
        "uri": f"{base_url}/productExtend/{product_extend_id}" if base_url else f"/productExtend/{product_extend_id}",
        "data": None,
    }


def build_sku_map_preview_request(*, base_url: str, page: int = 1, limit: int = 200) -> Dict[str, Any]:
    uri = f"{base_url}/productExtend?page={page}&limit={limit}" if base_url else f"/productExtend?page={page}&limit={limit}"
    return {
        "method": "GET",
        "uri": uri,
        "data": None,
        "meta": {"purpose": "sku_map_preview"},
    }


def _filter_by_sku(products: List[Dict[str, Any]], sku: str) -> List[Dict[str, Any]]:
    sku = (sku or "").strip()
    if not sku:
        return products
    return [p for p in products if str(p.get("sku", "")).strip() == sku]


def main() -> None:
    parser = argparse.ArgumentParser(description="Shop Sync preview runner (supplier-centric pipeline)")

    parser.add_argument("--write", action="store_true", help="Write debug output files")
    parser.add_argument("--sku", default=TEST_PRODUCT_SKU, help="Only process this SKU (or env TEST_PRODUCT_SKU)")
    parser.add_argument("--limit", type=int, default=0, help="Limit processed products (0 = no limit)")
    parser.add_argument("--quiet", action="store_true", help="Less console output")

    parser.add_argument("--master", default=os.getenv("MASTER_SUPPLIER", "natura"), help="Master supplier name")

    # delete previewhoz: id placeholder vagy konkrét id
    parser.add_argument("--delete-id", default="PRODUCT_EXTEND_ID_HERE", help="Delete preview ID placeholder")

    # sku_map preview GET requestek száma
    parser.add_argument("--sku-map-pages", type=int, default=1, help="How many SKU_MAP preview GET requests to write")

    # pipeline flags
    parser.add_argument("--no-enrich", action="store_true", help="Disable enrich phase")
    parser.add_argument("--no-pricing", action="store_true", help="Disable pricing phase")

    args = parser.parse_args()
    verbose = not args.quiet

    base_url = safe_base_url()
    if not base_url and verbose:
        print("WARN: SHOPRENTER_API_URL nincs beállítva (.env). A 'uri' mezők relative formában készülnek.")

    # -------------------------------------------------
    # 1) Pipeline (master -> enrich -> pricing)
    # -------------------------------------------------
    res = run_pipeline(
        master_supplier=args.master,
        enable_enrich=not args.no_enrich,
        enable_pricing=not args.no_pricing,
    )

    master_products = list(res.master)
    merged_products = list(res.merged)

    # SKU filter (MASTER SKU alapján)
    sku_filter = (args.sku or "").strip()
    if sku_filter:
        master_products = _filter_by_sku(master_products, sku_filter)
        merged_products = _filter_by_sku(merged_products, sku_filter)
        if not master_products:
            raise RuntimeError(f"Nincs ilyen SKU a masterben ({args.master}): {sku_filter}")

    # Limit (a sorrend maradjon összepárosítható)
    if args.limit and args.limit > 0:
        master_products = master_products[: args.limit]
        merged_products = merged_products[: args.limit]

    if verbose:
        print(f"MASTER={args.master} master_count={len(res.master)} merged_count={len(res.merged)}")
        print(f"PROCESSING: {len(master_products)} product(s)")
        if isinstance(res.stats, dict):
            print("PIPELINE stats:", res.stats)

    # -------------------------------------------------
    # 2) Output fájlok
    # -------------------------------------------------
    out_master_create = DEBUG_DIR / "master_create_all.jsonl"
    out_master_update = DEBUG_DIR / "master_update_all.jsonl"
    out_enrich_update = DEBUG_DIR / "enrich_update_all.jsonl"
    out_delete = DEBUG_DIR / "delete_all.jsonl"
    out_sku_map = DEBUG_DIR / "sku_map_preview_all.jsonl"
    out_wholesale = DEBUG_DIR / "wholesale_price_update_all.csv"
    out_errors = DEBUG_DIR / "errors.jsonl"

    if args.write:
        reset_file(out_master_create)
        reset_file(out_master_update)
        reset_file(out_enrich_update)
        reset_file(out_delete)
        reset_file(out_sku_map)
        reset_file(out_errors)

        init_csv(out_wholesale, ["sku", "model", "gross_price", "wholesale_price", "supplier"])

        for page in range(1, max(1, args.sku_map_pages) + 1):
            append_jsonl(out_sku_map, build_sku_map_preview_request(base_url=base_url, page=page, limit=200))

    ok_create = ok_update = ok_enrich = ok_wholesale = ok_delete = 0
    err_count = 0

    # -------------------------------------------------
    # 3) FŐ LOOP (MASTER + ENRICH_UPDATE + DELETE + WHOLESALE CSV)
    # -------------------------------------------------
    for p_master, p_merged in zip(master_products, merged_products):
        sku = str(p_master.get("sku", "")).strip()
        model = (p_master.get("model") or "").strip() or sku

        enriched_any = bool((p_merged.get("_enriched_by") or "").strip())

        # MASTER CREATE: merged (enrich+pricing után)
        try:
            payload = {
                "sku": sku,
                "method": "POST",
                "uri": f"{base_url}/productExtend" if base_url else "/productExtend",
                "data": build_payload(
                    "MASTER_CREATE",
                    p_merged,
                    language_id=DEFAULT_LANGUAGE_ID,
                    status_value=0,
                    stock1=0,
                ),
            }
            ok_create += 1
            if args.write:
                append_jsonl(out_master_create, payload)
        except Exception as e:
            err_count += 1
            if args.write:
                append_jsonl(out_errors, {"sku": sku, "stage": "MASTER_CREATE", "error": str(e)})

        # MASTER UPDATE: masterből (policy: “csak master update”)
        try:
            payload = {
                "sku": sku,
                "method": "PUT",
                "uri": f"{base_url}/productExtend/PRODUCT_EXTEND_ID_HERE" if base_url else "/productExtend/PRODUCT_EXTEND_ID_HERE",
                "data": build_payload(
                    "MASTER_UPDATE",
                    p_master,
                    language_id=DEFAULT_LANGUAGE_ID,
                ),
            }
            ok_update += 1
            if args.write:
                append_jsonl(out_master_update, payload)
        except Exception as e:
            err_count += 1
            if args.write:
                append_jsonl(out_errors, {"sku": sku, "stage": "MASTER_UPDATE", "error": str(e)})

        # ENRICH UPDATE: csak ha tényleg történt enrich
        try:
            if enriched_any:
                payload = {
                    "sku": sku,
                    "method": "PUT",
                    "uri": f"{base_url}/productExtend/PRODUCT_EXTEND_ID_HERE" if base_url else "/productExtend/PRODUCT_EXTEND_ID_HERE",
                    "data": build_payload("ENRICH_UPDATE", p_merged, language_id=DEFAULT_LANGUAGE_ID),
                }
                ok_enrich += 1
                if args.write:
                    append_jsonl(out_enrich_update, payload)
        except Exception as e:
            err_count += 1
            if args.write:
                append_jsonl(out_errors, {"sku": sku, "stage": "ENRICH_UPDATE", "error": str(e)})

        # DELETE preview JSON
        try:
            del_req = build_delete_request(
                base_url=base_url,
                product_extend_id=str(args.delete_id),
                sku=sku,
            )
            ok_delete += 1
            if args.write:
                append_jsonl(out_delete, del_req)
        except Exception as e:
            err_count += 1
            if args.write:
                append_jsonl(out_errors, {"sku": sku, "stage": "DELETE_PREVIEW", "error": str(e)})

        # WHOLESALE CSV: merged (pricing plugin után)
        try:
            gp = p_merged.get("gross_price")
            wp = p_merged.get("wholesale_price")

            # ki írta felül? pricing plugin teheti bele _priced_by-t
            supplier = (p_merged.get("_priced_by") or "").strip() or (p_merged.get("supplier") or "natura")

            ok_wholesale += 1
            if args.write:
                append_csv_row(
                    out_wholesale,
                    [
                        sku,
                        model,
                        "" if gp is None else f"{float(gp):.4f}",
                        "" if wp is None else f"{float(wp):.4f}",
                        supplier,
                    ],
                )
        except Exception as e:
            err_count += 1
            if args.write:
                append_jsonl(out_errors, {"sku": sku, "stage": "WHOLESALE_CSV_EXPORT", "error": str(e)})

    if verbose:
        print("ALL preview kész.")
        print(
            f"MASTER_CREATE ok={ok_create} | "
            f"MASTER_UPDATE ok={ok_update} | "
            f"ENRICH_UPDATE ok={ok_enrich} | "
            f"DELETE_PREVIEW ok={ok_delete} | "
            f"WHOLESALE ok={ok_wholesale} | "
            f"errors={err_count}"
        )
        if args.write:
            print("Files saved to:", DEBUG_DIR.resolve())


if __name__ == "__main__":
    main()