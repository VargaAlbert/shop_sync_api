"""
sync_natura_test.py
===================

Cél
---
Ez a teszt/preview script arra szolgál, hogy Natura termékadatokból
Shoprenter kompatibilis *payloadokat* generáljon és (opcionálisan) fájlba írja őket
debug / ellenőrzés céljából.

A script NEM szinkronizál élesben (nem hív API-t), csak payload preview-t készít:
- MASTER_CREATE: POST /productExtend (teljes create payload)
- MASTER_UPDATE: PUT /productExtend/{id} (update payload – itt placeholder ID-val)
- WHOLESALE CSV export: árfrissítéshez CSV kimenet (Natura alapból, Haldepó felülírhat)

Opcionálisan Haldepó (enricher) adatokkal is összefésüli a Natura rekordokat:
- match_key / master_keys alapján enrichment merge
- wholesale áraknál model alapján felülírhatja a gross/wholesale mezőket

Beállítások (.env)
------------------
A script a .env fájlból is tud dolgozni (python-dotenv).

Ajánlott változók:
- SHOPRENTER_API_URL
    Pl.: https://your-shoprenter-domain/api
    A payload "uri" mezőjéhez kell (csak preview).

- TEST_PRODUCT_SKU
    Ha be van állítva, akkor alapból erre az SKU-ra szűr.
    (Futtatásnál a --sku felülírja.)

Kimenetek (ha --write)
----------------------
A fájlok ide kerülnek:
    data/debug/payload/

Létrejövő fájlok:
- master_create_all.jsonl
    Natura -> Shoprenter POST /productExtend payloadok (JSONL formátumban)

- master_update_all.jsonl
    Natura -> Shoprenter PUT /productExtend/{id} payloadok (JSONL formátumban)
    Megjegyzés: a script "PRODUCT_EXTEND_ID_HERE" placeholder-t tesz a uri-ba.

- wholesale_price_update_all.csv
    ; szeparált CSV: sku;model;gross_price;wholesale_price;supplier
    supplier mező: "natura" vagy "haldepo" (ha Haldepó felülírta az árakat)

- errors.jsonl
    Bármely stage (MASTER_CREATE / MASTER_UPDATE / WHOLESALE_CSV_EXPORT) hibája ide kerül.

Futtatási példák
----------------
1) Alap preview (csak konzol summary, nem ír fájlt):
    python -m src.tests.sync_natura_test

2) Egy konkrét SKU tesztelése (nem ír fájlt):
    python -m src.tests.sync_natura_test --sku 12345

3) Írás fájlba (JSONL + CSV + hibák):
    python -m src.tests.sync_natura_test --write

4) Haldepó enrichment bekapcsolása is:
    python -m src.tests.sync_natura_test --write --with-haldepo

5) Limitált elemszám (pl. első 50 termék):
    python -m src.tests.sync_natura_test --write --limit 50

6) Csendes mód (kevesebb print):
    python -m src.tests.sync_natura_test --write --quiet

Megjegyzések
------------
- A script feltételezi, hogy az ingest/normalize réteg működik:
  - ingest_one_supplier_csv("natura"/"haldepo")
  - normalize_rows("natura"/"haldepo", raw)
- A Haldepó wholesale felülírás model alapján történik:
  - model = p_master["model"] vagy ha nincs, akkor sku
  - haldepo index: modelNumber vagy model mezőből

Jövőbeli bővítés
----------------
- ENRICH_UPDATE: ha lesz külön enrich update payload / endpoint, ide lehet betenni
  és az out_enrich_update_all.jsonl fájl is használható lesz.
"""
from __future__ import annotations

import os
import json
import csv
import argparse
from pathlib import Path
from typing import Dict, Any, List

from dotenv import load_dotenv

from src.ingest.suppliers_csv import ingest_one_supplier_csv
import src.normalize.suppliers  # registry bootstrap
from src.normalize import normalize_rows
from src.utils.numbers import net_to_gross_rounded_5
from src.shoprenter.payloads_natura import build_payload

from src.merge.merge_products import (
    build_master_keys,
    index_enricher_by_key,
    merge_master_with_enricher,
)

load_dotenv()

LANGUAGE_ID = "bGFuZ3VhZ2UtbGFuZ3VhZ2UfaWQ9MQ=="
DEBUG_DIR = Path("data/debug/payload")
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

TEST_PRODUCT_SKU = (os.getenv("TEST_PRODUCT_SKU") or "").strip()


# -------------------------------------------------
# LOADERS
# -------------------------------------------------
def load_natura_products(*, verbose: bool = True) -> List[Dict[str, Any]]:
    raw = ingest_one_supplier_csv("natura")
    products = normalize_rows("natura", raw)
    if verbose:
        print(f"NATURA: raw={len(raw)} normalized={len(products)}")
    return products


def load_haldepo_rows(*, verbose: bool = True) -> List[Dict[str, Any]]:
    raw = ingest_one_supplier_csv("haldepo")
    rows = normalize_rows("haldepo", raw)
    if verbose:
        print(f"HALDEPO: raw={len(raw)} normalized={len(rows)}")
    return rows


# -------------------------------------------------
# JSONL helpers
# -------------------------------------------------
def append_jsonl(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False, indent=2))
        f.write("\n\n")


def reset_file(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("", encoding="utf-8")


# -------------------------------------------------
# CSV helpers
# -------------------------------------------------
def init_csv(path: Path, headers: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(headers)


def append_csv_row(path: Path, row: List[Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(row)


# -------------------------------------------------
# Index helpers
# -------------------------------------------------
def build_haldepo_index_by_model(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        model = (r.get("modelNumber") or r.get("model") or "").strip()
        if model:
            out[model] = r
    return out


# -------------------------------------------------
# MAIN
# -------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Natura FULL payload preview")
    parser.add_argument("--write", action="store_true", help="Write debug output files")
    parser.add_argument("--sku", default=TEST_PRODUCT_SKU, help="Only process this SKU (or env TEST_PRODUCT_SKU)")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of processed products (0 = no limit)")
    parser.add_argument("--with-haldepo", action="store_true", help="Merge Haldepó rows for enrichment/wholesale price")
    parser.add_argument("--quiet", action="store_true", help="Less console output")
    args = parser.parse_args()

    verbose = not args.quiet

    natura_products = load_natura_products(verbose=verbose)
    base = (os.getenv("SHOPRENTER_API_URL", "") or "").rstrip("/")

    # SKU szűrés (ha meg van adva)
    sku_filter = (args.sku or "").strip()
    if sku_filter:
        natura_products = [p for p in natura_products if str(p.get("sku", "")).strip() == sku_filter]
        if not natura_products:
            raise RuntimeError(f"Nincs ilyen SKU Naturában: {sku_filter}")

    if args.limit and args.limit > 0:
        natura_products = natura_products[: args.limit]

    if verbose:
        print(f"PROCESSING: {len(natura_products)} product(s)")

    # ---------------------------------------------
    # Haldepó előkészítés
    # ---------------------------------------------
    p_enrich_list = natura_products
    haldepo_by_model: Dict[str, Dict[str, Any]] = {}

    if args.with_haldepo:
        haldepo_rows = load_haldepo_rows(verbose=verbose)
        haldepo_by_model = build_haldepo_index_by_model(haldepo_rows)

        # enrich merge (match_key alapú index)
        master_keys = build_master_keys(natura_products)
        haldepo_by_key = index_enricher_by_key(haldepo_rows, master_keys)
        p_enrich_list = merge_master_with_enricher(natura_products, haldepo_by_key)

    # ---------------------------------------------
    # Output fájlok
    # ---------------------------------------------
    out_master_create = DEBUG_DIR / "master_create_all.jsonl"
    out_master_update = DEBUG_DIR / "master_update_all.jsonl"
    out_enrich_update = DEBUG_DIR / "enrich_update_all.jsonl"  # (jelenleg nem írunk bele, csak előkészítve)
    out_wholesale = DEBUG_DIR / "wholesale_price_update_all.csv"
    out_errors = DEBUG_DIR / "errors.jsonl"

    if args.write:
        reset_file(out_master_create)
        reset_file(out_master_update)
        reset_file(out_enrich_update)
        reset_file(out_errors)
        init_csv(out_wholesale, ["sku", "model", "gross_price", "wholesale_price"])

    ok_create = ok_update = ok_enrich = ok_wholesale = 0
    err_count = 0

    # ---------------------------------------------
    # FŐ LOOP
    # ---------------------------------------------
    for p_master, p_enrich in zip(natura_products, p_enrich_list):
        sku = str(p_master.get("sku", "")).strip()
        model = (p_master.get("model") or "").strip() or sku

        # MASTER CREATE
        try:
            payload = {
                "sku": sku,
                "method": "POST",
                "uri": f"{base}/productExtend",
                "data": build_payload(
                    "MASTER_CREATE",
                    p_master,
                    language_id=LANGUAGE_ID,
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

        # MASTER UPDATE
        try:
            payload = {
                "sku": sku,
                "method": "PUT",
                "uri": f"{base}/productExtend/PRODUCT_EXTEND_ID_HERE",
                "data": build_payload(
                    "MASTER_UPDATE",
                    p_master,
                    language_id=LANGUAGE_ID,
                ),
            }
            ok_update += 1
            if args.write:
                append_jsonl(out_master_update, payload)
        except Exception as e:
            err_count += 1
            if args.write:
                append_jsonl(out_errors, {"sku": sku, "stage": "MASTER_UPDATE", "error": str(e)})

        # ENRICH UPDATE (opcionális: ha később lesz külön endpoint / payload)
        # Itt most csak a számláló marad, hogy a summary ne legyen félrevezető.
        # Ha később megírod, ide jöhet.
        # ok_enrich += 1

        # WHOLESALE (CSV export MySQL frissítéshez)
        try:
            source_for_wholesale = dict(p_master)
            price_source = "natura"

            if args.with_haldepo:
                h = haldepo_by_model.get(model)
                if h:
                    price_source = "haldepo"
                    if h.get("gross_price") is not None:
                        source_for_wholesale["gross_price"] = h["gross_price"]
                    if h.get("wholesale_price") is not None:
                        source_for_wholesale["wholesale_price"] = h["wholesale_price"]

            gp = source_for_wholesale.get("gross_price")
            wp = source_for_wholesale.get("wholesale_price")

            if price_source == "haldepo":
                if wp is not None:
                    wp = net_to_gross_rounded_5(wp)

            ok_wholesale += 1
            if args.write:
                append_csv_row(
                    out_wholesale,
                    [
                        sku,
                        model,
                        "" if gp is None else f"{float(gp):.4f}",
                        "" if wp is None else f"{float(wp):.4f}",
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
            f"WHOLESALE ok={ok_wholesale} | "
            f"errors={err_count}"
        )
        if args.write:
            print("Files saved to:", DEBUG_DIR)


if __name__ == "__main__":
    main()