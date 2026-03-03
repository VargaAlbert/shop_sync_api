from __future__ import annotations

"""
src/runner/preview_runner.py
===========================

🎯 Cél
------
Ez a preview runner Natura MASTER adatokból Shoprenter kompatibilis
request payloadokat generál debug/ellenőrzési célra.

A script NEM hív API-t, csak JSONL és CSV fájlokat ír.

Támogatott logikai rétegek:
  1) MASTER_CREATE   (POST /productExtend)
  2) MASTER_UPDATE   (PUT /productExtend/{id})
  3) ENRICH_UPDATE   (PUT /productExtend/{id})
     - description (Haldepó → Natura)
     - mainPicture (Haldepó kép)
     - imageAlt (SEO alt)
     - ❌ nem nyúl árhoz
     - ❌ nem nyúl státuszhoz
     - ❌ nem nyúl kategóriához
  4) DELETE_PREVIEW  (DELETE /productExtend/{id})
     - placeholder ID-val
  5) SKU_MAP_PREVIEW (GET /productExtend?page=...)
     - sku_map építéshez kapcsolódó request sablon
  + WHOLESALE CSV export (ár override plugin alapján)

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

TEST_PRODUCT_SKU=
    Opcionális default SKU szűrés.

------------------------------------------------------------
🚀 Futtatási példák
------------------------------------------------------------

1) Alap preview (csak konzol summary):
    python -m src.runner.preview_runner

2) Fájlba írással:
    python -m src.runner.preview_runner --write

3) Egy konkrét SKU tesztelése:
    python -m src.runner.preview_runner --sku 2954 --write

4) Haldepó ENRICH + wholesale override bekapcsolása:
    python -m src.runner.preview_runner --with-haldepo --write

5) Limitált elemszám:
    python -m src.runner.preview_runner --limit 50 --write

6) Csendes mód:
    python -m src.runner.preview_runner --write --quiet

7) SKU_MAP preview oldalszám megadása:
    python -m src.runner.preview_runner --write --sku-map-pages 3

8) DELETE preview ID override:
    python -m src.runner.preview_runner --write --delete-id 123456

------------------------------------------------------------
🧠 Logikai folyamat
------------------------------------------------------------

1️⃣ Natura = MASTER
    - ingest
    - normalize

2️⃣ Haldepó = ENRICHER (ha --with-haldepo)
    - match_key alapú merge
    - csak üres mezőket másol
    - _enriched_by flag kerül a rekordba

3️⃣ Payload generálás:
    - MASTER_CREATE
    - MASTER_UPDATE
    - ENRICH_UPDATE (ha enrich történt)
    - DELETE preview
    - SKU_MAP preview

4️⃣ Wholesale CSV:
    - Natura alap
    - Haldepó plugin felülírhat
    - nettó → bruttó
    - 5-re kerekítés felfelé

------------------------------------------------------------
🔒 Biztonsági megjegyzés
------------------------------------------------------------

Ez a runner NEM hív Shoprenter API-t.
Nem módosít éles adatot.
Csak preview requesteket generál.

Az éles szinkron a src/shoprenter/sync.py rétegben történik.

------------------------------------------------------------
📈 Skálázhatóság
------------------------------------------------------------

A wholesale override plugin alapú.
Új beszállító hozzáadásához:

  1) írj új plugin osztályt (merge/rules alatt)
  2) add hozzá a runnerben a plugin listához

Így a rendszer 5–8 beszállítóra is bővíthető
anélkül, hogy a runner szétágazna.

"""

import os
import argparse
from pathlib import Path
from typing import Dict, Any, List, Optional

from dotenv import load_dotenv

from src.ingest.suppliers_csv import ingest_one_supplier_csv
import src.normalize.suppliers  # registry bootstrap
from src.normalize import normalize_rows

from src.shoprenter.payloads_natura import build_payload
from src.shoprenter.payloads_enrich import build_enrich_update_payload

from src.utils.export_debug import (
    reset_file,
    append_jsonl,
    init_csv,
    append_csv_row,
)

from src.merge.merge_products import (
    build_master_keys,
    index_enricher_by_key,
    merge_master_with_enricher,
)

from src.merge.rules.haldepo_wholesale import HaldepoWholesalePlugin
from src.merge.rules.plugins import WholesalePlugin

load_dotenv()

LANGUAGE_ID = "bGFuZ3VhZ2UtbGFuZ3VhZ2UfaWQ9MQ=="
DEBUG_DIR = Path("data/debug/payload")
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

TEST_PRODUCT_SKU = (os.getenv("TEST_PRODUCT_SKU") or "").strip()


# -------------------------------------------------
# LOADERS
# -------------------------------------------------
def load_products(supplier: str, *, verbose: bool) -> List[Dict[str, Any]]:
    raw = ingest_one_supplier_csv(supplier)
    products = normalize_rows(supplier, raw)
    if verbose:
        print(f"{supplier.upper()}: raw={len(raw)} normalized={len(products)}")
    return products


def safe_base_url() -> str:
    return (os.getenv("SHOPRENTER_API_URL", "") or "").rstrip("/")


# -------------------------------------------------
# HELPERS: request builders (preview only)
# -------------------------------------------------
def build_delete_request(*, base_url: str, product_extend_id: str, sku: str) -> Dict[str, Any]:
    """
    Batch kompatibilis DELETE preview request.
    (Később: a valós delete run a sku_map-ből szedi az id-t.)
    """
    return {
        "sku": sku,
        "method": "DELETE",
        "uri": f"{base_url}/productExtend/{product_extend_id}" if base_url else f"/productExtend/{product_extend_id}",
        "data": None,
    }


def build_sku_map_preview_request(*, base_url: str, page: int = 1, limit: int = 200) -> Dict[str, Any]:
    """
    5. JSON: SKU_MAP építéshez kapcsolódó request preview.
    Mivel a konkrét sku_map endpoint nálad a Shoprenter kliens / lookups logikától függ,
    itt egy általános 'list products' sablont adunk.

    Ha nálad más endpoint kell, ezt a függvényt kell átírni 1 helyen.
    """
    # Példa query paramokkal:
    uri = f"{base_url}/productExtend?page={page}&limit={limit}" if base_url else f"/productExtend?page={page}&limit={limit}"
    return {
        "method": "GET",
        "uri": uri,
        "data": None,
        "meta": {"purpose": "sku_map_preview"}, #TOD: ezt majd élesben törölni kell
    }


# -------------------------------------------------
# MAIN
# -------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Shop Sync preview runner (payload + enrich + delete previews)")
    parser.add_argument("--write", action="store_true", help="Write debug output files")
    parser.add_argument("--sku", default=TEST_PRODUCT_SKU, help="Only process this SKU (or env TEST_PRODUCT_SKU)")
    parser.add_argument("--limit", type=int, default=0, help="Limit processed products (0 = no limit)")
    parser.add_argument("--with-haldepo", action="store_true", help="Enable Haldepó enrich merge + wholesale override")
    parser.add_argument("--quiet", action="store_true", help="Less console output")

    # delete previewhoz: id placeholder vagy konkrét id
    parser.add_argument("--delete-id", default="PRODUCT_EXTEND_ID_HERE", help="Delete preview ID placeholder")
    parser.add_argument("--sku-map-pages", type=int, default=1, help="How many SKU_MAP preview GET requests to write")

    args = parser.parse_args()
    verbose = not args.quiet

    base_url = safe_base_url()
    if not base_url and verbose:
        print("WARN: SHOPRENTER_API_URL nincs beállítva (.env). A 'uri' mezők relative formában készülnek.")

    # 1) MASTER: Natura
    natura_products = load_products("natura", verbose=verbose)

    # SKU filter
    sku_filter = (args.sku or "").strip()
    if sku_filter:
        natura_products = [p for p in natura_products if str(p.get("sku", "")).strip() == sku_filter]
        if not natura_products:
            raise RuntimeError(f"Nincs ilyen SKU Naturában: {sku_filter}")

    if args.limit and args.limit > 0:
        natura_products = natura_products[: args.limit]

    if verbose:
        print(f"PROCESSING: {len(natura_products)} product(s)")

    # 2) Haldepó enrich merge + plugin indexes
    merged_products = natura_products  # default: no enrich
    plugins: list[WholesalePlugin] = []
    plugin_indexes: dict[str, dict[str, Any]] = {}

    if args.with_haldepo:
        haldepo_rows = load_products("haldepo", verbose=verbose)

        # match_key alapú enrich merge a meglévő merge rétegeddel
        master_keys = build_master_keys(natura_products)
        haldepo_by_key = index_enricher_by_key(haldepo_rows, master_keys)
        merged_products = merge_master_with_enricher(natura_products, haldepo_by_key)

        # wholesale override plugin (csak ár CSV-hez)
        plugins.append(HaldepoWholesalePlugin())
        # NOTE: itt Haldepó rows megy be, később pluginonként a saját rows
        for plg in plugins:
            plugin_indexes[plg.name] = plg.build_indexes(haldepo_rows)

    # 3) Output fájlok
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

        # 5. JSON: sku_map preview GET-ek
        for page in range(1, max(1, args.sku_map_pages) + 1):
            append_jsonl(out_sku_map, build_sku_map_preview_request(base_url=base_url, page=page, limit=200))

    ok_create = ok_update = ok_enrich = ok_wholesale = ok_delete = 0
    err_count = 0

    # 4) Fő loop
    # Natura master + merged (enriched) párban
    for p_master, p_merged in zip(natura_products, merged_products):
        sku = str(p_master.get("sku", "")).strip()
        model = (p_master.get("model") or "").strip() or sku

        # 1) MASTER_CREATE
        try:
            payload = {
                "sku": sku,
                "method": "POST",
                "uri": f"{base_url}/productExtend" if base_url else "/productExtend",
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

        # 2) MASTER_UPDATE
        try:
            payload = {
                "sku": sku,
                "method": "PUT",
                "uri": f"{base_url}/productExtend/PRODUCT_EXTEND_ID_HERE" if base_url else "/productExtend/PRODUCT_EXTEND_ID_HERE",
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

        # 3) ENRICH_UPDATE (csak ha tényleg enrich-elt valaki)
        # A merge réteged beállítja: out["_enriched_by"] = "haldepo"

        try:
            enriched_by = (p_merged.get("_enriched_by") or "").strip()
            if enriched_by:
                payload = {
                    "sku": sku,
                    "method": "PUT",
                    "uri": f"{base_url}/productExtend/PRODUCT_EXTEND_ID_HERE" if base_url else "/productExtend/PRODUCT_EXTEND_ID_HERE",
                    "data": build_enrich_update_payload(p_merged, language_id=LANGUAGE_ID),
                    "meta": {"enriched_by": enriched_by}, #TOD: ezt majd élesben törölni kell
                }
                ok_enrich += 1
                if args.write:
                    append_jsonl(out_enrich_update, payload)
        except Exception as e:
            err_count += 1
            if args.write:
                append_jsonl(out_errors, {"sku": sku, "stage": "ENRICH_UPDATE", "error": str(e)})

        # 4) DELETE preview JSON (batch DELETE request sablon)
        # Preview-ban nincs sku_map, ezért placeholder ID-t használunk.
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

        # WHOLESALE CSV
        try:
            gp = p_master.get("gross_price")
            wp = p_master.get("wholesale_price")
            supplier = "natura"

            for plg in plugins:
                res = plg.apply(master=p_master, indexes=plugin_indexes.get(plg.name, {}))
                if res.supplier != "natura":
                    gp, wp, supplier = res.gross_price, res.wholesale_price, res.supplier

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
            print("Files saved to:", DEBUG_DIR)


if __name__ == "__main__":
    main()