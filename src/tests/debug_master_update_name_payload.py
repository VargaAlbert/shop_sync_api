from __future__ import annotations

import json
import os
from pathlib import Path

from dotenv import load_dotenv

from src.core.pipeline import run_pipeline
from src.payloads.shoprenter import build_payload, DEFAULT_LANGUAGE_ID
from src.shoprenter.client import ShoprenterClient
from src.shoprenter.lookups import build_product_sku_map

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env", override=True)


def _create_client() -> ShoprenterClient:
    return ShoprenterClient(
        base_url=os.getenv("SHOPRENTER_API_URL"),
        user=os.getenv("SHOPRENTER_API_USER"),
        password=os.getenv("SHOPRENTER_API_PASS"),
    )


def main() -> None:
    master_supplier = (os.getenv("MASTER_SUPPLIER") or "natura").strip()
    target_sku = (os.getenv("TEST_PRODUCT_SKU") or "").strip()

    if not target_sku:
        raise SystemExit("Hiányzik a TEST_PRODUCT_SKU env változó.")

    client = _create_client()

    print(f"SKU map building... sku={target_sku}")
    sku_map = build_product_sku_map(
        client,
        limit=int(os.getenv("SKU_MAP_LIMIT", "200")),
    )

    pid = sku_map.get(target_sku)
    if not pid:
        raise SystemExit(f"A SKU nincs a Shoprenter sku_map-ben: {target_sku}")

    print(f"Pipeline running... master={master_supplier}")
    res = run_pipeline(
        master_supplier=master_supplier,
        enable_enrich=False,
        enable_pricing=False,
    )

    product = None
    for p in res.master:
        sku = str(p.get("sku") or "").strip()
        if sku == target_sku:
            product = dict(p)
            break

    if not product:
        raise SystemExit(f"A SKU nincs benne a master pipeline eredményben: {target_sku}")

    try:
        payload = build_payload(
            "MASTER_UPDATE",
            product,
            language_id=DEFAULT_LANGUAGE_ID,
            product_id=pid,
        )
    except TypeError as e:
        raise SystemExit(
            "A build_payload/build_master_update_payload még nem fogad product_id paramétert. "
            "Előbb a payload builder kódot kell javítani. "
            f"Eredeti hiba: {e}"
        )

    print("\nMASTER_UPDATE payload:\n")
    print(json.dumps(payload, ensure_ascii=False, indent=2))

    product_descriptions = payload.get("productDescriptions")
    if not isinstance(product_descriptions, list) or not product_descriptions:
        raise AssertionError("A payloadból hiányzik a productDescriptions lista.")

    first = product_descriptions[0]
    if not isinstance(first, dict):
        raise AssertionError("A productDescriptions első eleme nem dict.")

    product_ref = first.get("product")
    if not isinstance(product_ref, dict):
        raise AssertionError(
            "A productDescriptions[0].product hiányzik. "
            "Ez a legvalószínűbb oka annak, hogy a névfrissítés nem működik."
        )

    actual_pid = str(product_ref.get("id") or "").strip()
    if actual_pid != pid:
        raise AssertionError(
            f"Hibás product id a payloadban. Várt: {pid!r}, kapott: {actual_pid!r}"
        )

    actual_language_id = str(((first.get("language") or {}).get("id") or "")).strip()
    if actual_language_id != DEFAULT_LANGUAGE_ID:
        raise AssertionError(
            f"Hibás language id. Várt: {DEFAULT_LANGUAGE_ID!r}, kapott: {actual_language_id!r}"
        )

    actual_name = str(first.get("name") or "").strip()
    if not actual_name:
        raise AssertionError("A productDescriptions[0].name üres.")

    print("\nOK: a MASTER_UPDATE payload tartalmazza a productDescriptions[0].product.id mezőt.")
    print(f"OK: name={actual_name!r}")
    print(f"OK: product_id={actual_pid!r}")


if __name__ == "__main__":
    main()