# src/tests/sync_natura_test.py
"""
Natura CSV -> Shoprenter payload "preview" (NEM SKU alapon)

Cél:
- Valódi Natura CSV-ből beolvas + normalizál
- Mindhárom request-szerkezetből ad 1 példát:
  - CREATE: full payload
  - UPDATE: build_update_payload_from_full(full_payload)
  - DELETE: szerkezet példa (placeholder id-vel)

Mentés:
- data/debug/payload/create_example.json
- data/debug/payload/update_example.json
- data/debug/payload/delete_example.json

Futtatás:
  python -m src.tests.sync_natura_test
"""

import os
import json
import argparse
from pathlib import Path
from typing import Dict, Any, List

from dotenv import load_dotenv

from src.ingest.suppliers_csv import ingest_one_supplier_csv
import src.normalize.suppliers  # regisztrál
from src.normalize import normalize_rows

from src.shoprenter.payloads_natura import (
    build_product_extend_from_natura,
    build_update_payload_from_full,
)

load_dotenv()

LANGUAGE_ID = "bGFuZ3VhZ2UtbGFuZ3VhZ2VfaWQ9MQ=="  # HU

DEBUG_DIR = Path("data/debug/payload")
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

TEST_PRODUCT_SKU = "1234"   # pl. "DOV7165" vagy amit akarsz

def load_natura_products() -> List[Dict[str, Any]]:
    raw = ingest_one_supplier_csv("natura")
    products = normalize_rows("natura", raw)
    print("RAW:", len(raw))
    print("NORMALIZED:", len(products))
    return products


def save_json(filename: str, data: Dict[str, Any]) -> None:
    path = DEBUG_DIR / filename
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Saved -> {path}")


def pick_test_product(products: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Ha TEST_PRODUCT_SKU meg van adva:
        - azt a SKU-t keresi
    Ha nincs:
        - első SKU-s terméket veszi
    """

    # 1️⃣ Ha konkrét SKU be van állítva
    if TEST_PRODUCT_SKU:
        for p in products:
            if str(p.get("sku", "")).strip() == str(TEST_PRODUCT_SKU).strip():
                print(f"Using TEST_PRODUCT_SKU = {TEST_PRODUCT_SKU}")
                return p

        raise RuntimeError(f"Nem találtam ilyen SKU-t a CSV-ben: {TEST_PRODUCT_SKU}")

    # 2️⃣ Fallback: első SKU-s
    for p in products:
        if str(p.get("sku", "")).strip():
            print("Using first available SKU from CSV.")
            return p

    raise RuntimeError("Nem találtam SKU-val rendelkező terméket.")

def main():
    parser = argparse.ArgumentParser(description="Natura payload preview")
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()

    # ✅ EZ HIÁNYZOTT
    products = load_natura_products()

    # ✅ Most már létezik a products
    p_create = pick_test_product(products)

    full_create_payload = build_product_extend_from_natura(
        p_create,
        language_id=LANGUAGE_ID
    )
    full_create_payload.pop("_debug", None)

    update_payload = build_update_payload_from_full(full_create_payload)

    sku = str(full_create_payload.get("sku", "")).strip()

    create_example = {
        "method": "POST",
        "uri": f"{os.getenv('SHOPRENTER_API_URL', '').rstrip('/')}/productExtend",
        "data": full_create_payload,
    }

    update_example = {
        "method": "PUT",
        "uri": f"{os.getenv('SHOPRENTER_API_URL', '').rstrip('/')}/productExtend/PRODUCT_EXTEND_ID_HERE",
        "data": update_payload,
    }

    delete_example = {
        "method": "DELETE",
        "uri": f"{os.getenv('SHOPRENTER_API_URL', '').rstrip('/')}/productExtend/PRODUCT_EXTEND_ID_HERE",
        "data": {},
    }

    if args.write:
        save_json("create_example.json", create_example)
        save_json("update_example.json", update_example)
        save_json("delete_example.json", delete_example)

    print("Preview kész.")
    
if __name__ == "__main__":
    main()