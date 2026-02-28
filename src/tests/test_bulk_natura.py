import os
import time
import json
from pathlib import Path

from dotenv import load_dotenv

from src.ingest.suppliers_csv import ingest_one_supplier_csv
from src.normalize.natura import normalize_natura_rows
from src.shoprenter.payloads_natura import build_product_extend_from_natura
from src.shoprenter.client import ShoprenterClient
from src.shoprenter.lookups import build_product_sku_map
from src.shoprenter.sync import upsert_product

load_dotenv()

LANGUAGE_ID = "bGFuZ3VhZ2UtbGFuZ3VhZ2VfaWQ9MQ=="  # HU (nálad ez működik)


def run():
    # 1) API kliens
    client = ShoprenterClient(
        base_url=os.getenv("SHOPRENTER_API_URL"),
        user=os.getenv("SHOPRENTER_API_USER"),
        password=os.getenv("SHOPRENTER_API_PASS"),
    )

    # 2) Natura beolvasás + normalizálás
    raw = ingest_one_supplier_csv("natura")
    print("RAW:", len(raw))

    products = normalize_natura_rows(raw)
    print("NORMALIZED:", len(products))

    # 3) SKU map (Shoprenter oldali termékek)
    sku_map = build_product_sku_map(client, limit=200, sleep_s=0.2)
    print("SKU_MAP SIZE:", len(sku_map))

    # 4) Debug mappa
    debug_dir = Path("data") / "debug"
    failed_dir = debug_dir / "failed_payloads"
    debug_dir.mkdir(parents=True, exist_ok=True)
    failed_dir.mkdir(parents=True, exist_ok=True)

    # 5) Limit: első körben csak 50!
    max_items = 50
    sleep_s = 0.35  # 429 ellen (ha kell, emeld 0.5-re)

    created = updated = failed = 0

    for idx, p in enumerate(products[:max_items], start=1):
        sku = (p.get("sku") or "").strip()

        try:
            payload = build_product_extend_from_natura(
                p,
                language_id=LANGUAGE_ID,
                status_value=1,  # aktív!
                stock1=0,
            )

            action = upsert_product(client, sku_map, payload)
            if action == "created":
                created += 1
            else:
                updated += 1

            print(f"{idx}/{max_items} {sku} -> {action}")

        except Exception as e:
            failed += 1
            print(f"{idx}/{max_items} {sku} -> FAILED: {e}")

            # hibás payload mentés
            try:
                out = failed_dir / f"{sku or f'row_{idx}'}_payload.json"
                out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            except Exception:
                pass

        time.sleep(sleep_s)

    print("\nDONE")
    print({"created": created, "updated": updated, "failed": failed, "total": max_items})


if __name__ == "__main__":
    run()