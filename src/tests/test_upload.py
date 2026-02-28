import os
import json
import time
from dotenv import load_dotenv

from src.shoprenter.client import ShoprenterClient
from src.shoprenter.payloads_natura import build_product_extend_from_natura
from src.shoprenter.lookups import build_product_sku_map
from src.shoprenter.sync import upsert_product

load_dotenv()

LANGUAGE_ID = "bGFuZ3VhZ2UtbGFuZ3VhZ2VfaWQ9MQ=="  # nálad HU

def run():
    client = ShoprenterClient(
        base_url=os.getenv("SHOPRENTER_API_URL"),
        user=os.getenv("SHOPRENTER_API_USER"),
        password=os.getenv("SHOPRENTER_API_PASS"),
    )

    # 1) SKU map betöltés (ez a kulcs!)
    sku_map = build_product_sku_map(client, limit=200, sleep_s=0.2)
    print("SKU_MAP SIZE:", len(sku_map))

    products = [
        {
            "sku": "TEST-101",
            "model": "TEST-101",
            "gtin": "",
            "name_hu": "Teszt termék 101 (upsert)",
            "gross_price": 1000,
            "csoport1_name": "TESZT",
        },
        {
            "sku": "TEST-102",
            "model": "TEST-102",
            "gtin": "",
            "name_hu": "Teszt termék 102 (upsert)",
            "gross_price": 1200,
            "csoport1_name": "TESZT",
        },
    ]

    for i, p in enumerate(products, start=1):
        payload = build_product_extend_from_natura(
            p,
            language_id=LANGUAGE_ID,
            status_value=1,   # aktív!
            stock1=0,
        )

        # debug ha kell
        print(json.dumps(payload, ensure_ascii=False, indent=2))

        action = upsert_product(client, sku_map, payload)
        print(f"{i}/{len(products)} {p['sku']} -> {action}")

        time.sleep(0.3)  # 429 ellen

if __name__ == "__main__":
    run()