from __future__ import annotations
from typing import Dict, Any
import time

def upsert_product(client, sku_map: dict[str, str], payload: Dict[str, Any]) -> str:
    sku = payload["sku"].strip()

    existing_id = sku_map.get(sku)

    if existing_id:
        client.update_product(existing_id, payload)
        return "updated"

    created = client.create_product(payload)

    new_id = created.get("id")
    if new_id:
        sku_map[sku] = new_id

    return "created"

def upsert_product_with_map(client, sku_map: dict[str, str], payload: dict) -> str:
    sku = payload["sku"]
    existing_id = sku_map.get(sku)

    if existing_id:
        client.update_product(existing_id, payload)
        return "updated"

    created = client.create_product(payload)
    # frissítjük a mapet, hogy ugyanabban a futásban se legyen duplikáció
    sku_map[sku] = created.get("id")
    return "created"