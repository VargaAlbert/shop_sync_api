# tömeges feldolgozás logika
from __future__ import annotations
from typing import Iterable, Dict, Any, Callable, List, Tuple
import time
import json
import os
import requests
from requests.exceptions import HTTPError


def run_bulk_upsert(
    *,
    client,
    products: Iterable[Dict[str, Any]],
    build_payload: Callable[[Dict[str, Any]], Dict[str, Any]],
    sku_map: Dict[str, str],
    sleep_s: float = 0.3,
    max_items: int | None = None,
) -> dict:
    os.makedirs("data/debug", exist_ok=True)
    log_path = "data/debug/bulk_result.jsonl"

    created = updated = failed = 0
    i = 0

    with open(log_path, "a", encoding="utf-8") as log:
        for p in products:
            i += 1
            if max_items is not None and i > max_items:
                break

            sku = str(p.get("sku", "")).strip()

            try:
                payload = build_payload(p)
                action = None

                existing_id = sku_map.get(payload["sku"])
                if existing_id:
                    client.update_product(existing_id, payload)
                    action = "updated"
                    updated += 1
                else:
                    resp = client.create_product(payload)
                    sku_map[payload["sku"]] = resp.get("id")
                    action = "created"
                    created += 1

                print(f"{i} {sku} -> {action}")
                log.write(json.dumps({"sku": sku, "action": action}, ensure_ascii=False) + "\n")

            except HTTPError as e:
                failed += 1
                status = getattr(e.response, "status_code", None)
                body = getattr(e.response, "text", "")[:2000]

                print(f"{i} {sku} -> FAILED ({status})")

                # mentsük ki a payloadot hiba esetén
                try:
                    os.makedirs("data/debug/failed_payloads", exist_ok=True)
                    with open(f"data/debug/failed_payloads/{sku}.json", "w", encoding="utf-8") as f:
                        json.dump(payload, f, ensure_ascii=False, indent=2)
                except Exception:
                    pass

                log.write(json.dumps({"sku": sku, "action": "failed", "status": status, "body": body}, ensure_ascii=False) + "\n")

                # 429-nél várjunk többet
                if status == 429:
                    time.sleep(max(2.0, sleep_s * 5))
                else:
                    time.sleep(sleep_s)

            except Exception as e:
                failed += 1
                print(f"{i} {sku} -> FAILED (exception): {e}")
                log.write(json.dumps({"sku": sku, "action": "failed", "error": str(e)}, ensure_ascii=False) + "\n")
                time.sleep(sleep_s)

            time.sleep(sleep_s)

    return {"created": created, "updated": updated, "failed": failed, "total": i}

def chunked(lst: List[Any], n: int) -> Iterable[List[Any]]:
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def post_batch(session: requests.Session, base_url: str, reqs: List[Dict[str, Any]]) -> Dict[str, Any]:
    # Shoprenter batch endpoint
    payload = {"data": {"requests": reqs}}
    r = session.post(f"{base_url.rstrip('/')}/batch", json=payload)
    r.raise_for_status()
    return r.json()

def build_batch_requests_for_products(
    *,
    base_url: str,
    sku_map: Dict[str, str],
    payloads: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    reqs: List[Dict[str, Any]] = []

    for payload in payloads:
        sku = payload["sku"].strip()
        existing_id = sku_map.get(sku)

        if existing_id:
            reqs.append({
                "method": "PUT",
                "uri": f"{base_url.rstrip('/')}/productExtend/{existing_id}",
                "data": payload,
            })
        else:
            reqs.append({
                "method": "POST",
                "uri": f"{base_url.rstrip('/')}/productExtend",
                "data": payload,
            })

    return reqs

def parse_batch_results(batch_response: Dict[str, Any]) -> List[Tuple[int, str]]:
    """
    Visszaad: [(statusCode, uri), ...]
    """
    out: List[Tuple[int, str]] = []
    requests_block = batch_response.get("requests", {}).get("request", []) or []
    for item in requests_block:
        uri = item.get("uri", "")
        status = int(item.get("response", {}).get("header", {}).get("statusCode", 0) or 0)
        out.append((status, uri))
    return out