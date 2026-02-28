# src/tests/run_natura__sync.py
"""
Natura -> Shoprenter szinkron (batch) egy fájlban, két külön futtatható fázissal.

Cél:
- UPsert (CREATE + UPDATE): gyakran futhat (pl. 4 óránként)
- DELETE: ritkán fusson (pl. napi 1× hajnalban), mert kockázatosabb

Működés:
1) UPsert fázis
   - beolvassa a Natura CSV-t (ingest)
   - normalizálja (normalize)
   - felépíti a SKU -> Shoprenter productExtend id map-et (sku_map)
   - minden SKU-ra:
       - ha van id a shopban -> PUT /productExtend/{id} (csak update mezők)
       - ha nincs id -> POST /productExtend (full payload)
   - batch endpointot használ: POST /batch

2) DELETE fázis
   - beolvassa + normalizálja a Natura CSV-t (csak SKU-khoz)
   - sku_map-ből (shop_skus) és csv_skus-ból képzi a különbséget:
       to_delete = shop_skus - csv_skus
   - DELETE /productExtend/{id} batch-ben
   - Safety: ha túl sok törlés lenne -> STOP (MAX_DELETE)

Futtatás:
  python -m src.tests.run_natura__sync --mode upsert
  python -m src.tests.run_natura__sync --mode delete
  python -m src.tests.run_natura__sync --mode all

Javasolt ütemezés:
- upsert: 4 óránként
- delete: napi 1× (hajnal), és maradjon MAX_DELETE limit

Megjegyzés:
- A kód a meglévő moduljaidra támaszkodik:
  - ingest_one_supplier_csv("natura")
  - normalize_natura_rows(raw)
  - build_product_extend_from_natura(p, ...)
  - build_update_payload_from_full(full_payload)
  - ShoprenterClient
  - build_product_sku_map(client, ...)
"""

import os
import time
import json
import argparse
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

from dotenv import load_dotenv

from src.ingest.suppliers_csv import ingest_one_supplier_csv

import src.normalize.suppliers  # fontos: regisztrálja a normalizálókat
from src.normalize import normalize_rows

from src.shoprenter.payloads_natura import (
    build_product_extend_from_natura,
    build_update_payload_from_full,
)
from src.shoprenter.client import ShoprenterClient
from src.shoprenter.lookups import build_product_sku_map


# -----------------------------
# Config
# -----------------------------
load_dotenv()

LANGUAGE_ID = "bGFuZ3VhZ2UtbGFuZ3VhZ2VfaWQ9MQ=="  # HU

BATCH_SIZE = 150
BATCH_SLEEP_S = 0.3

# SKU map lekérés limitje (nálad majd 20k-nál ezt emeld)
SKU_MAP_LIMIT = 5000
SKU_MAP_SLEEP_S = 0

# DELETE safety
MAX_DELETE = 200


# -----------------------------
# Utils
# -----------------------------
def fmt_seconds(sec: float) -> str:
    return str(timedelta(seconds=int(max(0, sec))))


def chunked(items: List[Any], size: int) -> List[List[Any]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def post_batch(client: ShoprenterClient, reqs: List[Dict[str, Any]]) -> Dict[str, Any]:
    r = client.session.post(
        f"{client.base_url.rstrip('/')}/batch",
        json={"data": {"requests": reqs}},
        timeout=getattr(client, "timeout", 60),
    )
    r.raise_for_status()
    return r.json()


def parse_batch_response(resp: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Robusztus parser többféle Shoprenter batch response struktúrára.
    Kimenet: list of {method, uri, statusCode, body}
    """
    candidates = None

    if isinstance(resp.get("requests"), dict) and isinstance(resp["requests"].get("request"), list):
        candidates = resp["requests"]["request"]

    if candidates is None and isinstance(resp.get("data"), dict) and isinstance(resp["data"].get("requests"), list):
        candidates = resp["data"]["requests"]

    if candidates is None and isinstance(resp.get("requests"), list):
        candidates = resp["requests"]

    if candidates is None:
        return []

    out: List[Dict[str, Any]] = []
    for item in candidates:
        method = item.get("method") or item.get("request", {}).get("method")
        uri = item.get("uri") or item.get("request", {}).get("uri")

        status = (
            item.get("statusCode")
            or item.get("response", {}).get("statusCode")
            or item.get("response", {}).get("header", {}).get("statusCode")
            or item.get("response", {}).get("headers", {}).get("statusCode")
        )

        body = item.get("body") or item.get("response", {}).get("body") or item.get("response", {}).get("data")

        try:
            status_int = int(status) if status is not None else 0
        except Exception:
            status_int = 0

        out.append(
            {
                "method": method,
                "uri": uri,
                "statusCode": status_int,
                "body": body,
            }
        )

    return out


def build_batch_requests_for_pack(
    *,
    base_url: str,
    sku_map: Dict[str, str],
    payloads: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    UPsert request pack:
    - ha SKU létezik (sku_map): PUT /productExtend/{id} update payload (szűrt)
    - ha nem létezik: POST /productExtend full payload
    """
    reqs: List[Dict[str, Any]] = []
    base = base_url.rstrip("/")

    for payload in payloads:
        sku = str(payload.get("sku", "")).strip()
        existing_id = sku_map.get(sku)

        if existing_id:
            update_data = build_update_payload_from_full(payload)
            reqs.append(
                {
                    "method": "PUT",
                    "uri": f"{base}/productExtend/{existing_id}",
                    "data": update_data,
                }
            )
        else:
            reqs.append(
                {
                    "method": "POST",
                    "uri": f"{base}/productExtend",
                    "data": payload,  # full create payload
                }
            )

    return reqs


def build_delete_requests(*, base_url: str, sku_map: Dict[str, str], skus: List[str]) -> List[Dict[str, Any]]:
    """
    DELETE request pack:
    - SKU -> productExtend id a sku_map-ben
    - DELETE /productExtend/{id}
    """
    base = base_url.rstrip("/")
    reqs: List[Dict[str, Any]] = []
    for sku in skus:
        pid = sku_map.get(sku)
        if not pid:
            continue
        reqs.append(
            {
                "method": "DELETE",
                "uri": f"{base}/productExtend/{pid}",
                "data": {},
            }
        )
    return reqs


def make_client() -> ShoprenterClient:
    return ShoprenterClient(
        base_url=os.getenv("SHOPRENTER_API_URL"),
        user=os.getenv("SHOPRENTER_API_USER"),
        password=os.getenv("SHOPRENTER_API_PASS"),
    )


def load_natura_products() -> List[Dict[str, Any]]:
    """
    Beolvassa + normalizálja a natura beszállító CSV-t.
    """
    raw = ingest_one_supplier_csv("natura")
    products = normalize_rows("natura", raw)
    print("RAW:", len(raw))
    print("NORMALIZED:", len(products))
    return products


# -----------------------------
# Phase 1: UPsert (CREATE+UPDATE)
# -----------------------------
def run_upsert(*, client: ShoprenterClient) -> Dict[str, int]:
    """
    Natura UPsert futás:
    - payload építés minden termékhez
    - batch POST/PUT küldés
    """
    t_start = time.perf_counter()

    products = load_natura_products()
    total = len(products)

    # SKU_MAP (shop oldali)
    t_sku0 = time.perf_counter()
    sku_map = build_product_sku_map(client, limit=SKU_MAP_LIMIT, sleep_s=SKU_MAP_SLEEP_S)
    t_sku = time.perf_counter() - t_sku0
    print(f"SKU_MAP SIZE: {len(sku_map)} | build_time={t_sku:.2f}s")

    debug_dir = Path("data") / "debug"
    failed_dir = debug_dir / "failed_payloads"
    debug_dir.mkdir(parents=True, exist_ok=True)
    failed_dir.mkdir(parents=True, exist_ok=True)

    log_file = debug_dir / "natura_sync_log.jsonl"

    created = updated = failed = 0

    # payloadok előkészítése (mindig 0-ról indul)
    payloads: List[Dict[str, Any]] = []
    for p in products:
        payload = build_product_extend_from_natura(
            p,
            language_id=LANGUAGE_ID,
            status_value=1,
            stock1=0,
        )
        payload.pop("_debug", None)
        payloads.append(payload)

    packs = chunked(payloads, BATCH_SIZE)
    processed_count = 0

    for pack_idx, pack_payloads in enumerate(packs, start=1):
        batch_t0 = time.perf_counter()

        reqs = build_batch_requests_for_pack(
            base_url=client.base_url,
            sku_map=sku_map,
            payloads=pack_payloads,
        )

        try:
            resp = post_batch(client, reqs)
        except Exception as e:
            print(f"[UPSERT BATCH {pack_idx}/{len(packs)}] FAILED to POST /batch: {e}")
            raise

        results = parse_batch_response(resp)
        if not results:
            (debug_dir / f"batch_raw_upsert_{pack_idx}.json").write_text(
                json.dumps(resp, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            raise RuntimeError("Nem tudtam értelmezni a /batch választ (upsert).")

        uri_to_payload: Dict[str, Dict[str, Any]] = {req["uri"]: pl for req, pl in zip(reqs, pack_payloads)}
        uri_to_sku: Dict[str, str] = {
            req["uri"]: str(pl.get("sku", "")).strip() for req, pl in zip(reqs, pack_payloads)
        }

        bad_items: List[Tuple[int, str]] = []

        for item in results:
            status = int(item.get("statusCode") or 0)
            uri = item.get("uri") or ""
            method = (item.get("method") or "").upper()
            sku = uri_to_sku.get(uri, "")

            ok = status in (200, 201, 204)
            if ok:
                if method == "POST":
                    created += 1
                    body = item.get("body")
                    if isinstance(body, dict) and body.get("id") and sku:
                        sku_map[sku] = body["id"]
                else:
                    updated += 1
            else:
                failed += 1
                bad_items.append((status, uri))

                pl = uri_to_payload.get(uri)
                if pl:
                    out = failed_dir / f"{sku or 'unknown'}_payload.json"
                    out.write_text(json.dumps(pl, ensure_ascii=False, indent=2), encoding="utf-8")

                body = item.get("body")
                try:
                    body_str = json.dumps(body, ensure_ascii=False)[:1500]
                except Exception:
                    body_str = str(body)[:1500]

                with open(log_file, "a", encoding="utf-8") as f:
                    f.write(
                        json.dumps(
                            {
                                "sku": sku,
                                "status": status,
                                "uri": uri,
                                "method": method,
                                "body": body_str,
                                "action": "failed",
                            },
                            ensure_ascii=False,
                        )
                        + "\n"
                    )

        processed_count += len(pack_payloads)

        batch_dt = time.perf_counter() - batch_t0
        total_dt = time.perf_counter() - t_start

        speed = processed_count / total_dt if total_dt > 0 else 0.0
        remaining = total - processed_count
        eta_s = (remaining / speed) if speed > 0 else 0.0

        ok_count = len(pack_payloads) - len(bad_items)
        print(
            f"[UPSERT {pack_idx}/{len(packs)}] "
            f"ok={ok_count} bad={len(bad_items)} "
            f"batch_dt={batch_dt:.2f}s "
            f"processed={processed_count}/{total} "
            f"speed={speed:.1f} item/s "
            f"ETA={fmt_seconds(eta_s)}"
        )

        if any(code == 429 for code, _ in bad_items):
            time.sleep(max(2.0, BATCH_SLEEP_S * 3))
        else:
            time.sleep(BATCH_SLEEP_S)

    total_dt = time.perf_counter() - t_start
    print("\nUPSERT DONE")
    print({"created": created, "updated": updated, "failed": failed, "total": total})
    print(f"UPSERT RUN TIME: {fmt_seconds(total_dt)} ({total_dt:.2f}s)")

    return {"created": created, "updated": updated, "failed": failed, "total": total}


# -----------------------------
# Phase 2: DELETE
# -----------------------------
def run_delete(*, client: ShoprenterClient) -> Dict[str, int]:
    """
    Natura DELETE futás:
    - csv_skus: Natura CSV-ből
    - shop_skus: sku_map-ből
    - to_delete = shop_skus - csv_skus
    - batch DELETE /productExtend/{id}
    """
    t_start = time.perf_counter()

    products = load_natura_products()
    csv_skus = {str(p.get("sku", "")).strip() for p in products}
    csv_skus.discard("")

    t_sku0 = time.perf_counter()
    sku_map = build_product_sku_map(client, limit=SKU_MAP_LIMIT, sleep_s=SKU_MAP_SLEEP_S)
    t_sku = time.perf_counter() - t_sku0
    print(f"SKU_MAP SIZE: {len(sku_map)} | build_time={t_sku:.2f}s")

    shop_skus = set(sku_map.keys())
    to_delete_skus = sorted(shop_skus - csv_skus)

    print("\n--- DELETE PHASE START ---")
    print(f"DELETE candidates: {len(to_delete_skus)}")

    if len(to_delete_skus) > MAX_DELETE:
        raise RuntimeError(f"Túl sok törlés ({len(to_delete_skus)}). STOP biztonsági okból. MAX_DELETE={MAX_DELETE}")

    deleted = 0
    if to_delete_skus:
        delete_reqs = build_delete_requests(
            base_url=client.base_url,
            sku_map=sku_map,
            skus=to_delete_skus,
        )

        delete_packs = chunked(delete_reqs, BATCH_SIZE)

        for pack_idx, pack in enumerate(delete_packs, start=1):
            resp = post_batch(client, pack)
            results = parse_batch_response(resp)

            for item in results:
                status = int(item.get("statusCode") or 0)
                if status in (200, 204):
                    deleted += 1

            print(f"[DELETE {pack_idx}/{len(delete_packs)}] deleted_so_far={deleted}")
            time.sleep(BATCH_SLEEP_S)

        print(f"DELETE DONE: {deleted}")
    else:
        print("No products to delete.")

    total_dt = time.perf_counter() - t_start
    print(f"DELETE RUN TIME: {fmt_seconds(total_dt)} ({total_dt:.2f}s)")

    return {"deleted": deleted, "candidates": len(to_delete_skus)}


# -----------------------------
# CLI
# -----------------------------
def main():
    parser = argparse.ArgumentParser(description="Natura -> Shoprenter sync (upsert / delete / all)")
    parser.add_argument(
        "--mode",
        choices=["upsert", "delete", "all"],
        default="upsert",
        help="Mit futtasson: upsert (create+update), delete, vagy all.",
    )
    args = parser.parse_args()

    client = make_client()

    if args.mode == "upsert":
        run_upsert(client=client)
    elif args.mode == "delete":
        run_delete(client=client)
    else:
        run_upsert(client=client)
        run_delete(client=client)


if __name__ == "__main__":
    main()