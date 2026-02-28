# src/tests/run_natura__sync_batch.py
import os
import time
import json
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

from dotenv import load_dotenv

from src.ingest.suppliers_csv import ingest_one_supplier_csv
from src.normalize.natura import normalize_natura_rows
from src.shoprenter.payloads_natura import build_product_extend_from_natura, build_update_payload_from_full
from src.shoprenter.client import ShoprenterClient
from src.shoprenter.lookups import build_product_sku_map  # a progress-os verziód


load_dotenv()

LANGUAGE_ID = "bGFuZ3VhZ2UtbGFuZ3VhZ2VfaWQ9MQ=="  # HU

BATCH_SIZE = 150            # 100 -> 150 ok
BATCH_SLEEP_S = 0.3         # próbáld 0.3-mal, ha 429, emeld
SKU_MAP_LIMIT = 200
SKU_MAP_SLEEP_S = 0         # simán mehet 0-ra

ONLY_LIGHT_UPDATE = False   # True = csak price+stock+status+sku (gyorsabb)


def fmt_seconds(sec: float) -> str:
    return str(timedelta(seconds=int(max(0, sec))))


def chunked(items: List[Any], size: int) -> List[List[Any]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def build_light_payload(full_payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "sku": full_payload.get("sku"),
        "price": full_payload.get("price"),
        "status": full_payload.get("status"),
        "stock1": full_payload.get("stock1"),
    }


def build_batch_requests_for_pack(
    *,
    base_url: str,
    sku_map: Dict[str, str],
    payloads: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    reqs: List[Dict[str, Any]] = []
    base = base_url.rstrip("/")

    for payload in payloads:
        sku = str(payload.get("sku", "")).strip()
        existing_id = sku_map.get(sku)

        data = build_light_payload(payload) if ONLY_LIGHT_UPDATE else payload

        if existing_id:
            update_data = build_update_payload_from_full(payload)

            reqs.append({
                "method": "PUT",
                "uri": f"{base}/productExtend/{existing_id}",
                "data": update_data,
            })
        else:
            reqs.append({
                "method": "POST",
                "uri": f"{base}/productExtend",
                "data": payload,  # full
            })

    return reqs


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

        body = (
            item.get("body")
            or item.get("response", {}).get("body")
            or item.get("response", {}).get("data")
        )

        try:
            status_int = int(status) if status is not None else 0
        except Exception:
            status_int = 0

        out.append({
            "method": method,
            "uri": uri,
            "statusCode": status_int,
            "body": body,
        })

    return out

def build_delete_requests(*, base_url: str, sku_map: dict, skus: list[str]) -> list[dict]:
    base = base_url.rstrip("/")
    reqs = []
    for sku in skus:
        pid = sku_map.get(sku)
        if not pid:
            continue
        reqs.append({
            "method": "DELETE",
            "uri": f"{base}/productExtend/{pid}",
            "data": {}  # általában üres; ha nem kell, akár el is hagyható
        })
    return reqs

def main():
    # ---- időmérés: teljes futás ----
    t_start = time.perf_counter()

    client = ShoprenterClient(
        base_url=os.getenv("SHOPRENTER_API_URL"),
        user=os.getenv("SHOPRENTER_API_USER"),
        password=os.getenv("SHOPRENTER_API_PASS"),
    )

    raw = ingest_one_supplier_csv("natura")
    products = normalize_natura_rows(raw)
    total = len(products)

    print("RAW:", len(raw))
    print("NORMALIZED:", total)

    # ---- SKU MAP építés időméréssel ----
    t_sku0 = time.perf_counter()
    sku_map = build_product_sku_map(client, limit=SKU_MAP_LIMIT, sleep_s=SKU_MAP_SLEEP_S)
    t_sku = time.perf_counter() - t_sku0
    print(f"SKU_MAP SIZE: {len(sku_map)} | build_time={t_sku:.2f}s")

    debug_dir = Path("data") / "debug"
    failed_dir = debug_dir / "failed_payloads"
    debug_dir.mkdir(parents=True, exist_ok=True)
    failed_dir.mkdir(parents=True, exist_ok=True)

    progress_file = debug_dir / "natura_sync_progress.json"
    log_file = debug_dir / "natura_sync_log.jsonl"

    start_index = 0
    """
    if progress_file.exists():
        try:
            start_index = int(json.loads(progress_file.read_text(encoding="utf-8")).get("index", 0))
        except Exception:
            start_index = 0
    """
    print(f"RESUME FROM index={start_index} / total={total}")
    print(f"CONFIG: BATCH_SIZE={BATCH_SIZE} BATCH_SLEEP_S={BATCH_SLEEP_S} ONLY_LIGHT_UPDATE={ONLY_LIGHT_UPDATE}")

    created = updated = failed = 0

    # payloadok előkészítése (resume-től)
    payloads: List[Dict[str, Any]] = []
    for p in products[start_index:]:
        payload = build_product_extend_from_natura(p, language_id=LANGUAGE_ID)
        # javasolt: ne küldj ismeretlen debug mezőt a Shoprenternek
        payload.pop("_debug", None)
        payloads.append(payload)

    packs = chunked(payloads, BATCH_SIZE)
    processed_count = start_index

    # ---- batch loop ----
    for pack_idx, pack_payloads in enumerate(packs, start=1):
        batch_t0 = time.perf_counter()

        reqs = build_batch_requests_for_pack(
            base_url=client.base_url,
            sku_map=sku_map,
            payloads=pack_payloads,
        )

        # küldés
        try:
            resp = post_batch(client, reqs)
        except Exception as e:
            print(f"[BATCH {pack_idx}/{len(packs)}] FAILED to POST /batch: {e}")
            raise

        results = parse_batch_response(resp)

        if not results:
            print(f"[BATCH {pack_idx}] WARNING: batch response parse failed. Raw keys: {list(resp.keys())}")
            (debug_dir / f"batch_raw_{pack_idx}.json").write_text(
                json.dumps(resp, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            raise RuntimeError("Nem tudtam értelmezni a /batch választ. Nézd meg a batch_raw_*.json-t.")

        # uri -> payload/sku map
        uri_to_payload: Dict[str, Dict[str, Any]] = {req["uri"]: pl for req, pl in zip(reqs, pack_payloads)}
        uri_to_sku: Dict[str, str] = {req["uri"]: str(pl.get("sku", "")).strip() for req, pl in zip(reqs, pack_payloads)}

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
                    # ha body-ban jön id, frissítsük a sku_map-et, hogy ugyanabban a futásban se duplikáljon
                    if isinstance(body, dict) and body.get("id") and sku:
                        sku_map[sku] = body["id"]
                else:
                    updated += 1
            else:
                failed += 1
                bad_items.append((status, uri))

                # payload mentése
                pl = uri_to_payload.get(uri)
                if pl:
                    out = failed_dir / f"{sku or 'unknown'}_payload.json"
                    out.write_text(json.dumps(pl, ensure_ascii=False, indent=2), encoding="utf-8")

                # rövid body log
                body = item.get("body")
                try:
                    body_str = json.dumps(body, ensure_ascii=False)[:1500]
                except Exception:
                    body_str = str(body)[:1500]

                with open(log_file, "a", encoding="utf-8") as f:
                    f.write(json.dumps({
                        "sku": sku,
                        "status": status,
                        "uri": uri,
                        "method": method,
                        "body": body_str,
                        "action": "failed"
                    }, ensure_ascii=False) + "\n")

        processed_count += len(pack_payloads)

        # progress (resume)
        progress_file.write_text(
            json.dumps({"index": processed_count}, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )

        # ---- időmérés: batch + teljes + ETA ----
        batch_dt = time.perf_counter() - batch_t0
        total_dt = time.perf_counter() - t_start

        speed = processed_count / total_dt if total_dt > 0 else 0.0
        remaining = total - processed_count
        eta_s = (remaining / speed) if speed > 0 else 0.0

        ok_count = len(pack_payloads) - len(bad_items)
        print(
            f"[BATCH {pack_idx}/{len(packs)}] "
            f"ok={ok_count} bad={len(bad_items)} "
            f"batch_dt={batch_dt:.2f}s "
            f"processed={processed_count}/{total} "
            f"speed={speed:.1f} item/s "
            f"ETA={fmt_seconds(eta_s)}"
        )

        # throttle
        if any(code == 429 for code, _ in bad_items):
            time.sleep(max(2.0, BATCH_SLEEP_S * 3))
        else:
            time.sleep(BATCH_SLEEP_S)

        # =========================
    # DELETE PHASE
    # =========================

    print("\n--- DELETE PHASE START ---")

    csv_skus = {str(p.get("sku", "")).strip() for p in products}
    csv_skus.discard("")

    shop_skus = set(sku_map.keys())

    to_delete_skus = sorted(shop_skus - csv_skus)

    print(f"DELETE candidates: {len(to_delete_skus)}")

    MAX_DELETE = 200
    if len(to_delete_skus) > MAX_DELETE:
        raise RuntimeError(
            f"Túl sok törlés ({len(to_delete_skus)}). STOP biztonsági okból."
        )

    if to_delete_skus:
        delete_reqs = build_delete_requests(
            base_url=client.base_url,
            sku_map=sku_map,
            skus=to_delete_skus,
        )

        delete_packs = chunked(delete_reqs, BATCH_SIZE)

        deleted = 0

        for pack_idx, pack in enumerate(delete_packs, start=1):
            resp = post_batch(client, pack)
            results = parse_batch_response(resp)

            for item in results:
                status = int(item.get("statusCode") or 0)
                if status in (200, 204):
                    deleted += 1

            print(f"[DELETE BATCH {pack_idx}/{len(delete_packs)}] deleted_so_far={deleted}")

        print(f"DELETE DONE: {deleted}")
    else:
        print("No products to delete.")

    # ---- végső összegzés ----
    total_dt = time.perf_counter() - t_start
    print("\nDONE")
    print({"created": created, "updated": updated, "failed": failed, "total": total})
    print(f"TOTAL RUN TIME: {fmt_seconds(total_dt)} ({total_dt:.2f}s)")


if __name__ == "__main__":
    main()