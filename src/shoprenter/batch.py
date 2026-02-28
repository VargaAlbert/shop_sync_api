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
    """
    Tömeges UPsert (CREATE + UPDATE) feldolgozás egyenkénti API hívásokkal.

    Cél:
        Egy iterálható terméklistán végigmenni, és minden termékre:
        - ha a SKU már létezik a Shoprenterben (sku_map alapján) -> UPDATE
        - ha nem létezik -> CREATE, majd a kapott id-t visszaírni a sku_map-be

    Fő jellemzők:
        - soronkénti feldolgozás (nem batch)
        - hibák logolása JSONL formátumba: data/debug/bulk_result.jsonl
        - hibás payloadok külön mentése: data/debug/failed_payloads/<sku>.json
        - rate limit (429) esetén extra várakozás

    Paraméterek:
        client:
            Olyan kliens objektum, ami az alábbi metódusokat biztosítja:
                - update_product(product_id: str, payload: dict) -> None / response
                - create_product(payload: dict) -> dict  (benne: {"id": "..."} )

        products (Iterable[Dict[str, Any]]):
            Normalizált termékek iterálható gyűjteménye.
            Elvárt, hogy minden termék tartalmazzon legalább "sku" mezőt.

        build_payload (Callable[[Dict[str, Any]], Dict[str, Any]]):
            Függvény, ami egy normalizált termékből Shoprenter kompatibilis payloadot épít.
            A visszatérő dict-nek tartalmaznia kell a "sku" kulcsot.

        sku_map (Dict[str, str]):
            SKU -> Shoprenter productExtend id map.
            Ez alapján döntjük el, hogy UPDATE vagy CREATE történjen.
            CREATE után a map frissül (sku -> új id).

        sleep_s (float):
            Alap várakozás két termék feldolgozása között (throttling).
            Alapértelmezett: 0.3 sec.

        max_items (int | None):
            Ha meg van adva, maximum ennyi terméket dolgoz fel (debug / teszt célra).

    Visszatérési érték:
        dict:
            Összegzés:
                {
                  "created": int,
                  "updated": int,
                  "failed": int,
                  "total": int
                }

    Hibakezelés:
        - HTTPError: logoljuk a status kódot és a response body első 2000 karakterét,
          payload mentése sku alapján.
        - 429 esetén nagyobb sleep.
        - általános Exception: log + sleep.

    Megjegyzés:
        Ez a megoldás egyszerű és robusztus, de lassabb, mint a batch endpoint használata.
        Nagy mennyiségnél ajánlott áttérni a /batch alapú módszerre (lásd lentebb).
    """
    # Debug könyvtár létrehozása (logok/payload dump)
    os.makedirs("data/debug", exist_ok=True)
    log_path = "data/debug/bulk_result.jsonl"

    created = updated = failed = 0
    i = 0

    # JSONL log (minden sor egy JSON objektum)
    with open(log_path, "a", encoding="utf-8") as log:
        for p in products:
            i += 1
            if max_items is not None and i > max_items:
                break

            sku = str(p.get("sku", "")).strip()

            try:
                # Payload építés beszállítói termékből
                payload = build_payload(p)
                action = None

                # Döntés: update vagy create
                existing_id = sku_map.get(payload["sku"])
                if existing_id:
                    # UPDATE
                    client.update_product(existing_id, payload)
                    action = "updated"
                    updated += 1
                else:
                    # CREATE
                    resp = client.create_product(payload)
                    sku_map[payload["sku"]] = resp.get("id")
                    action = "created"
                    created += 1

                print(f"{i} {sku} -> {action}")
                log.write(json.dumps({"sku": sku, "action": action}, ensure_ascii=False) + "\n")

            except HTTPError as e:
                # HTTP szintű hiba (4xx/5xx)
                failed += 1
                status = getattr(e.response, "status_code", None)
                body = getattr(e.response, "text", "")[:2000]

                print(f"{i} {sku} -> FAILED ({status})")

                # Hibás payload mentése későbbi elemzéshez
                try:
                    os.makedirs("data/debug/failed_payloads", exist_ok=True)
                    with open(f"data/debug/failed_payloads/{sku}.json", "w", encoding="utf-8") as f:
                        json.dump(payload, f, ensure_ascii=False, indent=2)
                except Exception:
                    # Mentési hiba ne állítsa meg a futást
                    pass

                # JSONL log sor
                log.write(
                    json.dumps(
                        {"sku": sku, "action": "failed", "status": status, "body": body},
                        ensure_ascii=False,
                    )
                    + "\n"
                )

                # Rate limit esetén agresszívebb backoff
                if status == 429:
                    time.sleep(max(2.0, sleep_s * 5))
                else:
                    time.sleep(sleep_s)

            except Exception as e:
                # Egyéb futásidejű hiba
                failed += 1
                print(f"{i} {sku} -> FAILED (exception): {e}")
                log.write(json.dumps({"sku": sku, "action": "failed", "error": str(e)}, ensure_ascii=False) + "\n")
                time.sleep(sleep_s)

            # Általános throttling két termék között
            time.sleep(sleep_s)

    return {"created": created, "updated": updated, "failed": failed, "total": i}


def chunked(lst: List[Any], n: int) -> Iterable[List[Any]]:
    """
    Lista feldarabolása n méretű darabokra (chunkokra).

    Paraméterek:
        lst (List[Any]): Feldarabolandó lista.
        n (int): Chunk méret.

    Yields:
        List[Any]: A következő chunk (max n elem).
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def post_batch(session: requests.Session, base_url: str, reqs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Shoprenter /batch endpoint meghívása.

    A Shoprenter batch formátum:
        {
          "data": {
            "requests": [ {method, uri, data}, ... ]
          }
        }

    Paraméterek:
        session (requests.Session): Autholt session a Shoprenter API-hoz.
        base_url (str): API alap URL (pl. https://.../api).
        reqs (List[Dict[str, Any]]): Batch request elemek listája.

    Visszatérési érték:
        Dict[str, Any]: A /batch válasza (JSON).

    Kivétel:
        requests.HTTPError: ha a HTTP státusz nem 2xx.
    """
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
    """
    Batch request lista építése termék payloadokból (UPsert logika).

    Szabály:
        - ha SKU benne van a sku_map-ben -> PUT /productExtend/{id}
        - ha nincs -> POST /productExtend

    Fontos:
        Itt a PUT/POST requestek "data" mezőjébe jelenleg a *teljes payload* kerül.
        Sok esetben érdemes PUT esetén csak update mezőket küldeni
        (pl. build_update_payload_from_full), hogy elkerüld a nem kívánt felülírásokat.

    Paraméterek:
        base_url (str): Shoprenter API base URL.
        sku_map (Dict[str, str]): SKU -> productExtend id map.
        payloads (List[Dict[str, Any]]): Termék payloadok (mind tartalmazza a "sku"-t).

    Visszatérési érték:
        List[Dict[str, Any]]: /batch-be küldhető request elemek listája.
    """
    reqs: List[Dict[str, Any]] = []

    base = base_url.rstrip("/")

    for payload in payloads:
        sku = payload["sku"].strip()
        existing_id = sku_map.get(sku)

        if existing_id:
            reqs.append(
                {
                    "method": "PUT",
                    "uri": f"{base}/productExtend/{existing_id}",
                    "data": payload,
                }
            )
        else:
            reqs.append(
                {
                    "method": "POST",
                    "uri": f"{base}/productExtend",
                    "data": payload,
                }
            )

    return reqs


def parse_batch_results(batch_response: Dict[str, Any]) -> List[Tuple[int, str]]:
    """
    Egyszerű batch eredmény parser.

    Kimenet:
        List[Tuple[int, str]]: [(statusCode, uri), ...]

    Elvárt input struktúra (gyakoribb Shoprenter válasz):
        {
          "requests": {
            "request": [
              {
                "uri": "...",
                "response": {
                  "header": {"statusCode": 200},
                  ...
                }
              }
            ]
          }
        }

    Paraméter:
        batch_response (Dict[str, Any]): /batch válasz JSON.

    Visszatérési érték:
        List[Tuple[int, str]]: státuszkód + uri párok.

    Megjegyzés:
        Ez a parser csak egy konkrét struktúrát kezel.
        Ha többféle válaszformátummal találkozol, érdemes a korábbi
        "parse_batch_response" robusztusabb változatot használni.
    """
    out: List[Tuple[int, str]] = []
    requests_block = batch_response.get("requests", {}).get("request", []) or []
    for item in requests_block:
        uri = item.get("uri", "")
        status = int(item.get("response", {}).get("header", {}).get("statusCode", 0) or 0)
        out.append((status, uri))
    return out