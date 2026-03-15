from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Set

import requests
from dotenv import load_dotenv

from src.shoprenter.lookups import build_product_sku_map, build_manufacturer_name_map
from src.core.pipeline import run_pipeline
from src.payloads.shoprenter import build_payload, DEFAULT_LANGUAGE_ID
from src.shoprenter.client import ShoprenterClient
from src.utils.images import (
    prepare_shoprenter_image_upload,
    build_main_picture_path_for_product,
)
from src.utils.log import setup_logging

load_dotenv()


@dataclass(frozen=True)
class RunStats:
    created: int = 0
    updated: int = 0
    enriched: int = 0
    deleted: int = 0
    skipped: int = 0
    errors: int = 0


def _create_client() -> ShoprenterClient:
    return ShoprenterClient(
        base_url=os.getenv("SHOPRENTER_API_URL"),
        user=os.getenv("SHOPRENTER_API_USER"),
        password=os.getenv("SHOPRENTER_API_PASS"),
    )


def _parse_csv_env(name: str) -> Optional[Set[str]]:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return None
    lowered = raw.lower().strip()
    if lowered in {"*", "all"}:
        return None
    items = {x.strip() for x in raw.split(",") if x.strip()}
    return items or None


def _delete_allowed_for_sku(sku: str) -> bool:
    """
    Biztonsági fék:
    - DELETE_ENABLED=1 nélkül sosem törlünk
    - ha DELETE_SKU_PREFIXES meg van adva, csak azokra a prefixekre törlünk
      pl: DELETE_SKU_PREFIXES=NAT,ABC
    """
    if os.getenv("DELETE_ENABLED", "0") != "1":
        return False

    prefixes = _parse_csv_env("DELETE_SKU_PREFIXES")
    if prefixes is None:
        return True
    return any(str(sku).startswith(p) for p in prefixes)


def _update_with_retry(
    client: ShoprenterClient,
    pid: str,
    payload: dict,
    *,
    max_retries: int = 6,
    base_sleep: float = 1.5,
    per_request_sleep: float = 0.40,
) -> None:
    """
    Shoprenter update throttling + retry 429/5xx esetén.
    """
    last_exc = None

    for attempt in range(max_retries + 1):
        try:
            client.update_product(pid, payload)
            time.sleep(per_request_sleep)
            return

        except requests.HTTPError as e:
            last_exc = e
            status = getattr(e.response, "status_code", None)

            if status in {429, 500, 502, 503, 504} and attempt < max_retries:
                retry_after = None
                try:
                    retry_after = e.response.headers.get("Retry-After")
                except Exception:
                    retry_after = None

                if retry_after:
                    try:
                        sleep_s = float(retry_after)
                    except ValueError:
                        sleep_s = base_sleep * (2 ** attempt)
                else:
                    sleep_s = base_sleep * (2 ** attempt)

                time.sleep(sleep_s)
                continue

            raise

        except Exception as e:
            last_exc = e
            raise

    if last_exc:
        raise last_exc


def _upload_file_with_retry(
    client: ShoprenterClient,
    *,
    file_path: str,
    base64_content: str,
    max_retries: int = 6,
    base_sleep: float = 1.5,
    per_request_sleep: float = 0.60,
) -> dict:
    """
    Shoprenter file upload throttling + retry 429/5xx esetén.
    """
    last_exc = None

    for attempt in range(max_retries + 1):
        try:
            resp = client.upload_file(
                file_path=file_path,
                base64_content=base64_content,
                file_type="image",
            )
            time.sleep(per_request_sleep)
            return resp

        except requests.HTTPError as e:
            last_exc = e
            status = getattr(e.response, "status_code", None)

            if status in {429, 500, 502, 503, 504} and attempt < max_retries:
                retry_after = None
                try:
                    retry_after = e.response.headers.get("Retry-After")
                except Exception:
                    retry_after = None

                if retry_after:
                    try:
                        sleep_s = float(retry_after)
                    except ValueError:
                        sleep_s = base_sleep * (2 ** attempt)
                else:
                    sleep_s = base_sleep * (2 ** attempt)

                time.sleep(sleep_s)
                continue

            raise

        except Exception as e:
            last_exc = e
            raise

    if last_exc:
        raise last_exc

    return {}

def _materialize_post_actions(payload: dict) -> dict:
    """
    A builder-ek belső `_post_actions` mezőjét átemeli
    valódi Shoprenter payload mezőkre.
    """
    out = dict(payload or {})
    post_actions = out.pop("_post_actions", None) or {}

    customer_group_prices = post_actions.get("customer_group_prices")
    if customer_group_prices:
        out["customer_group_prices"] = customer_group_prices

    return out

def _save_failed_payload(prefix: str, sku: str, payload: dict) -> None:
    fail_dir = Path("data/debug/failed_payloads")
    fail_dir.mkdir(parents=True, exist_ok=True)

    with open(
        fail_dir / f"{prefix}_{sku}.json",
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _resolve_enrich_main_picture(
    client: ShoprenterClient,
    product: dict,
    *,
    log,
) -> dict:
    """
    ENRICH képet egységes célpathra tölt fel:
    product/<CSOPORT1-folder>/<model>.jpg
    """
    p = dict(product)

    image_urls = p.get("image_urls")
    if not isinstance(image_urls, list) or not image_urls:
        return p

    image_url = str(image_urls[0] or "").strip()
    if not image_url:
        return p

    sku = str(p.get("sku") or "").strip() or None
    model = str(p.get("model") or "").strip() or sku

    target_file_path = build_main_picture_path_for_product(
        p,
        model=model,
        slot=1,
        ext=".jpg",
    )
    if not target_file_path:
        return p

    # Ha már belső path, akkor is az egységes célpathot írjuk be
    if image_url.startswith("product/"):
        p["shoprenter_main_picture"] = target_file_path
        return p

    try:
        prepared = prepare_shoprenter_image_upload(
            image_url=image_url,
            sku=sku,
            model=model,
            file_path=target_file_path,
        )

        _upload_file_with_retry(
            client,
            file_path=prepared["file_path"],
            base64_content=prepared["base64_data"],
            max_retries=int(os.getenv("SHOPRENTER_RETRY_MAX", "6")),
            base_sleep=float(os.getenv("SHOPRENTER_RETRY_BASE_SLEEP", "1.5")),
            per_request_sleep=float(os.getenv("SHOPRENTER_REQUEST_SLEEP", "0.60")),
        )

        p["shoprenter_main_picture"] = target_file_path

        log.debug(
            "ENRICH image uploaded sku=%s path=%s",
            sku,
            target_file_path,
        )

    except Exception as e:
        log.warning(
            "ENRICH image skipped sku=%s url=%s target=%s error=%s",
            sku,
            image_url,
            target_file_path,
            e,
        )

    return p

def _norm_manufacturer_name(value: str) -> str:
    import unicodedata

    s = str(value or "").strip().lower()
    if not s:
        return ""

    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return " ".join(s.split())


def _resolve_manufacturer_for_payload(
    product: dict,
    manufacturer_map: dict[str, str],
) -> dict:
    p = dict(product)

    name = str(p.get("manufacturer_name") or "").strip()
    if not name:
        raw = p.get("raw")
        if isinstance(raw, dict):
            name = str(raw.get("CSOPORT3") or "").strip()

    if not name:
        return p

    key = _norm_manufacturer_name(name)
    mid = manufacturer_map.get(key)
    if mid:
        p["_resolved_manufacturer_id"] = mid

    return p

def _extract_created_manufacturer_id_and_name(resp: dict) -> tuple[str, str]:
    m = resp.get("manufacturer")
    if not isinstance(m, dict):
        return "", ""

    mid = str(m.get("id") or "").strip()
    name = str(m.get("name") or "").strip()

    if not name:
        md = m.get("manufacturerDescription")
        if isinstance(md, dict):
            name = str(md.get("name") or "").strip()

    return mid, name

def run_master_create_all(*, master_supplier: str) -> RunStats:
    log = setup_logging()
    client = _create_client()

    log.info("SKU map building...")
    sku_map = build_product_sku_map(
        client,
        limit=int(os.getenv("SKU_MAP_LIMIT", "200")),
    )

    log.info("Manufacturer map building...")
    manufacturer_map = build_manufacturer_name_map(
        client,
        limit=int(os.getenv("SHOPRENTER_MANUFACTURER_MAP_LIMIT", "200")),
    )
    log.info("Manufacturer map loaded: %s", len(manufacturer_map))

    log.info("Pipeline running (master=%s)...", master_supplier)
    res = run_pipeline(
        master_supplier=master_supplier,
        enable_enrich=True,
        enable_pricing=True,
    )
    log.info("Pipeline finished: master=%s merged=%s", len(res.master), len(res.merged))

    created = skipped = errors = 0
    total = len(res.merged)

    for idx, p in enumerate(res.merged, start=1):
        if idx % 100 == 0:
            log.info("MASTER_CREATE progress: %s/%s", idx, total)

        sku = str(p.get("sku", "")).strip()
        if not sku:
            skipped += 1
            continue

        if sku in sku_map:
            skipped += 1
            continue

        prepared_product = _resolve_manufacturer_for_payload(
            dict(p),
            manufacturer_map,
        )

        payload = build_payload(
            "MASTER_CREATE",
            prepared_product,
            language_id=DEFAULT_LANGUAGE_ID,
        )

        try:
            api_payload = _materialize_post_actions(payload)
            resp = client.create_product(api_payload)
            created_mid, created_mname = _extract_created_manufacturer_id_and_name(resp)
            if created_mid and created_mname:
                manufacturer_map[_norm_manufacturer_name(created_mname)] = created_mid
            new_id = resp.get("id")
            if new_id:
                sku_map[sku] = str(new_id)

            time.sleep(float(os.getenv("SHOPRENTER_REQUEST_SLEEP", "0.40")))
            created += 1

        except requests.HTTPError as e:
            errors += 1
            body = ""
            try:
                body = e.response.text[:5000] if e.response is not None else ""
                _save_failed_payload("master_create", sku, payload)
            except Exception:
                body = ""

            log.exception(
                "MASTER_CREATE failed sku=%s status=%s error=%s body=%s",
                sku,
                getattr(e.response, "status_code", None),
                e,
                body,
            )

        except Exception as e:
            errors += 1
            log.exception("MASTER_CREATE failed sku=%s error=%s", sku, e)

    log.info(
        "MASTER_CREATE done: created=%s skipped=%s errors=%s",
        created,
        skipped,
        errors,
    )
    return RunStats(created=created, skipped=skipped, errors=errors)


def run_master_update_all(*, master_supplier: str) -> RunStats:
    log = setup_logging()
    client = _create_client()

    log.info("SKU map building...")
    sku_map = build_product_sku_map(
        client,
        limit=int(os.getenv("SKU_MAP_LIMIT", "200")),
    )

    log.info("Manufacturer map building...")
    manufacturer_map = build_manufacturer_name_map(
        client,
        limit=int(os.getenv("SHOPRENTER_MANUFACTURER_MAP_LIMIT", "200")),
    )
    log.info("Manufacturer map loaded: %s", len(manufacturer_map))

    log.info("Pipeline running (master=%s)...", master_supplier)
    res = run_pipeline(
        master_supplier=master_supplier,
        enable_enrich=False,
        enable_pricing=False,
    )
    log.info("Pipeline finished: master=%s merged=%s", len(res.master), len(res.merged))

    updated = skipped = errors = 0
    total = len(res.master)

    for idx, p in enumerate(res.master, start=1):
        if idx % 100 == 0:
            log.info("MASTER_UPDATE progress: %s/%s", idx, total)

        sku = str(p.get("sku", "")).strip()
        if not sku:
            skipped += 1
            continue

        pid = sku_map.get(sku)
        if not pid:
            skipped += 1
            continue

        prepared_product = _resolve_manufacturer_for_payload(
            dict(p),
            manufacturer_map,
        )

        try:
            payload = build_payload(
                "MASTER_UPDATE",
                prepared_product,
                language_id=DEFAULT_LANGUAGE_ID,
            )

            if not payload:
                skipped += 1
                continue

            api_payload = _materialize_post_actions(payload)

            _update_with_retry(
                client,
                pid,
                api_payload,
                max_retries=int(os.getenv("SHOPRENTER_RETRY_MAX", "6")),
                base_sleep=float(os.getenv("SHOPRENTER_RETRY_BASE_SLEEP", "1.5")),
                per_request_sleep=float(os.getenv("SHOPRENTER_REQUEST_SLEEP", "0.40")),
            )

            updated += 1

        except requests.HTTPError as e:
            errors += 1
            body = ""
            try:
                body = e.response.text[:5000] if e.response is not None else ""
                _save_failed_payload("master_update", sku, payload)
            except Exception:
                body = ""

            log.exception(
                "MASTER_UPDATE failed sku=%s id=%s status=%s error=%s body=%s",
                sku,
                pid,
                getattr(e.response, "status_code", None),
                e,
                body,
            )

        except Exception as e:
            errors += 1
            log.exception("MASTER_UPDATE failed sku=%s id=%s error=%s", sku, pid, e)

    log.info(
        "MASTER_UPDATE done: updated=%s skipped=%s errors=%s",
        updated,
        skipped,
        errors,
    )
    return RunStats(updated=updated, skipped=skipped, errors=errors)


def run_enrich_update_all(*, master_supplier: str) -> RunStats:
    log = setup_logging()
    client = _create_client()

    log.info("SKU map building...")
    sku_map = build_product_sku_map(
        client,
        limit=int(os.getenv("SKU_MAP_LIMIT", "200")),
    )

    log.info("Pipeline running (master=%s)...", master_supplier)
    res = run_pipeline(
        master_supplier=master_supplier,
        enable_enrich=True,
        enable_pricing=True,
    )
    log.info("Pipeline finished: master=%s merged=%s", len(res.master), len(res.merged))

    enriched = skipped = errors = 0
    total = len(res.merged)

    for idx, p in enumerate(res.merged, start=1):
        if idx % 100 == 0:
            log.info("ENRICH_UPDATE progress: %s/%s", idx, total)

        sku = str(p.get("sku", "")).strip()
        if not sku:
            skipped += 1
            continue

        pid = sku_map.get(sku)
        if not pid:
            skipped += 1
            continue

        if not str(p.get("_enriched_by") or "").strip():
            skipped += 1
            continue

        try:
            prepared_product = _resolve_enrich_main_picture(
                client,
                dict(p),
                log=log,
            )

            payload = build_payload(
                "ENRICH_UPDATE",
                prepared_product,
                language_id=DEFAULT_LANGUAGE_ID,
                product_id=pid,
            )

            if not payload:
                skipped += 1
                continue

            api_payload = _materialize_post_actions(payload)

            _update_with_retry(
                client,
                pid,
                api_payload,
                max_retries=int(os.getenv("SHOPRENTER_RETRY_MAX", "6")),
                base_sleep=float(os.getenv("SHOPRENTER_RETRY_BASE_SLEEP", "1.5")),
                per_request_sleep=float(os.getenv("SHOPRENTER_REQUEST_SLEEP", "0.60")),
            )

            enriched += 1

        except requests.HTTPError as e:
            errors += 1
            body = ""
            try:
                body = e.response.text[:5000] if e.response is not None else ""
                _save_failed_payload("enrich_update", sku, payload)
            except Exception:
                body = ""

            log.exception(
                "ENRICH_UPDATE failed sku=%s id=%s status=%s error=%s body=%s",
                sku,
                pid,
                getattr(e.response, "status_code", None),
                e,
                body,
            )

        except Exception as e:
            errors += 1
            log.exception("ENRICH_UPDATE failed sku=%s id=%s error=%s", sku, pid, e)

    log.info(
        "ENRICH_UPDATE done: enriched=%s skipped=%s errors=%s",
        enriched,
        skipped,
        errors,
    )
    return RunStats(enriched=enriched, skipped=skipped, errors=errors)


def run_delete_all(*, master_supplier: str) -> RunStats:
    """
    Shoprenterből törli azokat, amik nincsenek benne a master SKU listában.
    Erősen védett:
      - DELETE_ENABLED=1 kell
      - opcionális DELETE_SKU_PREFIXES szűkítés
    """
    log = setup_logging()
    client = _create_client()

    log.info("SKU map building...")
    sku_map = build_product_sku_map(
        client,
        limit=int(os.getenv("SKU_MAP_LIMIT", "200")),
    )

    log.info("Pipeline running (master=%s)...", master_supplier)
    res = run_pipeline(
        master_supplier=master_supplier,
        enable_enrich=False,
        enable_pricing=False,
    )
    log.info("Pipeline finished: master=%s merged=%s", len(res.master), len(res.merged))

    master_skus = {
        str(p.get("sku", "")).strip()
        for p in res.master
        if str(p.get("sku", "")).strip()
    }

    deleted = skipped = errors = 0

    for sku, pid in sku_map.items():
        if sku in master_skus:
            skipped += 1
            continue

        if not _delete_allowed_for_sku(sku):
            skipped += 1
            continue

        try:
            client.delete_product(pid)
            time.sleep(float(os.getenv("SHOPRENTER_REQUEST_SLEEP", "0.40")))
            deleted += 1

        except Exception as e:
            errors += 1
            log.exception("DELETE failed sku=%s id=%s error=%s", sku, pid, e)

    log.info(
        "DELETE_ALL done: deleted=%s skipped=%s errors=%s",
        deleted,
        skipped,
        errors,
    )
    return RunStats(deleted=deleted, skipped=skipped, errors=errors)


def run_master_all(*, master_supplier: str) -> RunStats:
    log = setup_logging()
    client = _create_client()

    log.info("SKU map building...")
    sku_map = build_product_sku_map(
        client,
        limit=int(os.getenv("SKU_MAP_LIMIT", "200")),
    )

    log.info("Manufacturer map building...")
    manufacturer_map = build_manufacturer_name_map(
        client,
        limit=int(os.getenv("SHOPRENTER_MANUFACTURER_MAP_LIMIT", "200")),
    )
    log.info("Manufacturer map loaded: %s", len(manufacturer_map))

    log.info("Pipeline running (master=%s)...", master_supplier)
    res = run_pipeline(
        master_supplier=master_supplier,
        enable_enrich=False,
        enable_pricing=False,
    )
    log.info("Pipeline finished: master=%s merged=%s", len(res.master), len(res.merged))

    created = updated = skipped = errors = 0
    total = len(res.master)

    for idx, p in enumerate(res.master, start=1):
        if idx % 100 == 0:
            log.info("MASTER_ALL progress: %s/%s", idx, total)

        sku = str(p.get("sku", "")).strip()
        if not sku:
            skipped += 1
            continue

        pid = sku_map.get(sku)

        prepared_product = _resolve_manufacturer_for_payload(
            dict(p),
            manufacturer_map,
        )

        try:
            if not pid:
                payload = build_payload(
                    "MASTER_CREATE",
                    prepared_product,
                    language_id=DEFAULT_LANGUAGE_ID,
                )

                if not payload:
                    skipped += 1
                    continue

                api_payload = _materialize_post_actions(payload)
                resp = client.create_product(api_payload)

                created_mid, created_mname = _extract_created_manufacturer_id_and_name(resp)
                if created_mid and created_mname:
                    manufacturer_map[_norm_manufacturer_name(created_mname)] = created_mid

                new_id = resp.get("id")
                if new_id:
                    sku_map[sku] = str(new_id)

                time.sleep(float(os.getenv("SHOPRENTER_REQUEST_SLEEP", "0.40")))
                created += 1
                continue

            payload = build_payload(
                "MASTER_UPDATE",
                prepared_product,
                language_id=DEFAULT_LANGUAGE_ID,
            )

            if not payload:
                skipped += 1
                continue

            api_payload = _materialize_post_actions(payload)

            _update_with_retry(
                client,
                pid,
                api_payload,
                max_retries=int(os.getenv("SHOPRENTER_RETRY_MAX", "6")),
                base_sleep=float(os.getenv("SHOPRENTER_RETRY_BASE_SLEEP", "1.5")),
                per_request_sleep=float(os.getenv("SHOPRENTER_REQUEST_SLEEP", "0.40")),
            )

            updated += 1

        except requests.HTTPError as e:
            errors += 1
            body = ""

            try:
                body = e.response.text[:5000] if e.response is not None else ""
                prefix = "master_create" if not pid else "master_update"
                _save_failed_payload(prefix, sku, payload)
            except Exception:
                body = ""

            log.exception(
                "MASTER_ALL failed sku=%s id=%s status=%s error=%s body=%s",
                sku,
                pid,
                getattr(e.response, "status_code", None),
                e,
                body,
            )

        except Exception as e:
            errors += 1
            log.exception("MASTER_ALL failed sku=%s id=%s error=%s", sku, pid, e)

    log.info(
        "MASTER_ALL done: created=%s updated=%s skipped=%s errors=%s",
        created,
        updated,
        skipped,
        errors,
    )
    return RunStats(
        created=created,
        updated=updated,
        skipped=skipped,
        errors=errors,
    )