from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth
import requests


ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env", override=True)

BASE = (os.getenv("SHOPRENTER_API_URL") or "").strip().rstrip("/")
USER = (os.getenv("SHOPRENTER_API_USER") or "").strip()
PASS = (os.getenv("SHOPRENTER_API_PASS") or "").strip()

if not BASE:
    raise SystemExit("Hiányzik: SHOPRENTER_API_URL")
if not USER or not PASS:
    raise SystemExit("Hiányzik: SHOPRENTER_API_USER vagy SHOPRENTER_API_PASS")


def _extract_id(ref: Any) -> str:
    if isinstance(ref, dict):
        rid = str(ref.get("id") or "").strip()
        if rid:
            return rid

        href = str(ref.get("href") or "").strip()
        if href:
            return href.rsplit("/", 1)[-1].strip()

    if isinstance(ref, str):
        s = ref.strip()
        if not s:
            return ""
        if "/" in s:
            return s.rsplit("/", 1)[-1].strip()
        return s

    return ""


def _print_block(title: str, value: Any) -> None:
    print(f"\n--- {title} ---")
    print(json.dumps(value, ensure_ascii=False, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Shoprenter product description debug by SKU"
    )
    parser.add_argument("--sku", default="13", help="Lekérendő SKU (default: 13)")
    parser.add_argument(
        "--save",
        action="store_true",
        help="Teljes JSON mentése data/debug alá",
    )
    args = parser.parse_args()

    sku = str(args.sku).strip()
    if not sku:
        raise SystemExit("Üres SKU")

    session = requests.Session()
    session.auth = HTTPBasicAuth(USER, PASS)
    session.headers.update(
        {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
    )

    resp = session.get(
        f"{BASE}/productExtend",
        params={
            "sku": sku,
            "page": 0,
            "limit": 1,
            "full": 1,
        },
        timeout=30,
    )
    resp.raise_for_status()

    data = resp.json() if resp.text.strip() else {}
    items = data.get("items") or []

    if not items:
        raise SystemExit(f"Nincs találat erre a SKU-ra: {sku}")

    item = items[0] or {}
    pid = _extract_id(item)

    print(f"SKU: {sku}")
    print(f"productExtend id: {pid or '(nincs id)'}")
    print(f"Top-level kulcsok: {sorted(item.keys())}")

    # Teljes item dump
    _print_block("TELJES productExtend ITEM", item)

    # Kiemelten a gyakori description mezők
    for key in [
        "productDescriptions",
        "productDescription",
        "shortDescription",
        "longDescription",
        "description",
        "short_description",
        "long_description",
        "productShortDescriptions",
        "productLongDescriptions",
    ]:
        if key in item:
            _print_block(key, item.get(key))

    # Minden olyan top-level mező, aminek a nevében benne van a 'desc'
    desc_like = {
        k: v
        for k, v in item.items()
        if "desc" in str(k).lower()
    }
    if desc_like:
        _print_block("DESC-SZERŰ TOP-LEVEL MEZŐK", desc_like)

    # productDescriptions elemek részletes kibontása
    product_descriptions = item.get("productDescriptions")
    if isinstance(product_descriptions, list):
        print("\n=== productDescriptions részletezés ===")
        for idx, row in enumerate(product_descriptions, start=1):
            if not isinstance(row, dict):
                print(f"[{idx}] nem dict elem: {row!r}")
                continue

            print(f"\n[{idx}]")
            print(f"keys: {sorted(row.keys())}")

            row_id = _extract_id(row)
            product_id = _extract_id(row.get("product"))
            language_id = _extract_id(row.get("language"))

            print(f"id: {row_id or '(nincs)'}")
            print(f"product.id: {product_id or '(nincs)'}")
            print(f"language.id: {language_id or '(nincs)'}")
            print(f"name: {row.get('name')!r}")
            print(f"description: {row.get('description')!r}")
            print(f"shortDescription: {row.get('shortDescription')!r}")
            print(f"longDescription: {row.get('longDescription')!r}")

            extra_desc = {
                k: v
                for k, v in row.items()
                if "desc" in str(k).lower()
            }
            if extra_desc:
                _print_block(f"productDescriptions[{idx}] desc mezők", extra_desc)

    if args.save:
        out_dir = ROOT / "data" / "debug"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"product_extend_sku_{sku}.json"
        out_path.write_text(
            json.dumps(item, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"\nMentve: {out_path}")


if __name__ == "__main__":
    main()