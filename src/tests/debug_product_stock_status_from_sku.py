from __future__ import annotations

import os
from pathlib import Path

import requests
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env", override=True)

BASE = (os.getenv("SHOPRENTER_API_URL") or "").strip().rstrip("/")
USER = (os.getenv("SHOPRENTER_API_USER") or "").strip()
PASS = (os.getenv("SHOPRENTER_API_PASS") or "").strip()

if not BASE:
    raise SystemExit("Hiányzik: SHOPRENTER_API_URL")
if not USER or not PASS:
    raise SystemExit("Hiányzik: SHOPRENTER_API_USER vagy SHOPRENTER_API_PASS")

session = requests.Session()
session.auth = HTTPBasicAuth(USER, PASS)
session.headers.update({"Accept": "application/json"})


def extract_id(ref):
    if isinstance(ref, dict):
        rid = str(ref.get("id") or "").strip()
        if rid:
            return rid
        href = str(ref.get("href") or "").strip()
        if href:
            return href.rsplit("/", 1)[-1].strip()
    if isinstance(ref, str):
        s = ref.strip()
        if "/" in s:
            return s.rsplit("/", 1)[-1].strip()
        return s
    return ""


r1 = session.get(f"{BASE}/stockStatuses", params={"full": 1, "limit": 200}, timeout=30)
r1.raise_for_status()
statuses = (r1.json() or {}).get("items", []) or []

r2 = session.get(f"{BASE}/stockStatusDescriptions", params={"full": 1, "limit": 200}, timeout=30)
r2.raise_for_status()
descs = (r2.json() or {}).get("items", []) or []

name_map = {}
for d in descs:
    sid = extract_id(d.get("stockStatus"))
    name = str(d.get("name") or "").strip()
    if sid and name:
        name_map[sid] = name

print("\nSTOCK STATUS LISTA:\n")
for s in statuses:
    sid = extract_id(s)
    print(f"{sid} -> {name_map.get(sid, '(nincs név)')}")

print("\nTALÁLATOK 'Raktáron' névre:\n")
for sid, name in name_map.items():
    if name.lower() == "raktáron":
        print(f"SHOPRENTER_CREATE_NO_STOCK_STATUS_ID={sid}")