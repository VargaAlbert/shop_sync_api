from __future__ import annotations

import json
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

from src.ingest.parsers.csv_parser import parse_csv_bytes

SUPPLIERS_DIR = Path("config") / "suppliers"
CACHE_DIR = Path("data") / "cache" / "suppliers"


@dataclass(frozen=True)
class SupplierCsvConfig:
    name: str
    url: str
    encoding: str = "utf-8"
    delimiter: str = ","


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_supplier_csv_config(supplier_dir: Path) -> SupplierCsvConfig:
    cfg = _read_json(supplier_dir / "supplier.json")
    if cfg.get("type", "").lower() != "csv":
        raise ValueError(f"Csak CSV támogatott most. Supplier: {supplier_dir.name}")

    src = cfg["source"]
    return SupplierCsvConfig(
        name=cfg["name"],
        url=src["url"],
        encoding=src.get("encoding", "utf-8"),
        delimiter=src.get("delimiter", ","),
    )


def download_bytes(url: str, timeout_sec: int = 60) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "shop-sync/1.0"})
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        return resp.read()


def cache_write(supplier_name: str, content: bytes) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    path = CACHE_DIR / supplier_name / f"{ts}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def ingest_one_supplier_csv(supplier_name: str) -> List[Dict[str, Any]]:
    supplier_dir = SUPPLIERS_DIR / supplier_name
    if not supplier_dir.exists():
        raise FileNotFoundError(f"Nincs ilyen mappa: {supplier_dir}")

    cfg = load_supplier_csv_config(supplier_dir)
    data = download_bytes(cfg.url)

    cache_write(cfg.name, data)

    rows = parse_csv_bytes(data, encoding=cfg.encoding, delimiter=cfg.delimiter)

    # Minimális “ráégetett” meta, hogy később merge-nél hasznos legyen
    for r in rows:
        r["_supplier"] = cfg.name

    return rows


def ingest_all_suppliers_csv() -> List[Dict[str, Any]]:
    if not SUPPLIERS_DIR.exists():
        return []

    out: List[Dict[str, Any]] = []
    for d in SUPPLIERS_DIR.iterdir():
        if d.is_dir() and (d / "supplier.json").exists():
            # csak csv-ket fogadunk most
            cfg = _read_json(d / "supplier.json")
            if cfg.get("type", "").lower() == "csv":
                out.extend(ingest_one_supplier_csv(d.name))
    return out