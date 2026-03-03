from __future__ import annotations

import os
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.ingest.downloaders.generic import download_generic
from src.ingest.downloaders.registry import get_downloader
import src.ingest.downloaders  # bootstrap: regisztrálók betöltése (side-effect)


# Konfigurációs mappák
SUPPLIERS_DIR = Path("config") / "suppliers"
CACHE_DIR = Path("data") / "cache" / "suppliers"


@dataclass(frozen=True)
class SupplierCsvCacheConfig:
    enabled: bool = True
    ttl_seconds: int = 86400


@dataclass(frozen=True)
class SupplierCsvConfig:
    """
    Egy CSV típusú beszállító konfigurációs modellje.
    """
    name: str
    url: str
    encoding: str = "utf-8"
    delimiter: str = ";"
    has_header: bool = True
    cache: SupplierCsvCacheConfig = SupplierCsvCacheConfig()


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_supplier_csv_config(supplier_dir: Path) -> SupplierCsvConfig:
    """
    supplier.json betöltése.

    Várt struktúra (ahogy nálad van):
    {
      "name": "haldepo",
      "type": "csv",
      "source": { "url": "https://..." },
      "encoding": "utf-8",
      "delimiter": ";",
      "has_header": true,
      "cache": { "enabled": true, "ttl_seconds": 86400 }
    }
    """
    cfg = _read_json(supplier_dir / "supplier.json")

    if cfg.get("type", "").lower() != "csv":
        raise ValueError(f"Csak CSV támogatott most. Supplier: {supplier_dir.name}")

    src = cfg.get("source") or {}
    url = src.get("url") or ""
    if not url:
        raise KeyError(f"Missing source.url in {supplier_dir / 'supplier.json'}")

    cache_cfg = cfg.get("cache") or {}
    cache = SupplierCsvCacheConfig(
        enabled=bool(cache_cfg.get("enabled", True)),
        ttl_seconds=int(cache_cfg.get("ttl_seconds", 86400)),
    )

    return SupplierCsvConfig(
        name=str(cfg.get("name") or supplier_dir.name),
        url=str(url),
        encoding=str(cfg.get("encoding", "utf-8")),
        delimiter=str(cfg.get("delimiter", ";")),
        has_header=bool(cfg.get("has_header", True)),
        cache=cache,
    )

import csv
import io


def parse_csv_bytes(
    data: bytes,
    *,
    encoding: str = "utf-8",
    delimiter: str = ";",
    has_header: bool = True,
) -> List[Dict[str, Any]]:
    """
    CSV bytes -> List[Dict[str, Any]]

    - kezeli a UTF-8 BOM-ot
    - delimiter paraméterezhető
    - ha has_header=False, akkor oszlopnevek: col1, col2, ...
    """
    # BOM-barát decode
    text = data.decode(encoding, errors="replace")
    if text.startswith("\ufeff"):
        text = text.lstrip("\ufeff")

    f = io.StringIO(text)

    reader = csv.reader(f, delimiter=delimiter)

    rows: List[Dict[str, Any]] = []

    try:
        first = next(reader)
    except StopIteration:
        return []

    if has_header:
        headers = [str(h).strip() for h in first]
    else:
        headers = [f"col{i+1}" for i in range(len(first))]
        rows.append({headers[i]: first[i] for i in range(len(headers))})

    for r in reader:
        # rövidebb sor esetén pad-eljük, hosszabbnál vágjuk
        r2 = list(r[: len(headers)]) + [""] * max(0, len(headers) - len(r))
        rows.append({headers[i]: r2[i] for i in range(len(headers))})

    return rows

def cache_write(supplier_name: str, content: bytes) -> Path:
    """
    Letöltött CSV tartalom mentése cache mappába.
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    ts = time.strftime("%Y%m%d_%H%M%S")
    path = CACHE_DIR / supplier_name / f"{ts}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def _download_supplier_csv(*, supplier_name: str, url: str, timeout_sec: int = 120) -> bytes:
    """
    Registry-s letöltés:
    - ha van supplier-specifikus downloader -> azt használja
    - különben generic downloader
    """
    fn = get_downloader(supplier_name)
    if fn is None:
        return download_generic(url, timeout_sec=timeout_sec)
    return fn(url, timeout_sec)


def _get_latest_cache_file(supplier_name: str) -> Optional[Path]:
    supplier_cache_dir = CACHE_DIR / supplier_name
    if not supplier_cache_dir.exists():
        return None

    files = sorted(
        supplier_cache_dir.glob("*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    return files[0] if files else None


def _is_cache_fresh(path: Path, ttl_seconds: int) -> bool:
    age = time.time() - path.stat().st_mtime
    return age <= ttl_seconds


def ingest_one_supplier_csv(supplier_name: str) -> List[Dict[str, Any]]:
    supplier_dir = SUPPLIERS_DIR / supplier_name
    if not supplier_dir.exists():
        raise FileNotFoundError(f"Nincs ilyen mappa: {supplier_dir}")

    cfg = load_supplier_csv_config(supplier_dir)

    test_mode = (os.getenv("TEST_MODE") or "").strip() == "1"

    data: bytes

    # ------------------------------------------------
    # TEST MODE: cache-ből dolgozzunk, ha friss
    # ------------------------------------------------
    if test_mode and cfg.cache.enabled:
        latest_cache = _get_latest_cache_file(cfg.name)

        if latest_cache and _is_cache_fresh(latest_cache, cfg.cache.ttl_seconds):
            print(f"[{cfg.name}] TEST MODE cache HIT → {latest_cache.name}")
            data = latest_cache.read_bytes()
        else:
            print(f"[{cfg.name}] TEST MODE cache MISS → downloading")
            data = _download_supplier_csv(
                supplier_name=cfg.name,
                url=cfg.url,
                timeout_sec=120,
            )
            cache_write(cfg.name, data)

    # ------------------------------------------------
    # NORMAL MODE (marad a jelenlegi logika)
    # ------------------------------------------------
    else:
        data = _download_supplier_csv(
            supplier_name=cfg.name,
            url=cfg.url,
            timeout_sec=120,
        )

        if cfg.cache.enabled:
            cache_write(cfg.name, data)

    # ------------------------------------------------
    # CSV parse
    # ------------------------------------------------
    rows = parse_csv_bytes(
        data,
        encoding=cfg.encoding,
        delimiter=cfg.delimiter,
        has_header=cfg.has_header,
    )

    for r in rows:
        r["_supplier"] = cfg.name

    return rows


def ingest_all_suppliers_csv() -> List[Dict[str, Any]]:
    """
    Az összes CSV típusú beszállító ingest folyamata.
    """
    if not SUPPLIERS_DIR.exists():
        return []

    out: List[Dict[str, Any]] = []

    for d in SUPPLIERS_DIR.iterdir():
        if not d.is_dir():
            continue
        supplier_json = d / "supplier.json"
        if not supplier_json.exists():
            continue

        cfg = _read_json(supplier_json)
        if cfg.get("type", "").lower() == "csv":
            out.extend(ingest_one_supplier_csv(d.name))

    return out