from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class SupplierConfig:
    name: str
    type: str  # "csv" (később: "api" stb.)

    # source
    url: str
    headers: Optional[Dict[str, str]] = None
    timeout_sec: int = 120

    # csv
    encoding: str = "utf-8"
    delimiter: str = ";"
    has_header: bool = True

    # cache
    cache_enabled: bool = True
    cache_ttl_sec: int = 86400
    cache_dir: str = "data/cache"

    # auth
    auth: Optional[Dict[str, Any]] = None


def _supplier_dir(supplier_name: str) -> Path:
    # src/suppliers/<supplier>/
    here = Path(__file__).resolve()
    src_root = here.parents[2]  # .../src
    return src_root / "suppliers" / supplier_name


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


@lru_cache(maxsize=128)
def load_supplier_json(supplier_name: str) -> SupplierConfig:
    supplier_name = (supplier_name or "").strip().lower()
    if not supplier_name:
        raise ValueError("supplier_name empty")

    d = _supplier_dir(supplier_name)
    p = d / "supplier.json"
    if not p.exists():
        raise FileNotFoundError(f"Missing supplier.json: {p.resolve()}")

    data = _read_json(p)

    name = (data.get("name") or supplier_name).strip().lower()
    typ = (data.get("type") or "csv").strip().lower()

    # ---- NEW schema: source.url ----
    source = data.get("source") or {}
    url = (source.get("url") or "").strip()

    # ---- Backward compatibility: root url ----
    if not url:
        url = (data.get("url") or "").strip()

    if not url:
        raise ValueError(f"{supplier_name}: supplier.json missing url (expected source.url)")

    headers = source.get("headers") or data.get("headers") or None
    timeout_sec = int(source.get("timeout_sec") or data.get("timeout_sec") or 120)

    # ---- CSV config (prefer new schema csv.*) ----
    csv_cfg = data.get("csv") or {}
    encoding = str(csv_cfg.get("encoding") or data.get("encoding") or "utf-8")
    delimiter = str(csv_cfg.get("delimiter") or data.get("delimiter") or ";")

    if "has_header" in csv_cfg:
        has_header = bool(csv_cfg.get("has_header"))
    else:
        has_header = bool(data.get("has_header", True))

    # ---- Cache config ----
    cache_cfg = data.get("cache") or {}
    cache_enabled = bool(cache_cfg.get("enabled", True))
    cache_ttl_sec = int(cache_cfg.get("ttl_seconds") or cache_cfg.get("ttl_sec") or 86400)
    cache_dir = str(cache_cfg.get("dir") or cache_cfg.get("cache_dir") or "data/cache")

    # ---- Auth (optional) ----
    auth_cfg = data.get("auth") or None

    return SupplierConfig(
        name=name,
        type=typ,
        url=url,
        headers=headers,
        timeout_sec=timeout_sec,
        encoding=encoding,
        delimiter=delimiter,
        has_header=has_header,
        cache_enabled=cache_enabled,
        cache_ttl_sec=cache_ttl_sec,
        cache_dir=cache_dir,
        auth=auth_cfg,
    )


@lru_cache(maxsize=128)
def load_mapping_json(supplier_name: str) -> Dict[str, Any]:
    supplier_name = (supplier_name or "").strip().lower()
    if not supplier_name:
        raise ValueError("supplier_name empty")

    d = _supplier_dir(supplier_name)
    p = d / "mapping.json"
    if not p.exists():
        raise FileNotFoundError(f"Missing mapping.json: {p.resolve()}")

    return _read_json(p)