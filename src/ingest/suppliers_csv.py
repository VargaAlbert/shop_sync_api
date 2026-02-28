from __future__ import annotations

import json
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

from src.ingest.parsers.csv_parser import parse_csv_bytes

# Konfigurációs mappák
SUPPLIERS_DIR = Path("config") / "suppliers"
CACHE_DIR = Path("data") / "cache" / "suppliers"


@dataclass(frozen=True)
class SupplierCsvConfig:
    """
    Egy CSV típusú beszállító konfigurációs modellje.

    Attribútumok:
        name (str):
            A beszállító neve.

        url (str):
            A CSV forrás URL-je.

        encoding (str):
            A CSV fájl karakterkódolása.
            Alapértelmezett: utf-8

        delimiter (str):
            A CSV mezőelválasztó karakter.
            Alapértelmezett: ","
    """
    name: str
    url: str
    encoding: str = "utf-8"
    delimiter: str = ","


def _read_json(path: Path) -> Dict[str, Any]:
    """
    JSON fájl beolvasása és Python dict-ként való visszaadása.

    Paraméter:
        path (Path): A JSON fájl elérési útja.

    Visszatérési érték:
        Dict[str, Any]: A beolvasott JSON tartalom.
    """
    return json.loads(path.read_text(encoding="utf-8"))


def load_supplier_csv_config(supplier_dir: Path) -> SupplierCsvConfig:
    """
    Egy beszállító könyvtárából betölti a CSV konfigurációt.

    A supplier.json fájlból olvassa ki az adatokat.
    Jelenleg kizárólag 'type: csv' támogatott.

    Paraméter:
        supplier_dir (Path): A beszállító mappája.

    Visszatérési érték:
        SupplierCsvConfig: A beszállító konfigurációja.

    Kivétel:
        ValueError: Ha a beszállító típusa nem CSV.
    """
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
    """
    Tartalom letöltése HTTP(S) URL-ről byte formátumban.

    Paraméterek:
        url (str): A letöltendő CSV URL-je.
        timeout_sec (int): Timeout másodpercben.

    Visszatérési érték:
        bytes: A letöltött fájl tartalma.

    Megjegyzés:
        Egyedi User-Agent header kerül beállításra.
    """
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "shop-sync/1.0"}
    )

    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        return resp.read()


def cache_write(supplier_name: str, content: bytes) -> Path:
    """
    Letöltött CSV tartalom mentése cache mappába.

    A fájl időbélyeg alapú néven kerül mentésre:
        YYYYMMDD_HHMMSS.csv

    Paraméterek:
        supplier_name (str): A beszállító neve.
        content (bytes): A mentendő CSV tartalom.

    Visszatérési érték:
        Path: A létrehozott cache fájl elérési útja.
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # Időbélyeg generálása
    ts = time.strftime("%Y%m%d_%H%M%S")

    path = CACHE_DIR / supplier_name / f"{ts}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)

    path.write_bytes(content)

    return path


def ingest_one_supplier_csv(supplier_name: str) -> List[Dict[str, Any]]:
    """
    Egyetlen CSV beszállító teljes ingest folyamata.

    Lépések:
        1. Konfiguráció betöltése
        2. CSV letöltése
        3. Cache-be mentés
        4. CSV parse-olása
        5. Metaadat (_supplier) hozzáadása minden sorhoz

    Paraméter:
        supplier_name (str): A beszállító mappájának neve.

    Visszatérési érték:
        List[Dict[str, Any]]:
            A CSV sorok listája, kiegészítve _supplier mezővel.

    Kivétel:
        FileNotFoundError: Ha a beszállító mappa nem létezik.
    """
    supplier_dir = SUPPLIERS_DIR / supplier_name

    if not supplier_dir.exists():
        raise FileNotFoundError(f"Nincs ilyen mappa: {supplier_dir}")

    # Konfiguráció betöltése
    cfg = load_supplier_csv_config(supplier_dir)

    # CSV letöltése
    data = download_bytes(cfg.url)

    # Cache mentés
    cache_write(cfg.name, data)

    # CSV parse-olása
    rows = parse_csv_bytes(
        data,
        encoding=cfg.encoding,
        delimiter=cfg.delimiter,
    )

    # Minimális meta hozzáadása merge/logika céljából
    for r in rows:
        r["_supplier"] = cfg.name

    return rows


def ingest_all_suppliers_csv() -> List[Dict[str, Any]]:
    """
    Az összes CSV típusú beszállító ingest folyamata.

    A config/suppliers mappában végigiterál,
    és minden 'type: csv' beszállítót feldolgoz.

    Visszatérési érték:
        List[Dict[str, Any]]:
            Az összes beszállító összes sora egyetlen listában.
    """
    if not SUPPLIERS_DIR.exists():
        return []

    out: List[Dict[str, Any]] = []

    for d in SUPPLIERS_DIR.iterdir():
        if d.is_dir() and (d / "supplier.json").exists():

            cfg = _read_json(d / "supplier.json")

            # Jelenleg csak CSV támogatott
            if cfg.get("type", "").lower() == "csv":
                out.extend(ingest_one_supplier_csv(d.name))

    return out