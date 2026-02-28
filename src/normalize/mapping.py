from __future__ import annotations

"""
normalize.mapping
=================

Ez a modul tartalmazza a normalizáláshoz szükséges *közös* (supplier-független)
eszközöket:

- supplier mapping betöltése: config/suppliers/<supplier>/mapping.json
- mező-kiválasztás több lehetséges forrás oszlopból (prioritásos)
- alap konverziók és tisztítások (string, float)

Miért van külön fájlban?
------------------------
A cél, hogy a supplier-specifikus normalizálók (pl. normalize/suppliers/natura.py)
csak a "milyen mező hova megy" logikát tartalmazzák, és ne legyen mindenhol
duplikált helper kód.

Elv:
- ami általános (bármely suppliernél használható) -> ide
- ami beszállító-specifikus -> normalize/suppliers/<supplier>.py
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional


# A beszállítók konfigurációinak gyökérmappája.
# Itt várjuk a mapping.json fájlokat:
#   config/suppliers/<supplier_name>/mapping.json
SUPPLIERS_DIR = Path("config") / "suppliers"


def _read_json(path: Path) -> Dict[str, Any]:
    """
    JSON fájl beolvasása és Python dict-ként való visszaadása.

    Paraméter:
        path (Path): A beolvasandó JSON fájl elérési útja.

    Visszatérési érték:
        Dict[str, Any]: A JSON tartalom (dict).

    Kivétel:
        FileNotFoundError: ha a fájl nem létezik.
        json.JSONDecodeError: ha a fájl nem valid JSON.
    """
    return json.loads(path.read_text(encoding="utf-8"))


def load_mapping(supplier_name: str) -> Dict[str, List[str]]:
    """
    Betölti egy beszállító mező-mapping konfigurációját.

    A mapping.json célja, hogy megadja:
      belső mezőnév -> lehetséges forrás oszlopnevek (prioritási sorrendben)

    Példa mapping.json:
        {
          "sku": ["SKU", "Cikkszám"],
          "name": ["Megnevezés", "Terméknév", "Name"],
          "gross_price": ["Bruttó ár", "Gross", "Price"]
        }

    Paraméter:
        supplier_name (str): A beszállító neve (mappanév).

    Visszatérési érték:
        Dict[str, List[str]]: mapping dict.

    Megjegyzés:
        Ha a mapping.json-ban egy mező értéke nem list (pl. string), az hibás
        konfigurációt jelez — ezt ebben a "v1" verzióban nem validáljuk külön,
        hanem a fogyasztó oldalon fog kiderülni (KeyError/TypeError).
        Ha szeretnéd, beépítünk szigorú validációt is.
    """
    p = SUPPLIERS_DIR / supplier_name / "mapping.json"
    return _read_json(p)


def first_value(row: Dict[str, Any], keys: List[str]) -> Any:
    """
    Visszaadja az első nem üres értéket a megadott kulcsok (oszlopnevek) közül.

    Használat:
        first_value(row, ["SKU", "Cikkszám", "ItemNo"])

    Paraméterek:
        row (Dict[str, Any]): Egy nyers CSV sor (DictReader-ből).
        keys (List[str]): Lehetséges oszlopnevek listája prioritás szerint.

    Visszatérési érték:
        Any: Az első érték, ami nem None és nem üres string, különben None.
    """
    for k in keys:
        v = row.get(k)
        if v not in (None, ""):
            return v
    return None


def clean_str(v: Any) -> str:
    """
    Biztonságos string tisztítás.

    Szabályok:
      - None vagy "" -> ""
      - egyéb -> str(v).strip()

    Paraméter:
        v (Any): Bemeneti érték.

    Visszatérési érték:
        str: Tisztított string.
    """
    return str(v).strip() if v not in (None, "") else ""


def to_float(v: Any) -> Optional[float]:
    """
    Rugalmas float konverzió beszállítói értékekhez.

    Kezelt esetek:
      - None vagy "" -> None
      - szóközök eltávolítása: "1 234,56" -> "1234,56"
      - vessző -> pont: "1234,56" -> "1234.56"
      - ha nem parse-olható -> None (nem dob kivételt)

    Paraméter:
        v (Any): Bemeneti érték.

    Visszatérési érték:
        Optional[float]: float, vagy None, ha nem konvertálható.
    """
    if v in (None, ""):
        return None

    s = str(v).strip().replace(" ", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def get_str(row: Dict[str, Any], mapping: Dict[str, List[str]], field: str) -> str:
    """
    Kényelmi függvény: mapping alapján kiválasztja a mezőt és stringgé tisztítja.

    Példa:
        name_hu = get_str(r, m, "name")

    Paraméterek:
        row (Dict[str, Any]): Nyers sor dict.
        mapping (Dict[str, List[str]]): load_mapping() eredménye.
        field (str): A belső mező neve, amit a mapping.json tartalmaz.

    Visszatérési érték:
        str: Tisztított string ("" ha nincs érték).

    Kivétel:
        KeyError: ha a field nincs a mapping-ben.
    """
    return clean_str(first_value(row, mapping[field]))


def get_float(row: Dict[str, Any], mapping: Dict[str, List[str]], field: str) -> Optional[float]:
    """
    Kényelmi függvény: mapping alapján kiválasztja a mezőt és float-tá alakítja.

    Példa:
        gross = get_float(r, m, "gross_price")

    Paraméterek:
        row (Dict[str, Any]): Nyers sor dict.
        mapping (Dict[str, List[str]]): load_mapping() eredménye.
        field (str): A belső mező neve, amit a mapping.json tartalmaz.

    Visszatérési érték:
        Optional[float]: float vagy None.

    Kivétel:
        KeyError: ha a field nincs a mapping-ben.
    """
    return to_float(first_value(row, mapping[field]))