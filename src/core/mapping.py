from __future__ import annotations

"""
core.mapping
============

Supplier-centrikus mező mapping helper.

Mit csinál?
-----------
- supplier mapping betöltése: src/suppliers/<supplier>/mapping.json
- mező-kiválasztás több lehetséges forrás oszlopból (prioritásos)
- alap konverziók és tisztítások (string, float)

Miért ide (core)?
-----------------
A mapping helper beszállító-független, de supplier-centrikus fájlokból dolgozik.
A supplier-ek csak a mapping.json-t és a normalize logikát hozzák.
"""

from functools import lru_cache
from typing import Any, Dict, List, Optional, Union

# mapping[field] elfogadott formák:
#   - ["COL1", "COL2"]   (prioritás)
#   - "COL1"             (egyszerű)
#   - {"path": "COL1"}   (későbbi bővíthetőség)
MappingValue = Union[List[str], str, Dict[str, Any]]
MappingDict = Dict[str, MappingValue]


@lru_cache(maxsize=128)
def load_mapping(supplier_name: str) -> MappingDict:
    """
    Betölti a beszállító mapping.json-ját supplier-centrikus helyről:
      src/suppliers/<supplier_name>/mapping.json
    """
    from src.core.io.supplier_files import load_mapping_json  # lazy import
    data = load_mapping_json((supplier_name or "").strip().lower())
    # runtime-ben toleránsak vagyunk, nem kényszerítünk sémát,
    # a getterek kezelik a különböző formákat.
    return data  # type: ignore[return-value]


def _as_keys(v: MappingValue) -> List[str]:
    """
    mapping érték -> oszlopnevek listája prioritás szerint.
    """
    if isinstance(v, list):
        return [str(x).strip() for x in v if isinstance(x, str) and str(x).strip()]
    if isinstance(v, str):
        s = v.strip()
        return [s] if s else []
    if isinstance(v, dict):
        for k in ("path", "col", "column", "key", "name"):
            if isinstance(v.get(k), str) and v.get(k).strip():
                return [v.get(k).strip()]
        return []
    return []


def first_value(row: Dict[str, Any], keys: List[str]) -> Any:
    """
    Visszaadja az első nem üres értéket a megadott kulcsok (oszlopnevek) közül.
    """
    for k in keys:
        v = row.get(k)
        if v not in (None, ""):
            return v
    return None


def clean_str(v: Any) -> str:
    """
    Biztonságos string tisztítás.
    """
    return str(v).strip() if v not in (None, "") else ""


def to_float(v: Any) -> Optional[float]:
    """
    Rugalmas float konverzió beszállítói értékekhez.
    """
    if v in (None, ""):
        return None
    s = str(v).strip().replace(" ", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def get_str(row: Dict[str, Any], mapping: MappingDict, field: str) -> str:
    """
    mapping alapján kiválasztja a mezőt és stringgé tisztítja.
    """
    if field not in mapping:
        raise KeyError(f"Missing mapping field: {field}")
    keys = _as_keys(mapping[field])
    return clean_str(first_value(row, keys))


def get_float(row: Dict[str, Any], mapping: MappingDict, field: str) -> Optional[float]:
    """
    mapping alapján kiválasztja a mezőt és float-tá alakítja.
    """
    if field not in mapping:
        raise KeyError(f"Missing mapping field: {field}")
    keys = _as_keys(mapping[field])
    return to_float(first_value(row, keys))