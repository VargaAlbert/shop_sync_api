from __future__ import annotations
from typing import Any, Dict, List, Optional
import json
from pathlib import Path

SUPPLIERS_DIR = Path("config") / "suppliers"


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_mapping(supplier_name: str) -> Dict[str, List[str]]:
    p = SUPPLIERS_DIR / supplier_name / "mapping.json"
    return _read_json(p)


def _first(row: Dict[str, Any], keys: List[str]) -> Any:
    for k in keys:
        v = row.get(k)
        if v not in (None, ""):
            return v
    return None


def _to_float(v: Any) -> Optional[float]:
    if v in (None, ""):
        return None
    s = str(v).strip().replace(" ", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def _clean(v: Any) -> str:
    return str(v).strip() if v not in (None, "") else ""


def normalize_natura_rows(raw_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    m = load_mapping("natura")
    out: List[Dict[str, Any]] = []

    for r in raw_rows:
        sku = _clean(_first(r, m["sku"]))
        if not sku:
            continue

        item = {
            # Shoprenter SKU (régi logika)
            "sku": sku,

            # Natura mezők -> belső Product-szerű mezők
            "model": _clean(_first(r, m["model"])),
            "gtin": _clean(_first(r, m["gtin"])),
            "name_hu": _clean(_first(r, m["name"])),
            "unit_name": _clean(_first(r, m["unit_name"])),

            "gross_price": _to_float(_first(r, m["gross_price"])),
            "wholesale_price": _to_float(_first(r, m["wholesale_price"])),

            "manufacturer_name": _clean(_first(r, m["manufacturer_name"])),
            "tax_class_id": _clean(_first(r, m["tax_class_id"])),

            "csoport1_name": _clean(_first(r, m["csoport1_name"])),

            "raw": r,
        }
        out.append(item)

    return out