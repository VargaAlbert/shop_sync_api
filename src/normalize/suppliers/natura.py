from __future__ import annotations

"""
normalize.suppliers.natura
=========================

Natura beszállító normalizáló modul.

Feladat
-------
A Natura CSV-ből (parse_csv_bytes -> raw_rows) érkező nyers sorokat egységes belső
struktúrára alakítja, a config/suppliers/natura/mapping.json alapján.

Ez a modul *csak* Natura-specifikus döntéseket tartalmaz:
- mely belső mezőket állítjuk elő
- mely mező kötelező (SKU)
- milyen belső kulcsneveket használunk (pl. name_hu)

A közös helper logika (mapping betöltés, clean/to_float/first_value, stb.)
a normalize.mapping modulban van.

Használat
---------
1) Importáld egyszer a normalizálókat (startupnál):
       import src.normalize.suppliers

2) Normalizálás:
       from src.normalize import normalize_rows
       normalized = normalize_rows("natura", raw_rows)

Mapping elvárás
---------------
A mapping.json-ban az alábbi kulcsok elvártako (minimum ezek):
- "sku"
- "model"
- "gtin"
- "name"
- "unit_name"
- "gross_price"
- "wholesale_price"
- "manufacturer_name"
- "tax_class_id"
- "csoport1_name"

Ha valamelyik hiányzik, a get_str/get_float KeyError-t dob (ez jó, mert
konfig hibát gyorsan észreveszel).
"""

from typing import Any, Dict, List

from src.normalize.mapping import get_float, get_str, load_mapping
from src.normalize.supplier_generic import register_normalizer


@register_normalizer("natura")
def normalize(raw_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Natura nyers sorok normalizálása.

    Bemenet:
        raw_rows: List[Dict[str, Any]]
            Nyers CSV sorok (parse_csv_bytes eredménye).

    Kimenet:
        List[Dict[str, Any]]
            Normalizált "Product-szerű" sorok listája.

    Szabályok:
        - SKU kötelező. Ha nincs SKU (vagy üres), a sor kimarad.
        - Árak float-tá konvertálódnak, hiba esetén None.
        - 'raw' mezőben eltároljuk az eredeti sort debug/trace célokra.

    Megjegyzés:
        A '_supplier' mezőt a supplier_generic.normalize_rows() opcionálisan
        hozzá tudja adni (ensure_supplier_meta=True), de ha ingest közben már
        ráégettél meta mezőt, az is ok.
    """
    m = load_mapping("natura")
    out: List[Dict[str, Any]] = []

    for r in raw_rows:
        # Kötelező azonosító (merge/export alap)
        sku = get_str(r, m, "sku")
        if not sku:
            continue

        item: Dict[str, Any] = {
            # Shoprenter SKU (régi logika) - elsődleges azonosító
            "sku": sku,

            # Natura mezők -> belső egységes mezők
            "model": get_str(r, m, "model"),
            "gtin": get_str(r, m, "gtin"),
            "name_hu": get_str(r, m, "name"),
            "unit_name": get_str(r, m, "unit_name"),

            # Ár mezők
            "gross_price": get_float(r, m, "gross_price"),
            "wholesale_price": get_float(r, m, "wholesale_price"),

            # Egyéb meta / besorolás
            "manufacturer_name": get_str(r, m, "manufacturer_name"),
            "tax_class_id": get_str(r, m, "tax_class_id"),

            # Ezt később felhasználhatod képek útvonalához / kategóriához
            "csoport1_name": get_str(r, m, "csoport1_name"),

            # Nyers adat eltárolása debug/trace célokra
            "raw": r,
        }

        out.append(item)

    return out