from __future__ import annotations

from typing import Any, Dict, Optional, Mapping
from src.utils.images import build_shop_image_path, image_alt_from_model

DEFAULT_CATEGORY_ID = "Y2F0ZWdvcnktY2F0ZWdvcnlfaWQ9MjM4"

UPDATE_FIELDS = {
    "sku",
    "price",
    "stock1",
    # ha akarod:
    # "status",
    # "productDescriptions",  # csak ha név is frissül
}

def build_update_payload_from_full(full_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    A full create payloadból csak az update-hez szükséges mezőket hagyja meg.
    """
    return {
        k: v
        for k, v in full_payload.items()
        if k in UPDATE_FIELDS and v is not None
    }

def build_product_extend_from_natura(
    p: Dict[str, Any],
    *,
    language_id: str,  # pl. "bGFuZ3VhZ2UtbGFuZ3VhZ2VfaWQ9MQ=="
    status_value: int = 0,
    stock1: int = 0,
    manufacturer_id: Optional[str] = None,
    tax_class_id: Optional[str] = None,
    # Kategória választás:
    category_id: Optional[str] = None,
    category_map: Optional[Mapping[str, str]] = None,  # pl. {"Etetőanyag": "Y2F0ZW..."}
    # Kép:
    image_slot: int = 1,
) -> Dict[str, Any]:
    """
    p: normalize_natura_rows() egy eleme.

    category_id: ha fixen megadod, ezt használja
    category_map: ha megadod, p["csoport1_name"] (vagy p["CSOPORT1"]) alapján választ
    """

    sku = str(p.get("sku", "")).strip()
    if not sku:
        raise ValueError("Missing sku")

    name_hu = (p.get("name_hu") or "").strip()
    if not name_hu:
        raise ValueError(f"Missing name_hu for sku={sku}")

    gross = p.get("gross_price")
    if gross is None:
        raise ValueError(f"Missing gross_price for sku={sku}")

    # Modell/cikkszám alap: ha van külön "model", azt preferáljuk, különben sku
    model = (p.get("model") or "").strip() or sku

    csoport1 = (p.get("csoport1_name") or p.get("CSOPORT1") or "").strip()

    # ---- kategória ID kiválasztása (CSOPORT1 -> map -> id) ----
    """
    resolved_category_id = category_id
    if not resolved_category_id and category_map and csoport1:
        resolved_category_id = category_map.get(csoport1)

    if not resolved_category_id:
    """
    resolved_category_id = DEFAULT_CATEGORY_ID

    # ---- képútvonal + alt ----
    # A te utiljaid: folder = csoport1, fájlnév = model (slot szerint)
    image_path = build_shop_image_path(
        csoport1=csoport1,
        model=model,
        slot=1,
    )

    image_alt = image_alt_from_model(model)

    payload: Dict[str, Any] = {
        "sku": sku,
        # Ha nálatok van ilyen mező; ha az API nem fogadja el, később kivesszük
        "modelNumber": model,
        "gtin": (p.get("gtin") or "").strip(),

        # Shoprenter gyakran stringeket vár
        "price": f"{float(gross):.4f}",
        "status": str(int(status_value)),
        "stock1": str(int(stock1)),

        # Leírások (doksi szerint itt van a name)
        "productDescriptions": [
            {
                "name": name_hu,
                "language": {"id": language_id},
            }
        ],

        # Kategória hozzárendelés (doksi szerint így jó)
        "productCategoryRelations": [
            {"category": {"id": resolved_category_id}}
        ],
    }

    # Kép mezők: a válaszokban "mainPicture" van; létrehozásnál is általában elfogadja
    # (Ha mégsem, akkor ezt később levesszük és külön image endpointtal töltöd)
    if image_path:
        payload["mainPicture"] = image_path
        payload["imageAlt"] = image_alt

    if manufacturer_id:
        payload["manufacturer"] = {"id": manufacturer_id}

    if tax_class_id:
        payload["taxClass"] = {"id": tax_class_id}

    # Debug mezők: csak akkor tedd be, ha biztosan nem dob 400-at
    # (Sok API elutasítja az ismeretlen mezőket.)
    payload["_debug"] = {
        "unit_name": (p.get("unit_name") or "").strip(),
        "manufacturer_name": (p.get("manufacturer_name") or "").strip(),
        "tax_class_raw": (p.get("tax_class_id") or "").strip(),
        "wholesale_price": p.get("wholesale_price"),
        "csoport1": csoport1,
    }

    return payload