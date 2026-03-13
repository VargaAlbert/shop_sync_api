from __future__ import annotations

"""
payloads.shoprenter

Egységes belső Product dict -> Shoprenter productExtend payload builder-ek.

Publikus API:
- build_payload(mode, p, **kwargs)

Fő elvek:
- MASTER_CREATE: teljes create payload
- MASTER_UPDATE: csak alap mezők, productDescriptions NINCS
- ENRICH_UPDATE: leírás / kép / nagyker ár frissítés
- A Shoprenter a price mezőt nettóként kezeli
- A vevőcsoport árakhoz a helyes inline mező:
    customerGroupProductPrices
"""

from typing import Any, Dict, Mapping, Optional, Set, Literal, Callable, TypedDict
import json
from pathlib import Path
import os

from src.utils.images import build_shop_image_path, image_alt_from_model


# ---------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------
DEFAULT_LANGUAGE_ID = os.getenv(
    "SHOPRENTER_LANGUAGE_ID",
    "bGFuZ3VhZ2UtbGFuZ3VhZ2VfaWQ9MQ==",
)

DEFAULT_CATEGORY_ID = os.getenv(
    "SHOPRENTER_DEFAULT_CATEGORY_ID",
    "Y2F0ZWdvcnktY2F0ZWdvcnlfaWQ9MjM4",
)

# 27%-os ÁFA tax class
DEFAULT_TAX_CLASS_ID = os.getenv(
    "SHOPRENTER_TAX_CLASS_ID",
    "dGF4Q2xhc3MtdGF4X2NsYXNzX2lkPTEw",
)

# NAGYKER customer group id
WHOLESALE_CUSTOMER_GROUP_ID = os.getenv(
    "SHOPRENTER_WHOLESALE_GROUP_ID",
    "Y3VzdG9tZXJHcm91cC1jdXN0b21lcl9ncm91cF9pZD0xMA==",
)


PayloadMode = Literal[
    "MASTER_CREATE",
    "MASTER_UPDATE",
    "ENRICH_UPDATE",
    "WHOLESALE_PRICE_UPDATE",
]


# ---------------------------------------------------------------------
# Field sets
# ---------------------------------------------------------------------
class PayloadFieldSets(TypedDict):
    MASTER_CREATE: Set[str]
    MASTER_UPDATE: Set[str]
    ENRICH_UPDATE: Set[str]
    WHOLESALE_PRICE_UPDATE: Set[str]


PAYLOAD_FIELDS: PayloadFieldSets = {
    "MASTER_CREATE": {
        "sku",
        "modelNumber",
        "gtin",
        "price",
        "taxClass",
        "status",
        "stock1",
        "productDescriptions",
        "productCategoryRelations",
        "mainPicture",
        "imageAlt",
        "customerGroupProductPrices",
    },
    "MASTER_UPDATE": {
        "sku",
        "modelNumber",
        "gtin",
        "price",
        "taxClass",
        "customerGroupProductPrices",
    },
    "ENRICH_UPDATE": {
        "sku",
        "productDescriptions",
        "mainPicture",
        "imageAlt",
        "customerGroupProductPrices",
    },
    "WHOLESALE_PRICE_UPDATE": {
        "sku",
        "modelNumber",
        "customerGroupProductPrices",
    },
}


# ---------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------
def filter_payload(data: Dict[str, Any], fields: Set[str]) -> Dict[str, Any]:
    return {k: v for k, v in data.items() if k in fields and v is not None}


def _fmt_price(x: float) -> str:
    return f"{float(x):.4f}"


def _require_str(p: Dict[str, Any], key: str, *, ctx: str) -> str:
    v = str(p.get(key, "")).strip()
    if not v:
        raise ValueError(f"Missing {key} ({ctx})")
    return v


def _pick_name_hu(p: Dict[str, Any]) -> str:
    for k in ("name_hu", "product_name_hu", "name", "title_hu"):
        v = str(p.get(k) or "").strip()
        if v:
            return v
    return _require_str(p, "sku", ctx="name_fallback")


def _pick_desc_hu(p: Dict[str, Any]) -> str:
    for k in ("description_hu", "desc_hu", "description"):
        v = str(p.get(k) or "").strip()
        if v:
            return v
    return ""


def _pick_main_image(p: Dict[str, Any]) -> Optional[str]:
    urls = p.get("image_urls")
    if isinstance(urls, list) and urls:
        u0 = str(urls[0] or "").strip()
        return u0 or None

    for k in ("main_image", "image_url", "image", "mainPicture"):
        v = str(p.get(k) or "").strip()
        if v:
            return v
    return None


def _pick_prepared_shoprenter_main_image(p: Dict[str, Any]) -> Optional[str]:
    """
    Előnyben részesítjük azt a képet, amit a live_runner már feltöltött
    a Shoprenter /files endpointon, és belső file pathként adott vissza.
    """
    for k in (
        "shoprenter_main_picture",
        "_shoprenter_main_picture",
        "resolved_main_picture",
    ):
        v = str(p.get(k) or "").strip()
        if v:
            return v
    return None


def load_category_map_for_supplier(supplier_name: str) -> Optional[Dict[str, str]]:
    p = Path("config") / "suppliers" / supplier_name / "category_map.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _gross_to_net_27(gross: float) -> float:
    return float(gross) / 1.27


def _tax_class_ref() -> Dict[str, str]:
    return {"id": DEFAULT_TAX_CLASS_ID}


def _customer_group_product_prices(p: Dict[str, Any]) -> list[dict]:
    wholesale = p.get("wholesale_price")
    if wholesale is None:
        return []

    net_price = _gross_to_net_27(float(wholesale))

    return [
        {
            "price": _fmt_price(net_price),
            "customerGroup": {
                "id": WHOLESALE_CUSTOMER_GROUP_ID,
            },
        }
    ]


def _resolve_category_id(
    p: Dict[str, Any],
    *,
    category_id: Optional[str],
    category_map: Optional[Mapping[str, str]],
) -> str:
    if category_id:
        return category_id

    if category_map:
        for k in ("category", "category_name", "group1", "CSOPORT1", "csoport1_name"):
            name = str(p.get(k) or "").strip()
            if name and name in category_map:
                return category_map[name]

    for k in ("category_id", "shoprenter_category_id"):
        v = str(p.get(k) or "").strip()
        if v:
            return v

    return DEFAULT_CATEGORY_ID


def _build_product_descriptions_for_create_or_update(
    *,
    language_id: str,
    name_hu: str,
    desc_hu: str,
) -> list[Dict[str, Any]]:
    item: Dict[str, Any] = {
        "language_id": language_id,
        "name": name_hu,
    }
    if desc_hu:
        item["description"] = desc_hu
    return [item]


def _build_product_descriptions_for_enrich(
    *,
    product_id: str,
    language_id: str,
    name_hu: str,
    desc_hu: str,
) -> list[Dict[str, Any]]:
    item: Dict[str, Any] = {
        "product": {"id": product_id},
        "language": {"id": language_id},
        "name": name_hu,
        "description": desc_hu,
    }
    return [item]


# ---------------------------------------------------------------------
# Mode-specifikus builder-ek
# ---------------------------------------------------------------------
def build_master_create_payload(
    p: Dict[str, Any],
    *,
    language_id: str,
    status_value: int = 0,
    stock1: int = 0,
    category_id: Optional[str] = None,
    category_map: Optional[Mapping[str, str]] = None,
) -> Dict[str, Any]:
    sku = _require_str(p, "sku", ctx="master_create")

    gross = p.get("gross_price")
    if gross is None:
        raise ValueError(f"Missing gross_price (master_create sku={sku})")

    model = str(p.get("model") or "").strip() or sku
    gtin = str(p.get("gtin") or p.get("ean") or "").strip() or None

    name_hu = _pick_name_hu(p)
    desc_hu = _pick_desc_hu(p)
    cat_id = _resolve_category_id(p, category_id=category_id, category_map=category_map)

    main_img = _pick_prepared_shoprenter_main_image(p)
    if not main_img:
        main_img = _pick_main_image(p)

    if not main_img:
        csoport1 = str(p.get("csoport1_name") or "").strip()
        generated = build_shop_image_path(csoport1, model, slot=1, ext=".jpg")
        if generated:
            main_img = generated

    payload: Dict[str, Any] = {
        "sku": sku,
        "modelNumber": model,
        "gtin": gtin,
        "price": _fmt_price(_gross_to_net_27(float(gross))),
        "taxClass": _tax_class_ref(),
        "status": int(status_value),
        "stock1": int(stock1),
        "productDescriptions": _build_product_descriptions_for_create_or_update(
            language_id=language_id,
            name_hu=name_hu,
            desc_hu=desc_hu,
        ),
        "productCategoryRelations": [{"category_id": cat_id}],
        "mainPicture": main_img,
        "imageAlt": image_alt_from_model(name_hu, model) if main_img else None,
        "customerGroupProductPrices": _customer_group_product_prices(p),
    }

    return filter_payload(payload, PAYLOAD_FIELDS["MASTER_CREATE"])


def build_master_update_payload(
    p: Dict[str, Any],
    *,
    language_id: str,
) -> Dict[str, Any]:
    sku = _require_str(p, "sku", ctx="master_update")

    gross = p.get("gross_price")
    if gross is None:
        raise ValueError(f"Missing gross_price (master_update sku={sku})")

    model = str(p.get("model") or "").strip() or sku
    gtin = str(p.get("gtin") or p.get("ean") or "").strip() or None

    payload: Dict[str, Any] = {
        "sku": sku,
        "modelNumber": model,
        "gtin": gtin,
        "price": _fmt_price(_gross_to_net_27(float(gross))),
        "taxClass": _tax_class_ref(),
        "customerGroupProductPrices": _customer_group_product_prices(p),
    }

    return filter_payload(payload, PAYLOAD_FIELDS["MASTER_UPDATE"])


def build_enrich_update_payload(
    p: Dict[str, Any],
    *,
    language_id: str,
    product_id: Optional[str] = None,
) -> Dict[str, Any]:
    sku = _require_str(p, "sku", ctx="enrich_update")

    desc_hu = _pick_desc_hu(p)
    name_hu = _pick_name_hu(p)
    model = str(p.get("model") or "").strip() or sku

    main_img = _pick_prepared_shoprenter_main_image(p)
    if not main_img:
        main_img = _pick_main_image(p)

    customer_group_prices = _customer_group_product_prices(p)

    has_description_part = bool(desc_hu)
    has_image_part = bool(main_img)
    has_wholesale_part = bool(customer_group_prices)

    if not (has_description_part or has_image_part or has_wholesale_part):
        return {}

    payload: Dict[str, Any] = {
        "sku": sku,
        "mainPicture": main_img,
        "imageAlt": image_alt_from_model(name_hu, model) if main_img else None,
        "customerGroupProductPrices": customer_group_prices,
    }

    if has_description_part:
        if not product_id:
            raise ValueError(f"Missing product_id (enrich_update sku={sku})")

        payload["productDescriptions"] = _build_product_descriptions_for_enrich(
            product_id=product_id,
            language_id=language_id,
            name_hu=name_hu,
            desc_hu=desc_hu,
        )

    return filter_payload(payload, PAYLOAD_FIELDS["ENRICH_UPDATE"])


def build_wholesale_price_update_payload(p: Dict[str, Any]) -> Dict[str, Any]:
    sku = _require_str(p, "sku", ctx="wholesale_price_update")
    model = str(p.get("model") or "").strip() or sku

    customer_group_prices = _customer_group_product_prices(p)
    if not customer_group_prices:
        raise ValueError(f"Missing wholesale_price (wholesale_price_update sku={sku})")

    payload: Dict[str, Any] = {
        "sku": sku,
        "modelNumber": model,
        "customerGroupProductPrices": customer_group_prices,
    }

    return filter_payload(payload, PAYLOAD_FIELDS["WHOLESALE_PRICE_UPDATE"])


# ---------------------------------------------------------------------
# build_payload registry
# ---------------------------------------------------------------------
_BUILDERS: Dict[PayloadMode, Callable[..., Dict[str, Any]]] = {
    "MASTER_CREATE": build_master_create_payload,
    "MASTER_UPDATE": build_master_update_payload,
    "ENRICH_UPDATE": build_enrich_update_payload,
    "WHOLESALE_PRICE_UPDATE": build_wholesale_price_update_payload,
}


def build_payload(mode: PayloadMode, p: Dict[str, Any], **kwargs) -> Dict[str, Any]:
    if mode not in _BUILDERS:
        raise KeyError(f"Unknown payload mode: {mode}")
    return _BUILDERS[mode](p, **kwargs)


# ---------------------------------------------------------------------
# Runner-friendly API
# ---------------------------------------------------------------------
def build_product_extend_from_product(
    p: Dict[str, Any],
    *,
    language_id: str = DEFAULT_LANGUAGE_ID,
    status_value: int = 0,
    stock1: int = 0,
    category_id: Optional[str] = None,
    category_map: Optional[Mapping[str, str]] = None,
) -> Dict[str, Any]:
    if category_map is None:
        sname = str(p.get("supplier") or "").strip().lower()
        if sname:
            category_map = load_category_map_for_supplier(sname)

    return build_master_create_payload(
        p,
        language_id=language_id,
        status_value=status_value,
        stock1=stock1,
        category_id=category_id,
        category_map=category_map,
    )


def build_update_payload_from_full(full_payload: Dict[str, Any]) -> Dict[str, Any]:
    return filter_payload(dict(full_payload), PAYLOAD_FIELDS["MASTER_UPDATE"])