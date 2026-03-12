from __future__ import annotations

"""
payloads.shoprenter

Egységes belső Product dict -> Shoprenter productExtend payload builder-ek.

Publikus API:
- build_payload(mode, p, **kwargs)

Fő elvek:
- MASTER_CREATE: teljes create payload
- MASTER_UPDATE: csak alap mezők, productDescriptions NINCS
- ENRICH_UPDATE: csak akkor épül, ha van tényleges leírás
- ENRICH_UPDATE productDescriptions már Shoprenter-kompatibilis:
    product + language + name + description
"""

from typing import Any, Dict, Mapping, Optional, Set, Literal, Callable, TypedDict
import json
from pathlib import Path
import os

from src.utils.images import build_shop_image_path, image_alt_from_model


# ---------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------
# FONTOS:
# Ez legyen a shopban ténylegesen létező language resource ID.
# Ha nem jó, ENRICH_UPDATE-nél 40007 hibát kapsz.
DEFAULT_LANGUAGE_ID = os.getenv(
    "SHOPRENTER_LANGUAGE_ID",
    "bGFuZ3VhZ2UtbGFuZ3VhZ2VfaWQ9MQ==",
)

DEFAULT_CATEGORY_ID = os.getenv(
    "SHOPRENTER_DEFAULT_CATEGORY_ID",
    "Y2F0ZWdvcnktY2F0ZWdvcnlfaWQ9MjM4",
)

WHOLESALE_GROUP_NAME_DEFAULT = os.getenv("WHOLESALE_GROUP_NAME", "NAGYKER")


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
        "status",
        "stock1",
        "productDescriptions",
        "productCategoryRelations",
        "mainPicture",
        "imageAlt",
    },
    "MASTER_UPDATE": {
        "sku",
        "modelNumber",
        "gtin",
        "price",
    },
    "ENRICH_UPDATE": {
        "sku",
        "productDescriptions",
        "mainPicture",
        "imageAlt",
    },
    "WHOLESALE_PRICE_UPDATE": {
        "sku",
        "modelNumber",
        "price",
        "_post_actions",
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


def load_category_map_for_supplier(supplier_name: str) -> Optional[Dict[str, str]]:
    p = Path("config") / "suppliers" / supplier_name / "category_map.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


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


def _wholesale_post_actions(p: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    wholesale = p.get("wholesale_price")
    if wholesale is None:
        return None

    return {
        "customer_group_prices": [
            {
                "customer_group_name": WHOLESALE_GROUP_NAME_DEFAULT,
                "price": _fmt_price(float(wholesale)),
            }
        ]
    }


def _build_product_descriptions_for_create_or_update(
    *,
    language_id: str,
    name_hu: str,
    desc_hu: str,
) -> list[Dict[str, Any]]:
    """
    CREATE esetén a régi egyszerű struktúra maradhat.
    """
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
    """
    ENRICH_UPDATE esetén a Shoprenter productDescriptions elemhez
    product + language kapcsolat is kell.
    """
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
        "price": _fmt_price(float(gross)),
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
    }

    return filter_payload(payload, PAYLOAD_FIELDS["MASTER_CREATE"])


def build_master_update_payload(
    p: Dict[str, Any],
    *,
    language_id: str,
) -> Dict[str, Any]:
    """
    MASTER_UPDATE = csak alap mezők.
    NINCS productDescriptions.
    """
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
        "price": _fmt_price(float(gross)),
    }

    return filter_payload(payload, PAYLOAD_FIELDS["MASTER_UPDATE"])


def build_enrich_update_payload(
    p: Dict[str, Any],
    *,
    language_id: str,
    product_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    ENRICH_UPDATE:
    - csak akkor ad payloadot, ha van tényleges leírás
    - productDescriptions Shoprenter-kompatibilis formában épül
    - product_id nélkül nem építünk leírás payloadot
    """
    sku = _require_str(p, "sku", ctx="enrich_update")

    desc_hu = _pick_desc_hu(p)
    if not desc_hu:
        return {}

    if not product_id:
        raise ValueError(f"Missing product_id (enrich_update sku={sku})")

    name_hu = _pick_name_hu(p)
    main_img = _pick_main_image(p)
    model = str(p.get("model") or "").strip() or sku

    payload: Dict[str, Any] = {
        "sku": sku,
        "productDescriptions": _build_product_descriptions_for_enrich(
            product_id=product_id,
            language_id=language_id,
            name_hu=name_hu,
            desc_hu=desc_hu,
        ),
        "mainPicture": main_img,
        "imageAlt": image_alt_from_model(name_hu, model) if main_img else None,
    }

    return filter_payload(payload, PAYLOAD_FIELDS["ENRICH_UPDATE"])


def build_wholesale_price_update_payload(p: Dict[str, Any]) -> Dict[str, Any]:
    sku = _require_str(p, "sku", ctx="wholesale_price_update")

    gross = p.get("gross_price")
    if gross is None:
        raise ValueError(f"Missing gross_price (wholesale_price_update sku={sku})")

    model = str(p.get("model") or "").strip() or sku

    post_actions = _wholesale_post_actions(p)
    if not post_actions:
        raise ValueError(f"Missing wholesale_price (wholesale_price_update sku={sku})")

    payload: Dict[str, Any] = {
        "sku": sku,
        "modelNumber": model,
        "price": _fmt_price(float(gross)),
        "_post_actions": post_actions,
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