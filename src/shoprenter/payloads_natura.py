from __future__ import annotations

from typing import Any, Dict, Mapping, Optional, Set, Literal, Callable, TypedDict


DEFAULT_CATEGORY_ID = "Y2F0ZWdvcnktY2F0ZWdvcnlfaWQ9MjM4"
WHOLESALE_GROUP_NAME_DEFAULT = "NAGYKER"


PayloadMode = Literal[
    "MASTER_CREATE",
    "MASTER_UPDATE",
    "ENRICH_UPDATE",
    "WHOLESALE_PRICE_UPDATE",
]


# -----------------------------
# Field sets
# -----------------------------
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
        "_post_actions",
        "mainPicture",
        "imageAlt",
    },
    "MASTER_UPDATE": {
        "sku",
        "modelNumber",
        "gtin",
        "price",
        "productDescriptions",
        "_post_actions",
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


# -----------------------------
# Helpers
# -----------------------------
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
        v = (p.get(k) or "").strip()
        if v:
            return v
    return _require_str(p, "sku", ctx="name_fallback")


def _pick_desc_hu(p: Dict[str, Any]) -> str:
    for k in ("description_hu", "desc_hu", "description"):
        v = (p.get(k) or "").strip()
        if v:
            return v
    return ""


def _pick_main_image(p: Dict[str, Any]) -> Optional[str]:
    urls = p.get("image_urls")
    if isinstance(urls, list) and urls:
        u0 = str(urls[0] or "").strip()
        return u0 or None

    for k in ("main_image", "image_url", "image", "mainPicture"):
        v = (p.get(k) or "").strip()
        if v:
            return v
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
        for k in ("category", "category_name", "group1", "CSOPORT1"):
            name = (p.get(k) or "").strip()
            if name and name in category_map:
                return category_map[name]

    for k in ("category_id", "shoprenter_category_id"):
        v = (p.get(k) or "").strip()
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


# -----------------------------
# Builder-ek
# -----------------------------
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

    model = (p.get("model") or "").strip() or sku
    gtin = (p.get("gtin") or p.get("ean") or "").strip() or None

    name_hu = _pick_name_hu(p)
    desc_hu = _pick_desc_hu(p)
    cat_id = _resolve_category_id(p, category_id=category_id, category_map=category_map)

    main_img = _pick_main_image(p)

    pd: Dict[str, Any] = {
        "language_id": language_id,
        "name": name_hu,
    }
    if desc_hu:
        pd["description"] = desc_hu

    payload: Dict[str, Any] = {
        "sku": sku,
        "modelNumber": model,
        "gtin": gtin,
        "price": _fmt_price(float(gross)),
        "status": int(status_value),
        "stock1": int(stock1),
        "productDescriptions": [pd],
        "productCategoryRelations": [{"category_id": cat_id}],
        "mainPicture": main_img,
        "imageAlt": name_hu if main_img else None,
        "_post_actions": _wholesale_post_actions(p),
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

    model = (p.get("model") or "").strip() or sku
    gtin = (p.get("gtin") or p.get("ean") or "").strip() or None

    name_hu = _pick_name_hu(p)
    desc_hu = _pick_desc_hu(p)

    pd: Dict[str, Any] = {
        "language_id": language_id,
        "name": name_hu,
    }
    if desc_hu:
        pd["description"] = desc_hu

    payload: Dict[str, Any] = {
        "sku": sku,
        "modelNumber": model,
        "gtin": gtin,
        "price": _fmt_price(float(gross)),
        "productDescriptions": [pd],
        "_post_actions": _wholesale_post_actions(p),
    }

    return filter_payload(payload, PAYLOAD_FIELDS["MASTER_UPDATE"])


def build_enrich_update_payload(
    p: Dict[str, Any],
    *,
    language_id: str,
) -> Dict[str, Any]:
    sku = _require_str(p, "sku", ctx="enrich_update")

    name_hu = _pick_name_hu(p)
    desc_hu = _pick_desc_hu(p)
    main_img = _pick_main_image(p)

    pd: Dict[str, Any] = {
        "language_id": language_id,
        "name": name_hu,
    }
    if desc_hu:
        pd["description"] = desc_hu

    payload: Dict[str, Any] = {
        "sku": sku,
        "productDescriptions": [pd],
        "mainPicture": main_img,
        "imageAlt": name_hu if main_img else None,
    }

    return filter_payload(payload, PAYLOAD_FIELDS["ENRICH_UPDATE"])


def build_wholesale_price_update_payload(p: Dict[str, Any]) -> Dict[str, Any]:
    sku = _require_str(p, "sku", ctx="wholesale_price_update")

    gross = p.get("gross_price")
    if gross is None:
        raise ValueError(f"Missing gross_price (wholesale_price_update sku={sku})")

    model = (p.get("model") or "").strip() or sku

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


# -----------------------------
# Registry
# -----------------------------
_BUILDERS: Dict[PayloadMode, Callable[..., Dict[str, Any]]] = {
    "MASTER_CREATE": build_master_create_payload,
    "MASTER_UPDATE": build_master_update_payload,
    "ENRICH_UPDATE": build_enrich_update_payload,
    "WHOLESALE_PRICE_UPDATE": build_wholesale_price_update_payload,
}


def build_payload(mode: PayloadMode, p: Dict[str, Any], **kwargs: Any) -> Dict[str, Any]:
    fn = _BUILDERS[mode]
    data = fn(p, **kwargs)  # type: ignore[misc]
    return filter_payload(data, PAYLOAD_FIELDS[mode])