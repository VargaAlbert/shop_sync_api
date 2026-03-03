from __future__ import annotations

import re
import unicodedata


def norm_text(s: str) -> str:
    """Alap normalizálás: whitespace trim + belső space-ek összevonása."""
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s


def clean_sku(value: str) -> str:
    """
    SKU / model fájlnévhez:
    - trim
    - space -> -
    - csak biztonságos karakterek: A-Z a-z 0-9 _ -
    """
    s = norm_text(value)
    if not s:
        return ""
    s = s.replace(" ", "-")
    s = re.sub(r"[^A-Za-z0-9_\-]", "", s)
    return s


def clean_folder_name(value: str) -> str:
    """
    Mappanévhez (CSOPORT1):
    - ékezetek eltávolítása
    - szóköz -> -
    - tiltott karakterek törlése
    """
    s = norm_text(value)
    if not s:
        return ""

    # ékezetek le
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))

    s = s.replace(" ", "-")
    s = re.sub(r"[^A-Za-z0-9_\-]", "", s)
    return s


def build_shop_image_path(csoport1: str, model: str, slot: int = 1, ext: str = ".jpg") -> str:
    folder = clean_folder_name(csoport1).lower()
    base = clean_sku(model)

    if not base or not folder:
        return ""

    if slot == 1:
        fname = f"{base}{ext}"
    else:
        fname = f"{base}-{slot-1}{ext}"

    return f"product/{folder}/{fname}"


def image_alt_from_model(name: str, model: str, *, keyword: str = "horgász termék") -> str:
    """
    SEO-barát ALT:
    - tartalmazza a terméknevet (ha van)
    - tartalmazza a modellt/cikkszámot (ha van)
    - opcionális kulcsszó
    """
    name = norm_text(name)
    model = norm_text(model)

    parts = []
    if name:
        parts.append(name)
    if model:
        parts.append(model)

    base = " - ".join(parts).strip()

    if base and keyword:
        return f"{base} | {keyword}"
    if base:
        return base
    return "Termékkép"
