from __future__ import annotations

import base64
import hashlib
import mimetypes
import os
import re
import unicodedata
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, unquote

from src.core.io.http import download_bytes


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

# ------------------------------------------------------------------
# ÚJ: Shoprenter file upload helper-ek
# ------------------------------------------------------------------
_ALLOWED_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif"}
_DEFAULT_EXT = ".jpg"


def _sanitize_file_stem(value: str) -> str:
    value = norm_text(value)
    if not value:
        return ""
    value = value.replace(" ", "-")
    value = re.sub(r"[^A-Za-z0-9_\-\(\)]", "", value)
    return value.strip("-_")


def _guess_ext_from_url(image_url: str) -> str:
    parsed = urlparse(image_url or "")
    raw_name = Path(unquote(parsed.path)).name
    ext = Path(raw_name).suffix.lower()

    if ext in _ALLOWED_EXTS:
        return ".jpg" if ext == ".jpeg" else ext

    guessed, _ = mimetypes.guess_type(raw_name)
    if guessed:
        ext2 = mimetypes.guess_extension(guessed) or ""
        ext2 = ext2.lower()
        if ext2 in _ALLOWED_EXTS:
            return ".jpg" if ext2 == ".jpeg" else ext2

    return _DEFAULT_EXT


def _filename_from_url(image_url: str) -> str:
    parsed = urlparse(image_url or "")
    raw_name = Path(unquote(parsed.path)).name
    return raw_name or ""


def _stable_hash(text: str, length: int = 10) -> str:
    return hashlib.sha1((text or "").encode("utf-8")).hexdigest()[:length]


def supplier_folder_name(supplier_name: str) -> str:
    s = clean_folder_name((supplier_name or "").strip())
    return (s or "SUPPLIER").upper()


def build_supplier_image_filepath(
    *,
    supplier_name: str,
    image_url: str,
    sku: Optional[str] = None,
    model: Optional[str] = None,
) -> str:
    """
    Stabil Shoprenter filePath:
      product/HALDEPO/149088-001.jpg
      product/CARPZOOM/CZ7809.jpg

    Ha az URL-ből nem kapunk normális nevet, fallback:
      sku / model / hash
    """
    folder = supplier_folder_name(supplier_name)
    ext = _guess_ext_from_url(image_url)

    raw_filename = _filename_from_url(image_url)
    raw_stem = Path(raw_filename).stem if raw_filename else ""

    stem = _sanitize_file_stem(raw_stem)
    if not stem:
        stem = _sanitize_file_stem(str(model or ""))
    if not stem:
        stem = _sanitize_file_stem(str(sku or ""))
    if not stem:
        stem = f"img-{_stable_hash(image_url)}"

    return f"product/{folder}/{stem}{ext}"


def download_image_bytes(image_url: str, *, timeout_sec: int = 120) -> bytes:
    url = str(image_url or "").strip()
    if not url:
        raise ValueError("image_url is empty")

    headers = {
        "User-Agent": os.getenv(
            "SHOPSYNC_IMAGE_USER_AGENT",
            "Mozilla/5.0 (compatible; ShopSync/1.0)",
        )
    }
    return download_bytes(url, timeout_sec=timeout_sec, headers=headers)


def bytes_to_base64(data: bytes) -> str:
    if not data:
        raise ValueError("empty image bytes")
    return base64.b64encode(data).decode("ascii")


def prepare_shoprenter_image_upload(
    *,
    supplier_name: str,
    image_url: str,
    sku: Optional[str] = None,
    model: Optional[str] = None,
) -> dict:
    """
    Visszaad:
    {
        "image_url": ...,
        "file_path": "product/HALDEPO/149088-001.jpg",
        "base64_data": "...."
    }
    """
    file_path = build_supplier_image_filepath(
        supplier_name=supplier_name,
        image_url=image_url,
        sku=sku,
        model=model,
    )
    raw = download_image_bytes(image_url)
    base64_data = bytes_to_base64(raw)

    return {
        "image_url": image_url,
        "file_path": file_path,
        "base64_data": base64_data,
    }