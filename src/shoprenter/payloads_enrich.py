from __future__ import annotations
from typing import Any, Dict


def build_enrich_update_payload(p: Dict[str, Any], *, language_id: str) -> Dict[str, Any]:
    """
    ENRICH_UPDATE:
      - description
      - mainPicture (TELJES URL)
      - imageAlt
    """

    sku = str(p.get("sku", "")).strip()
    if not sku:
        raise ValueError("Missing sku")

    name = (p.get("name_hu") or "").strip()
    description = (p.get("description_hu") or "").strip()

    payload: Dict[str, Any] = {
        "sku": sku,
        "productDescriptions": [
            {
                "language": {"id": language_id},
                "name": name,
                "description": description,
            }
        ],
    }

    # 🔥 TELJES URL használata
    image_urls = p.get("image_urls") or []

    if image_urls:
        full_url = str(image_urls[0]).strip()

        payload["mainPicture"] = full_url

        model = (p.get("model") or "").strip()

        if name and model:
            payload["imageAlt"] = f"{name} - {model}"
        elif name:
            payload["imageAlt"] = name
        elif model:
            payload["imageAlt"] = model

    return payload