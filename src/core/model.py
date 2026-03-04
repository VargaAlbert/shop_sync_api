from __future__ import annotations

from typing import List, Optional, TypedDict


class Product(TypedDict, total=False):
    """
    Egységes belső adatmodell.

    A normalize lépés után minden supplier ezt adja vissza.
    A merge / pricing / payload réteg csak ezt látja.
    """
    supplier: str
    sku: str
    model: Optional[str]
    gtin: Optional[str]
    match_key: Optional[str]

    name_hu: Optional[str]
    description_hu: Optional[str]
    image_urls: List[str]

    gross_price: Optional[float]
    wholesale_price: Optional[float]

    manufacturer_name: Optional[str]
    category: Optional[str]

    # debug/meta (nem kötelező)
    raw: dict