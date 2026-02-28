from __future__ import annotations

"""
shoprenter.upsert
=================

Egyszerű segédfüggvények egyetlen termék CREATE / UPDATE (UPsert) műveletéhez.

Feladat
-------
A SKU alapján eldönteni:
- ha már létezik a Shoprenterben (sku_map szerint) -> UPDATE
- ha nem létezik -> CREATE

A sku_map egy in-memory cache:
    SKU -> productExtend id

Ez lehetővé teszi, hogy:
- ne kelljen minden SKU-ra külön lekérdezést indítani
- ugyanazon futáson belül elkerüljük a duplikált create-et

Megjegyzés
----------
Nagy mennyiségű termék esetén a batch alapú megoldás hatékonyabb.
Ez a modul egyedi (soronkénti) hívásokhoz ideális.
"""

from typing import Dict, Any


def upsert_product(client, sku_map: dict[str, str], payload: Dict[str, Any]) -> str:
    """
    Egyetlen termék UPsert művelete SKU alapján.

    Működés:
        1) SKU kiolvasása a payload-ból
        2) Ha SKU benne van a sku_map-ben:
              -> UPDATE (PUT /productExtend/{id})
        3) Ha nincs:
              -> CREATE (POST /productExtend)
              -> visszakapott id eltárolása a sku_map-ben

    Paraméterek:
        client:
            ShoprenterClient példány, amely biztosítja:
                - update_product(id, payload)
                - create_product(payload)

        sku_map (dict[str, str]):
            SKU -> productExtend id map.
            A create után frissül, hogy a további műveleteknél
            ugyanazon futáson belül már UPDATE történjen.

        payload (Dict[str, Any]):
            Shoprenter kompatibilis termék payload.
            Kötelező kulcs: "sku"

    Visszatérési érték:
        str:
            "updated" vagy "created"

    Kivétel:
        KeyError:
            Ha a payload nem tartalmaz "sku" kulcsot.
        requests.HTTPError:
            Ha a Shoprenter API hibát ad.
    """
    sku = payload["sku"].strip()

    existing_id = sku_map.get(sku)

    if existing_id:
        # Már létező termék -> UPDATE
        client.update_product(existing_id, payload)
        return "updated"

    # Új termék -> CREATE
    created = client.create_product(payload)

    new_id = created.get("id")
    if new_id:
        # Map frissítése, hogy a futás további részében már UPDATE történjen
        sku_map[sku] = new_id

    return "created"


def upsert_product_with_map(client, sku_map: dict[str, str], payload: dict) -> str:
    """
    Alternatív, minimalista UPsert implementáció.

    Funkcionálisan nagyon hasonló az upsert_product()-hez,
    de kevésbé defensív (pl. nincs strip, nincs id None ellenőrzés).

    Működés:
        - Ha SKU benne van a sku_map-ben -> UPDATE
        - Ha nincs -> CREATE, majd az id eltárolása a map-ben

    Paraméterek:
        client:
            ShoprenterClient példány.

        sku_map (dict[str, str]):
            SKU -> id térkép (futás közbeni cache).

        payload (dict):
            Shoprenter payload, kötelező "sku" kulccsal.

    Visszatérési érték:
        str:
            "updated" vagy "created"

    Megjegyzés:
        Ez a verzió kevésbé robusztus (pl. nem strip-el SKU-t),
        ezért production környezetben inkább az upsert_product()
        ajánlott.
    """
    sku = payload["sku"]
    existing_id = sku_map.get(sku)

    if existing_id:
        client.update_product(existing_id, payload)
        return "updated"

    created = client.create_product(payload)

    # Frissítjük a map-et, hogy ugyanabban a futásban
    # se legyen duplikált create ugyanarra a SKU-ra
    sku_map[sku] = created.get("id")

    return "created"