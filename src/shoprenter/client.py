from __future__ import annotations

"""
shoprenter.client
=================

Shoprenter API kliens (minimál, de bővíthető) HTTP Basic Auth-al.

Feladat
-------
Ez az osztály egységesíti a Shoprenter API-hívásokat:
- session + auth + alap header beállítások
- create / update műveletek a productExtend erőforráson
- SKU alapján id keresés (ha a Shoprenter API támogatja a szűrést)
- lapozott lekérés a productExtend listából (sku_map építéshez)

Megjegyzés
----------
A Shoprenter API pontos szűrési szintaxisa projektenként eltérhet.
A find_product_id_by_sku() implementáció ezért "best effort" jellegű:
- ha a /products endpoint nem támogatja a sku paramot és 400-at ad,
  akkor None-t ad vissza.
"""

import requests
from requests.auth import HTTPBasicAuth
from typing import Dict, Any, Optional


class ShoprenterClient:
    """
    Egyszerű Shoprenter API kliens.

    Konstruktor:
        ShoprenterClient(base_url, user, password)

    Belső működés:
        - requests.Session-t használ újrahasznosítható kapcsolatokhoz
        - HTTP Basic Auth-tal autentikál
        - JSON request/response header-eket állít be

    Attribútumok:
        base_url (str):
            A Shoprenter API base URL, pl. "https://.../api".
            A végéről a "/" le van vágva.

        session (requests.Session):
            Az auth-olt session.

        timeout (int):
            Alap timeout másodpercben (jelenleg 30).
    """

    def __init__(self, base_url: str, user: str, password: str):
        # Base URL normalizálása: ne legyen a végén "/"
        self.base_url = base_url.rstrip("/")

        # Session: connection pooling + közös auth/header
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(user, password)

        # Alap header-ek JSON API-hoz
        self.session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )

        # Default timeout (használva a get_product_extend_page-ben)
        self.timeout = 30

    def create_product(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Új termék létrehozása (productExtend).

        Paraméter:
            payload (Dict[str, Any]):
                Shoprenter API kompatibilis create payload.

        Visszatérési érték:
            Dict[str, Any]:
                A Shoprenter API JSON válasza (jellemzően tartalmaz "id"-t).

        Kivétel:
            requests.HTTPError:
                ha a válasz nem 2xx.
        """
        r = self.session.post(f"{self.base_url}/productExtend", json=payload)
        r.raise_for_status()
        return r.json()

    def find_product_id_by_sku(self, sku: str) -> Optional[str]:
        """
        Termék azonosító (id) keresése SKU alapján.

        FIGYELEM:
            A Shoprenter API szűrési paraméterei eltérhetnek.
            Itt egy gyakori mintát használunk: GET /products?sku=...&limit=1&full=0

        Paraméter:
            sku (str): Keresett SKU.

        Visszatérési érték:
            Optional[str]:
                - product id (string), ha találat van
                - None, ha nincs találat vagy a filter nem támogatott

        Működés:
            - /products endpoint meghívása
            - ha 400: feltételezzük, hogy a filter szintaxis nem oké -> None
            - válaszban:
                - items[0].href -> id kinyerése az URL végéből
                - vagy items[0].id

        Kivétel:
            requests.HTTPError:
                egyéb (nem 400) hibakód esetén.
        """
        # A Shoprenter API általában támogat szűrést query parammal (sku=...)
        r = self.session.get(
            f"{self.base_url}/products",
            params={"limit": 1, "sku": sku, "full": 0},
        )

        if r.status_code == 400:
            # Ha nálatok más filter szintaxis van, ezt később finomítjuk.
            return None

        r.raise_for_status()

        data = r.json()
        items = data.get("items", [])
        if not items:
            return None

        # item gyakran csak href-et ad, pl: ".../products/123"
        href = items[0].get("href")
        if href:
            return href.rstrip("/").split("/")[-1]

        # vagy id mező
        return items[0].get("id")

    def update_product(self, product_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Termék frissítése (productExtend/{id}) PUT-tal.

        Paraméterek:
            product_id (str):
                A frissítendő productExtend azonosítója.

            payload (Dict[str, Any]):
                Update payload.
                Megjegyzés: érdemes itt "szűrt" update payloadot használni,
                hogy ne írj felül nem kívánt mezőket.

        Visszatérési érték:
            Dict[str, Any]:
                Shoprenter API válasz JSON-ja.

        Kivétel:
            requests.HTTPError:
                ha a válasz nem 2xx.
        """
        r = self.session.put(f"{self.base_url}/productExtend/{product_id}", json=payload)
        r.raise_for_status()
        return r.json()

    def get_product_extend_page(self, *, page: int, limit: int = 200, full: bool = True) -> dict:
        """
        Lapozott lekérés a productExtend listából (sku_map építéshez / importhoz).

        Paraméterek:
            page (int):
                Oldalszám (Shoprenter API lapozás szerint).
                (Hogy 0- vagy 1-indexelt-e, az API implementációtól függ.)

            limit (int):
                Oldalméret (alap: 200).

            full (bool):
                Ha True, a "full=1" paraméterrel részletesebb objektumokat kér.
                Ha False, "full=0" (gyorsabb, kisebb payload).

        Visszatérési érték:
            dict:
                A Shoprenter API válasza (általában items + paging/meta).

        Kivétel:
            requests.HTTPError:
                ha a válasz nem 2xx.

        Megjegyzés:
            A timeout itt self.timeout értéket használja.
        """
        r = self.session.get(
            f"{self.base_url}/productExtend",
            params={"page": page, "limit": limit, "full": 1 if full else 0},
            timeout=self.timeout,
        )
        r.raise_for_status()
        return r.json()