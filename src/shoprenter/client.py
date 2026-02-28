from __future__ import annotations
import requests
from requests.auth import HTTPBasicAuth
from typing import Dict, Any, Optional


class ShoprenterClient:
    def __init__(self, base_url: str, user: str, password: str):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(user, password)
        self.session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
        })
        self.timeout = 30


    def create_product(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        r = self.session.post(f"{self.base_url}/productExtend", json=payload)
        r.raise_for_status()
        return r.json()

    def find_product_id_by_sku(self, sku: str) -> Optional[str]:
        # A Shoprenter API általában támogat szűrést query parammal (sku=...)
        r = self.session.get(
            f"{self.base_url}/products",
            params={"limit": 1, "sku": sku, "full": 0},
        )
        if r.status_code == 400:
            # ha nálatok más filter szintaxis van, ezt majd finomítjuk
            return None
        r.raise_for_status()

        data = r.json()
        items = data.get("items", [])
        if not items:
            return None

        # item gyakran csak href-et ad
        href = items[0].get("href")
        if href:
            return href.rstrip("/").split("/")[-1]

        # vagy id mező
        return items[0].get("id")

    def update_product(self, product_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        r = self.session.put(f"{self.base_url}/productExtend/{product_id}", json=payload)
        r.raise_for_status()
        return r.json()

    def get_product_extend_page(self, *, page: int, limit: int = 200, full: bool = True) -> dict:
        r = self.session.get(
            f"{self.base_url}/productExtend",
            params={"page": page, "limit": limit, "full": 1 if full else 0},
            timeout=self.timeout,
        )
        r.raise_for_status()
        return r.json()