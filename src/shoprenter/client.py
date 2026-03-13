from __future__ import annotations

import requests
from requests.auth import HTTPBasicAuth
from typing import Dict, Any, Optional


class ShoprenterClient:
    def __init__(self, base_url: str, user: str, password: str):
        self.base_url = (base_url or "").rstrip("/")
        if not self.base_url:
            raise ValueError("ShoprenterClient: base_url is empty")

        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(user, password)
        self.session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )
        self.timeout = 30

    def _url(self, path: str) -> str:
        path = path or ""
        if path.startswith("http://") or path.startswith("https://"):
            return path
        if not path.startswith("/"):
            path = "/" + path
        return f"{self.base_url}{path}"

    def create_product(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        resp = self.session.post(
            self._url("/productExtend"),
            json=payload,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        if not resp.text.strip():
            return {}
        return resp.json()

    def update_product(self, product_extend_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        resp = self.session.put(
            self._url(f"/productExtend/{product_extend_id}"),
            json=payload,
            timeout=self.timeout,
        )

        resp.raise_for_status()
        if not resp.text.strip():
            return {}
        return resp.json()

    def delete_product(self, product_extend_id: str) -> None:
        resp = self.session.delete(
            self._url(f"/productExtend/{product_extend_id}"),
            timeout=self.timeout,
        )
        resp.raise_for_status()

    def get_product_extend_page(
        self,
        *,
        page: int = 1,
        limit: int = 200,
        full: bool = False,
    ) -> Dict[str, Any]:
        resp = self.session.get(
            self._url("/productExtend"),
            params={
                "page": page,
                "limit": limit,
                "full": str(full).lower(),
            },
            timeout=self.timeout,
        )
        resp.raise_for_status()
        if not resp.text.strip():
            return {}
        return resp.json()

    def find_product_id_by_sku(self, sku: str) -> Optional[str]:
        sku = str(sku or "").strip()
        if not sku:
            return None

        try:
            resp = self.session.get(
                self._url("/productExtend"),
                params={
                    "sku": sku,
                    "limit": 1,
                    "full": "true",
                },
                timeout=self.timeout,
            )
            resp.raise_for_status()
            data = resp.json() if resp.text.strip() else {}
        except requests.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            if status == 400:
                return None
            raise

        items = data.get("items") or []
        if not items:
            return None

        item = items[0] or {}
        pid = item.get("id")
        return str(pid) if pid else None

    def upload_file(
        self,
        *,
        file_path: str,
        base64_content: str,
        file_type: str = "image",
    ) -> Dict[str, Any]:
        payload = {
            "filePath": file_path,
            "type": file_type,
            "attachment": base64_content,
        }

        resp = self.session.post(
            self._url("/files"),
            json=payload,
            timeout=max(self.timeout, 60),
        )
        resp.raise_for_status()

        if not resp.text.strip():
            return {}

        return resp.json()