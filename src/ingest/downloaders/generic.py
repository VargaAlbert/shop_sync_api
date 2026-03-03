from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse, unquote

import requests


def download_generic(url: str, timeout_sec: int = 120) -> bytes:
    """
    Generic downloader:
    - http/https: requests GET
    - file:///: local file read (bytes)
    """
    u = (url or "").strip()

    # Local file support: file:///C:/... vagy file:/...
    if u.lower().startswith("file:"):
        parsed = urlparse(u)
        # Windows: parsed.path = '/C:/Tarhely/...'
        path_str = unquote(parsed.path)

        # vedd le az elejéről a vezető perjelet, ha Windows drive-os
        if len(path_str) >= 3 and path_str[0] == "/" and path_str[2] == ":":
            path_str = path_str[1:]

        p = Path(path_str)
        if not p.exists():
            raise FileNotFoundError(f"File URL not found: {u} -> {p}")

        return p.read_bytes()

    # Default: HTTP(S)
    r = requests.get(u, headers={"User-Agent": "shop-sync/1.0"}, timeout=timeout_sec)
    r.raise_for_status()
    return r.content