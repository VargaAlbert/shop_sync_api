from __future__ import annotations

import os
import urllib.request
from base64 import b64encode
from typing import Dict, Optional, Tuple


def _basic_auth_header(user: str, password: str) -> str:
    token = b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
    return f"Basic {token}"


def download_bytes(
    url: str,
    *,
    timeout_sec: int = 120,
    headers: Optional[Dict[str, str]] = None,
    basic_auth: Optional[Tuple[str, str]] = None,
) -> bytes:
    req = urllib.request.Request(url, method="GET")
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)

    if basic_auth:
        req.add_header("Authorization", _basic_auth_header(basic_auth[0], basic_auth[1]))

    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        return resp.read()