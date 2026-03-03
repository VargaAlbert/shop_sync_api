from __future__ import annotations
import re

_WS = re.compile(r"[\s\-_\/]+", re.UNICODE)

def normalize_match_key(value: str | None) -> str:
    if not value:
        return ""
    s = value.strip().upper()
    s = _WS.sub("", s)
    s = re.sub(r"[^A-Z0-9]", "", s)
    return s