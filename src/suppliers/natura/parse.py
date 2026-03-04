from __future__ import annotations

import json


def parse(raw_bytes: bytes) -> list[dict]:
    # ingest JSON bytes -> list[dict]
    if not raw_bytes:
        return []
    return json.loads(raw_bytes.decode("utf-8", errors="replace"))