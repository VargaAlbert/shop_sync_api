from __future__ import annotations
from typing import Callable, Dict, Optional

DownloaderFn = Callable[[str, int], bytes]  # (url, timeout_sec) -> bytes

_DOWNLOADERS: Dict[str, DownloaderFn] = {}


class UnknownDownloaderError(ValueError):
    pass


def register_downloader(name: str):
    if not name or not name.strip():
        raise ValueError("downloader name cannot be empty")
    key = name.strip().lower()

    def _decorator(fn: DownloaderFn) -> DownloaderFn:
        _DOWNLOADERS[key] = fn
        return fn

    return _decorator


def get_downloader(supplier_name: str) -> Optional[DownloaderFn]:
    return _DOWNLOADERS.get((supplier_name or "").strip().lower())


def list_downloaders() -> list[str]:
    return sorted(_DOWNLOADERS.keys())