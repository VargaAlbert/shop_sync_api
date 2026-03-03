from __future__ import annotations
from typing import Any, Dict, Iterable, List, Callable, Optional

from src.merge.match_key import normalize_match_key


def build_master_keys(natura_products: Iterable[Dict[str, Any]]) -> set[str]:
    keys: set[str] = set()
    for p in natura_products:
        model = p.get("modelNumber") or p.get("model") or ""
        mk = normalize_match_key(model)
        if mk:
            keys.add(mk)
    return keys


def index_enricher_by_key(
    enricher_rows: Iterable[Dict[str, Any]],
    master_keys: set[str],
) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for r in enricher_rows:
        mk = r.get("match_key") or ""
        if mk and mk in master_keys:
            out[mk] = r  # last wins
    return out


def merge_master_with_enricher(
    natura_products: List[Dict[str, Any]],
    enricher_by_key: Dict[str, Dict[str, Any]],
    *,
    apply_rule: Callable[[Dict[str, Any], Dict[str, Any]], Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Master + enricher merge.
    A konkrét merge szabályt (apply_rule) kívülről kapja, így nincs hardcoded rule import.
    """
    merged: List[Dict[str, Any]] = []
    for p in natura_products:
        model = p.get("modelNumber") or p.get("model") or ""
        mk = normalize_match_key(model)
        e = enricher_by_key.get(mk)
        merged.append(apply_rule(p, e) if e else p)
    return merged