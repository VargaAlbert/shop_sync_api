#/merge/merge_products.ps
from __future__ import annotations
from typing import Any, Dict, Iterable, List

from src.merge.match_key import normalize_match_key
from src.merge.rules import natura_haldepo


def build_master_keys(natura_products: Iterable[Dict[str, Any]]) -> set[str]:
    keys: set[str] = set()
    for p in natura_products:
        # nálad Natura-ban "model" van (modelNumber nincs), de ez így is ok
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
) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    for p in natura_products:
        model = p.get("modelNumber") or p.get("model") or ""
        mk = normalize_match_key(model)
        e = enricher_by_key.get(mk)
        merged.append(natura_haldepo.apply(p, e) if e else p)
    return merged