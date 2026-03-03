#/merge/rules/base.py
from typing import Dict, Any

def copy_if_empty(target: Dict[str, Any], source: Dict[str, Any], field: str):
    if source.get(field) and not target.get(field):
        target[field] = source[field]