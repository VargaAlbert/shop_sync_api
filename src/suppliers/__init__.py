# src/suppliers/__init__.py
from __future__ import annotations

import importlib
import os
import warnings
from pathlib import Path
from typing import Iterable, Optional, Set


def _parse_csv_env(name: str) -> Optional[Set[str]]:
    """
    ENV lista parser:
      - üres / nincs megadva -> None (jelentése: minden supplier)
      - "all" vagy "*" -> None (minden supplier)
      - "natura,haldepo" -> {"natura","haldepo"}
    """
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return None

    lowered = raw.lower().strip()
    if lowered in {"*", "all"}:
        return None

    items = {x.strip().lower() for x in raw.split(",") if x.strip()}
    return items or None


def _discover_supplier_packages(suppliers_dir: Path) -> list[str]:
    """
    Felfedezi a suppliers/<name>/ csomagokat.
    Feltétel:
      - mappa
      - van benne __init__.py (python package)
      - nem __pycache__ és nem "_" prefix
    """
    names: list[str] = []
    for p in suppliers_dir.iterdir():
        if not p.is_dir():
            continue
        if p.name == "__pycache__" or p.name.startswith("_"):
            continue
        if not (p / "__init__.py").exists():
            continue
        names.append(p.name.lower())
    return sorted(names)


def load_suppliers(*, enabled: Optional[Iterable[str]] = None) -> list[str]:
    """
    Betölti a supplier package-eket (import side-effect = register_supplier()).

    enabled:
      - None -> ENV alapján dönt (SUPPLIERS_ENABLED / SUPPLIERS_DISABLED), default: mind
      - Iterable -> explicit allowlist (csak ezek töltődnek)
    """
    suppliers_dir = Path(__file__).resolve().parent
    available = _discover_supplier_packages(suppliers_dir)

    # ENV policy
    env_enabled = _parse_csv_env("SUPPLIERS_ENABLED")
    env_disabled = _parse_csv_env("SUPPLIERS_DISABLED") or set()

    if enabled is not None:
        allow = {x.strip().lower() for x in enabled if str(x).strip()}
    else:
        allow = set(available) if env_enabled is None else set(env_enabled)

    allow -= set(env_disabled)

    loaded: list[str] = []
    missing = sorted([n for n in allow if n not in available])
    if missing:
        warnings.warn(
            f"SUPPLIERS_ENABLED contains unknown suppliers: {missing}. "
            f"Available: {available}"
        )

    for name in available:
        if name not in allow:
            continue
        # Import package: src.suppliers.<name>
        importlib.import_module(f"{__name__}.{name}")
        loaded.append(name)

    return loaded


# Side-effect: a pipeline bootstrapban elég az `import src.suppliers`
# és itt automatikusan betöltjük a (szűrt) supplier-eket.
load_suppliers()