from __future__ import annotations

"""
normalize.supplier_generic
=========================

Ez a modul adja a normalizálási réteg *egyetlen* publikus belépési pontját:

    normalize_rows(supplier_name, raw_rows)

A modul egy registry (regiszter) alapú megoldást használ:
- Minden beszállító normalizálója (pl. normalize/suppliers/natura.py)
  regisztrálja magát egy névvel.
- A rendszer futásidőben a supplier_name alapján kiválasztja a megfelelő
  normalizálót.

Miért jó ez?
------------
- Új beszállító hozzáadása = új fájl + egy @register_normalizer dekorátor.
- Nincs if/else lánc, nincs központi módosítgatás minden új suppliernél.
- Tesztelhető: a normalizálók külön-külön unit tesztelhetők.

Alapelv:
--------
- normalize/suppliers/<supplier>.py: beszállító specifikus "mező hova megy"
- normalize/mapping.py: közös helper/mapping logika
- normalize/supplier_generic.py: registry + dispatcher
"""

from typing import Any, Callable, Dict, List, Optional


# A normalizáló függvény típusa:
# Bemenet: nyers sorok (CSV dict-ek listája)
# Kimenet: normalizált sorok (általában List[Dict[str, Any]])
NormalizerFn = Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]]


# Globális registry: supplier_name -> normalizer függvény
_NORMALIZERS: Dict[str, NormalizerFn] = {}


class UnknownSupplierError(ValueError):
    """
    Akkor dobjuk, ha nincs regisztrált normalizáló a megadott supplier névhez.
    """
    pass


def register_normalizer(supplier_name: str) -> Callable[[NormalizerFn], NormalizerFn]:
    """
    Dekorátor egy normalizáló függvény regisztrálásához.

    Használat a supplier modulban:

        from src.normalize.supplier_generic import register_normalizer

        @register_normalizer("natura")
        def normalize(rows):
            ...

    Paraméter:
        supplier_name (str):
            A beszállító neve, amire a normalizáló vonatkozik.
            Ajánlás: kisbetűs, mappanévvel egyezzen.

    Visszatérési érték:
        Callable: dekorátor, ami regisztrálja a függvényt.

    Kivétel:
        ValueError: ha üres supplier név érkezik.
    """
    if not supplier_name or not supplier_name.strip():
        raise ValueError("supplier_name nem lehet üres a normalizer regisztrációhoz.")

    name = supplier_name.strip().lower()

    def _decorator(fn: NormalizerFn) -> NormalizerFn:
        _NORMALIZERS[name] = fn
        return fn

    return _decorator


def get_registered_suppliers() -> List[str]:
    """
    Visszaadja az összes regisztrált beszállító nevét (rendezve).

    Hasznos debug/admin felületeken, vagy startup check-re.

    Visszatérési érték:
        List[str]: regisztrált supplier nevek listája.
    """
    return sorted(_NORMALIZERS.keys())


def get_normalizer(supplier_name: str) -> NormalizerFn:
    """
    Lekéri a supplier-hez tartozó normalizáló függvényt.

    Paraméter:
        supplier_name (str): beszállító neve (pl. "natura")

    Visszatérési érték:
        NormalizerFn: a regisztrált normalizáló függvény

    Kivétel:
        UnknownSupplierError: ha nincs ilyen supplier regisztrálva.
    """
    name = (supplier_name or "").strip().lower()
    fn = _NORMALIZERS.get(name)

    if fn is None:
        raise UnknownSupplierError(
            f"Nincs normalizáló regisztrálva ehhez a supplierhez: '{supplier_name}'. "
            f"Elérhető: {', '.join(get_registered_suppliers()) or '(nincs)'}"
        )

    return fn


def normalize_rows(
    supplier_name: str,
    raw_rows: List[Dict[str, Any]],
    *,
    ensure_supplier_meta: bool = True,
) -> List[Dict[str, Any]]:
    """
    Normalizálja a nyers sorokat a megadott beszállító szerint.

    Ez az a függvény, amit a rendszer többi része hív (export/merge előtt),
    és nem kell tudnia arról, hogy konkrétan melyik supplier modul mit csinál.

    Paraméterek:
        supplier_name (str):
            A beszállító azonosító neve (pl. "natura").
            Ennek egyeznie kell a regisztrált normalizáló nevével.

        raw_rows (List[Dict[str, Any]]):
            Nyers sorok (CSV parse után).

        ensure_supplier_meta (bool):
            Ha True, akkor ellenőrzi/rásegít, hogy minden normalizált sorban
            legyen '_supplier' mező. (Merge és debug miatt hasznos.)
            Alapértelmezett: True

    Visszatérési érték:
        List[Dict[str, Any]]:
            Normalizált sorok listája (supplier-specifikus mapping alapján).

    Kivétel:
        UnknownSupplierError:
            Ha nincs regisztrált normalizáló.

    Megjegyzés:
        A registry működéséhez a supplier modulokat be kell importálni.
        Ajánlott:
            - src/normalize/suppliers/__init__.py importálja a beszállítókat
            - a projekt belépési pontján (pl. main.py) egyszer importáld:
                import src.normalize.suppliers
    """
    fn = get_normalizer(supplier_name)

    # Lefuttatjuk a beszállító-specifikus normalizálót
    normalized = fn(raw_rows)

    # Opcionális rásegítés: legyen _supplier meta minden sorban
    if ensure_supplier_meta:
        s = (supplier_name or "").strip()
        for r in normalized:
            r.setdefault("_supplier", s)

    return normalized