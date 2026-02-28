# src/app.py
"""
Shop Sync - Központi CLI belépési pont

Cél
----
Ez a modul a teljes szinkron rendszer belépési pontja.
Feladata:
- beszállító kiválasztása (--supplier)
- futtatási mód kiválasztása (--mode)
- megfelelő beszállítói modul meghívása

Ez a fájl NEM tartalmaz üzleti logikát.
A tényleges szinkron logika a src/sync/ mappában található.

Használat
---------
Upsert (create + update):
    python -m src.app --supplier natura --mode upsert

Delete:
    python -m src.app --supplier natura --mode delete

Mindkettő:
    python -m src.app --supplier natura --mode all

Javasolt ütemezés
-----------------
- upsert: 4 óránként
- delete: napi 1 hajnalban

Architektúra elv
----------------
- SUPPLIERS dict → mely beszállítók érhetők el
- ACTIONS dict → milyen műveletek érhetők el
- main() → CLI parsing + dispatch

Bővítés
-------
Új beszállító hozzáadásához:

1) Készíts új modult:
   src/sync/uj_beszallito.py

2) A modulnak implementálnia kell:
   - run_upsert()
   - run_delete()

3) Regisztráld a SUPPLIERS dict-ben:
   SUPPLIERS["uj"] = uj_beszallito

Ennyi. A main logikát nem kell módosítani.
"""

import argparse
import sys
import os

from typing import Callable, Dict
from dotenv import load_dotenv

from src.shoprenter.client import ShoprenterClient

from src.sync import natura
# később:
# from src.sync import masik

load_dotenv()

# ==============================================================
# CLIENT FACTORY
# ==============================================================

def create_client() -> ShoprenterClient:
    """
    Létrehozza a Shoprenter API klienst.
    Minden supplier ugyanazt a klienst kapja.
    """
    return ShoprenterClient(
        base_url=os.getenv("SHOPRENTER_API_URL"),
        user=os.getenv("SHOPRENTER_API_USER"),
        password=os.getenv("SHOPRENTER_API_PASS"),
    )

# ==============================================================
# SUPPLIER REGISTRY
# ==============================================================
"""
Itt regisztráljuk az elérhető beszállítókat.

Kulcs:
    CLI-ből használható név (--supplier)

Érték:
    A modul, amely tartalmazza:
        - run_upsert()
        - run_delete()
"""
SUPPLIERS = {
    "natura": natura,
    # "masik": masik,
}


# ==============================================================
# ACTION REGISTRY
# ==============================================================
"""
Itt definiáljuk az elérhető műveleteket (--mode).

Mindegyik action egy függvény,
amely megkapja a beszállító modult paraméterként.
"""


def run_upsert(supplier, client):
    """Create + Update futtatása."""
    supplier.run_upsert(client=client)


def run_delete(supplier, client):
    """Delete futtatása."""
    supplier.run_delete(client=client)


def run_all(supplier, client):
    """Upsert majd Delete futtatása."""
    supplier.run_upsert(client=client)
    supplier.run_delete(client=client)


ACTIONS: Dict[str, Callable] = {
    "upsert": run_upsert,
    "delete": run_delete,
    "all": run_all,
}


# ==============================================================
# MAIN
# ==============================================================

def main(argv=None) -> int:
    """
    CLI argumentum feldolgozás + dispatch.

    Visszatérési kód:
        0 = siker
        1 = hiba
    """

    parser = argparse.ArgumentParser(
        description="Shop Sync - beszállítói szinkron futtató"
    )

    parser.add_argument(
        "--supplier",
        required=True,
        choices=SUPPLIERS.keys(),
        help="Beszállító neve (pl. natura)",
    )

    parser.add_argument(
        "--mode",
        default="upsert",
        choices=ACTIONS.keys(),
        help="Művelet típusa: upsert | delete | all",
    )

    args = parser.parse_args(argv)

    supplier = SUPPLIERS.get(args.supplier)
    action = ACTIONS.get(args.mode)

    client = create_client()

    try:
        action(supplier, client)
        return 0
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())