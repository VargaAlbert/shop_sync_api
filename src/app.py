"""
Shop Sync Runner
================
Ez az alkalmazás a beszállítói adatokat feldolgozza és a Shoprenter API-n keresztül
szinkronizálja a webshop termékeit.

A rendszer 4 külön "csatornát" kezel:

1. MASTER_CREATE_ALL
   ------------------
   Új termékeket hoz létre a Shoprenterben a master beszállító alapján.
   Csak azokat a SKU-kat hozza létre, amelyek még nem léteznek a Shoprenterben.

2. MASTER_UPDATE_ALL
   ------------------
   A már létező termékek alapadatait frissíti:
   - ár
   - készlet
   - név
   - alap termékadatok

3. ENRICH_UPDATE_ALL
   ------------------
   Kiegészítő adatokat frissít:
   - leírás
   - képek
   - egyéb enrich adatok

   Csak azokon a termékeken fut le, ahol az enrich pipeline
   tényleges változást talált.

4. DELETE_ALL
   ------------------
   Azokat a termékeket törli a Shoprenterből,
   amelyek már nem szerepelnek a master beszállító listájában.

   FONTOS:
   A törlés csak akkor történik meg ha a .env fájlban:

       DELETE_ENABLED=1

   Ha ez nincs beállítva, a törlés automatikusan skipelődik
   biztonsági okból.

--------------------------------------------------------------------
PARANCS SOROS FUTTATÁS
--------------------------------------------------------------------

A program a projekt gyökérkönyvtárából futtatható.

Windows / Linux / Mac:

    python -m src.app --mode master_create_all
    python -m src.app --mode master_update_all
    python -m src.app --mode enrich_update_all
    python -m src.app --mode delete_all

--------------------------------------------------------------------
MASTER SUPPLIER MEGADÁSA
--------------------------------------------------------------------

Alapértelmezett master beszállító a .env fájlban:

    MASTER_SUPPLIER=natura

De parancssorban felülírható:

    python -m src.app --mode master_update_all --master natura

--------------------------------------------------------------------
TESZTELÉSI SORREND (AJÁNLOTT)
--------------------------------------------------------------------
1 Új termékek létrehozása

    python -m src.app --mode master_create_all

2 Termékek frissítése

    python -m src.app --mode master_update_all

3 Enrich adatok frissítése (képek, leírás)

    python -m src.app --mode enrich_update_all

4 Törlés tesztelése

    python -m src.app --mode delete_all

--------------------------------------------------------------------
AJÁNLOTT NAPI ÜTEMEZÉS
--------------------------------------------------------------------
MASTER_CREATE + MASTER_UPDATE

    07:00
    11:00
    15:00
    18:00
    00:00

ENRICH_UPDATE

    03:00

DELETE_ALL

    06:00

--------------------------------------------------------------------
LOGOK
--------------------------------------------------------------------
A rendszer automatikusan logol ide:

    data/logs/shop_sync.log

A log tartalmazza:

    - futási idő
    - feldolgozott termékek száma
    - hibák
    - API válaszok

--------------------------------------------------------------------
PÉLDA TELJES TESZT FUTTATÁS
--------------------------------------------------------------------

Fejlesztés közben érdemes így végigfuttatni:

    python -m src.app --mode master_create_all
    python -m src.app --mode master_update_all
    python -m src.app --mode enrich_update_all

--------------------------------------------------------------------
FONTOS
--------------------------------------------------------------------
A delete művelet csak akkor fut le ha:

    DELETE_ENABLED=1

Ez védi a webshopot véletlen tömeges törlés ellen.

"""
# src/app.py
from __future__ import annotations

import argparse
import os
from dotenv import load_dotenv

from src.runner.prefetch import prefetch_all_sources

from src.runner.live_runner import (
    run_master_create_all,
    run_master_update_all,
    run_master_all,
    run_enrich_update_all,
    run_delete_all,
)

load_dotenv()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Shop Sync (live runner)")

    parser.add_argument(
        "--master",
        default=os.getenv("MASTER_SUPPLIER", "natura"),
        help="Master supplier (default: MASTER_SUPPLIER env)",
    )

    parser.add_argument(
        "--mode",
        required=True,
        choices=[
            "prefetch_all",
            "master_create_all",
            "master_update_all",
            "master_all",
            "enrich_update_all",
            "delete_all",
        ],
        help="Which channel to run",
    )

    args = parser.parse_args(argv)
    master = str(args.master).strip()

    if args.mode == "master_create_all":
        run_master_create_all(master_supplier=master)
    elif args.mode == "master_update_all":
        run_master_update_all(master_supplier=master)
    elif args.mode == "enrich_update_all":
        run_enrich_update_all(master_supplier=master)
    elif args.mode == "delete_all":
        run_delete_all(master_supplier=master)
    elif args.mode == "prefetch_all":
        prefetch_all_sources(skip={master})
    elif args.mode == "master_all":
        run_master_all(master_supplier=master)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())