"""
Shop Sync Runner
================

Ez az alkalmazás a beszállítói adatokat feldolgozza, majd a Shoprenter API-n
keresztül szinkronizálja a webshop termékeit.

A rendszer több külön futtatási módot kezel, amelyek egymástól függetlenül
is indíthatók.

--------------------------------------------------------------------
FUTTATÁSI MÓDOK
--------------------------------------------------------------------

1. PREFETCH_ALL
   ------------------
   Előre letölti és cache-eli a nem master beszállítók forrásadatait.

   Célja:
   - gyorsítani a későbbi enrich futásokat
   - csökkenteni az élő futás közbeni letöltési időt
   - előre felépíteni a beszállítói cache állományokat

   FONTOS:
   A master beszállító itt automatikusan skipelődik, mert azt a master folyamat
   saját maga kezeli.

   Példa:
       python -m src.app --mode prefetch_all

2. MASTER_CREATE_ALL
   ------------------
   Új termékeket hoz létre a Shoprenterben a master beszállító alapján.

   Csak azokat a SKU-kat hozza létre, amelyek még nem léteznek a Shoprenterben.

   Példa:
       python -m src.app --mode master_create_all

3. MASTER_UPDATE_ALL
   ------------------
   A már létező termékek alapadatait frissíti.

   Tipikusan ilyen mezők:
   - ár
   - készlet
   - név
   - alap termékadatok

   Példa:
       python -m src.app --mode master_update_all

4. MASTER_ALL
   ------------------
   Kombinált master futás.

   Egymás után végrehajtja:
   - MASTER_CREATE_ALL
   - MASTER_UPDATE_ALL

   Akkor hasznos, ha napi szinkronnál egyetlen paranccsal szeretnéd kezelni
   az új termékek létrehozását és a meglévők frissítését.

   Példa:
       python -m src.app --mode master_all

5. ENRICH_UPDATE_ALL
   ------------------
   Kiegészítő adatokat frissít a termékeken.

   Tipikusan ilyen adatok:
   - leírás
   - képek
   - egyéb enrich mezők

   Csak azokon a termékeken fut le, ahol az enrich pipeline tényleges változást
   talált.

   Példa:
       python -m src.app --mode enrich_update_all

6. DELETE_ALL
   ------------------
   Azokat a termékeket törli a Shoprenterből, amelyek már nem szerepelnek
   a master beszállító listájában.

   FONTOS:
   A törlés csak akkor történik meg, ha a .env fájlban ez engedélyezve van:

       DELETE_ENABLED=1

   Ha ez nincs beállítva, a törlés automatikusan skipelődik biztonsági okból.

   Példa:
       python -m src.app --mode delete_all

--------------------------------------------------------------------
PARANCS SOROS FUTTATÁS
--------------------------------------------------------------------

A program a projekt gyökérkönyvtárából futtatható.

Általános forma:

    python -m src.app --mode <futtatasi_mod>

Példák:

    python -m src.app --mode prefetch_all
    python -m src.app --mode master_create_all
    python -m src.app --mode master_update_all
    python -m src.app --mode master_all
    python -m src.app --mode enrich_update_all
    python -m src.app --mode delete_all

--------------------------------------------------------------------
MASTER SUPPLIER MEGADÁSA
--------------------------------------------------------------------

Az alapértelmezett master beszállító a .env fájlban állítható be:

    MASTER_SUPPLIER=natura

Ez parancssorból felülírható:

    python -m src.app --mode master_update_all --master natura

A --master paraméter az alábbi módoknál releváns:
- master_create_all
- master_update_all
- master_all
- enrich_update_all
- delete_all
- prefetch_all esetén a megadott master automatikusan skipelődik

--------------------------------------------------------------------
AJÁNLOTT TESZTELÉSI SORREND
--------------------------------------------------------------------

Fejlesztés vagy első élesítés előtt ajánlott sorrend:

1. Források előtöltése

    python -m src.app --mode prefetch_all

2. Új termékek létrehozása

    python -m src.app --mode master_create_all

3. Meglévő termékek alapfrissítése

    python -m src.app --mode master_update_all

4. Enrich adatok frissítése

    python -m src.app --mode enrich_update_all

5. Törlés tesztelése külön, fokozott óvatossággal

    python -m src.app --mode delete_all

--------------------------------------------------------------------
AJÁNLOTT NAPI ÜTEMEZÉS
--------------------------------------------------------------------

1. PREFETCH_ALL
   Nem kötelező minden futás előtt, de hasznos lehet:

    01:00

2. MASTER_ALL
   A legtöbb napi szinkronhoz ez a legpraktikusabb:


    17:00
    23:00

3. ENRICH_UPDATE_ALL

    02:00

4. DELETE_ALL

    07:00

Megjegyzés:
Ha nem a kombinált futást használod, akkor a MASTER_CREATE_ALL és
MASTER_UPDATE_ALL külön is ütemezhető.

--------------------------------------------------------------------
LOGOK
--------------------------------------------------------------------

A rendszer automatikusan logol ide:

    data/logs/shop_sync.log

A log tipikusan tartalmazza:
- futási idő
- feldolgozott termékek száma
- hibák
- API válaszok
- skip események
- létrehozott / frissített / törölt elemek száma

--------------------------------------------------------------------
PÉLDA TELJES FEJLESZTŐI FUTTATÁS
--------------------------------------------------------------------

Fejlesztés közben tipikus teljes futtatás:

    python -m src.app --mode prefetch_all
    python -m src.app --mode master_all
    python -m src.app --mode enrich_update_all

Vagy külön bontva:

    python -m src.app --mode master_create_all
    python -m src.app --mode master_update_all
    python -m src.app --mode enrich_update_all

--------------------------------------------------------------------
BIZTONSÁGI MEGJEGYZÉS
--------------------------------------------------------------------

A delete művelet csak akkor fut le, ha:

    DELETE_ENABLED=1

Ez védi a webshopot a véletlen tömeges törlés ellen.

Éles környezetben ajánlott:
- a delete futást külön időzíteni
- a logokat rendszeresen ellenőrizni
- a törlési logikát először kis mintán tesztelni

--------------------------------------------------------------------
ÖSSZEFOGLALÁS
--------------------------------------------------------------------

A legfontosabb módok:

- prefetch_all       -> nem master források előtöltése
- master_create_all  -> új termékek létrehozása
- master_update_all  -> meglévő termékek frissítése
- master_all         -> create + update együtt
- enrich_update_all  -> leírások, képek, enrich mezők frissítése
- delete_all         -> hiányzó termékek törlése, csak engedélyezve

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
    parser = argparse.ArgumentParser(
        description="Shop Sync runner for Shoprenter product synchronization"
    )

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