from __future__ import annotations

"""
shoprenter.lookups
==================

Segédfüggvények a Shoprenter API-ból történő adatok lekéréséhez.

Ez a modul jelenleg egy fontos lookup funkciót tartalmaz:

    build_product_sku_map()

Feladat
-------
A Shoprenterben már létező termékekből felépít egy:

    SKU -> productExtend id

térképet (dict), amely elengedhetetlen az UPsert (CREATE + UPDATE) logikához.

Miért fontos?
-------------
UPsert során el kell dönteni:
- ha egy SKU már létezik a Shoprenterben -> UPDATE
- ha nem létezik -> CREATE

Ehhez gyors lookup kell, nem minden SKU-ra külön API hívás.
Ez a függvény lapozva beolvassa az összes productExtend elemet,
és memóriában felépíti a teljes SKU map-et.
"""

from typing import Dict
import time


def build_product_sku_map(
    client,
    *,
    limit: int = 200,
    sleep_s: float = 0.2,
    max_pages: int = 2000,   # vészfék
) -> Dict[str, str]:
    """
    Felépíti a Shoprenter SKU -> productExtend id térképet.

    Működés:
        - lapozva meghívja a client.get_product_extend_page(...) metódust
        - minden oldalon:
            - kiolvassa az items listát
            - minden elemnél:
                sku = item["sku"]
                id  = item["id"]
            - ha mindkettő létezik -> eltárolja a dict-ben
        - addig megy, amíg:
            - el nem éri a pageCount-ot, vagy
            - el nem éri a max_pages limitet (vészfék)

    Paraméterek:
        client:
            ShoprenterClient példány, amely biztosítja:
                get_product_extend_page(page, limit, full)

        limit (int):
            Oldalméret (hány terméket kér le egyszerre).
            Nagyobb érték = kevesebb kör, de nagyobb payload.

        sleep_s (float):
            Várakozás két oldal lekérése között (rate limit védelem).

        max_pages (int):
            Biztonsági vészfék.
            Ha ennyi oldal után még nem értünk a végére,
            RuntimeError dobódik (valószínűleg pageCount probléma).

    Visszatérési érték:
        Dict[str, str]:
            SKU -> productExtend id map.

    Példa:
        {
          "ABC123": "4567",
          "XYZ999": "8910"
        }

    Hibakezelés:
        - Ha page >= max_pages -> RuntimeError
        - A get_product_extend_page belül HTTPError dobhat, ha API hiba van.

    Teljesítmény:
        - O(N) ahol N a shopban lévő termékek száma.
        - 10-20k termék esetén érdemes a limit-et növelni (pl. 500-1000),
          ha az API engedi.
    """
    page = 0
    out: Dict[str, str] = {}

    while True:
        t0 = time.time()

        # Oldal lekérése a Shoprenter API-ból
        data = client.get_product_extend_page(
            page=page,
            limit=limit,
            full=True,   # full=True: biztosan legyen sku és id
        )

        # API által visszaadott teljes oldalszám
        page_count = int(data.get("pageCount") or 0)

        # Az adott oldalon lévő elemek
        items = data.get("items", []) or []

        # SKU -> id kinyerése
        for it in items:
            sku = (it.get("sku") or "").strip()
            pid = it.get("id")
            if sku and pid:
                out[sku] = pid

        dt = time.time() - t0

        print(
            f"[SKU_MAP] "
            f"page={page+1}/{page_count} "
            f"items={len(items)} "
            f"map_size={len(out)} "
            f"dt={dt:.2f}s"
        )

        page += 1

        # -----------------------------
        # STOP feltételek
        # -----------------------------

        # Ha az API jelzi a teljes oldalszámot, és elértük a végét
        if page_count and page >= page_count:
            break

        # Vészfék: túl sok oldal (valószínűleg pageCount/oldalazási hiba)
        if page >= max_pages:
            raise RuntimeError(
                f"Vészfék: max_pages elérve ({max_pages}). "
                "Valószínű pageCount/oldalazás gond van."
            )

        # Rate limit védelem
        time.sleep(sleep_s)

    return out