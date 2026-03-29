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
import unicodedata
from typing import Dict, Any

from src.shoprenter.client import ShoprenterClient

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

def _norm_lookup_name(value: str) -> str:
    s = str(value or "").strip().lower()
    if not s:
        return ""

    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return " ".join(s.split())


def _extract_manufacturer_name(item: Dict[str, Any]) -> str:
    for k in ("name", "manufacturer_name", "title"):
        v = str(item.get(k) or "").strip()
        if v:
            return v

    desc = item.get("manufacturerDescription")
    if isinstance(desc, dict):
        for k in ("name", "title"):
            v = str(desc.get(k) or "").strip()
            if v:
                return v

    return ""


def _extract_id(ref: Any) -> str:
    if isinstance(ref, dict):
        rid = str(ref.get("id") or "").strip()
        if rid:
            return rid

        href = str(ref.get("href") or "").strip()
        if href:
            return href.rsplit("/", 1)[-1].strip()

    if isinstance(ref, str):
        s = ref.strip()
        if not s:
            return ""
        if "/" in s:
            return s.rsplit("/", 1)[-1].strip()
        return s

    return ""

def _extract_manufacturer_name_from_product_extend(item: Dict[str, Any]) -> str:
    m = item.get("manufacturer")

    if isinstance(m, dict):
        for k in ("name", "title"):
            v = str(m.get(k) or "").strip()
            if v:
                return v

        md = m.get("manufacturerDescription")
        if isinstance(md, dict):
            for k in ("name", "title"):
                v = str(md.get(k) or "").strip()
                if v:
                    return v

    return ""


def build_manufacturer_name_map(
    client,
    *,
    limit: int = 200,
    sleep_s: float = 0.2,
    max_pages: int = 2000,
) -> Dict[str, str]:
    """
    A teljes manufacturer listából épít name -> id mapet.
    Ez megbízhatóbb, mint a productExtend alapú megoldás.
    """
    out: Dict[str, str] = {}
    started = time.time()

    for page in range(0, max_pages):
        data = client.get_page("/manufacturers", page=page, limit=limit, full=True)
        items = data.get("items") or []
        if not isinstance(items, list):
            break

        for item in items:
            if not isinstance(item, dict):
                continue

            mid = _extract_id(item)
            name = _extract_manufacturer_name(item)

            key = _norm_lookup_name(name)
            if key and mid and key not in out:
                out[key] = mid

        page_count = int(data.get("pageCount") or 0)

        print(
            f"[MANUFACTURER_MAP] page={page}/{page_count or '?'} "
            f"items={len(items)} map_size={len(out)} dt={time.time()-started:.2f}s"
        )

        if page_count and page >= page_count - 1:
            break
        if not page_count and len(items) < limit:
            break

        time.sleep(sleep_s)

    return out

def _extract_product_descriptions_for_language(
    item: Dict[str, Any],
    *,
    language_id: str,
) -> Dict[str, str]:
    out = {
        "name": "",
        "short_description": "",
        "description": "",
        "product_description_id": "",
        "language_id": "",
    }

    rows = item.get("productDescriptions") or []
    if not isinstance(rows, list):
        return out

    for row in rows:
        if not isinstance(row, dict):
            continue

        row_language_id = _extract_id(row.get("language"))
        if row_language_id and row_language_id != language_id:
            continue

        out["name"] = str(row.get("name") or "").strip()
        out["short_description"] = str(row.get("shortDescription") or "").strip()
        out["description"] = str(row.get("description") or "").strip()
        out["product_description_id"] = _extract_id(row)
        out["language_id"] = row_language_id
        return out

    return out


def build_product_description_map(
    client,
    *,
    language_id: str,
    limit: int = 200,
    sleep_s: float = 0.2,
    max_pages: int = 2000,
) -> Dict[str, Dict[str, str]]:
    page = 0
    out: Dict[str, Dict[str, str]] = {}

    while True:
        data = client.get_product_extend_page(
            page=page,
            limit=limit,
            full=True,
        )

        page_count = int(data.get("pageCount") or 0)
        items = data.get("items", []) or []

        for item in items:
            sku = str(item.get("sku") or "").strip()
            if not sku:
                continue

            out[sku] = _extract_product_descriptions_for_language(
                item,
                language_id=language_id,
            )

        print(
            f"[DESCRIPTION_MAP] "
            f"page={page+1}/{page_count} "
            f"items={len(items)} "
            f"map_size={len(out)}"
        )

        page += 1

        if page_count and page >= page_count:
            break

        if page >= max_pages:
            raise RuntimeError(
                f"Vészfék: max_pages elérve ({max_pages}) a build_product_description_map közben."
            )

        time.sleep(sleep_s)

    return out