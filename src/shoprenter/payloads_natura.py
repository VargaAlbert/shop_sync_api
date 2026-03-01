from __future__ import annotations

"""
shoprenter.payloads_natura
=========================

Natura normalizált termék -> Shoprenter productExtend payload építése.

Feladat
-------
Ez a modul a normalize réteg által előállított Natura "Product-szerű" dict-ekből
Shoprenter API kompatibilis payloadot készít.

Két fő funkció:
1) build_product_extend_from_natura(...)
   - FULL payload CREATE művelethez (POST /productExtend)
   - opcionálisan tartalmaz képet, kategóriát, gyártót, áfakulcsot

2) build_update_payload_from_full(...)
   - a FULL payloadból csak az UPDATE-hez szükséges mezőket hagyja meg
     (PUT /productExtend/{id})

Miért kell külön update payload?
--------------------------------
Sok API-nál kockázatos a teljes create payloadot PUT-tal elküldeni, mert:
- felülírhat olyan mezőket, amit nem akarsz módosítani
- bizonyos mezők csak create-nél érvényesek
Ezért UPDATE-nél célszerű minimalizált mezőkészletet küldeni.

Képútvonal kezelés
------------------
A build_product_extend_from_natura() a src.utils.images modul segédfüggvényeire támaszkodik:
- build_shop_image_path(csoport1, model, slot)
- image_alt_from_model(model)

Elv:
- mappa név = csoport1 (Natura CSOPORT1)
- fájlnév = model (vagy SKU fallback)
"""

from typing import Any, Dict, Optional, Mapping
from src.utils.images import build_shop_image_path, image_alt_from_model


# Alapértelmezett kategória, ha nincs (vagy nincs bekötve) category_map.
DEFAULT_CATEGORY_ID = "Y2F0ZWdvcnktY2F0ZWdvcnlfaWQ9MjM4"


# UPDATE esetén csak ezt a pár mezőt küldjük (biztonságosabb).
UPDATE_FIELDS = {
    "sku",
    "price",
    "gtin",
    "modelNumber",
    "productDescriptions",
    "customer_group_prices",
    "_post_actions"
}


from typing import Any, Dict, Optional, Mapping, List, TypedDict

WHOLESALE_GROUP_NAME_DEFAULT = "NAGYKER"


class CustomerGroupPriceIntent(TypedDict, total=False):
    customer_group_name: str
    price: str  # Shoprenter stringként szereti


def build_customer_group_product_price_payload(
    *,
    product_id: str,
    customer_group_id: str,
    price: float,
) -> Dict[str, Any]:
    """
    Customer Group Product Price payload (POST/PUT /customerGroupProductPrices)

    Fixture alapján:
      {
        "price": "4237.5",
        "customerGroup": {"id": "..."},
        "product": {"id": "..."}
      }
    """
    return {
        "price": f"{float(price):.4f}",
        "customerGroup": {"id": customer_group_id},
        "product": {"id": product_id},
    }

def build_update_payload_from_full(full_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Full CREATE payload -> minimal UPDATE payload.

    Cél:
        A build_product_extend_from_natura() által épített teljes payloadból
        csak azokat a mezőket tartja meg, amiket UPDATE-nél valóban küldeni akarsz.

    Paraméter:
        full_payload (Dict[str, Any]):
            A teljes create payload.

    Visszatérési érték:
        Dict[str, Any]:
            Csak az UPDATE_FIELDS-ben szereplő kulcsok, None értékek nélkül.

    Megjegyzés:
        Ha később több mezőt akarsz frissíteni, az UPDATE_FIELDS halmazt bővítsd.
    """
    return {k: v for k, v in full_payload.items() if k in UPDATE_FIELDS and v is not None}

def build_product_extend_from_natura(
    p: Dict[str, Any],
    *,
    language_id: str,  # pl. "bGFuZ3VhZ2UtbGFuZ3VhZ2VfaWQ9MQ=="
    status_value: int = 0,
    stock1: int = 0,
    manufacturer_id: Optional[str] = None,
    tax_class_id: Optional[str] = None,
    # Kategória választás:
    category_id: Optional[str] = None,
    category_map: Optional[Mapping[str, str]] = None,  # pl. {"Etetőanyag": "Y2F0ZW..."}
    # Kép:
    image_slot: int = 1,
) -> Dict[str, Any]:
    """
    Natura normalizált termékből Shoprenter productExtend FULL payloadot épít.

    Bemenet:
        p (Dict[str, Any]):
            A normalize réteg által előállított Natura termék dict.
            Elvárt mezők (minimum):
                - sku (kötelező)
                - name_hu (kötelező)
                - gross_price (kötelező)
            Opcionális:
                - model
                - gtin
                - unit_name, manufacturer_name, tax_class_id, wholesale_price
                - csoport1_name (vagy CSOPORT1)

    Paraméterek:
        language_id (str):
            Shoprenter language id (pl. HU), amit a productDescriptions-nél használunk.

        status_value (int):
            Termék státusz (Shoprenter logika szerint: 0 tiltott / 1 aktív, stb).
            Stringgé konvertálva kerül payloadba.

        stock1 (int):
            Készlet mező (Shoprenter "stock1"). Stringgé konvertálva kerül payloadba.

        manufacturer_id (Optional[str]):
            Ha megadod, a payload tartalmazni fogja:
                "manufacturer": {"id": manufacturer_id}

        tax_class_id (Optional[str]):
            Ha megadod, a payload tartalmazni fogja:
                "taxClass": {"id": tax_class_id}

        category_id (Optional[str]):
            Ha fix kategóriát akarsz, add meg közvetlenül.
            Ha meg van adva, elsőbbséget élvez a category_map-pel szemben.

        category_map (Optional[Mapping[str, str]]):
            CSOPORT1 -> category_id map.
            Ha nincs category_id és van category_map, akkor a
            p["csoport1_name"] (vagy p["CSOPORT1"]) alapján választ.

        image_slot (int):
            Kép slot (ha több képnév/variáns van). Jelenleg a build_shop_image_path
            slot paraméterét vezérli.

    Visszatérési érték:
        Dict[str, Any]:
            Shoprenter POST /productExtend kompatibilis payload.

    Kivétel:
        ValueError:
            Ha hiányzik a sku / name_hu / gross_price.

    Kép kezelés:
        - image_path = build_shop_image_path(csoport1, model, slot=image_slot)
        - image_alt = image_alt_from_model(model)
        - ezek a payloadba mainPicture + imageAlt mezőként kerülnek

    Megjegyzés:
        A "_debug" mező nem Shoprenter standard mező.
        Ha az API elutasít ismeretlen mezőket (400), akkor ezt töröld/kapcsold ki.
    """

    # -----------------------------
    # Kötelező mezők ellenőrzése
    # -----------------------------
    sku = str(p.get("sku", "")).strip()
    if not sku:
        raise ValueError("Missing sku")

    name_hu = (p.get("name_hu") or "").strip()
    if not name_hu:
        raise ValueError(f"Missing name_hu for sku={sku}")

    gross = p.get("gross_price")
    if gross is None:
        raise ValueError(f"Missing gross_price for sku={sku}")

    # Modell/cikkszám alap: ha van külön "model", azt preferáljuk, különben sku
    model = (p.get("model") or "").strip() or sku

    # Natura csoport mező (kategória / kép mappa)
    csoport1 = (p.get("csoport1_name") or p.get("CSOPORT1") or "").strip()

    # -----------------------------
    # Kategória ID kiválasztása
    # -----------------------------
    # Prioritás:
    # 1) category_id paraméter (fix)
    # 2) category_map (CSOPORT1 alapján)
    # 3) DEFAULT_CATEGORY_ID
    resolved_category_id = category_id
    if not resolved_category_id and category_map and csoport1:
        resolved_category_id = category_map.get(csoport1)
    if not resolved_category_id:
        resolved_category_id = DEFAULT_CATEGORY_ID

    # -----------------------------
    # Képútvonal + alt generálás
    # -----------------------------
    # Folder = csoport1, filename = model (slot szerint)
    image_path = build_shop_image_path(
        csoport1=csoport1,
        model=model,
        slot=image_slot,
    )
    image_alt = image_alt_from_model(model)

    # -----------------------------
    # Payload összeállítás
    # -----------------------------
    payload: Dict[str, Any] = {
        "sku": sku,

        # Ha az API nem fogadja el, később kivesszük / átnevezzük
        "modelNumber": model,
        "gtin": (p.get("gtin") or "").strip(),

        # Shoprenter gyakran stringet vár
        "price": f"{float(gross):.4f}",
        "status": str(int(status_value)),
        "stock1": str(int(stock1)),

        # Leírások (név nyelvhez kötve)
        "productDescriptions": [
            {
                "name": name_hu,
                "language": {"id": language_id},
            }
        ],

        # Kategória reláció
        "productCategoryRelations": [{"category": {"id": resolved_category_id}}],
    }

    # --- vevőcsoport ár intent (NAGYKER) ---
    wholesale = p.get("wholesale_price")
    if wholesale is not None:
        payload["_post_actions"] = {
            "customer_group_prices": [
                {
                    "customer_group_name": WHOLESALE_GROUP_NAME_DEFAULT,
                    "price": f"{float(wholesale):.4f}",
                }
            ]
        }

    # Kép mezők: mainPicture + imageAlt
    # (Ha az API create-nél sem fogadja, később külön image endpointtal kell feltölteni.)
    if image_path:
        payload["mainPicture"] = image_path
        payload["imageAlt"] = image_alt

    # Opcionális kapcsolt mezők
    if manufacturer_id:
        payload["manufacturer"] = {"id": manufacturer_id}

    if tax_class_id:
        payload["taxClass"] = {"id": tax_class_id}

    # Debug mezők: API-k gyakran elutasítják az ismeretlen kulcsokat.
    # Ha 400-as hibát kapsz "unknown field" jelleggel, ezt töröld vagy feltételessé tedd.
    payload["_debug"] = {
        "unit_name": (p.get("unit_name") or "").strip(),
        "manufacturer_name": (p.get("manufacturer_name") or "").strip(),
        "tax_class_raw": (p.get("tax_class_id") or "").strip(),
        "wholesale_price": p.get("wholesale_price"),
        "csoport1": csoport1,
    }

    return payload