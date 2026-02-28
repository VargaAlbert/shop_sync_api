import csv
from io import StringIO
from typing import Any, Dict, List


def parse_csv_bytes(
    data: bytes,
    *,
    encoding: str = "utf-8",
    delimiter: str = ",",
) -> List[Dict[str, Any]]:
    """
    CSV fájl feldolgozása byte formátumból.

    A függvény egy byte formátumban kapott CSV állományt alakít át
    Python szótárak listájává. A fejléc sor alapján kulcs-érték
    párokat hoz létre minden egyes adatsorhoz.

    Tipikus felhasználás:
        - FastAPI UploadFile.read()
        - API-n keresztül kapott CSV tartalom
        - háttérfolyamatban történő CSV import

    Paraméterek:
        data (bytes):
            A CSV fájl teljes tartalma byte formátumban.

        encoding (str, alapértelmezett: "utf-8"):
            A fájl karakterkódolása. Hibás karakterek esetén
            a rendszer nem dob kivételt, hanem lecseréli azokat.

        delimiter (str, alapértelmezett: ","):
            A CSV mezőelválasztó karaktere (pl. ",", ";", "\t").

    Visszatérési érték:
        List[Dict[str, Any]]:
            Egy lista, amelyben minden elem egy CSV sor.
            A kulcsok a fejléc oszlopnevei,
            az értékek az adott sor cellaértékei (stringként).

    Megjegyzések:
        - Minden mező string típusú lesz.
        - Típuskonverziót külön kell végezni (pl. int(), float()).
        - A CSV-nek tartalmaznia kell fejléc sort.
        - Üres mezők üres stringként ("") jelennek meg.
    """

    # Byte adat dekódolása szöveggé a megadott encoding szerint.
    # Hibás karakterek esetén nem dob hibát, hanem lecseréli azokat.
    text = data.decode(encoding, errors="replace")

    # A szöveget egy memóriabeli "fájllá" alakítjuk,
    # hogy a csv.DictReader fájlként tudja kezelni.
    f = StringIO(text)

    # DictReader használata:
    # - Az első sort fejlécnek tekinti
    # - Minden további sort dict formában ad vissza
    reader = csv.DictReader(f, delimiter=delimiter)

    # A reader iterálható objektum, ezért listába gyűjtjük.
    # Minden sorból explicit dict-et készítünk.
    return [dict(row) for row in reader]