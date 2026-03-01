import json
from pathlib import Path

from src.normalize import normalize_rows
from src.ingest.suppliers_csv import ingest_one_supplier_csv
import src.normalize.suppliers.natura
# ÚJ normalize architektúra

from src.shoprenter.payloads_natura import build_product_extend_from_natura

LANGUAGE_ID = "bGFuZ3VhZ2UtbGFuZ3VhZ2VfaWQ9MQ=="  # TODO: ide a valós Shoprenter language id


def run() -> None:
    # 1) Ingest (CSV letöltés + parse + _supplier meta)
    raw = ingest_one_supplier_csv("natura")
    print("RAW:", len(raw))

    # 2) Normalize (registry alapú)
    norm = normalize_rows("natura", raw)
    print("NORMALIZED:", len(norm))

    if not norm:
        print("Nincs normalizált termék (üres input vagy minden sor kiesett).")
        return

    # csak első 10
    first10 = norm[:1]

    print("\nElső normalizált elem:")
    print(first10[0])

    payloads = []

    for p in first10:
        payload = build_product_extend_from_natura(p, language_id=LANGUAGE_ID)

        # ha nem akarod, hogy debug mező bekavarjon később
        payload.pop("_debug", None)

        payloads.append(payload)

    print("\nElső payload:")
    print(payloads[0])

    # 3) Debug mentés
    debug_dir = Path("data") / "debug"
    debug_dir.mkdir(parents=True, exist_ok=True)

    out_file = debug_dir / "natura_payload_first10.json"
    out_file.write_text(
        json.dumps(payloads, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print("\nJSON mentve ide:")
    print(out_file.resolve())


if __name__ == "__main__":
    run()