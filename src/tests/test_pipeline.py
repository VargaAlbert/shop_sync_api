import json
from pathlib import Path

from src.ingest.suppliers_csv import ingest_one_supplier_csv
from src.normalize.natura import normalize_natura_rows
from src.shoprenter.payloads_natura import build_product_extend_from_natura

def run():
    raw = ingest_one_supplier_csv("natura")
    print("RAW:", len(raw))

    norm = normalize_natura_rows(raw)
    print("NORMALIZED:", len(norm))

    # csak első 10
    first10 = norm[:10]

    print("\nElső normalizált elem:")
    print(first10[0])

    payloads = []

    for p in first10:
        payload = build_product_extend_from_natura(
            p,
            language_id="HU_LANGUAGE_ID_IDE",
            status_value=0,
            stock1=0,
        )
        payloads.append(payload)

    print("\nElső payload:")
    print(payloads[0])

    # 📁 debug mentés
    debug_dir = Path("data") / "debug"
    debug_dir.mkdir(parents=True, exist_ok=True)

    out_file = debug_dir / "natura_payload_first10.json"
    out_file.write_text(
        json.dumps(payloads, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )

    print("\nJSON mentve ide:")
    print(out_file.resolve())


if __name__ == "__main__":
    run()