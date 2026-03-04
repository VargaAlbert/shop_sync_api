from __future__ import annotations

import csv
from io import StringIO
from typing import Any, Dict, List


def parse_csv_bytes(
    raw: bytes,
    *,
    delimiter: str = ";",
    encoding: str = "utf-8",
    has_header: bool = True,
) -> List[Dict[str, Any]]:
    text = raw.decode(encoding, errors="replace")
    f = StringIO(text)

    if has_header:
        reader = csv.DictReader(f, delimiter=delimiter)
        return [dict(r) for r in reader]
    else:
        # header nélküli: col0, col1...
        reader2 = csv.reader(f, delimiter=delimiter)
        out: List[Dict[str, Any]] = []
        for row in reader2:
            out.append({f"col{i}": v for i, v in enumerate(row)})
        return out