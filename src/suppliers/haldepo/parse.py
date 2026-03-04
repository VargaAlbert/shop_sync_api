from __future__ import annotations

from src.core.io.csv import parse_csv_bytes
from src.core.io.supplier_files import load_supplier_json


def parse(raw_bytes: bytes) -> list[dict]:
    cfg = load_supplier_json("haldepo")
    return parse_csv_bytes(
        raw_bytes,
        delimiter=cfg.delimiter,
        encoding=cfg.encoding,
        has_header=cfg.has_header,
    )