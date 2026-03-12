# src/suppliers/carpzoom/parse.py
from __future__ import annotations

from typing import Any, Dict, List
import xml.etree.ElementTree as ET


def _t(el):
    return (el.text or "").strip() if el is not None else ""


def parse(raw_bytes: bytes) -> List[Dict[str, Any]]:
    root = ET.fromstring(raw_bytes)

    products = root.findall(".//termek")

    out: List[Dict[str, Any]] = []

    for p in products:
        rec: Dict[str, Any] = {
            "termek_nev": _t(p.find("termek_nev")),
            "termek_leiras": _t(p.find("termek_leiras")),
            "termek_kategoria": _t(p.find("termek_kategoria")),
            "termek_cikkszam": _t(p.find("termek_cikkszam")),
            "termek_kep": _t(p.find("termek_kep")),
            "termek_ar": _t(p.find("termek_ar")),
            "termek_kisker_ar": _t(p.find("termek_kisker_ar")),
            "termek_vonalkod": _t(p.find("termek_vonalkod")),
            "termek_keszlet": _t(p.find("termek_keszlet")),
            "termek_tovabbi_kepek": [],
            "parameterek": {},
        }

        # további képek
        for img in p.findall(".//termek_tovabbi_kepek/termek_tovabbi_kep"):
            val = _t(img)
            if val:
                rec["termek_tovabbi_kepek"].append(val)

        # paraméterek (dinamikus tag-ek)
        params = p.find("parameterek")
        if params is not None:
            for child in list(params):
                key = child.tag.strip()
                val = _t(child)
                if key and val:
                    rec["parameterek"][key] = val

        # csak ha van cikkszám
        if rec["termek_cikkszam"]:
            out.append(rec)

    return out