import os
import csv
import requests
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

load_dotenv()

BASE = os.getenv("SHOPRENTER_API_URL").rstrip("/")
AUTH = HTTPBasicAuth(
    os.getenv("SHOPRENTER_API_USER"),
    os.getenv("SHOPRENTER_API_PASS")
)

session = requests.Session()
session.auth = AUTH
session.headers.update({"Accept": "application/json"})

# mappa létrehozás ha nem létezik
os.makedirs("data/debug", exist_ok=True)

output_file = "data/debug/categories.csv"

with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
    writer = csv.writer(f, delimiter=";")
    writer.writerow(["name", "id", "innerId", "parentInnerId"])

    page = 0
    limit = 100

    while True:
        r = session.get(
            f"{BASE}/categoryExtend",
            params={"full": 1, "page": page, "limit": limit},
        )
        r.raise_for_status()
        data = r.json()

        for item in data.get("items", []):
            descs = item.get("categoryDescriptions") or []
            name = descs[0].get("name") if descs else ""

            parent = item.get("parentCategory")
            parent_inner = None
            if isinstance(parent, dict):
                parent_inner = parent.get("innerId")

            writer.writerow([
                name,
                item.get("id"),
                item.get("innerId"),
                parent_inner
            ])

        page += 1
        if page >= data.get("pageCount", 0):
            break

print("Mentve:", output_file)
