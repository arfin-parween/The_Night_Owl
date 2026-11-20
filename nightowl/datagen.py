"""Synthetic quick-commerce data (fully made up; no real company data)."""
from __future__ import annotations

import csv
import random
import shutil
from datetime import datetime, timedelta

from . import settings

CITIES = ["Bengaluru", "Mumbai", "Delhi NCR", "Hyderabad", "Pune", "Chennai"]
CATEGORIES = {
    "dairy": (25, 120), "fruits_veg": (20, 250), "snacks": (10, 200), "beverages": (20, 180),
    "personal_care": (60, 600), "household": (50, 500), "atta_rice_dal": (80, 900), "frozen": (90, 450),
}
PAYMENT_MODES = ["UPI", "UPI", "UPI", "CARD", "COD", "WALLET"]

ORDER_COLUMNS = ["order_id", "store_id", "sku", "category", "quantity", "order_amount",
                 "payment_mode", "order_ts", "delivery_minutes", "app_version"]


def stores() -> list[dict]:
    rng = random.Random(7)
    rows = []
    for i in range(1, 61):
        city = CITIES[(i - 1) % len(CITIES)]
        rows.append({"store_id": f"ST{i:03d}", "city": city,
                     "store_name": f"{city.split()[0]} Darkstore {i:03d}", "opened_year": rng.choice([2021, 2022, 2023, 2024])})
    return rows


def write_stores() -> None:
    settings.STORES_FILE.parent.mkdir(parents=True, exist_ok=True)
    with settings.STORES_FILE.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["store_id", "city", "store_name", "opened_year"])
        w.writeheader()
        w.writerows(stores())


def partition_dir(dt: str):
    return settings.ORDERS_DIR / f"dt={dt}"


def write_orders(dt: str, rows: int = 40000, defect: str | None = None) -> str | None:
    """Write one day's orders partition. `defect` reproduces a real-world upstream problem."""
    pdir = partition_dir(dt)
    if pdir.exists():
        shutil.rmtree(pdir)
    if defect == "missing_partition":
        return None  # upstream never delivered today's export

    rng = random.Random(int(dt.replace("-", "")))
    store_ids = [s["store_id"] for s in stores()]
    base = datetime.fromisoformat(dt)
    header = list(ORDER_COLUMNS)
    if defect == "schema_drift":
        header[header.index("order_amount")] = "order_value_inr"  # upstream rename

    pdir.mkdir(parents=True, exist_ok=True)
    out = pdir / "orders.csv"
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for n in range(rows):
            cat = rng.choice(list(CATEGORIES))
            lo, hi = CATEGORIES[cat]
            qty = rng.choice([1, 1, 1, 2, 2, 3, 4])
            if defect == "bad_data":
                app = rng.choices(["7.1.4", "7.2.0"], weights=[62, 38])[0]
            else:
                app = rng.choices(["7.1.3", "7.1.4"], weights=[30, 70])[0]
            store = "" if (defect == "bad_data" and app == "7.2.0") else rng.choice(store_ids)
            ts = base + timedelta(seconds=rng.randint(0, 86399))
            w.writerow([f"ORD{dt.replace('-', '')}{n:06d}", store, f"SKU{rng.randint(1000, 9999)}", cat, qty,
                        round(rng.uniform(lo, hi) * qty, 2), rng.choice(PAYMENT_MODES),
                        ts.isoformat(), rng.randint(6, 25), app])
    return str(out)
