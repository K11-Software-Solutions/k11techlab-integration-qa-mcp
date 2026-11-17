"""Batch job: total reserved quantity per SKU into sku_totals.

Correct version recomputes the totals (safe to run twice). With SHOP_BUGS containing "job" it adds
to the existing totals instead, the classic non-idempotent batch job.

    SHOP_DB=shop.db python jobs/sku_totals.py
"""

import os
import sqlite3
import sys

db = os.environ.get("SHOP_DB", "shop.db")
buggy = "job" in os.environ.get("SHOP_BUGS", "")
with sqlite3.connect(db) as c:
    rows = c.execute("SELECT sku, SUM(qty) FROM orders WHERE status = 'reserved' GROUP BY sku").fetchall()
    for sku, qty in rows:
        if buggy:
            c.execute("INSERT INTO sku_totals (sku, qty) VALUES (?, ?) ON CONFLICT(sku) DO UPDATE SET qty = qty + excluded.qty", (sku, qty))
        else:
            c.execute("INSERT INTO sku_totals (sku, qty) VALUES (?, ?) ON CONFLICT(sku) DO UPDATE SET qty = excluded.qty", (sku, qty))
print(f"sku_totals: {len(rows)} sku(s) updated")
sys.exit(0)
