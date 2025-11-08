#!/usr/bin/env python3
"""Batch ingestion script that fetches data from DummyJSON API and writes
timestamped batch files to data/raw_batches/ on a 5-minute interval.

Usage:
    python scripts/batch_ingestor.py --count 3 --dry-run
    python scripts/batch_ingestor.py              # runs forever, 5 min between batches
    python scripts/batch_ingestor.py --count 10   # 10 batches then stop
"""

import argparse
import json
import os
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CARTS_URL = "https://dummyjson.com/carts?limit=50"
USERS_URL = "https://dummyjson.com/users?limit=100"

SPAIN_REGIONS = [
    "Madrid",
    "Barcelona",
    "Valencia",
    "Sevilla",
    "Zaragoza",
    "Málaga",
    "Bilbao",
    "Murcia",
    "Alicante",
    "Córdoba",
    "Granada",
    "Valladolid",
    "Vitoria",
    "Palma de Mallorca",
    "Las Palmas",
]

ORDER_STATUSES = ["completed", "pending", "cancelled", "refunded"]
STATUS_WEIGHTS = [0.65, 0.18, 0.10, 0.07]

BATCH_DIR = Path(__file__).resolve().parent.parent / "data" / "raw_batches"
PROCESSED_DIR = Path(__file__).resolve().parent.parent / "data" / "processed"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def fetch_json(url: str, label: str) -> dict:
    """GET a URL, return parsed JSON. Exits on failure."""
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as exc:
        print(f"[ERROR] Failed to fetch {label}: {exc}", file=sys.stderr)
        sys.exit(1)


def build_batch(users: list[dict], carts: list[dict], batch_ts: str) -> dict:
    """Combine users + carts into a batch of orders with line items."""

    orders: list[dict] = []
    customers: list[dict] = []
    products_seen: dict[int, dict] = {}
    order_items: list[dict] = []

    # Deduplicate customers
    for u in users:
        customers.append(
            {
                "customer_id": u["id"],
                "customer_name": f"{u['firstName']} {u['lastName']}",
                "email": u["email"],
                "country": random.choice(["ES", "ES", "ES", "FR", "DE", "PT", "IT"]),
                "signup_date": u.get("birthDate", "2023-01-01"),
                "preferred_language": random.choice(["es", "es", "es", "en", "fr"]),
            }
        )

    order_id_offset = int(datetime.now(timezone.utc).timestamp()) * 1000

    for idx, cart in enumerate(carts):
        order_id = order_id_offset + idx
        customer_id = cart["userId"]
        region = random.choice(SPAIN_REGIONS)
        status = random.choices(ORDER_STATUSES, weights=STATUS_WEIGHTS, k=1)[0]

        orders.append(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "order_date": batch_ts[:10],
                "status": status,
                "region": region,
                "currency": "EUR",
            }
        )

        for item in cart.get("products", []):
            pid = item["id"]
            if pid not in products_seen:
                products_seen[pid] = {
                    "product_id": pid,
                    "product_name": item.get("title", f"Product {pid}"),
                    "category": item.get("category", "Other"),
                    "subcategory": "General",
                    "unit_price_cents": int(round(item.get("price", 0) * 100)),
                }

            order_items.append(
                {
                    "order_item_id": order_id * 100 + len(order_items),
                    "order_id": order_id,
                    "product_id": pid,
                    "quantity": item.get("quantity", 1),
                    "unit_price_cents": int(round(item.get("price", 0) * 100)),
                }
            )

    return {
        "batch_timestamp": batch_ts,
        "order_count": len(orders),
        "orders": orders,
        "customers": customers,
        "products": list(products_seen.values()),
        "order_items": order_items,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def run_batch(dry_run: bool = False) -> Path | None:
    """Fetch one batch from the API and write to disk. Returns output path."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    print(f"[{ts}] Fetching data from DummyJSON …")

    users_data = fetch_json(USERS_URL, "users")
    carts_data = fetch_json(CARTS_URL, "carts")

    users = users_data.get("users", [])
    carts = carts_data.get("carts", [])

    batch = build_batch(users, carts, ts)

    if dry_run:
        print(
            f"  [DRY RUN] Generated {batch['order_count']} orders, "
            f"{len(batch['order_items'])} line items, "
            f"{len(batch['customers'])} customers, "
            f"{len(batch['products'])} products"
        )
        print(json.dumps(batch["orders"][:2], indent=2, ensure_ascii=False))
        return None

    BATCH_DIR.mkdir(parents=True, exist_ok=True)
    out_path = BATCH_DIR / f"batch_{ts}.json"
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(batch, fh, indent=2, ensure_ascii=False)

    size_kb = out_path.stat().st_size / 1024
    print(f"  Written {out_path.name} ({size_kb:.1f} KB)")
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Batch ingestion from DummyJSON API")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch one batch and print summary without writing files",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=None,
        help="Number of batches to generate (default: run forever)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=300,
        help="Seconds between batches (default: 300 = 5 min)",
    )
    args = parser.parse_args()

    if args.dry_run and args.count is None:
        args.count = 1

    batch_num = 0
    while True:
        batch_num += 1
        run_batch(dry_run=args.dry_run)

        if args.count is not None and batch_num >= args.count:
            print(f"\nCompleted {args.count} batch(es). Exiting.")
            break

        print(f"  Sleeping {args.interval}s until next batch …\n")
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
