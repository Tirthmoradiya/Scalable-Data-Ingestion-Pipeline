"""
High-speed synthetic dataset generator for scale testing.

Generates realistic CSV data (orders, customers, products) in a streaming,
memory-efficient manner using Python's built-in csv writer.

Usage
-----
    python scripts/generate_dataset.py --rows 1000000 \
        --out data/scale/orders_1m.csv
    python scripts/generate_dataset.py --rows 5000000 \
        --out data/scale/orders_5m.csv --type orders
    python scripts/generate_dataset.py --rows 500000 \
        --out data/scale/products_500k.csv --type products
"""

from __future__ import annotations

import argparse
import csv
import random
import time
from datetime import datetime, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Constant pools — pre-generated to avoid per-row computation
# ---------------------------------------------------------------------------
FIRST_NAMES = [
    "Alice",
    "Bob",
    "Carol",
    "Dave",
    "Eve",
    "Frank",
    "Grace",
    "Henry",
    "Iris",
    "Jack",
    "Kate",
    "Leo",
    "Mia",
    "Noah",
    "Olivia",
    "Paul",
    "Quinn",
    "Rachel",
    "Sam",
    "Tina",
    "Uma",
    "Victor",
    "Wendy",
    "Xander",
    "Yara",
    "Zoe",
    "Aaron",
    "Beth",
    "Carl",
    "Diana",
]

LAST_NAMES = [
    "Smith",
    "Johnson",
    "Williams",
    "Brown",
    "Jones",
    "Garcia",
    "Miller",
    "Davis",
    "Wilson",
    "Anderson",
    "Taylor",
    "Thomas",
    "Jackson",
    "White",
    "Harris",
    "Martin",
    "Thompson",
    "Young",
    "Hall",
    "Allen",
]

DOMAINS = [
    "gmail.com",
    "yahoo.com",
    "outlook.com",
    "company.io",
    "enterprise.net",
    "example.com",
    "corp.ai",
    "techfirm.co",
]

STATUSES = ["pending", "completed", "shipped", "cancelled", "processing", "returned"]

CATEGORIES = [
    "Electronics",
    "Books",
    "Clothing",
    "Home",
    "Sports",
    "Toys",
    "Beauty",
    "Automotive",
    "Garden",
    "Food",
    "Health",
    "Office",
]

PRODUCT_ADJECTIVES = [
    "Super",
    "Ultra",
    "Pro",
    "Max",
    "Plus",
    "Elite",
    "Prime",
    "Advanced",
    "Smart",
    "Precision",
    "Compact",
    "Deluxe",
]

PRODUCT_NOUNS = [
    "Widget",
    "Gadget",
    "Device",
    "Tool",
    "Kit",
    "Module",
    "Unit",
    "System",
    "Pack",
    "Set",
    "Bundle",
    "Collection",
]

BASE_DATE = datetime(2020, 1, 1)
MAX_DATE_OFFSET = (datetime(2024, 12, 31) - BASE_DATE).days

PHONE_PREFIXES = ["+1-555", "+44 20", "+91-98", "+61-4", "+49-30", "+33-1"]

STATUS_WEIGHTS = [0.35, 0.40, 0.15, 0.05, 0.03, 0.02]  # weighted distribution


def random_date() -> str:
    offset = random.randint(0, MAX_DATE_OFFSET)
    dt = BASE_DATE + timedelta(days=offset)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def random_email(first: str, last: str, uid: int) -> str:
    return f"{first.lower()}.{last.lower()}{uid % 1000}@{random.choice(DOMAINS)}"


def random_phone() -> str:
    prefix = random.choice(PHONE_PREFIXES)
    suffix = f"{random.randint(1000000, 9999999)}"
    return f"{prefix}-{suffix}" if random.random() > 0.15 else ""  # 15% null phones


def random_price(min_p: float = 0.99, max_p: float = 999.99) -> str:
    return f"{random.uniform(min_p, max_p):.2f}"


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------
def generate_orders(writer: csv.DictWriter, n: int, progress_every: int = 100_000) -> int:
    writer.writeheader()
    for i in range(1, n + 1):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        qty = random.randint(1, 10)
        unit_price = float(random_price(0.99, 499.99))
        total = round(qty * unit_price, 2)
        writer.writerow(
            {
                "order_id": i,
                "customer_email": random_email(first, last, i),
                "customer_name": f"{first} {last}",
                "customer_phone": random_phone(),
                "status": random.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0],
                "total_amount": f"{total:.2f}",
                "ordered_at": random_date(),
                "product_sku": f"SKU-{random.randint(1, 50000):06d}",
                "quantity": qty,
                "unit_price": f"{unit_price:.2f}",
                "category": random.choice(CATEGORIES),
            }
        )
        if i % progress_every == 0:
            pct = i / n * 100
            print(f"  {i:>10,} / {n:,}  ({pct:.0f}%)", end="\r", flush=True)
    return n


def generate_products(writer: csv.DictWriter, n: int, progress_every: int = 100_000) -> int:
    writer.writeheader()
    for i in range(1, n + 1):
        adj = random.choice(PRODUCT_ADJECTIVES)
        noun = random.choice(PRODUCT_NOUNS)
        writer.writerow(
            {
                "sku": f"SKU-{i:06d}",
                "name": f"{adj} {noun} {i}",
                "category": random.choice(CATEGORIES),
                "price": random_price(0.99, 9999.99),
            }
        )
        if i % progress_every == 0:
            pct = i / n * 100
            print(f"  {i:>10,} / {n:,}  ({pct:.0f}%)", end="\r", flush=True)
    return n


def generate_customers(writer: csv.DictWriter, n: int, progress_every: int = 100_000) -> int:
    writer.writeheader()
    for i in range(1, n + 1):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        writer.writerow(
            {
                "name": f"{first} {last}",
                "email": random_email(first, last, i),
                "phone": random_phone(),
            }
        )
        if i % progress_every == 0:
            pct = i / n * 100
            print(f"  {i:>10,} / {n:,}  ({pct:.0f}%)", end="\r", flush=True)
    return n


GENERATORS = {
    "orders": (
        generate_orders,
        [
            "order_id",
            "customer_email",
            "customer_name",
            "customer_phone",
            "status",
            "total_amount",
            "ordered_at",
            "product_sku",
            "quantity",
            "unit_price",
            "category",
        ],
    ),
    "products": (generate_products, ["sku", "name", "category", "price"]),
    "customers": (generate_customers, ["name", "email", "phone"]),
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate large synthetic datasets")
    parser.add_argument("--rows", type=int, default=100_000, help="Number of rows")
    parser.add_argument("--out", type=str, required=True, help="Output CSV path")
    parser.add_argument("--type", choices=list(GENERATORS), default="orders")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    args = parser.parse_args()

    random.seed(args.seed)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    gen_fn, fieldnames = GENERATORS[args.type]

    print(f"Generating {args.rows:,} {args.type} rows → {out_path}")
    t0 = time.perf_counter()

    with out_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        gen_fn(writer, args.rows)

    elapsed = time.perf_counter() - t0
    size_mb = out_path.stat().st_size / 1e6
    throughput = args.rows / elapsed

    print(f"\n✓ Done in {elapsed:.1f}s  |  {size_mb:.1f} MB  |  {throughput:,.0f} rows/sec")


if __name__ == "__main__":
    main()
