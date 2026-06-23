"""
CLI entry point for the Scalable Data Ingestion Pipeline.

Usage examples
--------------
  python main.py --source csv  --file data/sample_orders.csv
  python main.py --source json --file data/sample_products.json
  python main.py --source ndjson --file data/sample_events.ndjson
  python main.py --source api  --url https://api.example.com/orders
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from pipeline.config import Config
from pipeline.ingestion.csv_ingester import CSVIngester
from pipeline.ingestion.json_ingester import JSONIngester
from pipeline.ingestion.api_ingester import APIIngester
from pipeline.loader.db_loader import DBLoader
from pipeline.models import Base
from pipeline.transformations.transformer import DataTransformer
from pipeline.utils.logger import get_logger
from pipeline.utils.metrics import PipelineMetrics

logger = get_logger("main")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Scalable Data Ingestion Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--source",
        required=True,
        choices=["csv", "json", "ndjson", "api"],
        help="Ingestion source type.",
    )
    parser.add_argument("--file", help="Path to CSV or JSON / NDJSON file.")
    parser.add_argument("--url", help="REST API URL (when --source api).")
    parser.add_argument(
        "--db-url",
        default=None,
        help="SQLAlchemy DB URL. Defaults to MySQL from .env; pass 'sqlite' for SQLite.",
    )
    parser.add_argument("--batch-size", type=int, default=500, help="DB flush batch size.")
    return parser


def run(args: argparse.Namespace) -> int:
    # ------------------------------------------------------------------
    # Engine setup
    # ------------------------------------------------------------------
    if args.db_url == "sqlite":
        db_url = "sqlite:///pipeline.db"
    elif args.db_url:
        db_url = args.db_url
    else:
        db_url = Config.mysql_url()

    engine = create_engine(db_url, echo=False)
    Base.metadata.create_all(engine)
    logger.info("Connected to DB: %s", db_url.split("@")[-1] if "@" in db_url else db_url)

    # ------------------------------------------------------------------
    # Ingestion
    # ------------------------------------------------------------------
    source_label = args.file or args.url or args.source
    metrics = PipelineMetrics(source=source_label)

    if args.source == "csv":
        if not args.file:
            logger.error("--file is required for CSV source")
            return 1
        raw = CSVIngester(args.file).ingest()

    elif args.source in ("json", "ndjson"):
        if not args.file:
            logger.error("--file is required for JSON/NDJSON source")
            return 1
        raw = JSONIngester(args.file, ndjson=(args.source == "ndjson")).ingest()

    elif args.source == "api":
        if not args.url:
            logger.error("--url is required for API source")
            return 1
        raw = APIIngester(args.url).ingest()

    else:
        logger.error("Unknown source: %s", args.source)
        return 1

    logger.info("Ingested %d raw records from %s", len(raw), source_label)

    # ------------------------------------------------------------------
    # Transform & Load
    # ------------------------------------------------------------------
    with Session(engine) as session:
        transformer = DataTransformer(session, metrics)
        loader = DBLoader(session, batch_size=args.batch_size)

        # For CSV order files: extract customers / categories / orders
        if args.source == "csv":
            # Categories
            unique_cats = list({r.get("category") for r in raw if r.get("category")})
            categories = transformer.transform_categories([{"name": c} for c in unique_cats])
            loader.load_categories(categories)
            session.flush()
            cat_map = loader.get_category_map()

            # Customers
            customer_recs = [
                {
                    "name": r.get("customer_name", ""),
                    "email": r.get("customer_email", ""),
                    "phone": r.get("customer_phone"),
                }
                for r in raw
                if r.get("customer_email")
            ]
            customers = transformer.transform_customers(customer_recs)
            loader.load_customers(customers)
            session.flush()
            customer_map = loader.get_customer_map()

            # Orders
            order_recs = [
                {
                    "customer_email": r.get("customer_email", ""),
                    "status": r.get("status", "pending"),
                    "total_amount": r.get("total_amount", "0"),
                    "ordered_at": r.get("ordered_at", ""),
                }
                for r in raw
                if r.get("customer_email")
            ]
            orders = transformer.transform_orders(order_recs, customer_map)
            loader.load_orders(orders)

        elif args.source in ("json", "ndjson"):
            # Treat as product feed by default
            cat_names = list({r.get("category") for r in raw if r.get("category")})
            categories = transformer.transform_categories([{"name": c} for c in cat_names])
            loader.load_categories(categories)
            session.flush()
            cat_map = loader.get_category_map()

            products = transformer.transform_products(raw, cat_map)
            loader.load_products(products)

        session.flush()
        metrics.finish()
        run_record = loader.save_pipeline_run(metrics)

    logger.info("Pipeline complete — run_id=%s | %s", run_record.id, metrics.summary())
    return 0


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    sys.exit(run(args))


if __name__ == "__main__":
    main()
