"""
End-to-end pipeline test:
  sample_orders.csv → clean → validate → transform → SQLite → assertions
"""
from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from pipeline.cleaning.cleaner import DataCleaner
from pipeline.ingestion.csv_ingester import CSVIngester
from pipeline.ingestion.json_ingester import JSONIngester
from pipeline.loader.db_loader import DBLoader
from pipeline.models import Category, Customer, Order, Product
from pipeline.transformations.transformer import DataTransformer
from pipeline.utils.metrics import PipelineMetrics

SAMPLE_DIR = Path(__file__).parent.parent / "data"


class TestE2EOrdersPipeline:
    """
    Full pipeline run: CSV → clean → transform → load → assert.
    Uses the bundled sample_orders.csv which contains known edge cases.
    """

    def _run_pipeline(self, session: Session) -> PipelineMetrics:
        metrics = PipelineMetrics(source="sample_orders.csv")

        # 1. Ingest raw records from CSV
        raw = CSVIngester(SAMPLE_DIR / "sample_orders.csv").ingest()

        # 2. Seed categories & customers from the CSV data
        transformer = DataTransformer(session, metrics)
        loader = DBLoader(session)

        # Unique categories from CSV
        unique_cats = list({r["category"] for r in raw if r.get("category")})
        cat_records = [{"name": c} for c in unique_cats]
        categories = transformer.transform_categories(cat_records)
        loader.load_categories(categories)
        session.flush()
        cat_map = loader.get_category_map()

        # Unique customers from CSV
        customer_records = [
            {"name": r["customer_name"], "email": r["customer_email"], "phone": r.get("customer_phone")}
            for r in raw
            if r.get("customer_email")
        ]
        customers = transformer.transform_customers(customer_records)
        loader.load_customers(customers)
        session.flush()
        customer_map = loader.get_customer_map()

        # 3. Transform and load orders
        order_records = [
            {
                "customer_email": r["customer_email"],
                "status": r["status"],
                "total_amount": r["total_amount"],
                "ordered_at": r["ordered_at"],
            }
            for r in raw
            if r.get("customer_email")
        ]
        orders = transformer.transform_orders(order_records, customer_map)
        loader.load_orders(orders)
        session.flush()

        metrics.finish()
        loader.save_pipeline_run(metrics)
        return metrics

    def test_e2e_customers_loaded(self, session) -> None:
        self._run_pipeline(session)
        rows = session.execute(select(Customer)).scalars().all()
        # sample_orders.csv has 8 rows; 1 has missing email (skipped), alice appears twice (dedup'd)
        # Unique emails: alice, bob, carol, dave, eve, frank → 6
        assert len(rows) == 6

    def test_e2e_orders_loaded(self, session) -> None:
        self._run_pipeline(session)
        rows = session.execute(select(Order)).scalars().all()
        # 6 rows have email; 1 has quantity=0 (still valid order), dave@'s amount=0 is valid
        # 1 row has missing email → skipped during customer extraction
        assert len(rows) >= 5

    def test_e2e_no_integrity_errors(self, session) -> None:
        """Ensure pipeline commits without FK violations."""
        metrics = self._run_pipeline(session)
        # Pipeline ran without unhandled exceptions; metrics have been recorded
        assert metrics.finished_at is not None

    def test_e2e_pipeline_run_audit_persisted(self, session) -> None:
        from pipeline.models import PipelineRun
        self._run_pipeline(session)
        run = session.execute(select(PipelineRun)).scalars().first()
        assert run is not None
        assert run.source == "sample_orders.csv"


class TestE2EProductsPipeline:
    """Full pipeline run from sample_products.json."""

    def test_products_loaded_deduped(self, session) -> None:
        metrics = PipelineMetrics(source="sample_products.json")
        raw = JSONIngester(SAMPLE_DIR / "sample_products.json").ingest()

        transformer = DataTransformer(session, metrics)
        loader = DBLoader(session)

        products = transformer.transform_products(raw, category_map={})
        loader.load_products(products)
        session.flush()

        rows = session.execute(select(Product)).scalars().all()
        skus = [p.sku for p in rows]
        # WIDGET-001 appears twice, empty SKU + invalid price → only 4 valid unique products
        assert len(rows) == 4
        assert len(skus) == len(set(skus))  # no duplicate SKUs

    def test_failed_records_are_tracked(self, session) -> None:
        metrics = PipelineMetrics(source="sample_products.json")
        raw = JSONIngester(SAMPLE_DIR / "sample_products.json").ingest()
        transformer = DataTransformer(session, metrics)
        transformer.transform_products(raw, category_map={})
        # empty SKU + invalid price should register as failures
        assert metrics.rows_failed >= 2
