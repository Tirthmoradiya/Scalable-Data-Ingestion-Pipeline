"""Integration tests for DBLoader against in-memory SQLite."""

from __future__ import annotations

from pipeline.loader.db_loader import DBLoader
from pipeline.models import Category, Customer, Order
from pipeline.utils.metrics import PipelineMetrics


class TestDBLoaderCategories:
    def test_load_categories_returns_count(self, session, metrics) -> None:
        loader = DBLoader(session)
        cats = [Category(name="Electronics"), Category(name="Books")]
        count = loader.load_categories(cats)
        session.flush()
        assert count == 2

    def test_load_empty_list_returns_zero(self, session, metrics) -> None:
        loader = DBLoader(session)
        assert loader.load_categories([]) == 0

    def test_get_category_map(self, session) -> None:
        loader = DBLoader(session)
        loader.load_categories([Category(name="Electronics"), Category(name="Books")])
        session.flush()
        cat_map = loader.get_category_map()
        assert "Electronics" in cat_map
        assert isinstance(cat_map["Electronics"], int)


class TestDBLoaderCustomers:
    def test_load_customers_persists_to_db(self, session) -> None:
        loader = DBLoader(session)
        custs = [
            Customer(name="Alice", email="alice@x.com"),
            Customer(name="Bob", email="bob@x.com"),
        ]
        loader.load_customers(custs)
        session.flush()
        from sqlalchemy import select

        rows = session.execute(select(Customer)).scalars().all()
        assert len(rows) == 2

    def test_get_customer_map(self, session) -> None:
        loader = DBLoader(session)
        loader.load_customers([Customer(name="Alice", email="alice@x.com")])
        session.flush()
        cm = loader.get_customer_map()
        assert "alice@x.com" in cm

    def test_batching_respects_batch_size(self, session) -> None:
        """Loader with batch_size=2 should handle 5 records correctly."""
        loader = DBLoader(session, batch_size=2)
        custs = [Customer(name=f"User{i}", email=f"u{i}@x.com") for i in range(5)]
        count = loader.load_customers(custs)
        session.flush()
        assert count == 5


class TestDBLoaderOrders:
    def _setup_customer(self, session) -> int:
        loader = DBLoader(session)
        loader.load_customers([Customer(name="Alice", email="alice@x.com")])
        session.flush()
        return loader.get_customer_map()["alice@x.com"]

    def test_load_order_persists(self, session) -> None:
        from datetime import datetime

        customer_id = self._setup_customer(session)
        loader = DBLoader(session)
        orders = [
            Order(
                customer_id=customer_id,
                status="pending",
                total_amount=99.99,
                ordered_at=datetime(2024, 1, 15),
            )
        ]
        count = loader.load_orders(orders)
        session.flush()
        assert count == 1


class TestSavePipelineRun:
    def test_saves_run_and_returns_object(self, session) -> None:
        metrics = PipelineMetrics(source="test_source")
        metrics.record_success(10)
        metrics.record_failure("bad row")
        metrics.finish()

        loader = DBLoader(session)
        run = loader.save_pipeline_run(metrics)
        assert run.id is not None
        assert run.rows_ingested == 10
        assert run.rows_failed == 1
        assert "bad row" in run.error_log
