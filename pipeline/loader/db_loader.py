"""
DBLoader — bulk-upserts ORM model instances into the database.

Uses SQLAlchemy 2.x ``Session.add_all()`` with ``merge()`` for upsert
semantics on unique-constrained columns (email for customers, sku for products,
name for categories).

For tables without natural unique keys (orders, order_items, pipeline_runs),
plain inserts are used since duplicates are not expected at the ORM level.
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy.orm import Session

from pipeline.models import (
    Category,
    Customer,
    Order,
    OrderItem,
    PipelineRun,
    Product,
)
from pipeline.utils.logger import get_logger
from pipeline.utils.metrics import PipelineMetrics

logger = get_logger(__name__)

BATCH_SIZE = 500  # rows per flush


class DBLoader:
    """
    Loads transformed ORM objects into the database in configurable batch sizes.

    Parameters
    ----------
    session:
        An active SQLAlchemy ``Session``.
    batch_size:
        Number of rows to flush per batch (default: 500).
    """

    def __init__(self, session: Session, batch_size: int = BATCH_SIZE) -> None:
        self._session = session
        self._batch_size = batch_size

    # ------------------------------------------------------------------
    # Generic bulk loader
    # ------------------------------------------------------------------
    def _bulk_load(self, objects: list) -> int:
        """Insert objects in batches; returns the count successfully committed."""
        loaded = 0
        for i in range(0, len(objects), self._batch_size):
            batch = objects[i : i + self._batch_size]
            self._session.add_all(batch)
            self._session.flush()
            loaded += len(batch)
            logger.debug("Flushed batch of %d %s rows", len(batch), type(batch[0]).__name__)
        return loaded

    # ------------------------------------------------------------------
    # Entity-specific loaders
    # ------------------------------------------------------------------
    def load_categories(self, categories: list[Category]) -> int:
        if not categories:
            return 0
        merged = [self._session.merge(c) for c in categories]
        return self._bulk_load(merged)

    def load_customers(self, customers: list[Customer]) -> int:
        if not customers:
            return 0
        merged = [self._session.merge(c) for c in customers]
        return self._bulk_load(merged)

    def load_products(self, products: list[Product]) -> int:
        if not products:
            return 0
        merged = [self._session.merge(p) for p in products]
        return self._bulk_load(merged)

    def load_orders(self, orders: list[Order]) -> int:
        if not orders:
            return 0
        return self._bulk_load(orders)

    def load_order_items(self, items: list[OrderItem]) -> int:
        if not items:
            return 0
        return self._bulk_load(items)

    def commit(self) -> None:
        self._session.commit()

    def rollback(self) -> None:
        self._session.rollback()

    # ------------------------------------------------------------------
    # Audit log
    # ------------------------------------------------------------------
    def save_pipeline_run(self, metrics: PipelineMetrics) -> PipelineRun:
        run = PipelineRun(
            source=metrics.source,
            rows_ingested=metrics.rows_ingested,
            rows_failed=metrics.rows_failed,
            error_log=metrics.error_log,
            started_at=metrics.started_at,
            finished_at=metrics.finished_at or datetime.now(UTC),
        )
        self._session.add(run)
        self._session.commit()
        logger.info("Pipeline run saved: id=%s %s", run.id, metrics.summary())
        return run

    # ------------------------------------------------------------------
    # FK map helpers
    # ------------------------------------------------------------------
    def get_category_map(self) -> dict[str, int]:
        """Return {name: id} for all categories currently in DB."""
        rows = self._session.query(Category.name, Category.id).all()
        return {name: id_ for name, id_ in rows}

    def get_customer_map(self) -> dict[str, int]:
        """Return {email: id} for all customers currently in DB."""
        rows = self._session.query(Customer.email, Customer.id).all()
        return {email: id_ for email, id_ in rows}

    def get_product_map(self) -> dict[str, int]:
        """Return {sku: id} for all products currently in DB."""
        rows = self._session.query(Product.sku, Product.id).all()
        return {sku: id_ for sku, id_ in rows}
