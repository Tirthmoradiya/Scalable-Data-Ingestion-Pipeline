"""
DBLoader — bulk-upserts ORM model instances into the database.

Uses database-dialect-specific bulk upserts (`on_conflict_do_update` for SQLite/PostgreSQL, 
`on_duplicate_key_update` for MySQL) on unique-constrained columns (email for customers,
sku for products, name for categories).

For tables without natural unique keys (orders, order_items, pipeline_runs),
optimized bulk inserts are used since duplicates are not expected.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

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
    def _bulk_load(self, objects: list[Any]) -> int:
        """Insert objects in batches using fast bulk insert."""
        if not objects:
            return 0
        model_class = objects[0].__class__

        # Align keys: determine which columns to include based on objects
        cols_to_include = []
        for col in model_class.__table__.columns:
            if col.key == "id" and all(getattr(obj, "id", None) is None for obj in objects):
                continue
            if col.key == "created_at" and all(getattr(obj, "created_at", None) is None for obj in objects):
                continue
            cols_to_include.append(col.key)

        # Convert ORM objects to dictionary records with matching keys
        records = []
        for obj in objects:
            record = {key: getattr(obj, key) for key in cols_to_include}
            records.append(record)

        loaded = 0
        from sqlalchemy import insert
        for i in range(0, len(records), self._batch_size):
            batch = records[i : i + self._batch_size]
            stmt = insert(model_class).values(batch)
            self._session.execute(stmt)
            self._session.flush()
            loaded += len(batch)
            logger.debug("Flushed batch of %d %s rows via bulk insert", len(batch), model_class.__name__)
        return loaded

    # ------------------------------------------------------------------
    # Dynamic dialect-specific upsert
    # ------------------------------------------------------------------
    def _execute_upsert(
        self,
        model_class: Any,
        unique_keys: list[str],
        update_keys: list[str],
        objects: list[Any],
    ) -> int:
        if not objects:
            return 0

        dialect_name = self._session.bind.dialect.name

        # Align keys: determine which columns to include based on objects
        cols_to_include = []
        for col in model_class.__table__.columns:
            if col.key == "id" and all(getattr(obj, "id", None) is None for obj in objects):
                continue
            if col.key == "created_at" and all(getattr(obj, "created_at", None) is None for obj in objects):
                continue
            cols_to_include.append(col.key)

        # Convert ORM objects to dictionary records with matching keys
        records = []
        for obj in objects:
            record = {key: getattr(obj, key) for key in cols_to_include}
            records.append(record)

        loaded = 0
        for i in range(0, len(records), self._batch_size):
            batch = records[i : i + self._batch_size]
            if dialect_name == "sqlite":
                from sqlalchemy.dialects.sqlite import insert as sqlite_insert

                stmt = sqlite_insert(model_class).values(batch)
                set_dict = {k: getattr(stmt.excluded, k) for k in update_keys}
                stmt = stmt.on_conflict_do_update(
                    index_elements=unique_keys, set_=set_dict
                )
                self._session.execute(stmt)
            elif dialect_name in ("postgresql", "postgres"):
                from sqlalchemy.dialects.postgresql import insert as pg_insert

                stmt = pg_insert(model_class).values(batch)
                set_dict = {k: getattr(stmt.excluded, k) for k in update_keys}
                stmt = stmt.on_conflict_do_update(
                    index_elements=unique_keys, set_=set_dict
                )
                self._session.execute(stmt)
            elif dialect_name == "mysql":
                from sqlalchemy.dialects.mysql import insert as mysql_insert

                stmt = mysql_insert(model_class).values(batch)
                set_dict = {k: getattr(stmt.inserted, k) for k in update_keys}
                stmt = stmt.on_duplicate_key_update(**set_dict)
                self._session.execute(stmt)
            else:
                # Fallback to merge
                for r in objects[i : i + self._batch_size]:
                    self._session.merge(r)

            self._session.flush()
            loaded += len(batch)
            logger.debug(
                "Upserted batch of %d %s rows via %s dialect",
                len(batch),
                model_class.__name__,
                dialect_name,
            )
        return loaded

    # ------------------------------------------------------------------
    # Entity-specific loaders
    # ------------------------------------------------------------------
    def load_categories(self, categories: list[Category]) -> int:
        return self._execute_upsert(
            model_class=Category,
            unique_keys=["name"],
            update_keys=["parent_id"],
            objects=categories,
        )

    def load_customers(self, customers: list[Customer]) -> int:
        return self._execute_upsert(
            model_class=Customer,
            unique_keys=["email"],
            update_keys=["name", "phone"],
            objects=customers,
        )

    def load_products(self, products: list[Product]) -> int:
        return self._execute_upsert(
            model_class=Product,
            unique_keys=["sku"],
            update_keys=["name", "category_id", "price"],
            objects=products,
        )

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
