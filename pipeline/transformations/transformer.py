"""
DataTransformer — resolves foreign keys, deduplicates records, and converts
validated Pydantic schemas into ORM model instances ready for bulk insert.
"""

from __future__ import annotations

from typing import Any

from pydantic import ValidationError
from sqlalchemy.orm import Session

from pipeline.cleaning.cleaner import DataCleaner
from pipeline.cleaning.validators import (
    CategorySchema,
    CustomerSchema,
    OrderSchema,
    ProductSchema,
)
from pipeline.models import Category, Customer, Order, Product
from pipeline.utils.logger import get_logger
from pipeline.utils.metrics import PipelineMetrics

logger = get_logger(__name__)


class DataTransformer:
    """
    Validates, deduplicates, and transforms raw dicts → ORM model instances.

    Usage
    -----
    transformer = DataTransformer(session, metrics)
    customers = transformer.transform_customers(raw_records)
    """

    def __init__(self, session: Session, metrics: PipelineMetrics) -> None:
        self._session = session
        self._metrics = metrics

    # ------------------------------------------------------------------
    # Customers
    # ------------------------------------------------------------------
    def transform_customers(self, raw: list[dict[str, Any]]) -> list[Customer]:
        cleaned = DataCleaner.clean_records(raw)
        seen_emails: set[str] = set()
        customers: list[Customer] = []

        for record in cleaned:
            try:
                schema = CustomerSchema.model_validate(record)
            except ValidationError as exc:
                self._metrics.record_failure(f"customer validation: {exc}")
                logger.warning("Customer validation failed: %s", exc)
                continue

            email = str(schema.email).lower()
            if email in seen_emails:
                logger.debug("Duplicate customer email skipped: %s", email)
                continue
            seen_emails.add(email)

            customers.append(Customer(name=schema.name, email=email, phone=schema.phone))
            self._metrics.record_success()

        return customers

    # ------------------------------------------------------------------
    # Categories
    # ------------------------------------------------------------------
    def transform_categories(self, raw: list[dict[str, Any]]) -> list[Category]:
        cleaned = DataCleaner.clean_records(raw)
        seen_names: set[str] = set()
        categories: list[Category] = []

        for record in cleaned:
            try:
                schema = CategorySchema.model_validate(record)
            except ValidationError as exc:
                self._metrics.record_failure(f"category validation: {exc}")
                logger.warning("Category validation failed: %s", exc)
                continue

            if schema.name in seen_names:
                continue
            seen_names.add(schema.name)
            categories.append(Category(name=schema.name))
            self._metrics.record_success()

        return categories

    # ------------------------------------------------------------------
    # Products
    # ------------------------------------------------------------------
    def transform_products(
        self, raw: list[dict[str, Any]], category_map: dict[str, int]
    ) -> list[Product]:
        """
        Parameters
        ----------
        raw:
            Raw product dicts.
        category_map:
            ``{category_name: category_id}`` for FK resolution.
        """
        cleaned = DataCleaner.clean_records(raw)
        seen_skus: set[str] = set()
        products: list[Product] = []

        for record in cleaned:
            try:
                schema = ProductSchema.model_validate(record)
            except ValidationError as exc:
                self._metrics.record_failure(f"product validation: {exc}")
                logger.warning("Product validation failed: %s", exc)
                continue

            if schema.sku in seen_skus:
                logger.debug("Duplicate SKU skipped: %s", schema.sku)
                continue
            seen_skus.add(schema.sku)

            cat_id = category_map.get(schema.category.title()) if schema.category else None
            products.append(
                Product(
                    sku=schema.sku,
                    name=schema.name,
                    category_id=cat_id,
                    price=float(schema.price),
                )
            )
            self._metrics.record_success()

        return products

    # ------------------------------------------------------------------
    # Orders
    # ------------------------------------------------------------------
    def transform_orders(
        self, raw: list[dict[str, Any]], customer_map: dict[str, int]
    ) -> list[Order]:
        """
        Parameters
        ----------
        raw:
            Raw order dicts.
        customer_map:
            ``{email: customer_id}`` for FK resolution.
        """
        cleaned = DataCleaner.clean_records(raw)
        orders: list[Order] = []

        for record in cleaned:
            try:
                schema = OrderSchema.model_validate(record)
            except ValidationError as exc:
                self._metrics.record_failure(f"order validation: {exc}")
                logger.warning("Order validation failed: %s", exc)
                continue

            email = str(schema.customer_email).lower()
            customer_id = customer_map.get(email)
            if customer_id is None:
                self._metrics.record_failure(f"Unknown customer email: {email}")
                logger.warning("Unknown customer email — order skipped: %s", email)
                continue

            orders.append(
                Order(
                    customer_id=customer_id,
                    status=schema.status,
                    total_amount=float(schema.total_amount),
                    ordered_at=schema.ordered_at,
                )
            )
            self._metrics.record_success()

        return orders
