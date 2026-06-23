"""Unit tests for DataTransformer."""
from __future__ import annotations

import pytest

from pipeline.transformations.transformer import DataTransformer
from pipeline.utils.metrics import PipelineMetrics


class TestTransformCustomers:
    def test_returns_customer_objects(self, session, metrics, raw_customers) -> None:
        t = DataTransformer(session, metrics)
        result = t.transform_customers(raw_customers)
        # N/A phone should become None, so 3 valid records (Alice, Bob, Carol)
        assert len(result) == 3

    def test_deduplicates_emails(self, session, metrics) -> None:
        raw = [
            {"name": "Alice", "email": "alice@x.com"},
            {"name": "Alice Duplicate", "email": "alice@x.com"},
        ]
        t = DataTransformer(session, metrics)
        result = t.transform_customers(raw)
        assert len(result) == 1

    def test_invalid_email_is_skipped_and_recorded(self, session, metrics) -> None:
        raw = [{"name": "Bad", "email": "not-an-email"}]
        t = DataTransformer(session, metrics)
        result = t.transform_customers(raw)
        assert result == []
        assert metrics.rows_failed == 1

    def test_email_is_lowercased(self, session, metrics) -> None:
        raw = [{"name": "Alice", "email": "Alice@Example.COM"}]
        t = DataTransformer(session, metrics)
        result = t.transform_customers(raw)
        assert result[0].email == "alice@example.com"

    def test_na_phone_becomes_none(self, session, metrics) -> None:
        raw = [{"name": "Alice", "email": "alice@x.com", "phone": "N/A"}]
        t = DataTransformer(session, metrics)
        result = t.transform_customers(raw)
        assert result[0].phone is None

    def test_metrics_incremented_on_success(self, session, metrics) -> None:
        raw = [{"name": "Alice", "email": "alice@x.com"}]
        t = DataTransformer(session, metrics)
        t.transform_customers(raw)
        assert metrics.rows_ingested == 1


class TestTransformCategories:
    def test_deduplicates_categories(self, session, metrics, raw_categories) -> None:
        t = DataTransformer(session, metrics)
        result = t.transform_categories(raw_categories)
        names = {c.name for c in result}
        assert len(names) == len(result)  # no dupes

    def test_three_unique_categories(self, session, metrics, raw_categories) -> None:
        t = DataTransformer(session, metrics)
        result = t.transform_categories(raw_categories)
        assert len(result) == 3  # electronics, books, gadgets (dup removed)


class TestTransformProducts:
    def test_deduplicates_skus(self, session, metrics, raw_products) -> None:
        t = DataTransformer(session, metrics)
        result = t.transform_products(raw_products, category_map={})
        skus = [p.sku for p in result]
        assert len(skus) == len(set(skus))

    def test_empty_sku_is_rejected(self, session, metrics) -> None:
        raw = [{"sku": "", "name": "Item", "price": "9.99"}]
        t = DataTransformer(session, metrics)
        result = t.transform_products(raw, category_map={})
        assert result == []
        assert metrics.rows_failed >= 1

    def test_category_fk_resolved(self, session, metrics) -> None:
        raw = [{"sku": "W-001", "name": "Widget", "category": "Electronics", "price": "99.99"}]
        cat_map = {"Electronics": 42}
        t = DataTransformer(session, metrics)
        result = t.transform_products(raw, category_map=cat_map)
        assert result[0].category_id == 42

    def test_unknown_category_leaves_fk_none(self, session, metrics) -> None:
        raw = [{"sku": "W-001", "name": "Widget", "category": "Unknown", "price": "99.99"}]
        t = DataTransformer(session, metrics)
        result = t.transform_products(raw, category_map={})
        assert result[0].category_id is None


class TestTransformOrders:
    def test_valid_orders_returned(self, session, metrics, raw_orders) -> None:
        customer_map = {"alice@example.com": 1, "bob@example.com": 2}
        t = DataTransformer(session, metrics)
        result = t.transform_orders(raw_orders, customer_map)
        assert len(result) == 2  # 3rd has unknown email

    def test_unknown_customer_email_skipped(self, session, metrics) -> None:
        raw = [{"customer_email": "nobody@x.com", "total_amount": "10", "ordered_at": "2024-01-01"}]
        t = DataTransformer(session, metrics)
        result = t.transform_orders(raw, customer_map={})
        assert result == []
        assert metrics.rows_failed == 1

    def test_status_normalised_to_lowercase(self, session, metrics) -> None:
        raw = [{"customer_email": "a@x.com", "status": "COMPLETED", "total_amount": "50", "ordered_at": "2024-01-01"}]
        t = DataTransformer(session, metrics)
        result = t.transform_orders(raw, customer_map={"a@x.com": 1})
        assert result[0].status == "completed"
