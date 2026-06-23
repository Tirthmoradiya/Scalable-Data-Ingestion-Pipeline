"""
Tests for DataProfiler — data quality checks against SQLite.
"""
from __future__ import annotations

import pytest
from sqlalchemy.orm import Session

from pipeline.models import Category, Customer
from pipeline.quality.profiler import ColumnProfile, DataProfiler, TableProfile


class TestColumnProfile:
    def test_null_rate_calculation(self) -> None:
        col = ColumnProfile(name="email", total_rows=100, null_count=25, distinct_count=80)
        assert col.null_rate == pytest.approx(0.25)
        assert col.to_dict()["null_rate_pct"] == pytest.approx(25.0)

    def test_distinct_rate_calculation(self) -> None:
        col = ColumnProfile(name="name", total_rows=100, null_count=0, distinct_count=50)
        assert col.distinct_rate == pytest.approx(0.5)

    def test_zero_rows_does_not_divide_by_zero(self) -> None:
        col = ColumnProfile(name="x", total_rows=0, null_count=0, distinct_count=0)
        assert col.null_rate == 0.0
        assert col.distinct_rate == 0.0

    def test_to_dict_keys(self) -> None:
        col = ColumnProfile(name="email", total_rows=10, null_count=2, distinct_count=8)
        d = col.to_dict()
        assert "column" in d
        assert "null_rate_pct" in d
        assert "distinct_count" in d


class TestTableProfile:
    def test_reconciliation_ok_when_db_lte_ingested(self) -> None:
        # DB has 90 rows, ingested 100 (10 were duped or invalid) → ok
        tp = TableProfile(table_name="customers", total_rows=90, ingested_rows=100)
        assert tp.reconciliation_ok is True

    def test_reconciliation_fails_when_db_gt_ingested(self) -> None:
        # DB has MORE rows than we ingested — data appeared from elsewhere
        tp = TableProfile(table_name="customers", total_rows=110, ingested_rows=100)
        assert tp.reconciliation_ok is False

    def test_high_null_columns_detected(self) -> None:
        cols = [
            ColumnProfile("email", 100, 0, 100),
            ColumnProfile("phone", 100, 80, 20),  # 80% null — high
        ]
        tp = TableProfile("t", 100, 100, cols)
        assert len(tp.high_null_columns) == 1
        assert tp.high_null_columns[0].name == "phone"

    def test_report_contains_table_name(self) -> None:
        tp = TableProfile("orders", 50, 60)
        assert "orders" in tp.report()


class TestDataProfilerIntegration:
    def test_profiles_customers_table(self, session: Session) -> None:
        # Seed some customers
        session.add_all([
            Customer(name="Alice", email="alice@x.com"),
            Customer(name="Bob", email="bob@x.com", phone=None),
        ])
        session.flush()

        profiler = DataProfiler(session)
        profile = profiler.profile_table(
            "customers", ingested_rows=2, columns=["name", "email", "phone"]
        )

        assert profile.table_name == "customers"
        assert profile.total_rows == 2
        assert len(profile.columns) == 3

    def test_phone_null_rate_correct(self, session: Session) -> None:
        session.add_all([
            Customer(name="Alice", email="a@x.com", phone=None),
            Customer(name="Bob", email="b@x.com", phone=None),
            Customer(name="Carol", email="c@x.com", phone="+1-555"),
        ])
        session.flush()

        profiler = DataProfiler(session)
        profile = profiler.profile_table(
            "customers", ingested_rows=3, columns=["phone"]
        )
        phone_col = profile.columns[0]
        assert phone_col.null_count == 2
        assert phone_col.null_rate == pytest.approx(2 / 3)

    def test_profiles_categories_table(self, session: Session) -> None:
        session.add_all([Category(name="Electronics"), Category(name="Books")])
        session.flush()

        profiler = DataProfiler(session)
        profile = profiler.profile_table("categories", ingested_rows=2, columns=["name"])
        assert profile.total_rows == 2
