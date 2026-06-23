"""
Shared PyTest fixtures — all tests use an in-memory SQLite database so no
MySQL server is required to run the test suite.
"""
from __future__ import annotations

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session

from pipeline.models import Base
from pipeline.utils.metrics import PipelineMetrics


# ---------------------------------------------------------------------------
# Database fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="function")
def engine():
    """Fresh in-memory SQLite engine per test function."""
    _engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
    )
    # Enable FK enforcement in SQLite (disabled by default)
    @event.listens_for(_engine, "connect")
    def set_sqlite_pragma(dbapi_conn, _):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    Base.metadata.create_all(_engine)
    yield _engine
    Base.metadata.drop_all(_engine)
    _engine.dispose()


@pytest.fixture(scope="function")
def session(engine):
    """Transactional session that rolls back after each test."""
    connection = engine.connect()
    transaction = connection.begin()
    _session = Session(bind=connection)
    yield _session
    _session.close()
    transaction.rollback()
    connection.close()


# ---------------------------------------------------------------------------
# Metrics fixture
# ---------------------------------------------------------------------------
@pytest.fixture
def metrics():
    return PipelineMetrics(source="test")


# ---------------------------------------------------------------------------
# Sample raw data fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def raw_customers():
    return [
        {"name": "Alice Johnson", "email": "alice@example.com", "phone": "+1-555-0101"},
        {"name": "Bob Smith",     "email": "bob@example.com",   "phone": None},
        {"name": "  Carol  ",     "email": "carol@example.com", "phone": "N/A"},
    ]


@pytest.fixture
def raw_categories():
    return [
        {"name": "electronics"},
        {"name": "BOOKS"},
        {"name": "Gadgets"},
        {"name": "electronics"},  # duplicate — should be deduplicated
    ]


@pytest.fixture
def raw_products():
    return [
        {"sku": "widget-001", "name": "Super Widget", "category": "Electronics", "price": "149.99"},
        {"sku": "BOOK-042",   "name": "Clean Code",   "category": "Books",       "price": "29.99"},
        {"sku": "widget-001", "name": "Duplicate",    "category": "Electronics", "price": "1.00"},
        {"sku": "",           "name": "No SKU",       "category": "Misc",        "price": "9.99"},
    ]


@pytest.fixture
def raw_orders():
    return [
        {
            "customer_email": "alice@example.com",
            "status": "completed",
            "total_amount": "299.98",
            "ordered_at": "2024-01-15 09:23:00",
        },
        {
            "customer_email": "bob@example.com",
            "status": "PENDING",
            "total_amount": "49.99",
            "ordered_at": "2024-01-16",
        },
        {
            "customer_email": "unknown@nowhere.com",  # unknown customer
            "status": "pending",
            "total_amount": "10.00",
            "ordered_at": "2024-01-17",
        },
    ]
