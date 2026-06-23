"""Unit tests for Pydantic validators."""

from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic import ValidationError

from pipeline.cleaning.validators import (
    CategorySchema,
    CustomerSchema,
    OrderSchema,
    ProductSchema,
    _parse_datetime,
)


# ---------------------------------------------------------------------------
# _parse_datetime helper
# ---------------------------------------------------------------------------
class TestParseDatetime:
    @pytest.mark.parametrize(
        "value,expected_year,expected_month",
        [
            ("2024-01-15 09:23:00", 2024, 1),
            ("2024-01-15T09:23:00", 2024, 1),
            ("2024-01-15T09:23:00Z", 2024, 1),
            ("2024-01-15", 2024, 1),
            ("15/01/2024", 2024, 1),
            ("01/15/2024", 2024, 1),
        ],
    )
    def test_parses_various_formats(self, value, expected_year, expected_month) -> None:
        dt = _parse_datetime(value)
        assert dt.year == expected_year
        assert dt.month == expected_month

    def test_raises_on_garbage(self) -> None:
        with pytest.raises(ValueError, match="Cannot parse datetime"):
            _parse_datetime("not-a-date")


# ---------------------------------------------------------------------------
# CustomerSchema
# ---------------------------------------------------------------------------
class TestCustomerSchema:
    def test_valid_customer(self) -> None:
        s = CustomerSchema(name="Alice", email="alice@example.com", phone="+1-555-0101")
        assert s.name == "Alice"

    def test_name_is_stripped(self) -> None:
        s = CustomerSchema(name="  Bob  ", email="bob@x.com")
        assert s.name == "Bob"

    def test_invalid_email_raises(self) -> None:
        with pytest.raises(ValidationError):
            CustomerSchema(name="Alice", email="not-an-email")

    def test_empty_name_raises(self) -> None:
        with pytest.raises(ValidationError):
            CustomerSchema(name="", email="alice@x.com")

    def test_invalid_phone_raises(self) -> None:
        with pytest.raises(ValidationError):
            CustomerSchema(name="Alice", email="alice@x.com", phone="abc")

    def test_none_phone_is_accepted(self) -> None:
        s = CustomerSchema(name="Alice", email="alice@x.com", phone=None)
        assert s.phone is None

    def test_empty_string_phone_becomes_none(self) -> None:
        s = CustomerSchema(name="Alice", email="alice@x.com", phone="")
        assert s.phone is None


# ---------------------------------------------------------------------------
# CategorySchema
# ---------------------------------------------------------------------------
class TestCategorySchema:
    def test_name_is_title_cased(self) -> None:
        s = CategorySchema(name="electronics")
        assert s.name == "Electronics"

    def test_empty_name_raises(self) -> None:
        with pytest.raises(ValidationError):
            CategorySchema(name="")


# ---------------------------------------------------------------------------
# ProductSchema
# ---------------------------------------------------------------------------
class TestProductSchema:
    def test_valid_product(self) -> None:
        s = ProductSchema(sku="widget-001", name="Widget", price="149.99")
        assert s.sku == "WIDGET-001"  # normalized to upper

    def test_negative_price_raises(self) -> None:
        with pytest.raises(ValidationError):
            ProductSchema(sku="X", name="Item", price="-1.00")

    def test_non_numeric_price_raises(self) -> None:
        with pytest.raises(ValidationError):
            ProductSchema(sku="X", name="Item", price="abc")

    def test_sku_normalized_to_uppercase(self) -> None:
        s = ProductSchema(sku="  widget-001  ", name="Widget", price="10.00")
        assert s.sku == "WIDGET-001"

    def test_empty_sku_raises(self) -> None:
        with pytest.raises(ValidationError):
            ProductSchema(sku="", name="Item", price="9.99")


# ---------------------------------------------------------------------------
# OrderSchema
# ---------------------------------------------------------------------------
class TestOrderSchema:
    def test_valid_order(self) -> None:
        s = OrderSchema(
            customer_email="alice@x.com",
            status="PENDING",
            total_amount="99.99",
            ordered_at="2024-01-15",
        )
        assert s.status == "pending"  # normalized to lower
        assert s.total_amount == Decimal("99.99")

    def test_negative_amount_raises(self) -> None:
        with pytest.raises(ValidationError):
            OrderSchema(
                customer_email="a@b.com",
                total_amount="-1.00",
                ordered_at="2024-01-15",
            )

    def test_bad_date_raises(self) -> None:
        with pytest.raises(ValidationError):
            OrderSchema(
                customer_email="a@b.com",
                total_amount="10.00",
                ordered_at="not-a-date",
            )

    def test_default_status_is_pending(self) -> None:
        s = OrderSchema(
            customer_email="a@b.com",
            total_amount="10.00",
            ordered_at="2024-01-15",
        )
        assert s.status == "pending"
