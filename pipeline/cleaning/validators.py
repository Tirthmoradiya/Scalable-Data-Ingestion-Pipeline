"""
Pydantic v2 schemas used to validate raw records before they enter the DB.

Each schema mirrors its ORM counterpart but accepts loose input types
(strings for dates, optional/missing fields) and coerces them into
typed, clean values.
"""

from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, EmailStr, Field, field_validator, model_validator

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
PHONE_RE = re.compile(r"^\+?[\d\s\-().]{7,20}$")


def _parse_datetime(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        return value
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
    ):
        try:
            return datetime.strptime(str(value).strip(), fmt)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse datetime: {value!r}")


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------
class CustomerSchema(BaseModel):
    name: str = Field(..., min_length=1, max_length=256)
    email: EmailStr
    phone: str | None = None

    @field_validator("name")
    @classmethod
    def strip_name(cls, v: str) -> str:
        return v.strip()

    @field_validator("phone")
    @classmethod
    def validate_phone(cls, v: str | None) -> str | None:
        if v is None or v.strip() == "":
            return None
        v = v.strip()
        if not PHONE_RE.match(v):
            raise ValueError(f"Invalid phone number: {v!r}")
        return v


class CategorySchema(BaseModel):
    name: str = Field(..., min_length=1, max_length=128)

    @field_validator("name")
    @classmethod
    def strip_name(cls, v: str) -> str:
        return v.strip().title()


class ProductSchema(BaseModel):
    sku: str = Field(..., min_length=1, max_length=64)
    name: str = Field(..., min_length=1, max_length=256)
    category: str | None = None  # resolved to category_id by transformer
    price: Decimal = Field(..., ge=Decimal("0"))

    @field_validator("sku")
    @classmethod
    def normalize_sku(cls, v: str) -> str:
        return v.strip().upper()

    @field_validator("price", mode="before")
    @classmethod
    def coerce_price(cls, v: object) -> Decimal:
        try:
            return Decimal(str(v)).quantize(Decimal("0.01"))
        except Exception as exc:
            raise ValueError(f"Invalid price: {v!r}") from exc


class OrderSchema(BaseModel):
    customer_email: str  # FK resolved by transformer
    status: str = Field(default="pending", max_length=32)
    total_amount: Decimal = Field(default=Decimal("0"), ge=Decimal("0"))
    ordered_at: datetime

    @field_validator("ordered_at", mode="before")
    @classmethod
    def parse_date(cls, v: object) -> datetime:
        return _parse_datetime(v)  # type: ignore[arg-type]

    @field_validator("status")
    @classmethod
    def normalise_status(cls, v: str) -> str:
        return v.strip().lower()

    @model_validator(mode="after")
    def check_amount(self) -> OrderSchema:
        if self.total_amount < 0:
            raise ValueError("total_amount cannot be negative")
        return self


class OrderItemSchema(BaseModel):
    order_index: int  # positional reference within the batch
    product_sku: str  # FK resolved by transformer
    quantity: int = Field(..., ge=1)
    unit_price: Decimal = Field(..., ge=Decimal("0"))

    @field_validator("product_sku")
    @classmethod
    def normalize_sku(cls, v: str) -> str:
        return v.strip().upper()

    @field_validator("unit_price", mode="before")
    @classmethod
    def coerce_price(cls, v: object) -> Decimal:
        try:
            return Decimal(str(v)).quantize(Decimal("0.01"))
        except Exception as exc:
            raise ValueError(f"Invalid unit_price: {v!r}") from exc
