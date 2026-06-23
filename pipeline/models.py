"""
SQLAlchemy ORM models — normalized 3NF schema.

Tables
------
categories      – product category hierarchy
products        – product catalogue
customers       – customer master data
orders          – order header
order_items     – order line items (bridge between orders & products)
pipeline_runs   – audit log for every pipeline execution
"""
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# Categories
# ---------------------------------------------------------------------------
class Category(Base):
    __tablename__ = "categories"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    parent_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("categories.id"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), nullable=False
    )

    products: Mapped[list["Product"]] = relationship(back_populates="category")

    def __repr__(self) -> str:
        return f"<Category id={self.id} name={self.name!r}>"


# ---------------------------------------------------------------------------
# Products
# ---------------------------------------------------------------------------
class Product(Base):
    __tablename__ = "products"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sku: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    category_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("categories.id"), nullable=True
    )
    price: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False, default=0.0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), nullable=False
    )

    category: Mapped["Category | None"] = relationship(back_populates="products")
    order_items: Mapped[list["OrderItem"]] = relationship(back_populates="product")

    def __repr__(self) -> str:
        return f"<Product id={self.id} sku={self.sku!r}>"


# ---------------------------------------------------------------------------
# Customers
# ---------------------------------------------------------------------------
class Customer(Base):
    __tablename__ = "customers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    email: Mapped[str] = mapped_column(String(256), nullable=False, unique=True)
    phone: Mapped[str | None] = mapped_column(String(32), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), nullable=False
    )

    orders: Mapped[list["Order"]] = relationship(back_populates="customer")

    def __repr__(self) -> str:
        return f"<Customer id={self.id} email={self.email!r}>"


# ---------------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------------
class Order(Base):
    __tablename__ = "orders"
    __table_args__ = (
        # Composite index for customer history queries
        Index("ix_orders_customer_ordered_at", "customer_id", "ordered_at"),
        # Covering index for status-based dashboard queries
        Index("ix_orders_status_ordered_at", "status", "ordered_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    customer_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("customers.id"), nullable=False
    )
    status: Mapped[str] = mapped_column(
        String(32), nullable=False, default="pending"
    )
    total_amount: Mapped[float] = mapped_column(
        Numeric(12, 2), nullable=False, default=0.0
    )
    ordered_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), nullable=False
    )

    customer: Mapped["Customer"] = relationship(back_populates="orders")
    items: Mapped[list["OrderItem"]] = relationship(back_populates="order")

    def __repr__(self) -> str:
        return f"<Order id={self.id} status={self.status!r}>"


# ---------------------------------------------------------------------------
# Order Items
# ---------------------------------------------------------------------------
class OrderItem(Base):
    __tablename__ = "order_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    order_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("orders.id"), nullable=False
    )
    product_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("products.id"), nullable=False
    )
    quantity: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    unit_price: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)

    order: Mapped["Order"] = relationship(back_populates="items")
    product: Mapped["Product"] = relationship(back_populates="order_items")

    def __repr__(self) -> str:
        return f"<OrderItem order={self.order_id} product={self.product_id}>"


# ---------------------------------------------------------------------------
# Pipeline Runs (audit log)
# ---------------------------------------------------------------------------
class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(256), nullable=False)
    rows_ingested: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    rows_failed: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    error_log: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    def __repr__(self) -> str:
        return (
            f"<PipelineRun id={self.id} source={self.source!r} "
            f"ingested={self.rows_ingested}>"
        )
