-- ============================================================
-- Scalable Data Ingestion Pipeline — MySQL Schema
-- ============================================================
-- Run once against a fresh database:
--   mysql -u root -p data_pipeline < sql/schema.sql
-- ============================================================

-- ------------------------------------
-- Categories (self-referential hierarchy)
-- ------------------------------------
CREATE TABLE IF NOT EXISTS categories (
    id         INT          NOT NULL AUTO_INCREMENT,
    name       VARCHAR(128) NOT NULL,
    parent_id  INT          NULL REFERENCES categories(id),
    created_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_categories_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ------------------------------------
-- Products
-- ------------------------------------
CREATE TABLE IF NOT EXISTS products (
    id          INT            NOT NULL AUTO_INCREMENT,
    sku         VARCHAR(64)    NOT NULL,
    name        VARCHAR(256)   NOT NULL,
    category_id INT            NULL REFERENCES categories(id),
    price       DECIMAL(10,2)  NOT NULL DEFAULT 0.00,
    created_at  DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_products_sku (sku),
    INDEX ix_products_category (category_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ------------------------------------
-- Customers
-- ------------------------------------
CREATE TABLE IF NOT EXISTS customers (
    id         INT          NOT NULL AUTO_INCREMENT,
    name       VARCHAR(256) NOT NULL,
    email      VARCHAR(256) NOT NULL,
    phone      VARCHAR(32)  NULL,
    created_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_customers_email (email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ------------------------------------
-- Orders
-- ------------------------------------
CREATE TABLE IF NOT EXISTS orders (
    id           INT            NOT NULL AUTO_INCREMENT,
    customer_id  INT            NOT NULL REFERENCES customers(id),
    status       VARCHAR(32)    NOT NULL DEFAULT 'pending',
    total_amount DECIMAL(12,2)  NOT NULL DEFAULT 0.00,
    ordered_at   DATETIME       NOT NULL,
    created_at   DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    -- Composite index for customer history queries
    INDEX ix_orders_customer_ordered_at (customer_id, ordered_at),
    -- Covering index for status-based dashboard queries
    INDEX ix_orders_status_ordered_at (status, ordered_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Simulated partial index for pending orders (MySQL doesn't support partial
-- indexes natively; use a generated column + index or a filtered view)
CREATE OR REPLACE VIEW pending_orders AS
    SELECT * FROM orders WHERE status = 'pending';

-- ------------------------------------
-- Order Items
-- ------------------------------------
CREATE TABLE IF NOT EXISTS order_items (
    id         INT           NOT NULL AUTO_INCREMENT,
    order_id   INT           NOT NULL REFERENCES orders(id),
    product_id INT           NOT NULL REFERENCES products(id),
    quantity   INT           NOT NULL DEFAULT 1,
    unit_price DECIMAL(10,2) NOT NULL,
    PRIMARY KEY (id),
    INDEX ix_order_items_order   (order_id),
    INDEX ix_order_items_product (product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ------------------------------------
-- Pipeline Runs (audit log)
-- ------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id             INT          NOT NULL AUTO_INCREMENT,
    source         VARCHAR(256) NOT NULL,
    rows_ingested  BIGINT       NOT NULL DEFAULT 0,
    rows_failed    BIGINT       NOT NULL DEFAULT 0,
    error_log      TEXT         NULL,
    started_at     DATETIME     NOT NULL,
    finished_at    DATETIME     NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
