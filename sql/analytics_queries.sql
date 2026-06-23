-- ============================================================
-- Optimized analytical queries for downstream analytics
-- ============================================================

-- 1. Customer order history (uses ix_orders_customer_ordered_at)
-- Returns all orders for a given customer, newest first.
SELECT
    o.id          AS order_id,
    o.status,
    o.total_amount,
    o.ordered_at
FROM orders o
WHERE o.customer_id = :customer_id
ORDER BY o.ordered_at DESC;

-- 2. Pending orders dashboard (uses ix_orders_status_ordered_at + pending_orders view)
-- Returns pending orders within a date range, ordered by oldest first (SLA risk).
SELECT
    o.id          AS order_id,
    c.name        AS customer_name,
    c.email       AS customer_email,
    o.total_amount,
    o.ordered_at
FROM pending_orders o
JOIN customers c ON c.id = o.customer_id
WHERE o.ordered_at BETWEEN :start_date AND :end_date
ORDER BY o.ordered_at ASC;

-- 3. Revenue by category (uses ix_products_category)
-- Aggregates GMV by product category.
SELECT
    cat.name           AS category,
    COUNT(DISTINCT o.id)   AS order_count,
    SUM(oi.quantity)       AS units_sold,
    SUM(oi.quantity * oi.unit_price) AS revenue
FROM order_items oi
JOIN products  p   ON p.id  = oi.product_id
JOIN categories cat ON cat.id = p.category_id
JOIN orders    o   ON o.id  = oi.order_id
WHERE o.status = 'completed'
GROUP BY cat.id, cat.name
ORDER BY revenue DESC;

-- 4. Daily ingestion summary (pipeline monitoring)
SELECT
    DATE(started_at)  AS run_date,
    COUNT(*)          AS runs,
    SUM(rows_ingested) AS total_ingested,
    SUM(rows_failed)   AS total_failed,
    AVG(TIMESTAMPDIFF(SECOND, started_at, finished_at)) AS avg_duration_sec
FROM pipeline_runs
GROUP BY run_date
ORDER BY run_date DESC;

-- 5. Top 10 products by revenue (covers oi + p join)
SELECT
    p.sku,
    p.name,
    SUM(oi.quantity * oi.unit_price) AS revenue
FROM order_items oi
JOIN products p ON p.id = oi.product_id
GROUP BY p.id, p.sku, p.name
ORDER BY revenue DESC
LIMIT 10;
