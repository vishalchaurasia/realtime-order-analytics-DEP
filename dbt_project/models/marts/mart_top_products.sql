-- models/marts/mart_top_products.sql
-- Product performance: revenue rank, order volume, cancellation rate

WITH enriched AS (
    SELECT * FROM {{ ref('int_orders_enriched') }}
),

product_stats AS (
    SELECT
        product_id,
        product_name,
        category,
        COUNT(DISTINCT order_id)                                    AS total_orders,
        SUM(CASE WHEN is_successful THEN amount ELSE 0 END)        AS total_revenue,
        SUM(CASE WHEN is_successful THEN quantity ELSE 0 END)      AS total_units_sold,
        AVG(CASE WHEN is_successful THEN amount END)               AS avg_order_value,
        COUNT(CASE WHEN status = 'cancelled' THEN 1 END)           AS cancelled_orders,
        ROUND(
            COUNT(CASE WHEN status = 'cancelled' THEN 1 END)::NUMERIC
            / NULLIF(COUNT(DISTINCT order_id), 0) * 100, 2
        )                                                           AS cancellation_rate_pct
    FROM enriched
    GROUP BY product_id, product_name, category
),

ranked AS (
    SELECT
        *,
        RANK() OVER (ORDER BY total_revenue DESC)                   AS revenue_rank,
        RANK() OVER (PARTITION BY category ORDER BY total_revenue DESC) AS category_revenue_rank
    FROM product_stats
)

SELECT * FROM ranked
ORDER BY revenue_rank