-- models/marts/mart_daily_revenue.sql
-- Daily revenue aggregation by city and category
-- Materialized as TABLE so queries are fast

WITH enriched AS (
    SELECT * FROM {{ ref('int_orders_enriched') }}
    WHERE is_successful = TRUE
),

daily_revenue AS (
    SELECT
        order_date,
        city,
        category,
        COUNT(DISTINCT order_id)            AS total_orders,
        COUNT(DISTINCT user_id)             AS unique_customers,
        SUM(amount)                         AS gross_revenue,
        AVG(amount)                         AS avg_order_value,
        SUM(quantity)                       AS total_units_sold,
        COUNT(CASE WHEN is_high_value THEN 1 END) AS high_value_orders
    FROM enriched
    GROUP BY order_date, city, category
)

SELECT * FROM daily_revenue
ORDER BY order_date DESC, gross_revenue DESC