-- models/marts/mart_order_funnel.sql
-- Tracks how orders move through the funnel: placed → confirmed → shipped → delivered
-- Cancellation rate per city and payment type

WITH enriched AS (
    SELECT * FROM {{ ref('int_orders_enriched') }}
),

funnel AS (
    SELECT
        order_date,
        city,
        payment_type,
        value_bucket,

        COUNT(DISTINCT order_id)                                                AS total_orders,
        COUNT(CASE WHEN status = 'placed'    THEN 1 END)                       AS placed,
        COUNT(CASE WHEN status = 'confirmed' THEN 1 END)                       AS confirmed,
        COUNT(CASE WHEN status = 'shipped'   THEN 1 END)                       AS shipped,
        COUNT(CASE WHEN status = 'delivered' THEN 1 END)                       AS delivered,
        COUNT(CASE WHEN status = 'cancelled' THEN 1 END)                       AS cancelled,

        ROUND(
            COUNT(CASE WHEN status = 'delivered' THEN 1 END)::NUMERIC
            / NULLIF(COUNT(DISTINCT order_id), 0) * 100, 2
        )                                                                       AS delivery_rate_pct,

        ROUND(
            COUNT(CASE WHEN status = 'cancelled' THEN 1 END)::NUMERIC
            / NULLIF(COUNT(DISTINCT order_id), 0) * 100, 2
        )                                                                       AS cancellation_rate_pct,

        SUM(CASE WHEN is_successful THEN amount ELSE 0 END)                    AS net_revenue

    FROM enriched
    GROUP BY order_date, city, payment_type, value_bucket
)

SELECT * FROM funnel
ORDER BY order_date DESC, total_orders DESC