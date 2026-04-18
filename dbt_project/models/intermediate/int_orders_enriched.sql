-- models/intermediate/int_orders_enriched.sql
-- Adds business logic on top of stg_orders:
--   - Order value bucket (low / mid / high / premium)
--   - Is the order a repeat within same day for same user?
--   - Payment type grouping

WITH staged AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

enriched AS (
    SELECT
        order_id,
        user_id,
        product_id,
        product_name,
        category,
        quantity,
        unit_price,
        amount,
        status,
        city,
        payment_method,
        event_ts,
        order_date,
        order_hour,
        is_high_value,
        ingested_at,

        -- Value bucket
        CASE
            WHEN amount < 500   THEN 'low'
            WHEN amount < 2000  THEN 'mid'
            WHEN amount < 6000  THEN 'high'
            ELSE                     'premium'
        END AS value_bucket,

        -- Payment type grouping
        CASE
            WHEN payment_method IN ('upi', 'net banking') THEN 'digital'
            WHEN payment_method IN ('credit card', 'debit card') THEN 'card'
            WHEN payment_method = 'cod' THEN 'cash_on_delivery'
            ELSE 'other'
        END AS payment_type,

        -- Is the order a successful (non-cancelled) one?
        CASE WHEN status != 'cancelled' THEN TRUE ELSE FALSE END AS is_successful,

        -- Day of week (useful for trend analysis)
        TO_CHAR(event_ts, 'Day') AS day_of_week

    FROM staged
)

SELECT * FROM enriched