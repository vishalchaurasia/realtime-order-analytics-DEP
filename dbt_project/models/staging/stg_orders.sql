-- models/staging/stg_orders.sql
-- Reads from raw.orders (written by PySpark)
-- Cleans column names, casts types, filters junk rows

WITH source AS (
    SELECT * FROM {{ source('raw', 'orders') }}
),

cleaned AS (
    SELECT
        order_id,
        user_id,
        product_id,
        TRIM(product_name)                          AS product_name,
        TRIM(category)                              AS category,
        quantity,
        unit_price,
        amount,
        LOWER(TRIM(status))                         AS status,
        INITCAP(TRIM(city))                         AS city,
        LOWER(TRIM(payment_method))                 AS payment_method,
        event_ts,
        is_high_value,
        ingested_at,
        DATE(event_ts)                              AS order_date,
        EXTRACT(HOUR FROM event_ts)                 AS order_hour
    FROM source
    WHERE
        order_id IS NOT NULL
        AND amount > 0
        AND status IN ('placed', 'confirmed', 'shipped', 'delivered', 'cancelled')
        AND event_ts IS NOT NULL
)

SELECT * FROM cleaned