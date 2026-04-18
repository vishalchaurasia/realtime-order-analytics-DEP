-- Run this once before starting the pipeline
-- psql -U postgres -f setup/init_postgres.sql

CREATE DATABASE order_analytics;

\c order_analytics;

-- ── Raw layer (written by PySpark) ────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.orders (
    order_id        VARCHAR(36),
    user_id         VARCHAR(36),
    product_id      VARCHAR(20),
    product_name    VARCHAR(100),
    category        VARCHAR(50),
    quantity        INTEGER,
    unit_price      NUMERIC(10, 2),
    amount          NUMERIC(10, 2),
    status          VARCHAR(20),
    city            VARCHAR(50),
    payment_method  VARCHAR(30),
    event_ts        TIMESTAMP,
    is_high_value   BOOLEAN,
    ingested_at     TIMESTAMP DEFAULT NOW()
);

-- Index for dbt incremental models
CREATE INDEX IF NOT EXISTS idx_orders_event_ts  ON raw.orders (event_ts);
CREATE INDEX IF NOT EXISTS idx_orders_status    ON raw.orders (status);
CREATE INDEX IF NOT EXISTS idx_orders_city      ON raw.orders (city);

-- ── Analytics layer (written by dbt) ─────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS analytics;

-- dbt will create the actual tables/views in this schema

-- ── Verify ────────────────────────────────────────────────────────────────
SELECT schemaname, tablename FROM pg_tables
WHERE schemaname IN ('raw', 'analytics')
ORDER BY schemaname, tablename;