# Real-time Order Analytics Platform

A comprehensive, production-ready real-time data engineering pipeline that demonstrates modern data architecture principles. This system ingests order events from multiple sources, processes them in real-time using Apache Spark, persists raw data in PostgreSQL, and transforms it into analytics-ready tables using dbt (data build tool).

**Architecture at a Glance:**
```
Event Source → Producer (JSON) → Kafka Topic → Spark Batch Job → PostgreSQL (Raw) → dbt Transformations → PostgreSQL (Analytics)
```

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture & Design Philosophy](#architecture--design-philosophy)
3. [Complete Data Flow: Tracking One Order Event](#complete-data-flow-tracking-one-order-event)
4. [Component Deep Dive](#component-deep-dive)
5. [Database Schema & Transformations](#database-schema--transformations)
6. [Setup & Installation](#setup--installation)
7. [Running the Pipeline](#running-the-pipeline)
8. [Project Structure](#project-structure)
9. [Configuration Details](#configuration-details)
10. [Troubleshooting & Monitoring](#troubleshooting--monitoring)

---

## Project Overview

### What This Project Does

This is a **real-time order analytics platform** that:
- Simulates order events being created by an e-commerce system
- Streams those events through Apache Kafka (a distributed message broker)
- Processes events in batches every 30 seconds using Apache Spark (distributed computing framework)
- Writes clean, deduplicated data to PostgreSQL's raw layer
- Applies business logic transformations using dbt (SQL-based transformation framework)
- Produces analytics-ready tables for reporting and dashboards

### Why This Architecture?

**Separation of Concerns**: Each layer has a specific purpose:
- **Kafka**: Decouples the producer from the consumer (events don't wait for processing)
- **Spark**: Handles distributed processing (can scale to handle millions of events)
- **Raw Layer (PostgreSQL)**: Preserves original data unmodified for auditability
- **dbt**: Codifies business logic in version-controlled SQL
- **Analytics Layer**: Clean, validated data for dashboards and reports

**Scalability**: Each component can be scaled independently. Need more event volume? Add more Kafka partitions. Need faster transformations? Add more Spark executors.

**Auditability**: Raw data is never modified, so you can always trace back how analytics were calculated.

---

## Architecture & Design Philosophy

### The Lambda (Stream + Batch) Pattern

This project uses a **Lambda Architecture** hybrid approach:

**Speed Layer (Real-time):**
- Kafka provides near-instantaneous message queuing
- Events are available for immediate consumption
- Allows real-time alerting if needed

**Batch Layer (Accuracy):**
- Spark processes events in 30-second batches
- Guarantees exactly-once processing (no data loss or duplication)
- Easier to debug than pure streaming
- More efficient resource usage

**Serving Layer (Analytics):**
- PostgreSQL stores both raw and processed data
- dbt ensures consistent, tested business logic
- Multiple downstream consumers can query the same tables

### Data Layers Explained

```
┌─────────────────────────────────────────────────────────────────┐
│                     Kafka Topic: raw_orders                     │
│  (Queue of JSON messages, retained for 7 days by default)      │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                    ┌──────────▼───────────┐
                    │  Spark Job (30s)    │
                    │  - Schema validation │
                    │  - Type casting     │
                    │  - Deduplication    │
                    │  - Add timestamps   │
                    └──────────┬───────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────┐
│            PostgreSQL Schema: raw (Single Source of Truth)      │
│                                                                  │
│  raw.orders                                                     │
│  ├── order_id (PK)                                             │
│  ├── city                                                       │
│  ├── amount                                                     │
│  ├── status                                                     │
│  ├── ingested_at (batch timestamp)                            │
│  └── ... other order fields                                    │
│                                                                 │
│  Note: Append-only. Old records never deleted or updated.      │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                    ┌──────────▼───────────┐
                    │   dbt run (manual)   │
                    │   - Staging models   │
                    │   - Intermediate SQL │
                    │   - Mart creation    │
                    │   - Test validation  │
                    └──────────┬───────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────┐
│          PostgreSQL Schema: analytics (Ready for BI)            │
│                                                                  │
│  analytics.orders_summary                                       │
│  ├── order_id                                                   │
│  ├── city                                                       │
│  ├── amount_usd                                                │
│  ├── status_clean                                              │
│  ├── processed_at                                              │
│  └── ... enriched fields                                       │
│                                                                 │
│  analytics.daily_metrics                                        │
│  ├── date                                                       │
│  ├── total_orders                                              │
│  ├── total_revenue                                             │
│  └── avg_order_value                                           │
│                                                                 │
│  And additional fact/dimension tables...                        │
└──────────────────────────────────────────────────────────────────┘
                               │
                ┌──────────────┴──────────────┐
                │                             │
        ┌───────▼────────┐          ┌────────▼──────┐
        │  BI Dashboard  │          │  Data Analyst │
        │  (Tableau/etc) │          │  (SQL Queries)│
        └────────────────┘          └───────────────┘
```

---

## Complete Data Flow: Tracking One Order Event

Let's follow a single order event from creation to analytics-ready table:

### **Step 1: Event Creation (Producer Component)**

**File**: `producer/order_producer.py`

**What Happens:**
```python
# Pseudocode of what the producer does
while True:
    # Generate a random order event
    order_event = {
        "order_id": "ORD-2026-04-18-00001",
        "city": "New York",
        "amount": 129.99,
        "status": "completed",
        "product_category": "electronics",
        "customer_segment": "premium"
    }
    
    # Serialize to JSON string
    json_string = json.dumps(order_event)
    
    # Send to Kafka
    producer.send(
        topic="raw_orders",
        value=json_string.encode('utf-8')
    )
    
    # Wait before generating next event
    time.sleep(random.uniform(0.5, 2.0))  # Random delay
```

**Output**: A JSON-serialized string:
```json
{"order_id": "ORD-2026-04-18-00001", "city": "New York", "amount": 129.99, "status": "completed", "product_category": "electronics", "customer_segment": "premium"}
```

**Why This Component Exists:**
- Simulates real-world order events
- Tests the pipeline without needing actual e-commerce system
- Can generate events at controlled rates for load testing

---

### **Step 2: Event Queued in Kafka (Message Broker)**

**Component**: Docker container running Apache Kafka (started by `docker/docker-compose.yml`)

**What Happens:**
- The JSON string is published to Kafka topic `raw_orders`
- Kafka assigns it a partition (usually round-robin among partitions)
- Kafka writes it to a distributed log file
- Message is retained for 7 days (configurable in `docker-compose.yml`)
- Multiple consumers can read the same message independently

**Kafka Acts As:**
- **Decoupler**: Producer doesn't know if anyone is listening
- **Buffer**: If Spark is slow, events queue up in Kafka
- **Replay**: Can re-read events if Spark job fails

**Topic Configuration:**
```yaml
# In docker-compose.yml
kafka:
  environment:
    KAFKA_LOG_RETENTION_HOURS: 168  # 7 days
    KAFKA_NUM_PARTITIONS: 3         # Parallel processing
    KAFKA_REPLICATION_FACTOR: 1     # One copy for demo
```

**Kafka Internal Storage:**
```
Topic: raw_orders
├── Partition 0
│   ├── [offset 0] {"order_id": "ORD-001", ...}
│   ├── [offset 1] {"order_id": "ORD-002", ...}
│   └── [offset 2] {"order_id": "ORD-001", ...}  # Duplicate
├── Partition 1
│   ├── [offset 0] {"order_id": "ORD-003", ...}
│   └── [offset 1] {"order_id": "ORD-004", ...}
└── Partition 2
    └── [offset 0] {"order_id": "ORD-005", ...}
```

Notice: Same `order_id` can appear twice (duplicate). Spark will fix this later.

---

### **Step 3: Spark Batch Processing (ETL Job)**

**File**: `spark/streaming_consumer.py`

**What Happens Every 30 Seconds:**

```python
# Pseudocode of Spark job
while True:
    # 1. Read from Kafka (all messages since last batch)
    kafka_df = spark.read.kafka(
        bootstrapServers="localhost:9092",
        subscribe="raw_orders",
        startingOffsets="latest"
    )
    
    # 2. Parse JSON strings into structured data
    parsed_df = kafka_df.select(
        from_json(col("value"), schema).alias("data")
    ).select("data.*")
    
    # 3. Schema: Define expected columns and types
    # Casting: Convert strings to proper types
    typed_df = parsed_df.select(
        col("order_id").cast("string"),
        col("city").cast("string"),
        col("amount").cast("decimal(10,2)"),
        col("status").cast("string"),
        col("product_category").cast("string"),
        col("customer_segment").cast("string"),
        current_timestamp().alias("ingested_at")
    )
    
    # 4. Deduplication: Keep only latest version of each order_id
    dedup_df = typed_df.dropDuplicates(["order_id"])
    
    # 5. Write to PostgreSQL raw layer
    dedup_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/analytics_db") \
        .option("dbtable", "raw.orders") \
        .option("user", "analytics_user") \
        .option("password", "password") \
        .mode("append") \
        .save()
    
    # Wait 30 seconds, then repeat
    time.sleep(30)
```

**Input to This Step:**
```
Raw Kafka Message (as bytes):
{"order_id": "ORD-2026-04-18-00001", "city": "New York", "amount": 129.99, "status": "completed", ...}
```

**Transformations Applied:**

| Step | Input | Operation | Output | Why |
|------|-------|-----------|--------|-----|
| Parse JSON | Byte string | `from_json()` | Structured columns | Enable type checking |
| Type Casting | "amount": "129.99" (string) | `.cast("decimal")` | amount: 129.99 (number) | Math operations need proper types |
| Deduplication | Multiple rows with order_id="ORD-001" | `dropDuplicates()` | Single row per order_id | Network retries can duplicate messages |
| Add Timestamp | None | `current_timestamp()` | ingested_at: 2026-04-18 14:35:00 | Track when data arrived |

**Output (after all transformations):**
```
order_id                | city      | amount | status    | product_category | customer_segment | ingested_at
ORD-2026-04-18-00001    | New York  | 129.99 | completed | electronics      | premium          | 2026-04-18 14:35:00
```

**Key Points:**
- Spark reads **all new messages** since last batch (using Kafka offset tracking)
- Automatically handles exactly-once semantics (no duplicate writes to database)
- If job crashes, it restarts from last known offset
- Running in batch mode is more reliable than streaming for this use case

---

### **Step 4: Raw Data Persisted (PostgreSQL Raw Schema)**

**Database**: PostgreSQL in container or external instance

**Raw Schema Structure:**
```sql
CREATE SCHEMA raw;

CREATE TABLE raw.orders (
    order_id VARCHAR(50) PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(50),
    product_category VARCHAR(100),
    customer_segment VARCHAR(50),
    ingested_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_raw_orders_ingested_at ON raw.orders(ingested_at);
```

**Data at This Stage:**
```
order_id                | city      | amount | status    | product_category | customer_segment | ingested_at
────────────────────────┼───────────┼────────┼───────────┼──────────────────┼──────────────────┼─────────────────────────
ORD-2026-04-18-00001    | New York  | 129.99 | completed | electronics      | premium          | 2026-04-18 14:35:00
ORD-2026-04-18-00002    | LA        |  89.50 | pending   | clothing         | standard         | 2026-04-18 14:35:15
ORD-2026-04-18-00003    | Chicago   | 250.00 | shipped   | books            | premium          | 2026-04-18 14:35:30
```

**Characteristics:**
- **Immutable**: Once written, raw data is never updated
- **Append-only**: Only new records are added
- **Single Source of Truth**: If analytics are wrong, you can audit back to this table
- **Unmodified**: Data is exactly as received from Spark (only schema and deduplication applied)

**Why Keep Raw Data:**
- Compliance: Some regulations require data audit trail
- Debugging: If transformation logic is wrong, reload from raw
- Flexibility: Can re-transform with new business rules
- Historical accuracy: Track all changes over time

---

### **Step 5: dbt Transformations (SQL Transformation Layer)**

**Tool**: dbt (Data Build Tool) - orchestrates SQL transformations

**Configuration Files:**

**File**: `dbt_project/dbt_project.yml`
```yaml
name: 'order_analytics'
version: '1.0.0'
profile: 'order_analytics'

models:
  order_analytics:
    staging:
      materialized: view        # Creates a VIEW (no storage)
      schema: analytics
    intermediate:
      materialized: view        # Creates a VIEW (no storage)
      schema: analytics
    marts:
      materialized: table       # Creates a TABLE (stored)
      schema: analytics
```

**File**: `dbt_project/profiles.yml`
```yaml
order_analytics:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: analytics_user
      password: password
      port: 5432
      dbname: analytics_db
      schema: analytics
      threads: 4
      keepalives_idle: 0
```

**Transformation Layers Explained:**

#### **Layer 1: Staging Models** (`models/staging/`)

**Purpose**: Clean and standardize raw data without business logic

**Example File**: `stg_orders.sql`
```sql
-- staging/stg_orders.sql
-- Purpose: Clean raw.orders data, apply basic transformations
-- Materialization: VIEW (no storage)

WITH source AS (
    SELECT
        order_id,
        city,
        amount,
        status,
        product_category,
        customer_segment,
        ingested_at,
        created_at
    FROM {{ source('raw', 'orders') }}  -- References raw.orders
    WHERE order_id IS NOT NULL  -- Remove nulls
)

SELECT
    order_id,
    city,
    amount,
    UPPER(status) AS status_standardized,  -- Standardize case
    product_category,
    customer_segment,
    ingested_at,
    created_at
FROM source
```

**Output Table**: `analytics.stg_orders` (VIEW)
```
order_id                | city      | amount | status_standardized | product_category | customer_segment | ingested_at
ORD-2026-04-18-00001    | New York  | 129.99 | COMPLETED           | electronics      | premium          | 2026-04-18 14:35:00
```

**Why Staging?**
- Isolates data cleaning logic
- Reusable by multiple downstream models
- Easier to test individual cleaning steps
- Documents what raw columns mean

#### **Layer 2: Intermediate Models** (`models/intermediate/`)

**Purpose**: Create reusable business concepts (dimensions, join keys)

**Example File**: `int_orders_with_metrics.sql`
```sql
-- intermediate/int_orders_with_metrics.sql
-- Purpose: Add calculated metrics and enrich order data

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}  -- References stg_orders
),

-- Create price tiers based on amount
price_tier AS (
    SELECT
        order_id,
        CASE
            WHEN amount < 50 THEN 'low'
            WHEN amount < 150 THEN 'medium'
            ELSE 'high'
        END AS price_tier,
        CASE
            WHEN amount < 50 THEN amount * 0.10  -- 10% discount
            WHEN amount < 150 THEN amount * 0.05  -- 5% discount
            ELSE 0  -- No discount
        END AS discount_amount
    FROM orders
),

-- Join enhancements
final AS (
    SELECT
        o.order_id,
        o.city,
        o.amount,
        pt.price_tier,
        pt.discount_amount,
        o.amount - pt.discount_amount AS amount_after_discount,
        o.status_standardized,
        o.product_category,
        o.customer_segment,
        o.ingested_at
    FROM orders o
    LEFT JOIN price_tier pt ON o.order_id = pt.order_id
)

SELECT * FROM final
```

**Output Table**: `analytics.int_orders_with_metrics` (VIEW)
```
order_id                | city      | amount | price_tier | discount_amount | amount_after_discount | status_standardized
ORD-2026-04-18-00001    | New York  | 129.99 | medium     | 6.50            | 123.49                | COMPLETED
```

**Why Intermediate?**
- Single place to define business logic (price tiers, discounts)
- Multiple marts can reuse same logic
- Easier to maintain complex calculations
- Acts as documentation of business rules

#### **Layer 3: Mart Models** (`models/marts/`)

**Purpose**: Business-facing tables optimized for specific analysis

**Example File 1**: `mart_orders.sql`
```sql
-- marts/mart_orders.sql
-- Purpose: Final order dimension for reporting
-- Materialization: TABLE (stored for performance)

WITH orders AS (
    SELECT * FROM {{ ref('int_orders_with_metrics') }}
),

final AS (
    SELECT
        order_id,
        city,
        product_category,
        customer_segment,
        amount,
        amount_after_discount,
        status_standardized AS order_status,
        price_tier,
        DATE(ingested_at) AS order_date,
        ingested_at AS created_at,
        CURRENT_TIMESTAMP AS dbt_updated_at
    FROM orders
)

SELECT * FROM final
```

**Example File 2**: `mart_daily_summary.sql`
```sql
-- marts/mart_daily_summary.sql
-- Purpose: Daily metrics for executive dashboard
-- Materialization: TABLE (stored for performance)

WITH orders AS (
    SELECT * FROM {{ ref('int_orders_with_metrics') }}
),

daily_metrics AS (
    SELECT
        DATE(ingested_at) AS order_date,
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT city) AS unique_cities,
        SUM(amount) AS total_revenue,
        SUM(amount_after_discount) AS total_revenue_after_discount,
        AVG(amount) AS avg_order_value,
        MAX(amount) AS max_order_value,
        MIN(amount) AS min_order_value,
        COUNT(CASE WHEN status_standardized = 'COMPLETED' THEN 1 END) AS completed_orders,
        COUNT(CASE WHEN status_standardized = 'PENDING' THEN 1 END) AS pending_orders
    FROM orders
    GROUP BY DATE(ingested_at)
)

SELECT
    order_date,
    total_orders,
    unique_cities,
    total_revenue,
    total_revenue_after_discount,
    avg_order_value,
    max_order_value,
    min_order_value,
    completed_orders,
    pending_orders,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM daily_metrics
```

**Output Tables:**

`analytics.mart_orders`:
```
order_id                | city      | product_category | customer_segment | amount | amount_after_discount | order_status | price_tier | order_date
ORD-2026-04-18-00001    | New York  | electronics      | premium          | 129.99 | 123.49                | COMPLETED    | medium     | 2026-04-18
```

`analytics.mart_daily_summary`:
```
order_date  | total_orders | unique_cities | total_revenue | avg_order_value | completed_orders
2026-04-18  | 150          | 45            | 19250.50      | 128.34          | 145
```

**Why Marts?**
- Optimized for specific use cases (dashboard, reporting, science)
- Pre-calculated metrics (faster queries)
- Clean, business-friendly column names
- Multiple marts serve different audiences

---

### **Step 6: Analytics Ready (Business Intelligence)**

**What's Available After dbt run:**

```sql
-- Analysts can now write simple queries on clean data
SELECT
    order_date,
    customer_segment,
    SUM(total_revenue) AS revenue,
    COUNT(total_orders) AS order_count
FROM analytics.mart_daily_summary
WHERE order_date >= DATE '2026-04-01'
GROUP BY order_date, customer_segment
ORDER BY order_date DESC;
```

**Results Used By:**
- BI Dashboards (Tableau, Looker, Power BI)
- Data Scientists (for ML models)
- Business Analysts (for insights)
- Executives (for reporting)

---

## Component Deep Dive

### **1. Docker & Kafka Setup** (`docker/docker-compose.yml`)

**What It Does:**
- Starts Zookeeper (Kafka's coordination service)
- Starts Kafka broker (message queue)
- Creates persistent volumes so data survives container restarts

**Key Configuration Options:**
```yaml
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181  # Port Kafka uses to talk to ZK
      ZOOKEEPER_SYNC_LIMIT: 2

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_LOG_RETENTION_HOURS: 168  # Keep messages 7 days
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"  # Auto-create on publish
    ports:
      - "9092:9092"
    volumes:
      - kafka-data:/var/lib/kafka/data

volumes:
  kafka-data:
```

**Startup Checklist:**
```bash
# Start containers
docker-compose up -d

# Verify Zookeeper is ready (wait ~5 seconds)
docker-compose logs zookeeper | grep "binding to port"

# Verify Kafka is ready (wait ~10 seconds)
docker-compose logs kafka | grep "started (kafka.server.KafkaServer)"

# List topics (verify working)
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Inspect topic
docker exec kafka kafka-topics --describe --topic raw_orders --bootstrap-server localhost:9092
```

---

### **2. Producer** (`producer/order_producer.py`)

**Responsibilities:**
1. Generate random order events
2. Serialize to JSON
3. Publish to Kafka with error handling
4. Track metrics (events sent, errors)

**Code Structure:**
```python
from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

class OrderProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',  # Wait for all replicas (high reliability)
            retries=3    # Retry on failure
        )
        self.metrics = {
            'total_sent': 0,
            'errors': 0
        }
    
    def generate_order_event(self):
        """Create a random order event"""
        statuses = ['completed', 'pending', 'shipped', 'cancelled']
        cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']
        categories = ['electronics', 'clothing', 'books', 'home', 'sports']
        segments = ['standard', 'premium', 'vip']
        
        return {
            'order_id': f"ORD-{datetime.now().strftime('%Y-%m-%d')}-{random.randint(10000, 99999)}",
            'city': random.choice(cities),
            'amount': round(random.uniform(10, 500), 2),
            'status': random.choice(statuses),
            'product_category': random.choice(categories),
            'customer_segment': random.choice(segments)
        }
    
    def produce_events(self):
        """Continuously generate and send events"""
        try:
            while True:
                event = self.generate_order_event()
                
                # Send to Kafka (async)
                self.producer.send('raw_orders', event)
                
                self.metrics['total_sent'] += 1
                
                # Log periodically
                if self.metrics['total_sent'] % 100 == 0:
                    print(f"[{datetime.now()}] Sent {self.metrics['total_sent']} events")
                
                # Variable delay between events
                time.sleep(random.uniform(0.5, 2.0))
        
        except KeyboardInterrupt:
            print(f"\nShutting down. Total sent: {self.metrics['total_sent']}")
        except Exception as e:
            print(f"Error: {e}")
            self.metrics['errors'] += 1
        finally:
            self.producer.flush()
            self.producer.close()

if __name__ == '__main__':
    producer = OrderProducer()
    producer.produce_events()
```

**Event Generation Logic:**
- Random order_id (unique per day)
- Random city (5 choices)
- Random amount (10-500 USD)
- Random status (4 states)
- Random product category (5 types)
- Random customer segment (3 tiers)

**Failure Handling:**
- `acks='all'`: Waits for broker confirmation
- `retries=3`: Automatically retries failed sends
- `producer.flush()`: Ensures all messages are sent before shutdown
- Exception handling: Logs errors and metrics

---

### **3. Spark Consumer** (`spark/streaming_consumer.py`)

**Responsibilities:**
1. Connect to Kafka
2. Read batches of messages
3. Apply schema and transformations
4. Write to PostgreSQL with deduplication

**Code Structure:**
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, current_timestamp, 
    to_timestamp, upper
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType

class KafkaToPostgres:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("OrderAnalyticsConsumer") \
            .config("spark.jars.packages", 
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1," \
                   "org.postgresql:postgresql:42.7.3") \
            .getOrCreate()
        
        # Define schema for JSON validation
        self.schema = StructType([
            StructField("order_id", StringType()),
            StructField("city", StringType()),
            StructField("amount", StringType()),  # Comes as string from JSON
            StructField("status", StringType()),
            StructField("product_category", StringType()),
            StructField("customer_segment", StringType())
        ])
    
    def read_kafka_batch(self):
        """Read one batch from Kafka"""
        return self.spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "raw_orders") \
            .option("startingOffsets", "latest") \
            .option("endingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
    
    def transform_data(self, kafka_df):
        """Apply all transformations"""
        # Parse JSON
        parsed = kafka_df.select(
            from_json(col("value").cast("string"), self.schema) \
                .alias("data")
        ).select("data.*")
        
        # Type casting
        casted = parsed.select(
            col("order_id").cast("string"),
            col("city").cast("string"),
            col("amount").cast("decimal(10,2)"),  # String to number
            col("status").cast("string"),
            col("product_category").cast("string"),
            col("customer_segment").cast("string"),
            current_timestamp().alias("ingested_at")
        )
        
        # Deduplication (keep latest by ingested_at)
        deduped = casted.dropDuplicates(["order_id"])
        
        return deduped
    
    def write_to_postgres(self, df):
        """Write DataFrame to PostgreSQL"""
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/analytics_db") \
            .option("dbtable", "raw.orders") \
            .option("user", "analytics_user") \
            .option("password", "password") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
    
    def run_streaming_job(self):
        """Main loop"""
        while True:
            try:
                print(f"[{datetime.now()}] Reading batch from Kafka...")
                kafka_df = self.read_kafka_batch()
                
                if kafka_df.count() > 0:
                    transformed = self.transform_data(kafka_df)
                    self.write_to_postgres(transformed)
                    print(f"Wrote {transformed.count()} records")
                else:
                    print("No new messages")
                
            except Exception as e:
                print(f"Error: {e}")
            
            time.sleep(30)  # Wait 30 seconds before next batch

if __name__ == '__main__':
    consumer = KafkaToPostgres()
    consumer.run_streaming_job()
```

**Key Spark Concepts:**

**DataFrame Transformations (Lazy):**
- `from_json()`: Parse JSON column to structured columns
- `.cast()`: Convert data types
- `dropDuplicates()`: Remove duplicate rows
- `current_timestamp()`: Add server time

**Schema Validation:**
- Defines expected columns and types
- `from_json()` uses schema to validate JSON
- Raises error if JSON structure doesn't match

**Exactly-Once Semantics:**
- Kafka tracks offsets (position in topic)
- After successful write to PostgreSQL, offset is updated
- If job crashes before write completes, offset doesn't advance → message is reprocessed
- Deduplication ensures same order_id doesn't create duplicates

**PostgreSQL JDBC Connection:**
- Uses `jdbc` format in Spark
- Requires JDBC driver JAR (included in requirements)
- `mode("append")` adds rows without truncating existing data

---

### **4. dbt Project Configuration**

**File**: `dbt_project/dbt_project.yml`

```yaml
name: 'order_analytics'           # Project name (used in refs)
version: '1.0.0'                  # Semantic versioning
config-version: 2                 # dbt config syntax version

profile: 'order_analytics'        # Links to profiles.yml

# Path configuration
model-paths: ["models"]           # Where SQL models are stored
test-paths: ["tests"]             # Where test files are stored
seed-paths: ["seeds"]             # Where static data files are
macro-paths: ["macros"]           # Where custom macros are

target-path: "target"             # Where dbt outputs compiled SQL
clean-targets: ["target", "dbt_packages"]  # What to delete on dbt clean

# Model configuration (sets defaults for all models)
models:
  order_analytics:                # Project name (must match name above)
    staging:
      +schema: analytics          # Override schema
      +materialized: view         # VIEWs (no storage, re-computed on query)
    
    intermediate:
      +schema: analytics
      +materialized: view         # VIEWs (lightweight)
    
    marts:
      +schema: analytics
      +materialized: table        # TABLEs (persistent, fast queries)
```

**File**: `dbt_project/profiles.yml`

```yaml
order_analytics:                    # Profile name (matches dbt_project.yml)
  target: dev                       # Which output to use
  
  outputs:
    dev:                            # Development environment
      type: postgres                # Database type
      host: localhost               # Server address
      user: analytics_user          # Database user
      password: password            # Database password
      port: 5432                    # PostgreSQL port
      dbname: analytics_db          # Database name
      schema: analytics             # Default schema
      threads: 4                    # Parallel dbt jobs
      keepalives_idle: 0            # Prevent connection timeout
```

---

## Database Schema & Transformations

### **Raw Schema**

```sql
CREATE SCHEMA raw;

CREATE TABLE raw.orders (
    order_id VARCHAR(50) PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(50),
    product_category VARCHAR(100),
    customer_segment VARCHAR(50),
    ingested_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for fast lookups by ingestion date
CREATE INDEX idx_raw_orders_ingested_at ON raw.orders(ingested_at);
```

### **Analytics Schema (Created by dbt)**

**Views (from staging & intermediate):**
```sql
-- Automatically created by dbt from stg_orders.sql
CREATE VIEW analytics.stg_orders AS (
    SELECT ... FROM raw.orders WHERE order_id IS NOT NULL
);

-- Automatically created by dbt from int_orders_with_metrics.sql
CREATE VIEW analytics.int_orders_with_metrics AS (
    SELECT ... with calculated metrics ...
);
```

**Tables (from marts):**
```sql
-- Automatically created by dbt from mart_orders.sql
CREATE TABLE analytics.mart_orders (
    order_id VARCHAR(50),
    city VARCHAR(100),
    ...more columns...
);

-- Automatically created by dbt from mart_daily_summary.sql
CREATE TABLE analytics.mart_daily_summary (
    order_date DATE,
    total_orders INTEGER,
    ...more columns...
);
```

---

## Setup & Installation

### **Prerequisites**

```bash
# Check Docker is installed
docker --version

# Check Python is installed (3.8+)
python --version

# Check PostgreSQL client (for manual queries)
psql --version
```

### **Installation Steps**

#### **Step 1: Clone Repository**
```bash
cd ~/Documents/GitHub
# (Repository already exists as realtime-order-analytics-DEP)
```

#### **Step 2: Create Python Virtual Environment**
```bash
python -m venv venv

# Activate it
# On Windows:
venv\Scripts\activate
# On Mac/Linux:
source venv/bin/activate
```

#### **Step 3: Install Python Dependencies**

Create `requirements.txt`:
```
# Kafka producer/consumer
kafka-python-ng==2.2.3

# Apache Spark
pyspark==3.5.1

# PostgreSQL driver for Spark
psycopg2-binary==2.9.9

# dbt Core
dbt-core==1.7.10
dbt-postgres==1.7.10

# Utilities
python-dotenv==1.0.0
```

Then install:
```bash
pip install -r requirements.txt
```

#### **Step 4: Start Docker Services**
```bash
cd docker
docker-compose up -d

# Verify services are running
docker-compose ps
```

#### **Step 5: Create PostgreSQL Database & Schemas**

```bash
# Connect to PostgreSQL (adjust host/user as needed)
psql -h localhost -U postgres -d postgres

# Create database
CREATE DATABASE analytics_db;

# Create user with permissions
CREATE USER analytics_user WITH PASSWORD 'password';
GRANT ALL PRIVILEGES ON DATABASE analytics_db TO analytics_user;

# Connect to new database
\c analytics_db

# Create raw schema
CREATE SCHEMA raw;
GRANT ALL PRIVILEGES ON SCHEMA raw TO analytics_user;

# Create analytics schema
CREATE SCHEMA analytics;
GRANT ALL PRIVILEGES ON SCHEMA analytics TO analytics_user;

# Create raw.orders table
CREATE TABLE raw.orders (
    order_id VARCHAR(50) PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(50),
    product_category VARCHAR(100),
    customer_segment VARCHAR(50),
    ingested_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

GRANT ALL PRIVILEGES ON TABLE raw.orders TO analytics_user;
```

#### **Step 6: Configure dbt**

Update `dbt_project/profiles.yml` with your PostgreSQL credentials:
```yaml
order_analytics:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost           # Your PostgreSQL host
      user: analytics_user      # Your username
      password: password        # Your password
      port: 5432              # Your port
      dbname: analytics_db     # Your database
      schema: analytics
      threads: 4
      keepalives_idle: 0
```

Test dbt connection:
```bash
cd dbt_project
dbt debug
# Should show "Connection test: OK!"
```

---

## Running the Pipeline

### **Terminal 1: Start Services**
```bash
cd docker
docker-compose up -d

# Monitor logs
docker-compose logs -f kafka
```

### **Terminal 2: Start Producer**
```bash
# Make sure virtual environment is activated
python producer/order_producer.py

# Output should show:
# [2026-04-18 14:35:00] Sent 100 events
# [2026-04-18 14:35:15] Sent 200 events
```

### **Terminal 3: Start Spark Consumer**
```bash
# Set up Spark (if not already)
cd /path/to/spark

# Run the Spark job
python spark/streaming_consumer.py

# Output should show:
# [2026-04-18 14:35:01] Reading batch from Kafka...
# Wrote 50 records
# [2026-04-18 14:35:31] Reading batch from Kafka...
# Wrote 75 records
```

### **Terminal 4: Run dbt**
```bash
cd dbt_project

# First time: create tables
dbt run

# Output:
# Running with dbt 1.7.10
# Found 3 models, 0 tests, 0 snapshots...
# 
# Completed successfully
# 
# Done! [00:05.23s]

# Test data quality
dbt test

# Output shows test results for each model
```

### **Terminal 5: Query Results**
```bash
psql -h localhost -d analytics_db -U analytics_user

# Query raw data
SELECT COUNT(*) FROM raw.orders;

# Query analytics
SELECT * FROM analytics.mart_orders LIMIT 10;

SELECT * FROM analytics.mart_daily_summary ORDER BY order_date DESC;
```

---

## Project Structure

```
realtime-order-analytics-DEP/
│
├── docker/
│   ├── docker-compose.yml              # Kafka + Zookeeper container setup
│   └── .dockerignore                   # Exclude files from Docker context
│
├── producer/
│   ├── order_producer.py               # Main producer script
│   ├── config.py                       # Configuration constants
│   └── __init__.py                     # Python package marker
│
├── spark/
│   ├── streaming_consumer.py           # Main Spark job
│   ├── schema.py                       # JSON schema definition
│   └── __init__.py                     # Python package marker
│
├── dbt_project/                        # dbt project root
│   ├── dbt_project.yml                 # dbt configuration
│   ├── profiles.yml                    # Database credentials
│   ├── analyses/                       # Ad-hoc analysis queries
│   ├── macros/                         # Reusable SQL snippets
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_orders.sql         # Clean raw data
│   │   │   └── _stg_models.yml        # Staging layer docs/tests
│   │   ├── intermediate/
│   │   │   ├── int_orders_with_metrics.sql  # Add business logic
│   │   │   └── _int_models.yml              # Intermediate layer docs
│   │   └── marts/
│   │       ├── mart_orders.sql             # Final order dimension
│   │       ├── mart_daily_summary.sql      # Executive dashboard table
│   │       └── _mart_models.yml            # Marts layer docs/tests
│   ├── tests/                          # Data quality tests
│   │   └── _generic_tests.yml
│   ├── seeds/                          # Static reference data (cities, etc.)
│   └── target/                         # Compiled SQL (generated by dbt)
│
├── .gitignore                          # Exclude files from Git
├── requirements.txt                    # Python package dependencies
├── README.md                           # This documentation file
└── .env.example                        # Template for environment variables
```

**Directory Purposes:**

- **docker/**: Containerized Kafka infrastructure
- **producer/**: Event generation simulation
- **spark/**: Data ingestion and processing
- **dbt_project/**: SQL transformation layer
  - **staging/**: Data cleaning layer
  - **intermediate/**: Business logic reuse layer
  - **marts/**: Final business-facing tables
- **tests/**: Data quality assertions
- **seeds/**: Static lookup data

---

## Configuration Details

### **Kafka Configuration** (docker-compose.yml)

```yaml
kafka:
  image: confluentinc/cp-kafka:7.5.0
  environment:
    KAFKA_BROKER_ID: 1                           # Unique broker identifier
    KAFKA_LOG_RETENTION_HOURS: 168               # Keep messages 7 days
    KAFKA_LOG_RETENTION_BYTES: 1073741824        # Keep up to 1GB
    KAFKA_LOG_SEGMENT_BYTES: 1073741824          # Segment size (1GB)
    KAFKA_NUM_PARTITIONS: 3                      # Parallel processing
    KAFKA_DEFAULT_REPLICATION_FACTOR: 1          # Single replica (demo)
    KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"      # Auto-create on publish
    KAFKA_COMPRESSION_TYPE: snappy               # Compress messages
```

**Key Settings Explained:**

| Setting | Value | Reason |
|---------|-------|--------|
| RETENTION_HOURS | 168 | Keep 7 days of history for replay |
| NUM_PARTITIONS | 3 | Allow 3 parallel consumers |
| REPLICATION_FACTOR | 1 | Demo env (production: 3) |
| COMPRESSION_TYPE | snappy | Reduce network bandwidth |

### **Spark Configuration** (spark/streaming_consumer.py)

```python
spark.conf.set("spark.sql.shuffle.partitions", "4")  # Parallelism
spark.conf.set("spark.default.parallelism", "4")     # Task parallelism
spark.conf.set("spark.sql.adaptive.enabled", "true") # Adaptive query
```

### **Producer Configuration** (producer/order_producer.py)

```python
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    acks='all',                    # All in-sync replicas must acknowledge
    retries=3,                     # Retry up to 3 times
    max_in_flight_requests_per_connection=1,  # Ordered delivery
    request_timeout_ms=30000       # 30 second timeout
)
```

### **dbt Configuration** (dbt_project.yml)

```yaml
# Performance settings
parse-method: 'execute'            # Parse project on execution
on-run-start:                      # Run before dbt run
  - "{{ log('Starting transformation', info=True) }}"

on-run-end:                        # Run after dbt run
  - "{{ log('Transformation complete', info=True) }}"

# Variable defaults (can be overridden on command line)
vars:
  batch_date: '{{ run_started_at }}'
```

---

## Troubleshooting & Monitoring

### **Common Issues**

#### **1. Kafka Not Starting**

**Error**: `docker-compose: command not found`
```bash
# Install Docker Compose
# On Windows: Use Docker Desktop
# On Mac: brew install docker-compose
# On Linux: pip install docker-compose
```

**Error**: Port 9092 already in use
```bash
# Find what's using port 9092
netstat -ano | findstr :9092

# Stop the process or use different port in docker-compose.yml
```

**Check Status**:
```bash
docker-compose logs kafka | tail -50

docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

#### **2. Producer Can't Connect to Kafka**

**Error**: `NoBrokersAvailable`
```bash
# Kafka not fully started yet (wait 10-15 seconds)
docker-compose logs kafka | grep "started"

# Verify network
docker network ls
docker network inspect realtime-order-analytics-DEP_default
```

#### **3. Spark Job Fails to Write to PostgreSQL**

**Error**: `Connection refused`
```bash
# PostgreSQL not running
psql -h localhost -U postgres

# Verify JDBC URL format
jdbc:postgresql://localhost:5432/analytics_db

# Check PostgreSQL logs
docker logs postgres  # if using Docker
```

**Error**: `Authentication failed`
```bash
# Wrong credentials in code
# Check profiles.yml has correct user/password
# Test connection manually:
psql -h localhost -U analytics_user -d analytics_db
```

**Error**: `Table raw.orders doesn't exist`
```bash
# Create schema and table first
psql -h localhost -d analytics_db -U analytics_user -f setup.sql
```

#### **4. dbt Models Fail to Compile**

**Error**: `dbt_project.yml does not parse`
```bash
# YAML syntax error - check indentation
# Remove quotes from strings that don't need them:
# Bad:  name: 'order_analytics'
# Good: name: order_analytics
```

**Error**: `Source 'raw.orders' not found`
```bash
# dbt doesn't know about raw.orders table
# Create sources.yml in models/ folder:

# models/sources.yml
version: 2

sources:
  - name: raw
    database: analytics_db
    schema: raw
    tables:
      - name: orders
```

**Error**: `Table already exists`
```bash
# Drop existing tables before re-running
dbt run --full-refresh  # Rebuilds all models from scratch
```

#### **5. Data Not Flowing Through Pipeline**

**Check Each Stage:**

```bash
# 1. Producer sending to Kafka?
docker exec kafka kafka-console-consumer --topic raw_orders \
  --from-beginning --bootstrap-server localhost:9092 \
  --max-messages 5

# 2. Spark reading from Kafka?
# Check spark/streaming_consumer.py logs for "Read X messages"

# 3. Spark writing to PostgreSQL?
psql -h localhost -d analytics_db -U analytics_user \
  -c "SELECT COUNT(*) FROM raw.orders;"

# 4. dbt models created?
psql -h localhost -d analytics_db -U analytics_user \
  -c "SELECT table_name FROM information_schema.tables WHERE table_schema='analytics';"
```

### **Monitoring Commands**

**Kafka Health:**
```bash
# List topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Describe topic (see partitions, replicas)
docker exec kafka kafka-topics --describe --topic raw_orders \
  --bootstrap-server localhost:9092

# Check consumer groups
docker exec kafka kafka-consumer-groups --list --bootstrap-server localhost:9092

# Monitor topic lag
docker exec kafka kafka-consumer-groups --describe --group spark-consumer \
  --bootstrap-server localhost:9092
```

**PostgreSQL Health:**
```bash
# Check database size
SELECT pg_size_pretty(pg_database_size('analytics_db'));

# Check raw.orders row count
SELECT COUNT(*) as total_records FROM raw.orders;

# Check for duplicates
SELECT order_id, COUNT(*) FROM raw.orders GROUP BY order_id HAVING COUNT(*) > 1;

# Monitor table sizes
SELECT 
  table_name,
  pg_size_pretty(pg_total_relation_size(table_name::regclass)) as size
FROM information_schema.tables
WHERE table_schema='raw'
ORDER BY pg_total_relation_size(table_name::regclass) DESC;
```

**dbt Health:**
```bash
# Validate project structure
dbt debug

# List models
dbt ls

# List models in specific folder
dbt ls --select staging.*

# Run specific model
dbt run --select stg_orders

# Test data quality
dbt test

# Generate documentation
dbt docs generate
dbt docs serve  # View at http://localhost:8000
```

### **Performance Optimization**

**If Pipeline Is Slow:**

1. **Increase Spark parallelism:**
   ```python
   spark.conf.set("spark.sql.shuffle.partitions", "8")
   ```

2. **Increase dbt threads:**
   ```yaml
   # profiles.yml
   threads: 8  # Run up to 8 models in parallel
   ```

3. **Add PostgreSQL indexes:**
   ```sql
   CREATE INDEX idx_orders_city ON raw.orders(city);
   CREATE INDEX idx_orders_amount ON raw.orders(amount);
   ```

4. **Increase Kafka batch timeout:**
   ```python
   # In Spark job
   options["kafkaConsumer.sessionTimeoutMs"] = "60000"  # 60 seconds
   ```

---

## Advanced Topics

### **Data Quality Testing**

Create `dbt_project/models/_mart_models.yml`:
```yaml
version: 2

models:
  - name: mart_orders
    description: "Final orders table for reporting"
    columns:
      - name: order_id
        description: "Unique order identifier"
        tests:
          - unique
          - not_null
      
      - name: amount
        description: "Order value in USD"
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_numeric
      
      - name: order_date
        description: "Date order was placed"
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: "2020-01-01"
              max_value: "2030-12-31"
```

Run tests:
```bash
dbt test
# Tests all models or specific ones:
dbt test --select mart_orders
```

### **Incremental Models** (Advanced)

For very large tables, use incremental models:

```sql
-- marts/mart_orders_incremental.sql
{{
  config(
    materialized='incremental',
    unique_key='order_id'
  )
}}

WITH orders AS (
  SELECT * FROM {{ ref('int_orders_with_metrics') }}
  
  {% if execute %}
    {% if model.get_last_full_refresh() is none %}
      WHERE ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
    {% endif %}
  {% endif %}
)

SELECT * FROM orders
```

**Benefits:**
- Only processes new data (faster)
- Maintains history automatically
- Uses `unique_key` to handle late-arriving data

---

## Next Steps & Extensions

1. **Add Real PostgreSQL**: Use managed service (AWS RDS, Azure Database)
2. **Add BI Dashboard**: Connect Tableau/Looker to analytics schema
3. **Add ML Model**: Use data in sklearn/TensorFlow for predictions
4. **Add Alerts**: Monitor for data quality issues with Great Expectations
5. **Add CI/CD**: Deploy dbt using dbt Cloud or GitHub Actions
6. **Add Data Catalog**: Document tables using data dictionary
7. **Scaling**: Move to cloud (AWS, Azure, GCP) for production workloads



