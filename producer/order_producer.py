"""
Order Event Producer
Simulates a real-time e-commerce order stream into Kafka topic: raw_orders
Run: python producer/order_producer.py
"""

import json
import time
import uuid
import random
import logging
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker
from dotenv import load_dotenv
import os

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

fake = Faker("en_IN")

# ── Static reference data ──────────────────────────────────────────────────
PRODUCTS = [
    {"product_id": "SKU-001", "name": "Wireless Earbuds",    "category": "Electronics", "price": 1999},
    {"product_id": "SKU-002", "name": "Running Shoes",        "category": "Footwear",    "price": 3499},
    {"product_id": "SKU-003", "name": "Protein Powder 1kg",   "category": "Health",      "price": 1299},
    {"product_id": "SKU-004", "name": "Laptop Stand",         "category": "Electronics", "price": 899},
    {"product_id": "SKU-005", "name": "Yoga Mat",             "category": "Fitness",     "price": 599},
    {"product_id": "SKU-006", "name": "Mechanical Keyboard",  "category": "Electronics", "price": 4999},
    {"product_id": "SKU-007", "name": "Water Bottle 1L",      "category": "Fitness",     "price": 349},
    {"product_id": "SKU-008", "name": "Face Wash 100ml",      "category": "Grooming",    "price": 249},
    {"product_id": "SKU-009", "name": "Backpack 30L",         "category": "Bags",        "price": 1799},
    {"product_id": "SKU-010", "name": "Smart Watch",          "category": "Electronics", "price": 8999},
]

CITIES = [
    "Mumbai", "Delhi", "Bengaluru", "Hyderabad", "Chennai",
    "Pune", "Kolkata", "Ahmedabad", "Jaipur", "Lucknow"
]

ORDER_STATUSES = ["placed", "confirmed", "shipped", "delivered", "cancelled"]

# Weight: most orders get placed/confirmed, fewer get cancelled
STATUS_WEIGHTS = [30, 30, 20, 15, 5]


def generate_order_event() -> dict:
    product = random.choice(PRODUCTS)
    quantity = random.randint(1, 4)
    status = random.choices(ORDER_STATUSES, weights=STATUS_WEIGHTS, k=1)[0]

    return {
        "order_id":    str(uuid.uuid4()),
        "user_id":     str(uuid.uuid4()),
        "product_id":  product["product_id"],
        "product_name": product["name"],
        "category":    product["category"],
        "quantity":    quantity,
        "unit_price":  product["price"],
        "amount":      round(product["price"] * quantity, 2),
        "status":      status,
        "city":        random.choice(CITIES),
        "payment_method": random.choice(["UPI", "Credit Card", "Debit Card", "COD", "Net Banking"]),
        "event_ts":    datetime.utcnow().isoformat(),
    }


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
    )


def run(events_per_second: int = 5, total_events: int = None):
    topic = os.getenv("KAFKA_TOPIC", "raw_orders")
    producer = create_producer()
    logger.info(f"Producer started → topic: {topic} @ {events_per_second} events/sec")

    count = 0
    interval = 1.0 / events_per_second

    try:
        while True:
            event = generate_order_event()
            producer.send(
                topic,
                key=event["order_id"],
                value=event
            )
            count += 1

            if count % 100 == 0:
                logger.info(f"Produced {count} events. Last: order_id={event['order_id']} city={event['city']} status={event['status']}")

            if total_events and count >= total_events:
                logger.info(f"Reached target of {total_events} events. Stopping.")
                break

            time.sleep(interval)

    except KeyboardInterrupt:
        logger.info(f"Stopped by user. Total events produced: {count}")
    finally:
        producer.flush()
        producer.close()
        logger.info("Producer closed cleanly.")


if __name__ == "__main__":
    # Produces 5 events/sec continuously — run for a few hours to build volume
    # Change events_per_second to 50+ to build 5M events faster
    run(events_per_second=5)