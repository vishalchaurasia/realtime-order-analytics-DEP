"""
Live Pipeline Monitor
Shows real-time ingestion counts, batch stats, and mart summaries.
Run: python monitor.py
Refreshes every 10 seconds. Press Ctrl+C to stop.
"""

import time
import os
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname":   os.getenv("POSTGRES_DB", "order_analytics"),
    "user":     os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
}

REFRESH_SECONDS = 10


def connect():
    return psycopg2.connect(**DB_CONFIG)


def clear():
    os.system("cls" if os.name == "nt" else "clear")


def get_stats(conn):
    cur = conn.cursor()

    # Total raw records
    cur.execute("SELECT COUNT(*) FROM raw.orders")
    total = cur.fetchone()[0]

    # Records in last 1 minute
    cur.execute("""
        SELECT COUNT(*) FROM raw.orders
        WHERE ingested_at >= NOW() - INTERVAL '1 minute'
    """)
    last_1m = cur.fetchone()[0]

    # Records in last 5 minutes
    cur.execute("""
        SELECT COUNT(*) FROM raw.orders
        WHERE ingested_at >= NOW() - INTERVAL '5 minutes'
    """)
    last_5m = cur.fetchone()[0]

    # Latest ingestion timestamp
    cur.execute("SELECT MAX(ingested_at) FROM raw.orders")
    latest_ts = cur.fetchone()[0]

    # Orders by status
    cur.execute("""
        SELECT status, COUNT(*) as cnt
        FROM raw.orders
        GROUP BY status
        ORDER BY cnt DESC
    """)
    by_status = cur.fetchall()

    # Orders by city (top 5)
    cur.execute("""
        SELECT city, COUNT(*) as cnt
        FROM raw.orders
        GROUP BY city
        ORDER BY cnt DESC
        LIMIT 5
    """)
    by_city = cur.fetchall()

    # Check if mart tables exist and get their row counts
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'analytics_analytics'
        AND table_name LIKE 'mart_%'
    """)
    mart_tables = [r[0] for r in cur.fetchall()]

    mart_counts = {}
    for t in mart_tables:
        cur.execute(f"SELECT COUNT(*) FROM analytics_analytics.{t}")
        mart_counts[t] = cur.fetchone()[0]

    # Revenue summary if mart exists
    revenue_data = None
    if "mart_daily_revenue" in mart_tables:
        cur.execute("""
            SELECT
                SUM(gross_revenue) as total_revenue,
                SUM(total_orders) as total_orders,
                SUM(unique_customers) as unique_customers
            FROM analytics_analytics.mart_daily_revenue
        """)
        revenue_data = cur.fetchone()

    cur.close()
    return {
        "total": total,
        "last_1m": last_1m,
        "last_5m": last_5m,
        "latest_ts": latest_ts,
        "by_status": by_status,
        "by_city": by_city,
        "mart_counts": mart_counts,
        "revenue_data": revenue_data,
    }


def render(stats, refresh_count):
    clear()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ts = stats["latest_ts"].strftime("%H:%M:%S") if stats["latest_ts"] else "N/A"

    print("=" * 55)
    print("   REAL-TIME ORDER ANALYTICS — LIVE MONITOR")
    print("=" * 55)
    print(f"  Refreshed at : {now}  (#{refresh_count})")
    print(f"  Last event   : {ts}")
    print("-" * 55)

    # Ingestion stats
    print("\n  RAW INGESTION (raw.orders)")
    print(f"  {'Total records':<25} {stats['total']:>10,}")
    print(f"  {'Last 1 minute':<25} {stats['last_1m']:>10,}")
    print(f"  {'Last 5 minutes':<25} {stats['last_5m']:>10,}")

    # Status breakdown
    print("\n  ORDERS BY STATUS")
    for status, cnt in stats["by_status"]:
        bar = "█" * min(int(cnt / max(stats["total"], 1) * 30), 30)
        pct = cnt / max(stats["total"], 1) * 100
        print(f"  {status:<12} {cnt:>6,}  {bar:<30} {pct:.1f}%")

    # City breakdown
    print("\n  TOP 5 CITIES")
    for city, cnt in stats["by_city"]:
        bar = "█" * min(int(cnt / max(stats["total"], 1) * 30), 30)
        print(f"  {city:<12} {cnt:>6,}  {bar}")

    # dbt mart tables
    print("\n  DBT MART TABLES (analytics schema)")
    if stats["mart_counts"]:
        for table, cnt in stats["mart_counts"].items():
            print(f"  {table:<35} {cnt:>6,} rows")
    else:
        print("  Not built yet — run: dbt run")

    # Revenue summary
    if stats["revenue_data"] and stats["revenue_data"][0]:
        rev, orders, customers = stats["revenue_data"]
        print("\n  REVENUE SUMMARY (from mart)")
        print(f"  {'Gross Revenue':<25} ₹{float(rev):>12,.2f}")
        print(f"  {'Total Orders':<25} {int(orders):>12,}")
        print(f"  {'Unique Customers':<25} {int(customers):>12,}")

    print("\n" + "=" * 55)
    print(f"  Refreshing every {REFRESH_SECONDS}s — Ctrl+C to stop")
    print("=" * 55)


def run():
    print("Connecting to Postgres...")
    refresh_count = 0

    try:
        conn = connect()
        print("Connected. Starting monitor...")
        time.sleep(1)

        while True:
            refresh_count += 1
            try:
                stats = get_stats(conn)
                render(stats, refresh_count)
            except psycopg2.OperationalError:
                # Reconnect if connection dropped
                conn = connect()
                stats = get_stats(conn)
                render(stats, refresh_count)

            time.sleep(REFRESH_SECONDS)

    except KeyboardInterrupt:
        print("\n\nMonitor stopped.")
    except Exception as e:
        print(f"\nError: {e}")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    run()