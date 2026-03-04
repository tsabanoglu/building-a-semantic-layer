"""
Verify the ecommerce DuckDB database: print row counts and samples from each table.
"""

from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parent.parent / "data" / "ecommerce.duckdb"

TABLES = ["customers", "products", "orders", "order_items"]


def verify() -> None:
    con = duckdb.connect(str(DB_PATH), read_only=True)

    # ---- Row counts --------------------------------------------------------
    print("=" * 50)
    print("ROW COUNTS")
    print("=" * 50)
    for table in TABLES:
        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table:<15} {count:>8,} rows")

    # ---- Samples -----------------------------------------------------------
    for table in TABLES:
        print(f"\n{'=' * 50}")
        print(f"SAMPLE: {table} (5 rows)")
        print("=" * 50)
        df = con.execute(f"SELECT * FROM {table} LIMIT 5").df()
        print(df.to_string(index=False))

    # ---- Order status distribution -----------------------------------------
    print(f"\n{'=' * 50}")
    print("ORDER STATUS DISTRIBUTION")
    print("=" * 50)
    df = con.execute("""
        SELECT
            order_status,
            COUNT(*) AS count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
        FROM orders
        GROUP BY order_status
        ORDER BY count DESC
    """).df()
    print(df.to_string(index=False))

    # ---- Order items per order ---------------------------------------------
    print(f"\n{'=' * 50}")
    print("ORDER ITEMS PER ORDER")
    print("=" * 50)
    df = con.execute("""
        SELECT
            ROUND(AVG(item_count), 2) AS avg_items,
            MIN(item_count)           AS min_items,
            MAX(item_count)           AS max_items
        FROM (
            SELECT order_id, COUNT(*) AS item_count
            FROM order_items
            GROUP BY order_id
        )
    """).df()
    print(df.to_string(index=False))

    # ---- Revenue sanity check ----------------------------------------------
    print(f"\n{'=' * 50}")
    print("REVENUE SANITY CHECK")
    print("=" * 50)
    df = con.execute("""
        SELECT
            order_status,
            COUNT(*)                        AS orders,
            ROUND(SUM(order_total), 2)      AS total_revenue,
            ROUND(AVG(order_total), 2)      AS avg_order_value
        FROM orders
        GROUP BY order_status
        ORDER BY total_revenue DESC
    """).df()
    print(df.to_string(index=False))

    con.close()


if __name__ == "__main__":
    verify()
