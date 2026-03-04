"""
Generate synthetic e-commerce data and load it into a local DuckDB database.

Tables created:
  - customers     (~1,000 rows)
  - products      (100 rows)
  - orders        (~3,000 rows)
  - order_items   (~7,500 rows, 1-5 items per order)
"""

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from faker import Faker

fake = Faker()
np.random.seed(42)
Faker.seed(42)

ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "ecommerce.duckdb"

N_CUSTOMERS = 1000
N_PRODUCTS = 100
N_ORDERS = 3000


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------

def generate_customers(n: int) -> pd.DataFrame:
    return pd.DataFrame({
        "customer_id": [f"C{i:05d}" for i in range(1, n + 1)],
        "email": [fake.email() for _ in range(n)],
        "first_name": [fake.first_name() for _ in range(n)],
        "last_name": [fake.last_name() for _ in range(n)],
        "payment_method_preferred": np.random.choice(
            ["credit_card", "paypal", "apple_pay", "bank_transfer"],
            size=n,
            p=[0.45, 0.30, 0.15, 0.10],
        ),
        "created_at": pd.date_range(start="2021-01-01", periods=n, freq="8h").date,
    })


def generate_products(n: int) -> pd.DataFrame:
    categories = ["Electronics", "Fashion", "Home", "Beauty", "Sports", "Books"]
    return pd.DataFrame({
        "product_id": [f"P{i:04d}" for i in range(1, n + 1)],
        "product_name": [
            fake.word().capitalize() + " " + fake.word().capitalize()
            for _ in range(n)
        ],
        "product_category": np.random.choice(categories, size=n),
        "base_price": np.round(np.random.uniform(9.99, 499.99, size=n), 2),
        "stock_quantity": np.random.randint(0, 1000, size=n),
    })


def generate_orders_and_items(
    customers: pd.DataFrame,
    products: pd.DataFrame,
    n_orders: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:

    order_ids = [f"O{i:06d}" for i in range(1, n_orders + 1)]
    customer_ids = np.random.choice(customers["customer_id"].values, size=n_orders)
    purchase_dates = pd.Series(pd.date_range(start="2022-01-01", periods=n_orders, freq="6h"))
    order_statuses = pd.Series(np.random.choice(
        ["completed", "pending", "shipped", "cancelled", "returned"],
        size=n_orders,
        p=[0.60, 0.10, 0.15, 0.05, 0.10],
    ))

    ship_offsets = pd.to_timedelta(np.random.randint(1, 8, size=n_orders), unit="D")
    delivery_offsets = pd.to_timedelta(np.random.randint(1, 15, size=n_orders), unit="D")
    shipped_dates = (purchase_dates + ship_offsets).where(order_statuses.isin(["shipped", "completed", "returned"]))
    delivered_dates = (shipped_dates + delivery_offsets).where(order_statuses.isin(["completed", "returned"]))

    product_price_lookup = products.set_index("product_id")["base_price"].to_dict()
    product_ids = products["product_id"].values

    order_items_rows = []
    order_totals: dict[str, float] = {}
    item_counter = 1

    for order_id in order_ids:
        n_items = int(np.random.randint(1, 6))  # 1–5 items per order
        for _ in range(n_items):
            product_id = np.random.choice(product_ids)
            base_price = product_price_lookup[product_id]
            unit_price = round(float(base_price) * float(np.random.uniform(0.85, 1.0)), 2)
            quantity = int(np.random.randint(1, 5))
            line_total = round(unit_price * quantity, 2)

            order_items_rows.append({
                "order_item_id": f"OI{item_counter:07d}",
                "order_id": order_id,
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": unit_price,
                "line_total": line_total,
            })
            order_totals[order_id] = round(order_totals.get(order_id, 0.0) + line_total, 2)
            item_counter += 1

    order_items = pd.DataFrame(order_items_rows)

    orders = pd.DataFrame({
        "order_id": order_ids,
        "customer_id": customer_ids,
        "purchase_date": purchase_dates,
        "order_status": order_statuses,
        "shipped_date": shipped_dates,
        "delivered_date": delivered_dates,
        "shipping_address": [fake.address().replace("\n", ", ") for _ in range(n_orders)],
        "order_total": [order_totals.get(oid, 0.0) for oid in order_ids],
    })

    return orders, order_items


# ---------------------------------------------------------------------------
# DuckDB loader
# ---------------------------------------------------------------------------

DDL = {
    "customers": """
        CREATE TABLE customers (
            customer_id               VARCHAR PRIMARY KEY,
            email                     VARCHAR NOT NULL,
            first_name                VARCHAR,
            last_name                 VARCHAR,
            payment_method_preferred  VARCHAR,
            created_at                DATE
        )
    """,
    "products": """
        CREATE TABLE products (
            product_id        VARCHAR PRIMARY KEY,
            product_name      VARCHAR,
            product_category  VARCHAR,
            base_price        DOUBLE,
            stock_quantity    INTEGER
        )
    """,
    "orders": """
        CREATE TABLE orders (
            order_id          VARCHAR PRIMARY KEY,
            customer_id       VARCHAR NOT NULL REFERENCES customers(customer_id),
            purchase_date     TIMESTAMP,
            order_status      VARCHAR,
            shipped_date      TIMESTAMP,
            delivered_date    TIMESTAMP,
            shipping_address  VARCHAR,
            order_total       DOUBLE
        )
    """,
    "order_items": """
        CREATE TABLE order_items (
            order_item_id  VARCHAR PRIMARY KEY,
            order_id       VARCHAR NOT NULL REFERENCES orders(order_id),
            product_id     VARCHAR NOT NULL REFERENCES products(product_id),
            quantity       INTEGER,
            unit_price     DOUBLE,
            line_total     DOUBLE
        )
    """,
}


def load_to_duckdb(
    customers: pd.DataFrame,
    products: pd.DataFrame,
    orders: pd.DataFrame,
    order_items: pd.DataFrame,
    db_path: Path,
) -> None:
    con = duckdb.connect(str(db_path))

    # Drop in reverse FK order so constraints don't block
    for table in ["order_items", "orders", "products", "customers"]:
        con.execute(f"DROP TABLE IF EXISTS {table}")

    for table, ddl in DDL.items():
        con.execute(ddl)

    # Register DataFrames and insert
    frames = {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items,
    }
    for table, df in frames.items():
        con.register(f"__{table}", df)
        con.execute(f"INSERT INTO {table} SELECT * FROM __{table}")
        con.unregister(f"__{table}")

    con.close()
    print(f"Loaded data into {db_path}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("Generating customers...")
    customers = generate_customers(N_CUSTOMERS)

    print("Generating products...")
    products = generate_products(N_PRODUCTS)

    print("Generating orders and order items...")
    orders, order_items = generate_orders_and_items(customers, products, N_ORDERS)

    print(f"\nRow counts before loading:")
    print(f"  customers:   {len(customers):,}")
    print(f"  products:    {len(products):,}")
    print(f"  orders:      {len(orders):,}")
    print(f"  order_items: {len(order_items):,}")

    print("\nLoading into DuckDB...")
    load_to_duckdb(customers, products, orders, order_items, DB_PATH)
    print("Done.")
