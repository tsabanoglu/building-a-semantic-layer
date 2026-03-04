"""
Example Semantic Queries
========================
Demonstrates the SemanticEngine with six business questions.

Run with:
    uv run queries/example_queries.py
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from semantic.engine import SemanticEngine

# ── Display settings ─────────────────────────────────────────────────────────

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 110)
pd.set_option("display.float_format", "{:,.2f}".format)

engine = SemanticEngine()

WIDTH = 62


def section(title: str) -> None:
    print(f"\n{'━' * WIDTH}")
    print(f"  {title}")
    print(f"{'━' * WIDTH}")


def show(df: pd.DataFrame, pct_cols: list[str] | None = None) -> None:
    if pct_cols:
        for col in pct_cols:
            df[col] = (df[col] * 100).round(1).astype(str) + "%"
    print(df.to_string(index=False))


# ── 1. Gross revenue by product category ─────────────────────────────────────
#
# Which product categories drive the most revenue?
# Revenue is always computed from order_items.line_total, not orders.order_total.

section("1 · Gross Revenue by Product Category")

df = engine.query("gross_revenue", dimension="product_category")
df["gross_revenue"] = df["gross_revenue"].map("${:>12,.2f}".format)
show(df)


# ── 2. Net revenue by month ───────────────────────────────────────────────────
#
# How does revenue trend over time once returns are deducted?
# net_revenue = gross_revenue - returned_revenue (both from order_items.line_total)

section("2 · Net Revenue by Month")

df = engine.query("net_revenue", dimension="purchase_month")
df["net_revenue"] = df["net_revenue"].map("${:>12,.2f}".format)
show(df)


# ── 3. Average fulfillment time by product category ──────────────────────────
#
# How many days on average from purchase to shipment, broken out by category?
# Only completed, shipped, and returned orders with a non-NULL shipped_date
# are included — these are the orders that were actually dispatched.

section("3 · Average Fulfillment Time (days) by Product Category")

df = engine.query("avg_fulfillment_time", dimension="product_category")
df.rename(columns={"avg_fulfillment_time": "avg_days_to_ship"}, inplace=True)
show(df)


# ── 4. Customer LTV — top 10 ──────────────────────────────────────────────────
#
# Which customers have generated the highest lifetime value?
# customer_ltv automatically groups by customer_id (default_dimension in YAML).

section("4 · Top 10 Customers by Lifetime Value")

df = engine.query("customer_ltv", order_by="desc", limit=10)
df["customer_ltv"] = df["customer_ltv"].map("${:>12,.2f}".format)
show(df)


# ── 5. Return rate by product category ───────────────────────────────────────
#
# What fraction of orders are returned for each product category?
# return_rate = returned_orders / total_orders

section("5 · Return Rate by Product Category")

df = engine.query("return_rate", dimension="product_category")
show(df, pct_cols=["return_rate"])


# ── 6. Cancellation rate by month ────────────────────────────────────────────
#
# How does the cancellation rate change over time?
# cancellation_rate = cancelled_orders / total_orders

section("6 · Cancellation Rate by Month")

df = engine.query("cancellation_rate", dimension="purchase_month")
show(df, pct_cols=["cancellation_rate"])

print(f"\n{'━' * WIDTH}\n")
