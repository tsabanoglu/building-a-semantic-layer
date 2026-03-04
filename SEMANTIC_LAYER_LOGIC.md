# Semantic Layer — Design & Logic

This document explains how the semantic layer is structured, why certain design decisions were made, and how the engine translates a metric name into SQL.

---

## What is a semantic layer?

A semantic layer sits between raw database tables and the people (or tools) querying them. Instead of writing SQL by hand every time, you define **what** a metric means once — its formula, its data source, its business rules — and then query it by name.

The goal here is that `engine.query("net_revenue", dimension="product_category")` always returns the same number as `engine.query("net_revenue", dimension="purchase_month")`, because both use the same underlying definition. There is no risk of one analyst filtering on `order_status = 'completed'` while another forgets to.

---

## Project structure

```
semantic/
  data/
    ecommerce.duckdb       ← source database
  scripts/
    load_data.py           ← generates and loads synthetic data
    verify.py              ← row counts and sanity checks
  semantic/
    metrics.yaml           ← single source of truth for all metric definitions
    engine.py              ← reads the YAML, builds SQL, runs queries
  queries/
    example_queries.py     ← six demo queries
```

---

## Database schema

Four tables with foreign key relationships:

```
customers ──< orders ──< order_items >── products
```

| Table | Key columns |
|---|---|
| `customers` | `customer_id`, `email`, `payment_method_preferred`, `created_at` |
| `orders` | `order_id`, `customer_id`, `purchase_date`, `order_status`, `shipped_date`, `delivered_date`, `shipping_address`, `order_total` |
| `order_items` | `order_item_id`, `order_id`, `product_id`, `quantity`, `unit_price`, `line_total` |
| `products` | `product_id`, `product_name`, `product_category`, `base_price`, `stock_quantity` |

Order statuses in the data: `completed` (60%), `shipped` (15%), `pending` (10%), `returned` (10%), `cancelled` (5%).

### The revenue source rule

> **All revenue metrics use `order_items.line_total` as their source. `orders.order_total` is never used.**

`orders.order_total` is a pre-aggregated convenience column that can drift from the sum of its line items (e.g. if items are added or refunded). Reading from `order_items.line_total` is the single authoritative source and allows revenue to be correctly sliced by product, category, or any other item-level attribute.

---

## How the YAML is structured

`metrics.yaml` has two top-level sections: `contexts` and `metrics`.

### Contexts

A context defines the SQL skeleton — the `FROM` clause and any joins that are always required for a given group of metrics. It also declares how each dimension can be added to that skeleton.

There are two contexts:

**`order_items_context`**
```
FROM order_items oi
JOIN orders o ON oi.order_id = o.order_id
```
Used by all revenue metrics. The anchor is `order_items` because revenue lives at the line-item level. `orders` is always joined to give access to `order_status` and dates.

**`orders_context`**
```
FROM orders o
```
Used by rate metrics (cancellation, return) and fulfilment time metrics. The anchor is `orders` because these metrics count or average at the order level, not the line-item level.

### Dimension joins

Each context declares how every available dimension is reached from its anchor table. This is what makes the engine dimension-aware without hardcoding SQL per metric.

For example, `product_category` means different things depending on the context:

- In `order_items_context`: `JOIN products p ON oi.product_id = p.product_id` — products is one hop from `order_items`.
- In `orders_context`: `JOIN order_items oi ON o.order_id = oi.order_id`, then `JOIN products p ON oi.product_id = p.product_id` — two hops, because `orders` has no direct link to `products`.

The engine only adds these joins when a dimension is actually requested. A call without a dimension produces no extra joins.

### Metric types

Metrics come in three types:

**`aggregate`** — a single SQL expression evaluated in a `SELECT`. Business filters are baked into the expression using `CASE WHEN` rather than `WHERE`, so multiple metrics can be combined in the same query without conflicting filter scopes.

```yaml
gross_revenue:
  type: aggregate
  select_expr: >-
    SUM(CASE WHEN o.order_status IN ('completed', 'pending')
             THEN oi.line_total ELSE 0 END)
```

Using `CASE WHEN` instead of `WHERE` is important: if `net_revenue` used a `WHERE order_status IN ('completed', 'pending')` clause, a second `CASE WHEN order_status = 'returned'` for the returned component would either conflict or require a separate subquery. With `CASE WHEN` both live comfortably in the same `SELECT`.

**`derived`** — composed from other metrics via a formula. The engine substitutes each component's `select_expr` into the formula at query time, producing a single-pass SQL expression.

```yaml
net_revenue:
  type: derived
  components: [gross_revenue, returned_revenue]
  formula: "{gross_revenue} - {returned_revenue}"
```

At runtime this becomes:
```sql
(SUM(CASE WHEN o.order_status IN ('completed', 'pending') THEN oi.line_total ELSE 0 END))
- (SUM(CASE WHEN o.order_status = 'returned' THEN oi.line_total ELSE 0 END))
```

**`special`** — metrics that require SQL constructs (window functions, multi-step CTEs) that don't fit the aggregate pattern. The SQL is written in full and executed as-is. Dimension and filter arguments are not supported for special metrics.

```yaml
time_between_orders:
  type: special
  sql: |
    WITH order_counts AS ( ... ),
    gaps AS ( ... )
    SELECT AVG(days_between) AS time_between_orders, ...
```

---

## Metric reference

### Revenue metrics

All use `order_items_context`. All read from `order_items.line_total`.

| Metric | Formula | Included statuses |
|---|---|---|
| `gross_revenue` | `SUM(line_total)` | completed, pending |
| `returned_revenue` | `SUM(line_total)` | returned |
| `net_revenue` | `gross_revenue − returned_revenue` | — |
| `gross_aov` | `gross_revenue / count(distinct completed+pending orders)` | completed, pending |
| `net_aov` | `net_revenue / count(distinct completed+pending+returned orders)` | completed, pending, returned |

**Why does `net_aov` include returned orders in the denominator?**
Returned orders were fulfilled and shipped — they represent real operational cost. Including them in the denominator gives an honest picture of the average net value per fulfilled order, rather than making the average look artificially high by ignoring returns.

### Customer metrics

| Metric | Logic |
|---|---|
| `customer_ltv` | `gross_revenue` grouped by `customer_id`. Default dimension is `customer_id` so no dimension argument is needed. |
| `cancellation_rate` | `cancelled_orders / total_orders` — uses `orders_context`, counts at order grain. |
| `return_rate` | `returned_orders / total_orders` — same pattern as cancellation_rate. |
| `time_between_orders` | Window function (`LAG`) over purchase dates per customer. Repeat customers only; single-purchase customers are counted separately in the output. |

### `customer_segment` dimension

A derived dimension computed via a CTE:

| Segment | Rule |
|---|---|
| `new` | Customer has exactly 1 order |
| `repeat` | Customer has 2+ orders and last order was within 90 days of the dataset's latest order date |
| `churned` | Customer has 2+ orders but last order was more than 90 days before the dataset's latest order date |

> **Why dataset max date, not `CURRENT_DATE`?** The source data runs from 2022 to early 2024. Comparing against today would mark every customer with 2+ orders as churned — because everyone's last order is more than 90 days ago relative to 2026. Using `(SELECT MAX(purchase_date) FROM orders)` as the reference point keeps the segmentation meaningful within the dataset's own time range.

The CTE (`customer_stats`) is injected as a `WITH` clause at the top of the query whenever this dimension is requested.

### Operations metrics

All use `orders_context`. All apply strict NULL guards and status filters — only rows where the relevant date field is actually populated are included.

| Metric | Measurement | Included statuses | NULL guard |
|---|---|---|---|
| `avg_fulfillment_time` | `purchase_date` → `shipped_date` | completed, shipped, returned | `shipped_date IS NOT NULL` |
| `avg_delivery_time` | `shipped_date` → `delivered_date` | completed, returned | `delivered_date IS NOT NULL` |
| `avg_end_to_end_time` | `purchase_date` → `delivered_date` | completed, returned | `delivered_date IS NOT NULL` |

`shipped` orders are included in `avg_fulfillment_time` (they have a `shipped_date`) but excluded from delivery and end-to-end times (they don't yet have a `delivered_date`). `returned` orders went through the full delivery cycle before being returned, so their dates are valid for all three metrics.

---

## How the engine builds SQL

When you call `engine.query("net_revenue", dimension="purchase_month")`, the engine does this:

1. **Look up the metric** in `metrics.yaml` → finds `type: derived`, `context: order_items_context`.
2. **Resolve the SELECT expression** — substitutes `gross_revenue` and `returned_revenue` select_exprs into the formula.
3. **Load the context** → `FROM order_items oi JOIN orders o ON oi.order_id = o.order_id`.
4. **Look up the dimension** (`purchase_month`) in the context's `dimension_joins` → `STRFTIME('%Y-%m', o.purchase_date)`, no extra joins needed.
5. **Collect WHERE conditions** — the metric's own `filters` list (none for `net_revenue`), then any user-supplied `filters` dict.
6. **Assemble the SQL**:

```sql
SELECT
  STRFTIME('%Y-%m', o.purchase_date) AS purchase_month,
  (SUM(CASE WHEN o.order_status IN ('completed', 'pending') THEN oi.line_total ELSE 0 END))
  - (SUM(CASE WHEN o.order_status = 'returned' THEN oi.line_total ELSE 0 END)) AS net_revenue
FROM order_items oi
JOIN orders o ON oi.order_id = o.order_id
GROUP BY STRFTIME('%Y-%m', o.purchase_date)
ORDER BY STRFTIME('%Y-%m', o.purchase_date)
```

You can inspect the generated SQL for any query without running it:
```python
print(engine.get_sql("net_revenue", dimension="purchase_month"))
```

---

## Available dimensions

| Dimension | Source | Extra joins added |
|---|---|---|
| `purchase_date` | `orders.purchase_date` | none |
| `purchase_month` | `orders.purchase_date` (truncated) | none |
| `purchase_year` | `orders.purchase_date` (truncated) | none |
| `order_status` | `orders.order_status` | none |
| `product_category` | `products.product_category` | `products` (+ `order_items` if on `orders_context`) |
| `payment_method_preferred` | `customers.payment_method_preferred` | `customers` |
| `customer_id` | `customers.customer_id` | `customers` |
| `customer_segment` | derived (CTE) | `customers`, CTE `customer_stats` |

---

## Filtering

Pass a `filters` dict to `engine.query()` to add WHERE conditions on top of the metric's own logic.

```python
# Revenue for Electronics only
engine.query("gross_revenue", filters={"product_category": "Electronics"})

# Cancellation rate for 2023 only
engine.query("cancellation_rate", dimension="purchase_month",
             filters={"purchase_date": {"year": 2023}})

# Multiple statuses
engine.query("gross_revenue", filters={"order_status": ["completed", "pending"]})
```

`purchase_date` filters support `year`, `month`, and `day` keys.

If a filter references a table not already in scope (e.g. filtering by `product_category` on a metric that normally has no products join), the engine automatically adds the required joins by looking up the dimension's join recipe from the context.

---

## Implementation notes

**PyYAML and the `on` keyword**
PyYAML parses YAML 1.1, in which bare `on` is a boolean (`True`). Join predicates in the YAML therefore use the key `condition` instead of `on`. This is a known PyYAML quirk worth documenting.

**NULLIF for safe division**
All ratio metrics wrap their denominator in `NULLIF(..., 0)` to avoid division-by-zero errors when a dimension bucket has no qualifying orders.

**`CASE WHEN` vs `WHERE` for metric filters**
Metric-specific status filters are embedded in the `SELECT` expression as `CASE WHEN ... THEN value ELSE 0 END`, not as `WHERE` clauses. This means multiple metric components (e.g. gross and returned revenue) can coexist in a single scan of the table without conflicting filters.

---

## The dbt layer

Alongside the custom semantic engine, this project also has a dbt project (`ecommerce_semantic/`) that transforms the same four raw tables into a clean, layered model structure.

### What we built

**Staging models** (`models/staging/`) — materialized as views. Each staging model maps 1:1 to a raw source table. The only work done here is type casting and column renaming for consistency. No business logic.

| Model | Source table | Key changes |
|---|---|---|
| `stg_customers` | `customers` | casts `created_at` to `DATE` |
| `stg_orders` | `orders` | renames `purchase_date` → `purchased_at`, `shipped_date` → `shipped_at`, `delivered_date` → `delivered_at`; casts timestamps and `order_total` |
| `stg_order_items` | `order_items` | casts `quantity`, `unit_price`, `line_total` |
| `stg_products` | `products` | casts `base_price`, `stock_quantity` |

**Mart models** (`models/marts/`) — materialized as tables. Business logic lives here.

| Model | What it does |
|---|---|
| `customers_mart` | Aggregates order history per customer. Derives `customer_segment` (new / repeat / churned / no_orders) using the 90-day / dataset-max-date rule. |
| `orders_mart` | Enriches each order with `customer_segment`, item counts, `days_to_ship`, and `days_to_deliver`. References `customers_mart` for the segment. |
| `products_mart` | Sales metrics per product (total orders, units sold, revenue, avg selling price). Excludes cancelled orders. |

The `dbt_project.yml` is configured so staging models are views (cheap to rebuild, always fresh) and mart models are tables (pre-aggregated, fast to query).

### How the churn rule is defined in dbt

`customers_mart` derives `customer_segment` with a subquery rather than `CURRENT_DATE`:

```sql
case
    when os.total_orders is null then 'no_orders'
    when os.total_orders = 1     then 'new'
    when datediff('day', os.last_order_at,
         (select max(purchased_at) from orders)) > 90 then 'churned'
    else 'repeat'
end as customer_segment
```

---

## Two approaches compared

This project contains two independent ways of answering business questions from the same four raw tables. They are not redundant — they solve different problems.

### Custom semantic engine (`semantic/`)

The engine is a Python class that reads `metrics.yaml` and dynamically builds and executes SQL at query time. It queries the raw tables directly.

**What it's good at:**
- Ad-hoc queries in Python with a single line: `engine.query("net_revenue", dimension="product_category")`
- Enforcing a single source of truth for metric definitions — every caller gets the same SQL, no copy-paste drift
- Dynamic dimension and filter composition without writing SQL by hand
- Embedding in notebooks, scripts, or an API without a BI tool in the way

**Limitations:**
- No data lineage — if the raw tables change shape, nothing tells you which metrics are broken
- No incremental materialisation — every query hits the raw tables from scratch
- Dimensions and filters must be pre-declared in the YAML; truly ad-hoc SQL still requires writing SQL
- No test framework for the metric definitions themselves

### dbt layer (`ecommerce_semantic/`)

dbt transforms raw tables into clean, documented, tested views and tables that any tool can query. It does not define metrics — it defines the data model that metrics are computed from.

**What it's good at:**
- Lineage — dbt knows which models depend on which sources, so a schema change surfaces as a broken model
- Incremental builds — only recompute what changed
- Testing — `not_null`, `unique`, `accepted_values` tests can be added to any column in any model
- Compatibility — the mart tables are plain SQL tables, queryable by any BI tool (Metabase, Superset, Tableau, etc.)
- Documentation — `schema.yml` descriptions are rendered in the dbt docs site

**Limitations:**
- Mart tables are static snapshots — a new metric or segment requires a model change and a `dbt run`
- Business logic is spread across SQL files; cross-model consistency requires discipline (e.g. the churn threshold must be kept in sync between `customers_mart.sql` and `metrics.yaml` manually)
- No Python-callable API — you query the output tables, not a metric abstraction

### How they relate in this project

```
Raw tables (ecommerce.duckdb)
        │
        ├── dbt staging views   ← type-safe, renamed columns
        │        │
        │        └── dbt mart tables  ← pre-joined, business logic baked in
        │                              (customers_mart, orders_mart, products_mart)
        │
        └── Semantic engine  ← queries raw tables directly,
                               builds SQL dynamically from metrics.yaml
```

The two layers are independent. The semantic engine does not read from the dbt mart tables — it builds equivalent logic in SQL at query time. This is intentional: the engine is a metric API, the dbt marts are materialised reporting tables. A future integration could point the engine's contexts at the mart tables instead of the raw tables, which would reduce duplicated join logic at the cost of coupling the two layers together.
