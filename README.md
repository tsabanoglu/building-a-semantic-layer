# Semantic Layer — E-commerce

For anyone who wants hands-on experience building a semantic layer using realistic e-commerce data and meaningful business metrics. Includes a custom metric engine (Python + YAML) and a dbt transformation layer on the same four raw tables.

See [SEMANTIC_LAYER_LOGIC.md](SEMANTIC_LAYER_LOGIC.md) for full design notes.

## Setup

```bash
uv sync
uv run scripts/load_data.py   # generate + load synthetic data
uv run scripts/verify.py      # confirm row counts
```

## Usage

```python
from semantic.engine import SemanticEngine

engine = SemanticEngine()
engine.query("gross_revenue", dimension="product_category")
engine.query("net_revenue", dimension="purchase_month")
engine.query("customer_ltv", order_by="desc", limit=10)
```

```bash
uv run queries/example_queries.py   # six demo queries
```
