"""
Semantic Layer Engine
=====================
Reads metrics.yaml and translates metric/dimension/filter requests into SQL,
then executes them against the local DuckDB database.

Public API
----------
    engine = SemanticEngine()
    df     = engine.query("gross_revenue", dimension="product_category")
    sql    = engine.get_sql("net_revenue", dimension="purchase_month")

query() parameters
------------------
    metric    : str               — metric name defined in metrics.yaml
    dimension : str | None        — optional dimension to group by
                                    (e.g. "product_category", "purchase_month")
    filters   : dict | None       — additional WHERE conditions, e.g.
                                    {"purchase_date": {"year": 2023}}
                                    {"order_status": "completed"}
                                    {"product_category": ["Electronics", "Books"]}
    limit     : int | None        — LIMIT clause
    order_by  : "asc"|"desc"|None — orders by the metric column when supplied
                                    (default: order by dimension column)
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import yaml

_ROOT      = Path(__file__).parent.parent
_YAML_PATH = Path(__file__).parent / "metrics.yaml"
_DB_PATH   = _ROOT / "data" / "ecommerce.duckdb"


class SemanticEngine:

    def __init__(
        self,
        db_path:   Path = _DB_PATH,
        yaml_path: Path = _YAML_PATH,
    ) -> None:
        self._con = duckdb.connect(str(db_path), read_only=True)
        with open(yaml_path) as fh:
            cfg = yaml.safe_load(fh)
        self._metrics  = cfg["metrics"]
        self._contexts = cfg["contexts"]

    # ── Public API ────────────────────────────────────────────────────────────

    def query(
        self,
        metric:    str,
        dimension: str | None  = None,
        filters:   dict | None = None,
        limit:     int | None  = None,
        order_by:  str | None  = None,
    ) -> pd.DataFrame:
        """Execute a semantic query and return a DataFrame."""
        sql = self.get_sql(metric, dimension, filters, limit, order_by)
        return self._con.execute(sql).df()

    def get_sql(
        self,
        metric:    str,
        dimension: str | None  = None,
        filters:   dict | None = None,
        limit:     int | None  = None,
        order_by:  str | None  = None,
    ) -> str:
        """Return the SQL that would be executed (without running it)."""
        if metric not in self._metrics:
            available = ", ".join(self._metrics)
            raise ValueError(
                f"Unknown metric '{metric}'. Available metrics: {available}"
            )
        m = self._metrics[metric]

        # Special metrics ship with hand-written SQL (no dynamic generation)
        if m.get("type") == "special":
            return m["sql"].strip()

        return self._build_sql(metric, m, dimension, filters, limit, order_by)

    def describe(self, metric: str) -> str:
        """Return the human-readable description of a metric."""
        return self._metrics[metric].get("description", "(no description)")

    def list_metrics(self) -> list[str]:
        """Return a list of all defined metric names."""
        return list(self._metrics)

    # ── SQL builder ───────────────────────────────────────────────────────────

    def _build_sql(
        self,
        metric_name: str,
        metric:      dict,
        dimension:   str | None,
        filters:     dict | None,
        limit:       int | None,
        order_by:    str | None,
    ) -> str:
        context_name = metric["context"]
        context      = self._contexts[context_name]

        # SELECT expression — resolve derived metrics recursively
        select_expr = self._resolve_select_expr(metric)

        # FROM clause and base JOINs
        from_clause = context["from"]
        joins: list[dict] = [dict(j) for j in context.get("base_joins", [])]
        known_aliases: set[str] = {j["alias"] for j in joins}

        # Dimension — may add JOINs and a CTE
        ctes: list[tuple[str, str]] = []
        dim_select: str | None = None

        effective_dim = dimension or metric.get("default_dimension")
        if effective_dim:
            dim_cfg = context.get("dimension_joins", {}).get(effective_dim)
            if dim_cfg is None:
                available = ", ".join(context.get("dimension_joins", {}))
                raise ValueError(
                    f"Dimension '{effective_dim}' is not available for "
                    f"context '{context_name}'. Available: {available}"
                )

            # CTE-backed dimension (e.g. customer_segment)
            if "cte_name" in dim_cfg:
                ctes.append((dim_cfg["cte_name"], dim_cfg["cte_sql"].strip()))

            for j in dim_cfg.get("joins", []):
                if j["alias"] not in known_aliases:
                    joins.append(dict(j))
                    known_aliases.add(j["alias"])

            dim_select = dim_cfg["select"].strip()

        # WHERE — metric's own filters, then user-supplied filters
        where_parts: list[str] = list(metric.get("filters", []))
        if filters:
            user_conditions = self._translate_filters(
                filters, context, joins, known_aliases
            )
            where_parts.extend(user_conditions)

        # Assemble SQL
        lines: list[str] = []

        if ctes:
            cte_block = ",\n".join(
                f"{name} AS (\n{sql}\n)" for name, sql in ctes
            )
            lines.append(f"WITH {cte_block}")

        if dim_select:
            lines.append(
                f"SELECT\n  {dim_select} AS {effective_dim},"
                f"\n  {select_expr} AS {metric_name}"
            )
        else:
            lines.append(f"SELECT\n  {select_expr} AS {metric_name}")

        lines.append(f"FROM {from_clause}")

        for j in joins:
            lines.append(f"JOIN {j['table']} {j['alias']} ON {j['condition']}")

        if where_parts:
            lines.append("WHERE " + "\n  AND ".join(where_parts))

        if dim_select:
            lines.append(f"GROUP BY {dim_select}")
            if order_by in ("asc", "desc"):
                lines.append(f"ORDER BY {metric_name} {order_by.upper()}")
            else:
                lines.append(f"ORDER BY {dim_select}")
        elif order_by in ("asc", "desc"):
            lines.append(f"ORDER BY {metric_name} {order_by.upper()}")

        if limit:
            lines.append(f"LIMIT {limit}")

        return "\n".join(lines)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _resolve_select_expr(self, metric: dict) -> str:
        """
        Return the final SELECT expression.
        For derived metrics, substitutes component expressions into the formula.
        """
        if metric.get("type") != "derived":
            return metric["select_expr"].strip()

        expr = metric["formula"]
        for component in metric["components"]:
            comp_expr = self._metrics[component]["select_expr"].strip()
            expr = expr.replace(f"{{{component}}}", f"({comp_expr})")
        return expr

    def _translate_filters(
        self,
        filters:       dict,
        context:       dict,
        joins:         list[dict],
        known_aliases: set[str],
    ) -> list[str]:
        """
        Convert a user filter dict into SQL WHERE conditions.
        Mutates `joins` and `known_aliases` in-place if the filter requires
        a JOIN that is not yet present (e.g. filtering by product_category
        on a metric that doesn't normally join products).
        """
        conditions: list[str] = []
        dim_join_cfg = context.get("dimension_joins", {})

        for key, value in filters.items():

            # ── purchase_date: supports year / month / day decomposition ──
            if key == "purchase_date":
                if isinstance(value, dict):
                    if "year" in value:
                        conditions.append(f"YEAR(o.purchase_date) = {value['year']}")
                    if "month" in value:
                        conditions.append(f"MONTH(o.purchase_date) = {value['month']}")
                    if "day" in value:
                        conditions.append(f"DAY(o.purchase_date) = {value['day']}")
                else:
                    conditions.append(f"CAST(o.purchase_date AS DATE) = '{value}'")

            # ── order_status ──────────────────────────────────────────────
            elif key == "order_status":
                if isinstance(value, list):
                    vals = ", ".join(f"'{v}'" for v in value)
                    conditions.append(f"o.order_status IN ({vals})")
                else:
                    conditions.append(f"o.order_status = '{value}'")

            # ── dimension-keyed filters (product_category, payment_method, …) ──
            elif key in dim_join_cfg:
                cfg = dim_join_cfg[key]
                # Add any joins needed for this filter
                for j in cfg.get("joins", []):
                    if j["alias"] not in known_aliases:
                        joins.append(dict(j))
                        known_aliases.add(j["alias"])
                col = cfg["select"].strip()
                if isinstance(value, list):
                    vals = ", ".join(f"'{v}'" for v in value)
                    conditions.append(f"{col} IN ({vals})")
                else:
                    conditions.append(f"{col} = '{value}'")

            else:
                available = "purchase_date, order_status, " + ", ".join(dim_join_cfg)
                raise ValueError(
                    f"Unknown filter key '{key}'. Available: {available}"
                )

        return conditions
