"""Compile a metric request (metrics x dimensions, filtered) to one SQL query.

The query reads the metric view's source view with a LEFT JOIN per join it
uses, and groups by the requested dimensions. It is the same query a Unity
Catalog metric view runs for `SELECT dims, MEASURE(m) ... GROUP BY ALL`, so
local answers and Databricks answers agree.

Values never go into the SQL text when a parameter style is given: filters
become `?` (DuckDB) or `:p0` (Spark) markers with the values returned beside
the SQL. The literal style is only for SQL generated from trusted code (the
dashboard builder), and still escapes every value.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Optional, Sequence, Tuple

from gbdp.semantic.layer import Dimension, MetricView, SemanticLayer, SemanticLayerError, referenced_joins
from gbdp.semantic.render import Target, render

FILTER_OPS = {"eq": "=", "ne": "<>", "gt": ">", "gte": ">=", "lt": "<", "lte": "<=", "in": "IN", "not_in": "NOT IN",
              "is_null": "IS NULL", "not_null": "IS NOT NULL"}
MAX_LIMIT = 100_000
MAX_IN_VALUES = 1_000


@dataclass(frozen=True)
class Filter:
    dimension: str
    op: str
    value: Any = None


@dataclass
class MetricRequest:
    metrics: Sequence[str]
    dimensions: Sequence[str] = ()
    filters: Sequence[Filter] = ()
    start: Optional[str] = None  # inclusive, on the metric view's date dimension
    end: Optional[str] = None
    having: Sequence[Tuple[str, str, Any]] = ()  # (metric, op, number)
    order_by: Sequence[Tuple[str, str]] = ()  # (output column, asc|desc)
    limit: Optional[int] = None
    aliases: Dict[str, str] = field(default_factory=dict)  # metric/dimension -> output column name


@dataclass
class CompiledQuery:
    sql: str
    params: List[Any]
    metric_view: str
    columns: List[str]


def _coerce(dim: Dimension, value: Any) -> Any:
    if value is None:
        return None
    try:
        if dim.type == "int":
            return int(value)
        if dim.type == "date":
            return value if isinstance(value, date) else date.fromisoformat(str(value))
        if dim.type == "bool":
            if isinstance(value, bool):
                return value
            return str(value).lower() in ("1", "true", "yes")
        return str(value)
    except (TypeError, ValueError) as exc:
        raise SemanticLayerError(f"filter value {value!r} is not a valid {dim.type} for {dim.name}") from exc


class _Params:
    def __init__(self, style: str, target: Target) -> None:
        self.style, self.target, self.values = style, target, []  # type: ignore[var-annotated]

    def add(self, value: Any) -> str:
        if self.style == "qmark":
            self.values.append(value)
            return "?"
        if self.style == "named":
            self.values.append(value)
            return f":p{len(self.values) - 1}"
        if self.style == "literal":
            if isinstance(value, bool):
                return "TRUE" if value else "FALSE"
            if isinstance(value, (int, float)):
                return repr(value)
            if isinstance(value, date):
                return f"DATE {self.target.string(value.isoformat())}"
            return self.target.string(value)
        raise SemanticLayerError(f"unknown parameter style {self.style}")


def metric_view_for(layer: SemanticLayer, metrics: Sequence[str]) -> MetricView:
    if not metrics:
        raise SemanticLayerError("ask for at least one metric")
    views = {layer.metric(m).metric_view for m in metrics}
    if len(views) != 1:
        by_view = {v: [m for m in metrics if layer.metric(m).metric_view == v] for v in sorted(views)}
        raise SemanticLayerError(f"metrics come from different metric views and can't be grouped together: {by_view}")
    return layer.metric_views[views.pop()]


def compile_request(layer: SemanticLayer, req: MetricRequest, target: Target, param_style: str = "qmark") -> CompiledQuery:
    mv = metric_view_for(layer, req.metrics)
    params = _Params(param_style, target)
    dims = [mv.dimension(d) for d in req.dimensions]
    if len(set(req.dimensions)) != len(req.dimensions):
        raise SemanticLayerError("a dimension is listed twice")

    def alias(name: str) -> str:
        a = req.aliases.get(name, name)
        if not a.replace("_", "").isalnum():
            raise SemanticLayerError(f"bad output column name {a!r}")
        return a

    select: List[str] = []
    columns: List[str] = []
    for d in dims:
        select.append(f"{render(d.expr, layer, target)} AS {alias(d.name)}")
        columns.append(alias(d.name))
    for m in req.metrics:
        select.append(f"{layer.metric(m).expr} AS {alias(m)}")
        columns.append(alias(m))
    if len(set(columns)) != len(columns):
        raise SemanticLayerError(f"output columns collide: {columns}")

    where: List[str] = []
    if mv.filter:
        where.append(f"({render(mv.filter, layer, target)})")
    filters = list(req.filters)
    if req.start or req.end:
        date_dim = mv.dimension_for_role("date")
        if date_dim is None:
            raise SemanticLayerError(f"metric view {mv.name} has no date dimension for start/end")
        if req.start:
            filters.append(Filter(date_dim.name, "gte", req.start))
        if req.end:
            filters.append(Filter(date_dim.name, "lte", req.end))
    used_exprs = [d.expr for d in dims] + [layer.metric(m).expr for m in req.metrics]
    for f in filters:
        if f.op not in FILTER_OPS:
            raise SemanticLayerError(f"unknown filter op {f.op!r}; use one of {', '.join(FILTER_OPS)}")
        dim = mv.dimension(f.dimension)
        used_exprs.append(dim.expr)
        expr = render(dim.expr, layer, target)
        if f.op in ("is_null", "not_null"):
            where.append(f"{expr} {FILTER_OPS[f.op]}")
        elif f.op in ("in", "not_in"):
            values = f.value if isinstance(f.value, (list, tuple)) else [f.value]
            if not values:
                raise SemanticLayerError(f"filter {f.dimension} {f.op} needs at least one value")
            if len(values) > MAX_IN_VALUES:
                raise SemanticLayerError(f"filter {f.dimension} {f.op} has {len(values)} values; the limit is {MAX_IN_VALUES}")
            marks = ", ".join(params.add(_coerce(dim, v)) for v in values)
            where.append(f"{expr} {FILTER_OPS[f.op]} ({marks})")
        else:
            if f.value is None:
                raise SemanticLayerError(f"filter {f.dimension} {f.op} needs a value (use is_null for nulls)")
            where.append(f"{expr} {FILTER_OPS[f.op]} {params.add(_coerce(dim, f.value))}")

    having: List[str] = []
    for metric, op, value in req.having:
        if op not in ("eq", "ne", "gt", "gte", "lt", "lte"):
            raise SemanticLayerError(f"unknown having op {op!r}")
        if metric not in req.metrics:
            raise SemanticLayerError(f"having on {metric}, which isn't requested")
        try:
            number = float(value)
        except (TypeError, ValueError) as exc:
            raise SemanticLayerError(f"having value {value!r} is not a number") from exc
        number = int(number) if number.is_integer() else number
        having.append(f"{layer.metric(metric).expr} {FILTER_OPS[op]} {params.add(number)}")

    order: List[str] = []
    for col, direction in req.order_by:
        if col not in columns:
            raise SemanticLayerError(f"order by {col!r}, which isn't an output column ({', '.join(columns)})")
        if direction.lower() not in ("asc", "desc"):
            raise SemanticLayerError(f"order direction must be asc or desc, not {direction!r}")
        order.append(f"{col} {direction.upper()} NULLS LAST")

    joins_used = sorted({j for e in used_exprs for j in referenced_joins(e)})
    from_sql = [f"FROM {target.view(mv.source)} AS source"]
    for j in mv.joins:
        if j.name in joins_used:
            from_sql.append(f"LEFT JOIN {target.view(j.source)} AS {j.name} ON {render(j.on, layer, target)}")

    lines = ["SELECT", "  " + ",\n  ".join(select), *from_sql]
    if where:
        lines.append("WHERE " + "\n  AND ".join(where))
    if dims:
        lines.append("GROUP BY " + ", ".join(render(d.expr, layer, target) for d in dims))
    if having:
        lines.append("HAVING " + " AND ".join(having))
    if order:
        lines.append("ORDER BY " + ", ".join(order))
    if req.limit is not None:
        lines.append(f"LIMIT {max(1, min(int(req.limit), MAX_LIMIT))}")
    sql = "\n".join(lines)

    if target.inline_views:
        needed = layer.view_dependencies([mv.source] + [j.source for j in mv.joins if j.name in joins_used])
        ctes = ",\n".join(f"{v} AS (\n{_indent(render(layer.views[v].sql, layer, target))}\n)" for v in needed)
        sql = f"WITH {ctes}\n{sql}"
    return CompiledQuery(sql=sql, params=params.values, metric_view=mv.name, columns=columns)


def _indent(sql: str) -> str:
    return "\n".join(("  " + line) if line else line for line in sql.splitlines())


def view_ddl(layer: SemanticLayer, target: Target, name: str) -> str:
    """The SELECT body of a view, rendered for target."""
    return render(layer.views[name].sql, layer, target)
