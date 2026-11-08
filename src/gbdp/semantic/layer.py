"""Load and validate configs/semantic_layer.yaml.

The YAML is the single definition of the gold schema's meaning and of every
metric. This module turns it into typed objects and refuses a file that is
internally inconsistent (a metric naming a metric view that doesn't exist, a
view reading a table nobody describes, a dimension with an unknown role), so a
bad edit fails at load time rather than as a wrong number later.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

ROLES = ("player", "team", "league", "season", "date")
DIM_TYPES = ("string", "int", "date", "bool")
FORMATS = ("count", "rate", "percent", "decimal", "innings")

_REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PATH = Path("configs/semantic_layer.yaml")

_PLACEHOLDER = re.compile(r"\{\{(\w+):([^}]*)\}\}")
_JOIN_REF = re.compile(r"\b([A-Za-z_]\w*)\.[A-Za-z_`\"]")


class SemanticLayerError(ValueError):
    """The YAML is malformed or inconsistent, or a request doesn't fit it."""


@dataclass(frozen=True)
class Table:
    name: str
    description: str
    grain: str
    columns: Dict[str, str]
    primary_key: Tuple[str, ...] = ()
    foreign_keys: Dict[str, str] = field(default_factory=dict)
    sources: Tuple[str, ...] = ()
    caveats: Tuple[str, ...] = ()
    placeholder: bool = False
    written_by: str = "gold/publish.py"


@dataclass(frozen=True)
class View:
    name: str
    description: str
    sql: str

    @property
    def tables(self) -> Tuple[str, ...]:
        return tuple(dict.fromkeys(v for k, v in _PLACEHOLDER.findall(self.sql) if k == "table"))

    @property
    def views(self) -> Tuple[str, ...]:
        return tuple(dict.fromkeys(v for k, v in _PLACEHOLDER.findall(self.sql) if k == "view"))


@dataclass(frozen=True)
class Join:
    name: str
    source: str
    on: str


@dataclass(frozen=True)
class Dimension:
    name: str
    expr: str
    description: str
    role: Optional[str] = None
    type: str = "string"


@dataclass(frozen=True)
class MetricView:
    name: str
    description: str
    source: str
    joins: Tuple[Join, ...]
    dimensions: Tuple[Dimension, ...]
    filter: Optional[str] = None
    not_applicable_roles: Dict[str, str] = field(default_factory=dict)
    # Applied by the local engine when a request doesn't filter the dimension.
    default_filters: Tuple[Tuple[str, str, Any], ...] = ()

    def dimension(self, name: str) -> Dimension:
        for d in self.dimensions:
            if d.name == name:
                return d
        raise SemanticLayerError(
            f"metric view {self.name} has no dimension {name!r}; it has: {', '.join(d.name for d in self.dimensions)}"
        )

    def dimension_for_role(self, role: str) -> Optional[Dimension]:
        return next((d for d in self.dimensions if d.role == role), None)

    def join(self, name: str) -> Optional[Join]:
        return next((j for j in self.joins if j.name == name), None)


@dataclass(frozen=True)
class Metric:
    name: str
    metric_view: str
    description: str
    expr: str
    format: str
    source_rationale: str
    caveats: Tuple[str, ...] = ()


@dataclass(frozen=True)
class Vocabulary:
    name: str
    description: str
    values: Any  # dict code -> meaning/label, or a list
    groups: Dict[str, Tuple[str, ...]] = field(default_factory=dict)
    prefixes: Tuple[str, ...] = ()
    rules: Tuple[Dict[str, Any], ...] = ()  # season_phase: {phase, source, values, suffixes}


@dataclass
class SemanticLayer:
    raw: Dict[str, Any]
    tables: Dict[str, Table]
    audit_tables: Dict[str, str]
    vocabularies: Dict[str, Vocabulary]
    views: Dict[str, View]
    metric_views: Dict[str, MetricView]
    metrics: Dict[str, Metric]

    # ---- lookups ---------------------------------------------------------
    def metric(self, name: str) -> Metric:
        if name not in self.metrics:
            raise SemanticLayerError(f"unknown metric {name!r}; see list_metrics for the {len(self.metrics)} defined")
        return self.metrics[name]

    def metric_view_of(self, name: str) -> MetricView:
        return self.metric_views[self.metric(name).metric_view]

    def view_dependencies(self, names: List[str]) -> List[str]:
        """The named views and every view they read, dependencies first, in YAML order."""
        needed: set = set()

        def visit(v: str) -> None:
            if v in needed:
                return
            needed.add(v)
            for dep in self.views[v].views:
                visit(dep)

        for n in names:
            visit(n)
        return [v for v in self.views if v in needed]

    def base_tables(self, metric_view: str) -> List[str]:
        """Gold tables a metric view reads: its source view's first, then its joins'."""
        mv = self.metric_views[metric_view]
        views: List[str] = []
        for name in [mv.source] + [j.source for j in mv.joins]:
            views += self.view_dependencies([name])
        return list(dict.fromkeys(t for v in views for t in self.views[v].tables))

    def metric_summary(self, name: str) -> Dict[str, Any]:
        m = self.metric(name)
        mv = self.metric_views[m.metric_view]
        return {
            "name": m.name,
            "description": m.description,
            "metric_view": m.metric_view,
            "base_tables": self.base_tables(m.metric_view),
            "expr": m.expr,
            "format": m.format,
            "source_rationale": m.source_rationale,
            "caveats": list(m.caveats),
            "dimensions": [
                {"name": d.name, "role": d.role, "type": d.type, "description": d.description} for d in mv.dimensions
            ],
            "not_applicable_roles": dict(mv.not_applicable_roles),
            "default_filters": [{"dimension": d, "op": op, "value": v} for d, op, v in mv.default_filters],
        }


def _text(value: Any) -> str:
    return " ".join(str(value or "").split())


def _caveats(value: Any) -> Tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (_text(value),)
    return tuple(_text(v) for v in value)


_ALLOWED_KEYS = {
    "top": {"version", "conventions", "vocabularies", "tables", "audit_tables", "views", "metric_views", "metrics"},
    "table": {"description", "grain", "columns", "primary_key", "foreign_keys", "sources", "caveats", "placeholder",
              "written_by"},
    "vocabulary": {"description", "values", "groups", "prefixes", "rules"},
    "view": {"description", "sql"},
    "metric_view": {"description", "source", "joins", "dimensions", "filter", "not_applicable_roles",
                    "default_filters"},
    "join": {"name", "source", "on", True},
    "dimension": {"name", "expr", "description", "role", "type"},
    "metric": {"metric_view", "description", "expr", "format", "source_rationale", "caveats"},
}


def _check_keys(kind: str, where: str, value: Any) -> None:
    # A stray key is usually a YAML slip (an unquoted ", " inside a flow
    # mapping splits one description into two keys), so refuse it.
    if not isinstance(value, dict):
        raise SemanticLayerError(f"{where}: expected a mapping")
    extra = set(value) - _ALLOWED_KEYS[kind]
    if extra:
        raise SemanticLayerError(f"{where}: unknown keys {sorted(map(str, extra))} (quote text that contains ': ' or ', ')")


def parse(raw: Dict[str, Any]) -> SemanticLayer:
    if not isinstance(raw, dict) or raw.get("version") != 2:
        raise SemanticLayerError("semantic_layer.yaml must be a mapping with version: 2")
    _check_keys("top", "semantic_layer.yaml", raw)
    for kind, section in (("table", "tables"), ("vocabulary", "vocabularies"), ("view", "views"),
                          ("metric_view", "metric_views"), ("metric", "metrics")):
        for name, value in (raw.get(section) or {}).items():
            _check_keys(kind, f"{section}.{name}", value or {})
    for name, mv in (raw.get("metric_views") or {}).items():
        for j in mv.get("joins") or []:
            _check_keys("join", f"metric_views.{name}.joins", j)
        for d in mv.get("dimensions") or []:
            _check_keys("dimension", f"metric_views.{name}.dimensions.{d.get('name')}", d)

    tables: Dict[str, Table] = {}
    for name, t in (raw.get("tables") or {}).items():
        t = t or {}
        cols = {str(c): _text(d) for c, d in (t.get("columns") or {}).items()}
        tables[name] = Table(
            name=name,
            description=_text(t.get("description")),
            grain=_text(t.get("grain")),
            columns=cols,
            primary_key=tuple(t.get("primary_key") or ()),
            foreign_keys={str(k): str(v) for k, v in (t.get("foreign_keys") or {}).items()},
            sources=tuple(t.get("sources") or ()),
            caveats=_caveats(t.get("caveats")),
            placeholder=bool(t.get("placeholder")),
            written_by=t.get("written_by") or "gold/publish.py",
        )

    vocabularies: Dict[str, Vocabulary] = {}
    for name, v in (raw.get("vocabularies") or {}).items():
        vocabularies[name] = Vocabulary(
            name=name,
            description=_text(v.get("description")),
            values=v.get("values") or {},
            groups={g: tuple(str(x) for x in vals) for g, vals in (v.get("groups") or {}).items()},
            prefixes=tuple(v.get("prefixes") or ()),
            rules=tuple(dict(r) for r in v.get("rules") or ()),
        )

    views = {
        name: View(name=name, description=_text(v.get("description")), sql=str(v.get("sql") or "").strip())
        for name, v in (raw.get("views") or {}).items()
    }

    metric_views: Dict[str, MetricView] = {}
    for name, mv in (raw.get("metric_views") or {}).items():
        dims = tuple(
            Dimension(
                name=d["name"], expr=str(d["expr"]), description=_text(d.get("description")),
                role=d.get("role"), type=d.get("type") or "string",
            )
            for d in mv.get("dimensions") or []
        )
        # YAML 1.1 (PyYAML) reads a bare `on:` key as the boolean True.
        joins = tuple(
            Join(name=j["name"], source=j["source"], on=str(j["on"] if "on" in j else j[True]))
            for j in mv.get("joins") or []
        )
        metric_views[name] = MetricView(
            name=name, description=_text(mv.get("description")), source=mv["source"], joins=joins,
            dimensions=dims, filter=mv.get("filter"),
            not_applicable_roles={k: _text(v) for k, v in (mv.get("not_applicable_roles") or {}).items()},
            default_filters=tuple((f["dimension"], f.get("op", "eq"), f.get("value"))
                                  for f in mv.get("default_filters") or ()),
        )

    metrics = {
        name: Metric(
            name=name, metric_view=m["metric_view"], description=_text(m.get("description")),
            expr=_text(m.get("expr")), format=m.get("format") or "decimal",
            source_rationale=_text(m.get("source_rationale")), caveats=_caveats(m.get("caveats")),
        )
        for name, m in (raw.get("metrics") or {}).items()
    }

    layer = SemanticLayer(
        raw=raw, tables=tables, audit_tables=dict(raw.get("audit_tables") or {}), vocabularies=vocabularies,
        views=views, metric_views=metric_views, metrics=metrics,
    )
    validate(layer)
    return layer


def validate(layer: SemanticLayer) -> None:
    errors: List[str] = []
    known_tables = set(layer.tables) | set(layer.audit_tables)

    for t in layer.tables.values():
        if not t.description:
            errors.append(f"table {t.name}: no description")
        for col in t.primary_key:
            if col not in t.columns:
                errors.append(f"table {t.name}: primary key column {col} is not a column")
        for col, target in t.foreign_keys.items():
            if col not in t.columns:
                errors.append(f"table {t.name}: foreign key column {col} is not a column")
            ref_table, _, ref_col = target.partition(".")
            if ref_table not in layer.tables or ref_col not in layer.tables[ref_table].columns:
                errors.append(f"table {t.name}: foreign key {col} -> {target} points at nothing described")
        for col, desc in t.columns.items():
            if not desc:
                errors.append(f"table {t.name}: column {col} has no description")

    def check_placeholders(where: str, sql: str) -> None:
        for kind, arg in _PLACEHOLDER.findall(sql):
            if kind == "table" and arg not in known_tables:
                errors.append(f"{where}: {{{{table:{arg}}}}} is not a described table")
            elif kind == "view" and arg not in layer.views:
                errors.append(f"{where}: {{{{view:{arg}}}}} is not a defined view")
            elif kind == "list":
                vocab, _, group = arg.partition(".")
                v = layer.vocabularies.get(vocab)
                if v is None or (group and group not in v.groups) or (not group and not isinstance(v.values, list)):
                    errors.append(f"{where}: {{{{list:{arg}}}}} is not a vocabulary list")
            elif kind == "label":
                vocab = arg.partition(":")[0]
                if vocab not in layer.vocabularies or not isinstance(layer.vocabularies[vocab].values, dict):
                    errors.append(f"{where}: {{{{label:{arg}}}}} names no vocabulary with values")
            elif kind == "final" and "final_status" not in layer.vocabularies:
                errors.append(f"{where}: {{{{final:...}}}} needs vocabularies.final_status")
            elif kind == "league_of" and "source_league" not in layer.vocabularies:
                errors.append(f"{where}: {{{{league_of:...}}}} needs vocabularies.source_league")
            elif kind == "season_phase":
                v = layer.vocabularies.get("season_phase")
                if v is None or not v.rules:
                    errors.append(f"{where}: {{{{season_phase:...}}}} needs vocabularies.season_phase.rules")
                elif arg.count(":") != 1:
                    errors.append(f"{where}: write {{{{season_phase:GAME_TYPE_EXPR:SOURCE_EXPR}}}}")
            elif kind not in ("table", "view", "list", "label", "final", "league_of", "col", "season_phase"):
                errors.append(f"{where}: unknown placeholder {{{{{kind}:...}}}}")

    for v in layer.views.values():
        if not v.sql:
            errors.append(f"view {v.name}: no sql")
        check_placeholders(f"view {v.name}", v.sql)
        if v.name in known_tables:
            errors.append(f"view {v.name}: has the same name as a table")
    # Views may only read views defined before them, so YAML order is a valid creation order.
    order = list(layer.views)
    for v in layer.views.values():
        for dep in v.views:
            if dep in order and order.index(dep) >= order.index(v.name):
                errors.append(f"view {v.name}: reads {dep}, which must be defined before it")

    for mv in layer.metric_views.values():
        if mv.source not in layer.views:
            errors.append(f"metric view {mv.name}: source {mv.source} is not a defined view")
        names = [d.name for d in mv.dimensions]
        if len(names) != len(set(names)):
            errors.append(f"metric view {mv.name}: duplicate dimension names")
        join_names = {j.name for j in mv.joins}
        for j in mv.joins:
            if j.source not in layer.views:
                errors.append(f"metric view {mv.name}: join {j.name} reads {j.source}, not a defined view")
            if j.name == "source":
                errors.append(f"metric view {mv.name}: a join can't be named source")
        for dim, op, _ in mv.default_filters:
            if dim not in names:
                errors.append(f"metric view {mv.name}: default filter on unknown dimension {dim}")
            if op not in ("eq", "ne", "in", "not_in"):
                errors.append(f"metric view {mv.name}: default filter op {op} (use eq, ne, in or not_in)")
        for d in mv.dimensions:
            if d.role is not None and d.role not in ROLES:
                errors.append(f"metric view {mv.name}: dimension {d.name} has unknown role {d.role}")
            if d.type not in DIM_TYPES:
                errors.append(f"metric view {mv.name}: dimension {d.name} has unknown type {d.type}")
            check_placeholders(f"metric view {mv.name} dimension {d.name}", d.expr)
            for ref in referenced_joins(d.expr):
                if ref not in join_names:
                    errors.append(f"metric view {mv.name}: dimension {d.name} reads {ref}.*, which is not a join")
        for role in ROLES:
            if mv.dimension_for_role(role) is None and role not in mv.not_applicable_roles:
                errors.append(f"metric view {mv.name}: no {role} dimension (add one, or say why under not_applicable_roles)")
        if mv.dimension_for_role("date") is not None and mv.dimension_for_role("date").type != "date":
            errors.append(f"metric view {mv.name}: the date dimension must have type date")

    for m in layer.metrics.values():
        if m.metric_view not in layer.metric_views:
            errors.append(f"metric {m.name}: metric view {m.metric_view} is not defined")
            continue
        if not m.description or not m.expr or not m.source_rationale:
            errors.append(f"metric {m.name}: needs description, expr and source_rationale")
        if m.format not in FORMATS:
            errors.append(f"metric {m.name}: unknown format {m.format}")
        if m.name in {d.name for d in layer.metric_views[m.metric_view].dimensions}:
            errors.append(f"metric {m.name}: has the same name as a dimension of {m.metric_view}")
        if _PLACEHOLDER.search(m.expr):
            errors.append(f"metric {m.name}: placeholders aren't allowed in measures")
        join_names = {j.name for j in layer.metric_views[m.metric_view].joins}
        for ref in referenced_joins(m.expr):
            if ref not in join_names:
                errors.append(f"metric {m.name}: reads {ref}.*, which is not a join of {m.metric_view}")

    if errors:
        raise SemanticLayerError("semantic_layer.yaml is inconsistent:\n  " + "\n  ".join(errors))


def referenced_joins(expr: str) -> List[str]:
    """Join names an expression reads (`batter.primary_name` -> batter); `source` is the source view."""
    stripped = re.sub(r"'(?:[^'\\]|\\.)*'", "''", _PLACEHOLDER.sub("", expr))
    return sorted({m for m in _JOIN_REF.findall(stripped) if m != "source"})


def resolve_path(path: Path | str | None = None) -> Path:
    if path is not None:
        return Path(path)
    if DEFAULT_PATH.exists():
        return DEFAULT_PATH
    return _REPO_ROOT / DEFAULT_PATH


def load_raw(path: Path | str | None = None) -> Dict[str, Any]:
    with resolve_path(path).open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


@lru_cache(maxsize=4)
def _load_cached(path: str, mtime: float) -> SemanticLayer:
    return parse(load_raw(path))


def load(path: Path | str | None = None) -> SemanticLayer:
    """The validated semantic layer (cached until the file changes)."""
    p = resolve_path(path)
    return _load_cached(str(p), p.stat().st_mtime)
