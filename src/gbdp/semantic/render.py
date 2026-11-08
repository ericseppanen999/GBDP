"""Expand the semantic layer's SQL placeholders for one engine.

The same view and metric SQL runs on local DuckDB (parquet gold), local Spark
(delta gold) and Databricks (Unity Catalog). They differ in three ways that
matter here: how a table is named, what a column whose name starts with a
digit is called (Unity Catalog's managed copies rename 2b to c_2b), and how a
string literal escapes a quote.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Iterable

from gbdp.semantic.layer import SemanticLayer, SemanticLayerError

_PLACEHOLDER = re.compile(r"\{\{(\w+):([^}]*)\}\}")
_PLAIN_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def uc_managed_column_name(name: str) -> str:
    """The name catalog/managed_publish.py gives a column in a managed UC table."""
    from gbdp.catalog.managed_publish import uc_column_name

    return uc_column_name(name)


def _same_name(name: str) -> str:
    return name


@dataclass(frozen=True)
class Target:
    name: str
    table_prefix: str = ""
    view_prefix: str = ""
    quote_char: str = '"'
    backslash_escapes: bool = False
    column_name: Callable[[str], str] = _same_name
    # inline: views are CTEs named by bare view name (see compile.py)
    inline_views: bool = False

    def ident(self, name: str) -> str:
        physical = self.column_name(name)
        if _PLAIN_IDENT.match(physical):
            return physical
        q = self.quote_char
        return f"{q}{physical.replace(q, q + q)}{q}"

    def string(self, value: Any) -> str:
        s = str(value)
        if self.backslash_escapes:
            return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"
        return "'" + s.replace("'", "''") + "'"

    def table(self, name: str) -> str:
        return f"{self.table_prefix}{name}"

    def view(self, name: str) -> str:
        return name if self.inline_views else f"{self.view_prefix}{name}"


def duckdb_target() -> Target:
    """Local parquet gold through DuckDB: tables and views registered by bare name."""
    return Target(name="duckdb")


def spark_local_target() -> Target:
    """Local delta gold through Spark temp views: original column names, backtick quoting."""
    return Target(name="spark", quote_char="`", backslash_escapes=True)


def databricks_target(catalog: str = "gbdp", schema: str = "gold_gbdp", uc_mode: str = "managed",
                      inline_views: bool = False) -> Target:
    """Unity Catalog. Managed tables (the bundle's mode) carry managed_publish's renamed columns."""
    prefix = f"{catalog}.{schema}."
    rename = uc_managed_column_name if uc_mode in ("managed", "serverless") else _same_name
    return Target(name="databricks", table_prefix=prefix, view_prefix=prefix, quote_char="`",
                  backslash_escapes=True, column_name=rename, inline_views=inline_views)


def in_list(values: Iterable[Any], target: Target) -> str:
    vals = list(values)
    if not vals:
        raise SemanticLayerError("an IN-list needs at least one value")
    return "(" + ", ".join(target.string(v) for v in vals) + ")"


def render(sql: str, layer: SemanticLayer, target: Target) -> str:
    """Expand every {{kind:arg}} placeholder in sql for target."""

    def sub(match: "re.Match[str]") -> str:
        kind, arg = match.group(1), match.group(2)
        if kind == "table":
            return target.table(arg)
        if kind == "view":
            return target.view(arg)
        if kind == "col":
            return target.ident(arg)
        if kind == "list":
            vocab, _, group = arg.partition(".")
            v = layer.vocabularies[vocab]
            return in_list(v.groups[group] if group else v.values, target)
        if kind == "league_of":
            expr = render(arg, layer, target)
            mapping = layer.vocabularies["source_league"].values
            whens = " ".join(f"WHEN {target.string(s)} THEN {target.string(lg)}" for s, lg in mapping.items())
            return f"CASE {expr} {whens} END"
        if kind == "label":
            vocab, _, expr = arg.partition(":")
            expr = render(expr, layer, target)
            values = layer.vocabularies[vocab].values
            whens = " ".join(f"WHEN {_literal_key(k, target)} THEN {target.string(lbl)}" for k, lbl in values.items())
            return f"CASE {expr} {whens} ELSE CAST({expr} AS STRING) END"
        if kind == "season_phase":
            type_expr, _, source_expr = arg.partition(":")
            type_expr, source_expr = render(type_expr, layer, target), render(source_expr, layer, target)
            whens = [f"WHEN {type_expr} IS NULL THEN 'unknown'"]
            for rule in layer.vocabularies["season_phase"].rules:
                tests = []
                if rule.get("values"):
                    tests.append(f"{type_expr} IN {in_list(rule['values'], target)}")
                tests += [f"{type_expr} LIKE {target.string('%' + sfx)}" for sfx in rule.get("suffixes") or ()]
                whens.append(f"WHEN {source_expr} = {target.string(rule['source'])} AND ({' OR '.join(tests)}) "
                             f"THEN {target.string(rule['phase'])}")
            return "CASE " + " ".join(whens) + " ELSE 'other' END"
        if kind == "final":
            expr = render(arg, layer, target)
            v = layer.vocabularies["final_status"]
            parts = [f"{expr} IN {in_list(v.values, target)}"]
            parts += [f"{expr} LIKE {target.string(p + '%')}" for p in v.prefixes]
            return "(" + " OR ".join(parts) + ")"
        raise SemanticLayerError(f"unknown placeholder {{{{{kind}:{arg}}}}}")

    return _PLACEHOLDER.sub(sub, sql)


def _literal_key(key: Any, target: Target) -> str:
    if isinstance(key, bool):
        return "TRUE" if key else "FALSE"
    if isinstance(key, int):
        return str(key)
    return target.string(key)
