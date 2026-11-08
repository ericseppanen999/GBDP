"""Unity Catalog DDL from the semantic layer, and applying it.

Three kinds of statement, in this order:
  1. COMMENT ON TABLE / ALTER COLUMN ... COMMENT for every described table and
     column, so Catalog Explorer and Genie show the descriptions.
  2. CREATE OR REPLACE VIEW for each plain view (v_*).
  3. CREATE OR REPLACE VIEW ... WITH METRICS LANGUAGE YAML for each metric view
     (mv_*), Databricks metric view spec version 1.1.

`statements()` is pure and deterministic (scripts/build_semantic_layer.py
writes it to docs/semantic_layer_uc.sql, and a test checks the file is
current). `apply()` runs it through Spark, skipping comments on tables or
columns that don't exist yet and reporting every failure at the end.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import yaml

from gbdp.semantic.layer import SemanticLayer
from gbdp.semantic.render import Target, databricks_target, render

METRIC_VIEW_SPEC_VERSION = 1.1  # a YAML number, as in the Databricks examples
_COMMENT_MAX = 1024  # keep comments readable in Catalog Explorer


@dataclass(frozen=True)
class Statement:
    kind: str  # table_comment | column_comment | view | metric_view
    table: str  # the object it creates or comments on (bare name)
    sql: str
    column: Optional[str] = None  # the physical column, for column_comment


def _clip(text: str) -> str:
    text = " ".join(text.split())
    return text if len(text) <= _COMMENT_MAX else text[: _COMMENT_MAX - 3].rstrip() + "..."


def table_comment(layer: SemanticLayer, name: str) -> str:
    t = layer.tables[name]
    parts = [t.description, f"Grain: {t.grain}."]
    if t.sources:
        parts.append("Sources: " + ", ".join(t.sources) + ".")
    parts += [f"Caveat: {c}" for c in t.caveats]
    parts.append("Semantic layer: configs/semantic_layer.yaml.")
    return _clip(" ".join(parts))


def column_comment(layer: SemanticLayer, table: str, column: str) -> str:
    t = layer.tables[table]
    text = t.columns[column]
    fk = t.foreign_keys.get(column)
    if fk and fk.split(".")[0] not in text:
        text = f"{text} FK to {fk}."
    return _clip(text)


def metric_view_yaml(layer: SemanticLayer, name: str, target: Target) -> str:
    mv = layer.metric_views[name]
    doc: Dict[str, Any] = {
        "version": METRIC_VIEW_SPEC_VERSION,
        "comment": _clip(mv.description + " Defined in configs/semantic_layer.yaml."),
        "source": target.view(mv.source),
    }
    if mv.filter:
        doc["filter"] = render(mv.filter, layer, target)
    if mv.joins:
        doc["joins"] = [{"name": j.name, "source": target.view(j.source), "on": render(j.on, layer, target)}
                        for j in mv.joins]
    doc["dimensions"] = [{"name": d.name, "expr": render(d.expr, layer, target), "comment": _clip(d.description)}
                         for d in mv.dimensions]
    measures = []
    for m in layer.metrics.values():
        if m.metric_view != name:
            continue
        comment = m.description
        if m.caveats:
            comment += " Caveats: " + " ".join(m.caveats)
        measures.append({"name": m.name, "expr": m.expr, "comment": _clip(comment)})
    doc["measures"] = measures
    text = yaml.safe_dump(doc, sort_keys=False, allow_unicode=True, width=1_000_000, default_flow_style=False)
    if "$$" in text:
        raise ValueError(f"metric view {name}: its YAML contains $$, which would end the SQL body")
    return text


def statements(layer: SemanticLayer, catalog: str = "gbdp", schema: str = "gold_gbdp",
               uc_mode: str = "managed") -> List[Statement]:
    target = databricks_target(catalog, schema, uc_mode)
    out: List[Statement] = []
    for name, t in layer.tables.items():
        if t.placeholder:
            continue  # never written, so it has no Unity Catalog table
        fq = target.table(name)
        out.append(Statement("table_comment", name, f"COMMENT ON TABLE {fq} IS {target.string(table_comment(layer, name))}"))
        for col in t.columns:
            physical = target.column_name(col)
            out.append(Statement(
                "column_comment", name,
                f"ALTER TABLE {fq} ALTER COLUMN `{physical}` COMMENT {target.string(column_comment(layer, name, col))}",
                column=physical,
            ))
    for name, v in layer.views.items():
        body = render(v.sql, layer, target)
        out.append(Statement(
            "view", name,
            f"CREATE OR REPLACE VIEW {target.view(name)}\nCOMMENT {target.string(_clip(v.description))}\nAS\n{body}",
        ))
    for name in layer.metric_views:
        out.append(Statement(
            "metric_view", name,
            f"CREATE OR REPLACE VIEW {target.view(name)}\nWITH METRICS\nLANGUAGE YAML\nAS $$\n"
            f"{metric_view_yaml(layer, name, target)}$$",
        ))
    return out


def sql_script(layer: SemanticLayer, catalog: str = "gbdp", schema: str = "gold_gbdp", uc_mode: str = "managed") -> str:
    header = (
        "-- GENERATED by scripts/build_semantic_layer.py from configs/semantic_layer.yaml. Do not edit.\n"
        f"-- Unity Catalog: {catalog}.{schema} ({uc_mode} tables). Applied by `gbdp semantic-layer`\n"
        "-- (the gbdp_semantic_layer task in databricks.yml), or paste into the SQL editor.\n"
        "-- Comments on tables or columns that don't exist yet fail here; `gbdp semantic-layer` skips them.\n"
    )
    body = ";\n\n".join(s.sql for s in statements(layer, catalog, schema, uc_mode))
    return header + "\n" + body + ";\n"


@dataclass
class ApplyResult:
    applied: List[str] = field(default_factory=list)
    skipped: List[str] = field(default_factory=list)
    failed: List[str] = field(default_factory=list)


def apply(spark, layer: SemanticLayer, catalog: str, schema: str, uc_mode: str = "managed",
          logger=None) -> ApplyResult:
    """Run statements() on spark. Never raises for one statement; see ApplyResult.failed."""
    result = ApplyResult()
    columns: Dict[str, Optional[set]] = {}

    def existing_columns(table: str) -> Optional[set]:
        if table not in columns:
            fq = f"{catalog}.{schema}.{table}"
            try:
                columns[table] = set(spark.table(fq).columns) if spark.catalog.tableExists(fq) else None
            except Exception:
                columns[table] = None
        return columns[table]

    for s in statements(layer, catalog, schema, uc_mode):
        label = f"{s.kind} {s.table}" + (f".{s.column}" if s.column else "")
        if s.kind in ("table_comment", "column_comment"):
            cols = existing_columns(s.table)
            if cols is None:
                result.skipped.append(f"{label}: table does not exist yet")
                continue
            if s.column is not None and s.column not in cols:
                result.skipped.append(f"{label}: column does not exist yet")
                continue
        try:
            spark.sql(s.sql)
            result.applied.append(label)
        except Exception as exc:  # report them all, not just the first
            msg = str(exc).strip().splitlines()[0][:300] if str(exc).strip() else type(exc).__name__
            result.failed.append(f"{label}: {msg}")
            if logger:
                logger.warning("semantic layer: %s failed: %s", label, msg)
    if logger:
        logger.info("semantic layer: %d applied, %d skipped, %d failed",
                    len(result.applied), len(result.skipped), len(result.failed))
        for s in result.skipped:
            logger.info("semantic layer: skipped %s", s)
    return result
