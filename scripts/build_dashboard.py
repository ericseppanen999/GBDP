"""Generate dashboards/gbdp_season.lvdash.json, the Databricks AI/BI dashboard.

The SQL lives here so it can be reviewed and tested like code; the bundle
(databricks.yml, resources.dashboards) deploys the generated file.

    python scripts/build_dashboard.py           # write the dashboard file
    python scripts/build_dashboard.py --check   # run every dataset on the warehouse
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

G = "gbdp.gold_gbdp"
CUR_PLAYER = f"(SELECT max(dt) FROM {G}.dim_player)"
ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "dashboards" / "gbdp_season.lvdash.json"

sys.path.insert(0, str(ROOT / "src"))
from gbdp.semantic import layer as semantic  # noqa: E402
from gbdp.semantic.compile import Filter, MetricRequest, SqlExpr, compile_request  # noqa: E402
from gbdp.semantic.render import databricks_target  # noqa: E402

# The season the dashboard shows: the latest year with an MLB regular-season
# game, so it rolls over when next season's regular season starts (spring
# training alone doesn't). Gold's test days from other years (2004-05-18,
# 2024-05-18) are older, so max() skips them. Needs fact_game.game_type, which
# the gold rebuild adds; --check says so when it is missing.
SEASON_SQL = (f"SELECT max(YEAR(CAST(sg.dt AS DATE))) FROM {G}.fact_game sg "
              f"WHERE sg.source = 'mlb_statsapi' AND sg.game_type = 'R'")
SEASON = SqlExpr(SEASON_SQL)
# Every count on the season page reads only that season's partitions.
IN_SEASON = f"YEAR(CAST(dt AS DATE)) = ({SEASON_SQL})"

DATASETS = {
    "totals": ("Totals", f"""
SELECT
  CAST(({SEASON_SQL}) AS STRING) AS season,
  (SELECT count(DISTINCT game_id) FROM {G}.fact_game WHERE {IN_SEASON}) AS games,
  (SELECT count(*) FROM {G}.fact_plate_appearance WHERE {IN_SEASON}) AS plate_appearances,
  (SELECT count(*) FROM {G}.fact_pitch WHERE {IN_SEASON}) AS pitches,
  (SELECT count(*) FROM {G}.dim_player WHERE is_current AND dt = {CUR_PLAYER}) AS players,
  (SELECT count(DISTINCT dt) FROM {G}.fact_game WHERE {IN_SEASON}) AS days_loaded,
  (SELECT max(dt) FROM {G}.fact_game WHERE {IN_SEASON}) AS latest_day"""),

    "daily_coverage": ("Daily coverage", f"""
SELECT CAST(dt AS DATE) AS day,
       CASE source WHEN 'mlb_statcast' THEN 'MLB (Statcast)' WHEN 'npb_spaia' THEN 'NPB' ELSE source END AS feed,
       count(*) AS plate_appearances
FROM {G}.fact_plate_appearance
WHERE {IN_SEASON}
GROUP BY ALL"""),

    # Metric datasets: compiled from configs/semantic_layer.yaml (see
    # METRIC_DATASETS below), so they can't disagree with the API or Genie.
    "quality": ("Quality check results", f"""
SELECT CAST(dt AS DATE) AS day, `check`, coalesce(`table`, '') AS table_name,
       CASE WHEN ok THEN 'pass' ELSE 'fail' END AS result
FROM {G}.audit_quality
WHERE {IN_SEASON}"""),
}


def metric_sql(request: MetricRequest) -> str:
    """One dataset's SQL, compiled from the semantic layer's metric definitions.

    The views are inlined as CTEs over the gold tables, so the dashboard
    doesn't depend on the v_* / mv_* views having been deployed.
    """
    sl = semantic.load(ROOT / "configs" / "semantic_layer.yaml")
    return compile_request(sl, request, databricks_target(inline_views=True), param_style="literal").sql


MLB_SEASON = [Filter("league_code", "eq", "MLB"), Filter("season_year", "eq", SEASON)]


def metric_datasets() -> dict:
    re_matrix = metric_sql(MetricRequest(
        metrics=["exp_runs", "re_pa_count"], dimensions=["base_state", "runners", "outs"], filters=MLB_SEASON))
    return {
        # Season-to-date RE24, summed from run_expectancy_daily.
        "run_expectancy": ("Run expectancy (MLB, season to date)", f"""
SELECT runners,
       max(CASE WHEN outs = 0 THEN exp_runs END) AS outs_0,
       max(CASE WHEN outs = 1 THEN exp_runs END) AS outs_1,
       max(CASE WHEN outs = 2 THEN exp_runs END) AS outs_2,
       sum(re_pa_count) AS plate_appearances,
       base_state
FROM (
{re_matrix}
) matrix
GROUP BY runners, base_state
ORDER BY base_state"""),
        "pitch_mix": ("Pitch mix and velocity (MLB)", metric_sql(MetricRequest(
            metrics=["pitches", "avg_velocity", "max_velocity"], dimensions=["pitch_type_name"],
            # v_pitch turns '' into null, so not_null drops empty pitch types too.
            filters=[Filter("pitch_type", "not_null"), Filter("season_year", "eq", SEASON)],
            having=[("pitches", "gte", 50)], order_by=[("pitches", "desc")],
            aliases={"pitch_type_name": "pitch", "avg_velocity": "avg_mph", "max_velocity": "max_mph"}))),
        # PA and AVG from the governed is_pa / is_ab flags: TRUNCATED rows
        # aren't PAs; INT, BB, HBP, SAC and UNKNOWN aren't at-bats.
        "hitters": ("MLB hitters", metric_sql(MetricRequest(
            metrics=["pa", "home_runs", "hits", "walks", "strikeouts", "avg"],
            dimensions=["batter_id", "batter_name"],
            filters=MLB_SEASON + [Filter("data_source", "eq", "mlb_statcast")],
            having=[("pa", "gte", 20)], order_by=[("hr", "desc"), ("pa", "desc")], limit=25,
            aliases={"batter_name": "batter", "home_runs": "hr", "walks": "bb", "strikeouts": "k"}))),
        # Regular season only (season_phase from fact_game.game_type).
        "standings": ("MLB records from games", metric_sql(MetricRequest(
            metrics=["wins", "losses", "win_pct", "run_differential"], dimensions=["team_id", "team_name"],
            filters=MLB_SEASON + [Filter("data_source", "eq", "mlb_statsapi"),
                                  Filter("season_phase", "eq", "regular")],
            order_by=[("pct", "desc"), ("run_diff", "desc")],
            aliases={"team_name": "team", "wins": "w", "losses": "l", "win_pct": "pct",
                     "run_differential": "run_diff"}))),
    }


def all_datasets() -> dict:
    out = dict(DATASETS)
    out.update(metric_datasets())
    return out


def _field(name: str, expr: str | None = None) -> dict:
    return {"name": name, "expression": expr or f"`{name}`"}


def _query(dataset: str, fields: list, disaggregated: bool) -> list:
    return [{"name": "main_query", "query": {"datasetName": dataset, "fields": fields, "disaggregated": disaggregated}}]


def _frame(title: str, description: str | None = None) -> dict:
    frame = {"showTitle": True, "title": title}
    if description:
        frame.update(showDescription=True, description=description)
    return frame


def text(name: str, lines: list, x: int, y: int, w: int, h: int) -> dict:
    return {"widget": {"name": name, "multilineTextboxSpec": {"lines": lines}},
            "position": {"x": x, "y": y, "width": w, "height": h}}


def counter(name: str, dataset: str, column: str, title: str, x: int, y: int, w: int = 1, h: int = 2) -> dict:
    return {"widget": {"name": name, "queries": _query(dataset, [_field(column)], True),
                       "spec": {"version": 2, "widgetType": "counter",
                                "encodings": {"value": {"fieldName": column, "displayName": title}},
                                "frame": _frame(title)}},
            "position": {"x": x, "y": y, "width": w, "height": h}}


def table(name: str, dataset: str, columns: list, title: str, x: int, y: int, w: int, h: int,
          description: str | None = None) -> dict:
    """columns: (field, display name, 'string' | 'integer' | 'float')."""
    # Lakeview rejects a table column without its full display spec (the
    # three tables rendered "invalid widget" with only name/type/order), so
    # every column carries the properties Lakeview itself exports.
    number_format = {"integer": "0", "float": "0.000"}
    cols = []
    for i, (field, display, kind) in enumerate(columns):
        col = {
            "fieldName": field, "displayName": display, "title": display,
            "type": kind, "displayAs": "string" if kind == "string" else "number",
            "visible": True, "order": 100000 + i,
            "alignContent": "left" if kind == "string" else "right",
            "booleanValues": ["false", "true"],
            "imageUrlTemplate": "{{ @ }}", "imageTitleTemplate": "{{ @ }}", "imageWidth": "", "imageHeight": "",
            "linkUrlTemplate": "{{ @ }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}",
            "linkOpenInNewTab": True,
            "allowSearch": kind == "string", "allowHTML": False, "highlightLinks": False,
            "useMonospaceFont": False, "preserveWhitespace": False,
        }
        if kind in number_format:
            col["numberFormat"] = number_format[kind]
        cols.append(col)
    return {"widget": {"name": name, "queries": _query(dataset, [_field(c[0]) for c in columns], True),
                       "spec": {"version": 1, "widgetType": "table",
                                "encodings": {"columns": cols},
                                "invisibleColumns": [], "allowHTMLByDefault": False,
                                "itemsPerPage": 25, "paginationSize": "default",
                                "condensed": True, "withRowNumber": False,
                                "frame": _frame(title, description)}},
            "position": {"x": x, "y": y, "width": w, "height": h}}


def chart(name: str, kind: str, dataset: str, x_field: str, x_scale: str, y_field: str, y_agg: str,
          title: str, x: int, y: int, w: int, h: int, color: str | None = None,
          x_title: str | None = None, y_title: str | None = None, description: str | None = None) -> dict:
    y_name = f"{y_agg.lower()}({y_field})"
    # A temporal axis takes a time-binned field, the way Lakeview writes its own
    # charts; a raw date there makes the widget invalid.
    if x_scale == "temporal":
        x_name, x_expr = f"daily({x_field})", f'DATE_TRUNC("DAY", `{x_field}`)'
    else:
        x_name, x_expr = x_field, None
    fields = [_field(x_name, x_expr), _field(y_name, f"{y_agg.upper()}(`{y_field}`)")]
    encodings = {
        "x": {"fieldName": x_name, "scale": {"type": x_scale}, "displayName": x_title or x_field},
        "y": {"fieldName": y_name, "scale": {"type": "quantitative"}, "displayName": y_title or y_field},
    }
    if color:
        fields.append(_field(color))
        encodings["color"] = {"fieldName": color, "scale": {"type": "categorical"}, "displayName": color}
    return {"widget": {"name": name, "queries": _query(dataset, fields, False),
                       "spec": {"version": 3, "widgetType": kind, "encodings": encodings,
                                "frame": _frame(title, description)}},
            "position": {"x": x, "y": y, "width": w, "height": h}}


def build() -> dict:
    overview = [
        # Lakeview text widgets and page names are static, so the season itself
        # is a counter next to the title (a string, so it isn't shown as 2,026).
        text("title", ["# Global Baseball Data Platform: season to date",
                       "MLB and NPB from public feeds, rebuilt nightly on Databricks (bronze → silver → gold, Delta + Unity Catalog). "
                       "The season is the latest year with MLB regular-season games."],
             0, 0, 5, 2),
        counter("c_season", "totals", "season", "Season", 5, 0),
        counter("c_games", "totals", "games", "Games", 0, 2),
        counter("c_pas", "totals", "plate_appearances", "Plate appearances", 1, 2),
        counter("c_pitches", "totals", "pitches", "Pitches", 2, 2),
        counter("c_players", "totals", "players", "Players", 3, 2),
        counter("c_days", "totals", "days_loaded", "Days loaded", 4, 2),
        counter("c_latest", "totals", "latest_day", "Latest day", 5, 2),
        table("re24", "run_expectancy",
              [("runners", "Runners", "string"), ("outs_0", "0 outs", "float"), ("outs_1", "1 out", "float"),
               ("outs_2", "2 outs", "float"), ("plate_appearances", "PAs", "integer")],
              "Run expectancy (RE24), MLB season to date", 0, 4, 3, 6,
              "Average runs scored from each base/out state to the end of the inning."),
        table("standings", "standings",
              [("team", "Team", "string"), ("w", "W", "integer"), ("l", "L", "integer"),
               ("pct", "Pct", "float"), ("run_diff", "Run diff", "integer")],
              "MLB records, derived from game results", 3, 4, 3, 10),
        chart("pitch_velo", "bar", "pitch_mix", "pitch", "categorical", "avg_mph", "avg",
              "Average velocity by pitch type (MLB, Statcast)", 0, 10, 3, 6,
              x_title="Pitch", y_title="Avg mph"),
        chart("coverage", "line", "daily_coverage", "day", "temporal", "plate_appearances", "sum",
              "Plate appearances loaded per day", 0, 16, 6, 5, color="feed",
              x_title="Day", y_title="Plate appearances",
              description="One line per feed; gaps are days the source had no games or were not loaded."),
        table("hitters", "hitters",
              [("batter", "Batter", "string"), ("pa", "PA", "integer"), ("hr", "HR", "integer"),
               ("hits", "H", "integer"), ("bb", "BB", "integer"), ("k", "K", "integer"), ("avg", "AVG", "float")],
              "MLB home run leaders (min 20 PA), from Statcast plate appearances", 0, 21, 6, 8),
    ]
    quality = [
        text("q_title", ["# Data quality",
                         "Every gold publish runs these checks; hard failures fail the job. Results come from `gold_gbdp.audit_quality`."],
             0, 0, 6, 2),
        chart("q_by_check", "bar", "quality", "check", "categorical", "day", "count",
              "Check results by check", 0, 2, 3, 6, color="result", x_title="Check", y_title="Results"),
        chart("q_by_day", "bar", "quality", "day", "temporal", "check", "count",
              "Check results by day", 3, 2, 3, 6, color="result", x_title="Day", y_title="Results"),
        table("q_table", "quality",
              [("day", "Day", "string"), ("check", "Check", "string"), ("table_name", "Table", "string"),
               ("result", "Result", "string")],
              "All check results", 0, 8, 6, 8),
    ]
    return {
        "datasets": [{"name": k, "displayName": v[0], "queryLines": [line + "\n" for line in v[1].strip().splitlines()]}
                     for k, v in all_datasets().items()],
        "pages": [
            {"name": "season", "displayName": "Season", "layout": overview},
            {"name": "quality", "displayName": "Data quality", "layout": quality},
        ],
    }


def check() -> int:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from verify_lakehouse import Warehouse  # noqa: E402
    import os

    wh = Warehouse(os.getenv("GBDP_WAREHOUSE_ID", "a9d3d2589a30ab67"), os.getenv("DATABRICKS_CONFIG_PROFILE"))
    # Every season-page dataset picks its season from fact_game.game_type, so
    # before the gold rebuild adds it they would all fail the same way.
    schema = G.split(".")
    has_game_type = wh.query(
        f"SELECT count(*) FROM {schema[0]}.information_schema.columns WHERE table_schema = '{schema[1]}' "
        f"AND table_name = 'fact_game' AND column_name = 'game_type'")
    if not has_game_type or str(has_game_type[0][0]) == "0":
        print(f"FAIL  {G}.fact_game has no game_type yet: rebuild gold first "
              "(the dashboard's season and season phases come from it)")
        return 1
    print(f"OK    season: {wh.query(SEASON_SQL)[0][0]}")
    failed = 0
    for name, (_, sql) in all_datasets().items():
        try:
            rows = wh.query(sql)
            print(f"OK    {name}: {len(rows)} row(s)")
        except RuntimeError as e:
            failed += 1
            print(f"FAIL  {name}: {str(e)[:200]}")
    return 1 if failed else 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--check", action="store_true", help="run every dataset on the SQL warehouse")
    args = ap.parse_args()
    if args.check:
        return check()
    OUT.parent.mkdir(exist_ok=True)
    # "\n" on every platform: a Windows regen must not rewrite every line as CRLF.
    OUT.write_text(json.dumps(build(), indent=2) + "\n", encoding="utf-8", newline="\n")
    print(f"wrote {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
