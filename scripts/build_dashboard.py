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
CUR_TEAM = f"(SELECT max(dt) FROM {G}.dim_team)"
OUT = Path(__file__).resolve().parent.parent / "dashboards" / "gbdp_season.lvdash.json"

DATASETS = {
    "totals": ("Totals", f"""
SELECT
  (SELECT count(DISTINCT game_id) FROM {G}.fact_game) AS games,
  (SELECT count(*) FROM {G}.fact_plate_appearance) AS plate_appearances,
  (SELECT count(*) FROM {G}.fact_pitch) AS pitches,
  (SELECT count(*) FROM {G}.dim_player WHERE is_current AND dt = {CUR_PLAYER}) AS players,
  (SELECT count(DISTINCT dt) FROM {G}.fact_game) AS days_loaded,
  (SELECT max(dt) FROM {G}.fact_game) AS latest_day"""),

    "daily_coverage": ("Daily coverage", f"""
SELECT CAST(dt AS DATE) AS day,
       CASE source WHEN 'mlb_statcast' THEN 'MLB (Statcast)' WHEN 'npb_spaia' THEN 'NPB' ELSE source END AS feed,
       count(*) AS plate_appearances
FROM {G}.fact_plate_appearance
WHERE dt >= '2026-01-01'
GROUP BY ALL"""),

    "run_expectancy": ("Run expectancy (MLB, season to date)", f"""
WITH latest AS (
  SELECT max(dt) AS d FROM {G}.run_expectancy WHERE league_code = 'MLB' AND season_year = 2026
)
SELECT
  CASE base_state WHEN 0 THEN 'Bases empty' WHEN 1 THEN '1st' WHEN 2 THEN '2nd' WHEN 3 THEN '1st & 2nd'
                  WHEN 4 THEN '3rd' WHEN 5 THEN '1st & 3rd' WHEN 6 THEN '2nd & 3rd' ELSE 'Loaded' END AS runners,
  max(CASE WHEN outs = 0 THEN exp_runs END) AS outs_0,
  max(CASE WHEN outs = 1 THEN exp_runs END) AS outs_1,
  max(CASE WHEN outs = 2 THEN exp_runs END) AS outs_2,
  sum(pa_count) AS plate_appearances,
  base_state
FROM {G}.run_expectancy, latest
WHERE league_code = 'MLB' AND season_year = 2026 AND dt = latest.d
GROUP BY base_state
ORDER BY base_state"""),

    "pitch_mix": ("Pitch mix and velocity (MLB)", f"""
SELECT CASE pitch_type WHEN 'FF' THEN '4-seam fastball' WHEN 'SI' THEN 'Sinker' WHEN 'FC' THEN 'Cutter'
         WHEN 'SL' THEN 'Slider' WHEN 'ST' THEN 'Sweeper' WHEN 'CU' THEN 'Curveball' WHEN 'KC' THEN 'Knuckle curve'
         WHEN 'CH' THEN 'Changeup' WHEN 'FS' THEN 'Splitter' WHEN 'SV' THEN 'Slurve' ELSE pitch_type END AS pitch,
       count(*) AS pitches,
       round(avg(release_speed), 1) AS avg_mph,
       round(max(release_speed), 1) AS max_mph
FROM {G}.fact_pitch
WHERE pitch_type IS NOT NULL AND pitch_type <> '' AND release_speed IS NOT NULL
GROUP BY 1
HAVING count(*) >= 50
ORDER BY pitches DESC"""),

    "hitters": ("MLB hitters", f"""
WITH p AS (
  SELECT player_id, primary_name FROM {G}.dim_player WHERE is_current AND dt = {CUR_PLAYER}
)
SELECT p.primary_name AS batter,
       count(*) AS pa,
       count_if(event_type = 'HR') AS hr,
       count_if(event_type IN ('1B', '2B', '3B', 'HR')) AS hits,
       count_if(event_type = 'BB') AS bb,
       count_if(event_type = 'K') AS k,
       round(count_if(event_type IN ('1B', '2B', '3B', 'HR'))
             / nullif(count_if(event_type NOT IN ('BB', 'HBP', 'SF', 'SH')), 0), 3) AS avg
FROM {G}.fact_plate_appearance pa JOIN p ON p.player_id = pa.batter_id
WHERE pa.source = 'mlb_statcast'
GROUP BY p.primary_name
HAVING count(*) >= 20
ORDER BY hr DESC, pa DESC
LIMIT 25"""),

    "standings": ("MLB records from games", f"""
WITH g AS (
  SELECT DISTINCT game_id, home_team_id, away_team_id, home_score, away_score
  FROM {G}.fact_game
  WHERE source = 'mlb_statsapi' AND home_score IS NOT NULL AND away_score IS NOT NULL
    AND home_score <> away_score AND lower(status) LIKE 'final%'
), r AS (
  SELECT home_team_id AS team_id, home_score > away_score AS won, home_score AS rs, away_score AS ra FROM g
  UNION ALL
  SELECT away_team_id, away_score > home_score, away_score, home_score FROM g
), t AS (
  SELECT team_id, team_name FROM {G}.dim_team WHERE is_current AND dt = {CUR_TEAM}
)
SELECT t.team_name AS team, count_if(won) AS w, count_if(NOT won) AS l,
       round(count_if(won) / count(*), 3) AS pct, sum(rs) - sum(ra) AS run_diff
FROM r JOIN t ON t.team_id = r.team_id
GROUP BY t.team_name
ORDER BY pct DESC, run_diff DESC"""),

    "quality": ("Quality check results", f"""
SELECT CAST(dt AS DATE) AS day, `check`, coalesce(`table`, '') AS table_name,
       CASE WHEN ok THEN 'pass' ELSE 'fail' END AS result
FROM {G}.audit_quality
WHERE dt >= '2026-01-01'"""),
}


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
        text("title", ["# Global Baseball Data Platform: 2026 season",
                       "MLB and NPB from public feeds, rebuilt nightly on Databricks (bronze → silver → gold, Delta + Unity Catalog)."],
             0, 0, 6, 2),
        counter("c_games", "totals", "games", "Games", 0, 2),
        counter("c_pas", "totals", "plate_appearances", "Plate appearances", 1, 2),
        counter("c_pitches", "totals", "pitches", "Pitches", 2, 2),
        counter("c_players", "totals", "players", "Players", 3, 2),
        counter("c_days", "totals", "days_loaded", "Days loaded", 4, 2),
        counter("c_latest", "totals", "latest_day", "Latest day", 5, 2),
        table("re24", "run_expectancy",
              [("runners", "Runners", "string"), ("outs_0", "0 outs", "float"), ("outs_1", "1 out", "float"),
               ("outs_2", "2 outs", "float"), ("plate_appearances", "PAs", "integer")],
              "Run expectancy (RE24), MLB 2026 season to date", 0, 4, 3, 6,
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
                     for k, v in DATASETS.items()],
        "pages": [
            {"name": "season", "displayName": "2026 season", "layout": overview},
            {"name": "quality", "displayName": "Data quality", "layout": quality},
        ],
    }


def check() -> int:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from verify_lakehouse import Warehouse  # noqa: E402
    import os

    wh = Warehouse(os.getenv("GBDP_WAREHOUSE_ID", "a9d3d2589a30ab67"), os.getenv("DATABRICKS_CONFIG_PROFILE"))
    failed = 0
    for name, (_, sql) in DATASETS.items():
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
    OUT.write_text(json.dumps(build(), indent=2) + "\n", encoding="utf-8")
    print(f"wrote {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
