# GBDP Operations (Databricks + Local)

This document describes the end-to-end operational steps for running GBDP in Databricks, registering UC tables, and handling Retrosheet `plays.csv` shards.

## 1) Databricks Job Setup (Nightly + Backfill)

Use the job specs in:
- `configs/databricks_job.json` (nightly)
- `configs/databricks_backfill_job.json` (manual backfill)
- `configs/databricks_jobs.yml` (bundle)

If you are using **serverless** compute, you cannot set task env vars. Instead, pass UC settings as CLI params and use managed UC mode:
```
--uc-catalog gbdp
--uc-bronze-schema bronze_gbdp
--uc-silver-schema silver_gbdp
--uc-gold-schema gold_gbdp
--uc-mode managed
```

For non-serverless clusters, you can set these env vars instead:
```
GBDP_UC_CATALOG=gbdp
GBDP_UC_BRONZE_SCHEMA=bronze_gbdp
GBDP_UC_SILVER_SCHEMA=silver_gbdp
GBDP_UC_GOLD_SCHEMA=gold_gbdp
GBDP_UC_MODE=external
```

The pipeline runs the `register_uc` stage automatically. In **managed** mode it writes UC-managed tables; in **external** mode it registers LOCATION-based tables.

## 2) Required Volume Layout

Volumes (example):
```
dbfs:/Volumes/gbdp/bronze_gbdp/bronze_vol/gbdp/bronze
dbfs:/Volumes/gbdp/bronze_gbdp/bronze_vol/gbdp/manual
dbfs:/Volumes/gbdp/silver_gbdp/silver_vol/gbdp/silver
dbfs:/Volumes/gbdp/gold_gbdp/gold_vol/gbdp/gold
```

## 3) Retrosheet Manual Data

Retrosheet CSVs must be extracted into:
```
.../gbdp/manual/retrosheet/
```

Expected files:
```
allplayers.csv
gameinfo.csv
teamstats.csv
batting.csv
pitching.csv
fielding.csv
plays_part-00001.csv
plays_part-00002.csv
...
```

### Split `plays.csv` into 1GB parts
Use the local script:
```
python data/manual/split_plays.py
```

This reads `data/manual/plays.csv` and writes:
```
data/manual/plays_part-00001.csv
data/manual/plays_part-00002.csv
...
```

Then upload the parts into the volume under `manual/retrosheet/`.

## 4) KBO / LMB Local Data (Optional)

If you have manual JSON for KBO or LMB, place it under:
```
.../gbdp/manual/kbo/<entity>/dt=YYYY-MM-DD/*.json
.../gbdp/manual/lmb/<entity>/dt=YYYY-MM-DD/*.json
```

Supported entities:
- `games`
- `rosters`
- `boxscore_batting`
- `boxscore_pitching`

## 5) Running a Backfill (Databricks Job Parameters)

Recommended parameters:
```
["backfill",
 "--start","2004-05-18",
 "--end","2004-05-18",
 "--sources","configs/sources.yaml",
 "--force",
 "--storage-format","delta",
 "--bronze-root","dbfs:/Volumes/gbdp/bronze_gbdp/bronze_vol/gbdp/bronze",
 "--silver-root","dbfs:/Volumes/gbdp/silver_gbdp/silver_vol/gbdp/silver",
 "--gold-root","dbfs:/Volumes/gbdp/gold_gbdp/gold_vol/gbdp/gold",
 "--manual-root","dbfs:/Volumes/gbdp/bronze_gbdp/bronze_vol/gbdp/manual",
 "--retries","2",
 "--retry-delay","5",
 "--chunk","month",
 "--uc-catalog","gbdp",
 "--uc-bronze-schema","bronze_gbdp",
 "--uc-silver-schema","silver_gbdp",
 "--uc-gold-schema","gold_gbdp",
 "--uc-mode","managed"]
```

Optional league filter:
```
--leagues mlb,npb
```

ML tables only (`feature_batter_daily`, `ml_split`, `ml_breakout_examples`, `breakout_candidates`), over gold that is already published, e.g. after retraining the model or after a gold rebuild that predates the ML code. `gold_publish` writes them too, so this stage is never in the default list:
```
--stages ml_publish
```
Run it over the whole range in date order: labels are filled only once every day of their 14-day window is published.

## 6) Registering UC Tables Manually

If you need to register LOCATION-based tables outside the pipeline (non-serverless):
```
python src/gbdp/cli.py register-uc \
  --catalog gbdp \
  --bronze-schema bronze_gbdp \
  --silver-schema silver_gbdp \
  --gold-schema gold_gbdp \
  --storage-format delta \
  --bronze-root dbfs:/Volumes/gbdp/bronze_gbdp/bronze_vol/gbdp/bronze \
  --silver-root dbfs:/Volumes/gbdp/silver_gbdp/silver_vol/gbdp/silver \
  --gold-root dbfs:/Volumes/gbdp/gold_gbdp/gold_vol/gbdp/gold
```

Add `--semantic-layer` to apply the semantic layer to those tables afterwards (see 6b).

## 6b) Semantic Layer in UC (comments, views, metric views)

`configs/semantic_layer.yaml` becomes table/column COMMENTs, `v_*` views and
`mv_*` metric views in `gbdp.gold_gbdp`. It is not part of the nightly; run it
after a gold rebuild or a YAML change. Last applied to production on
2026-11-11, after the gold rebuild (validated first: 0 failures; applied: 311
statements, 0 failed, 12 skipped for `fact_standings`, which has no UC table yet):

1. Edit the YAML, then `python scripts/build_semantic_layer.py` and commit
   `docs/semantic_layer_uc.sql` (a test fails while it is stale).
2. Check it in a scratch schema first (creates views only in `gbdp.semantic_check`,
   reads the real tables, writes nothing in the gold schema):
   `python scripts/build_semantic_layer.py --validate-in semantic_check`,
   then `DROP SCHEMA gbdp.semantic_check CASCADE`. It ends with the game-type
   check, which also runs alone, read-only:
   `python scripts/build_semantic_layer.py --game-types` lists fact_game's
   game types per source and season phase and fails on any that map to
   `other` (those games drop out of the regular-season default record).
   It also fails while fact_plate_appearance has rows with a code listed in
   `vocabularies.event_type.legacy` (none since SAC was dropped, 2026-11-11).
3. `databricks bundle run gbdp_semantic_layer --target prod` (or
   `python dbx_entry.py semantic-layer --catalog gbdp --schema gold_gbdp` on a
   cluster). Comments on tables/columns that don't exist yet are skipped; any
   failed statement is listed and the job exits 1.

## 7) Where Data Appears in UC

After a successful run:
- `gbdp.bronze_gbdp.<source>_<entity>` (bronze parsed)
- `gbdp.silver_gbdp.<source>_<entity>`
- `gbdp.gold_gbdp.<table>`
- `gbdp.gold_gbdp.v_*` and `mv_*` once the semantic layer has been applied (6b)

## 7b) Serving the API from UC (cloud mode)

`python -m gbdp.cli serve` reads local gold by default. With `GBDP_SERVE_BACKEND=databricks` it answers the same endpoints from `gbdp.gold_gbdp.*` through a SQL warehouse (Statement Execution API, bearer token). Settings:

| Variable | Meaning | Default |
|---|---|---|
| `GBDP_SERVE_BACKEND` | `duckdb` or `databricks` | `duckdb` |
| `DATABRICKS_HOST`, `DATABRICKS_TOKEN` | workspace URL and token (environment only) | required |
| `GBDP_WAREHOUSE_ID` | SQL warehouse | `a9d3d2589a30ab67` |
| `GBDP_SERVE_CATALOG`, `GBDP_SERVE_SCHEMA` | where the gold tables are | `gbdp`, `gold_gbdp` |
| `GBDP_SERVE_UC_MODE` | `managed` renames columns like `2b` to `c_2b` | `managed` |
| `GBDP_SERVE_TIMEOUT_S` | cancel a statement after this long | `120` |
| `GBDP_API_TOKEN` | bearer token for `/query` and `/metrics/query` | unset |

Use a service principal or token with `USE CATALOG`/`USE SCHEMA`/`SELECT` on the gold schema only; `/query` is also guarded, wrapped in a LIMIT and limited to gold, but the grant is the real boundary. Semantic-layer views are inlined as CTEs, so the API does not need the `v_*` views applied in UC. The warehouse may cold-start (first request after idle takes longer). Smoke check: `python scripts/smoke_serve_cloud.py`.

## 8) Troubleshooting

If volumes look empty:
- Confirm job parameters use the **full subpaths** (`.../gbdp/bronze`, `.../gbdp/silver`, `.../gbdp/gold`).
- Confirm the run completed without errors.
- For serverless, ensure `--uc-mode managed` so tables are **written**, not just registered.

If you see DBFS I/O errors:
- Use the serverless-compatible code path (already implemented).
- Make sure paths use `dbfs:/Volumes/...` for Spark reads and write configuration.
If you see UC `LOCATION` errors on serverless:
- Use managed mode. Serverless does not allow LOCATION-based UC tables.
