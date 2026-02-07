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

## 7) Where Data Appears in UC

After a successful run:
- `gbdp.bronze_gbdp.<source>_<entity>` (bronze parsed)
- `gbdp.silver_gbdp.<source>_<entity>`
- `gbdp.gold_gbdp.<table>`

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
