# Global Baseball Data Platform (GBDP)

This repo implements the end-to-end Global Baseball Data Platform (GBDP) based on the TRD.
Initial scope:
- MLB Stats API (schedule, rosters, transactions)
- MLB Statcast (pitch/event CSV)
- NPB SPAIA endpoints (schedules, rosters, standings, stats, pbp, pitches)
- Indy (local file-based boxscore ingestion under `data/manual/indy/`)

Local dev output paths:
`data/bronze/...`

Run:
`python -m gbdp.cli run --start 2023-10-18 --end 2023-10-18`

Individual stages:
`python -m gbdp.cli ingest --source mlb_statsapi --entity schedule --start 2023-04-01 --end 2023-04-07`
`python -m gbdp.cli silver --source npb_spaia --entity game_pbp --start 2023-10-18 --end 2023-10-18`
`python -m gbdp.cli identity --start 2023-10-18 --end 2023-10-18`
`python -m gbdp.cli gold --start 2023-10-18 --end 2023-10-18`
`python -m gbdp.cli quality --start 2023-10-18 --end 2023-10-18`
`python -m gbdp.cli serve --host 0.0.0.0 --port 8000`

Backfill:
`python -m gbdp.cli backfill --start 1960-04-12 --end 1960-04-19 --force`

Nightly window (dt-1 with 7-day correction window):
`python -m gbdp.cli run --start 2000-01-01 --end 2000-01-01 --window nightly`

Storage format (Databricks Delta):
Set `GBDP_STORAGE_FORMAT=delta` (requires pyspark).
