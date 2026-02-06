# Global Baseball Data Platform (GBDP) — TRD (End-to-End)

**Document type:** Technical Requirements Document (TRD)  
**Cadence:** Nightly batch (no realtime)  
**Goal:** A reproducible, multi-league baseball lakehouse + APIs that supports analytics + ML (run expectancy, breakout modeling, projections).

---

## 1) System Goals and Non-Goals

### 1.1 Goals (REQUIRED)
1. **Nightly ingestion** for multiple leagues with immutable daily snapshots (`dt=YYYY-MM-DD`).
2. **Canonical data model** (players, teams, games, events) that works across leagues with different data granularity.
3. **Identity resolution** (cross-league player/team mapping) with deterministic rules + manual overrides.
4. **Bronze/Silver/Gold layers** with strong data contracts and idempotent pipelines.
5. **ML-ready gold outputs** (feature tables, labels, training splits) for:
   - run expectancy models
   - breakout prediction models
6. **Serving layer**: a read-only API to query canonical entities + derived metrics (and later serve models).

### 1.2 Non-Goals (REQUIRED)
- Intraday/minute-by-minute updates.
- Paid proprietary data providers (this system must rely on publicly accessible sources or user-supplied data).
- Betting integrations.

---

## 2) Architecture Overview

### 2.1 High-level components (REQUIRED)
1. **Source Connectors (Python)**  
   - MLB: Statcast + MLB Stats API (rosters/schedule/transactions)
   - NPB: SPAIA endpoints (schedule, rosters, stats, pitch-by-pitch / play-by-play)
   - Other leagues (KBO/LMB/Indy): boxscores, rosters, schedules via public endpoints or scraping

2. **Landing Storage (Object store)**  
   - Store raw payloads (JSON) + normalized raw tables (Parquet)
   - Partitioning by `source`, `entity`, and `dt`

3. **Lakehouse Storage (Delta Lake)**  
   - Bronze → Silver → Gold using Delta tables (or Iceberg if not on Databricks)
   - ACID merges for upserts and Type-2 SCD dims

4. **Orchestration**  
   - A nightly DAG with clear stage boundaries and idempotent outputs
   - Supports retries, backfills, and “rebuild from bronze”

5. **Data Quality + Observability**  
   - Assertions (row counts, referential integrity, duplicates)
   - Logging, metrics, and lineage (table-level)

6. **Serving/API layer**  
   - FastAPI read-only endpoints against Gold tables
   - Optional caching layer (Redis) for common queries (still nightly-refreshed)

### 2.2 Reference stack decision (REQUIRED)
The reference implementation MUST be deployable in two modes:

**Mode A: “Cloud Lakehouse” (primary)**
- Databricks + Delta Lake + Unity Catalog
- Databricks Jobs for orchestration
- Spark for transforms, plus Python for ingestion

**Mode B: “Local Dev” (required for iteration)**
- Parquet + DuckDB (or Spark local) + Python
- Same schemas and table layouts as cloud (paths mirror `bronze/silver/gold`)
- All transforms runnable from CLI without cloud credentials

> This dual-mode requirement prevents vendor lock-in while still looking “real” and scalable.

---

## 3) Environments, Naming, and Conventions

### 3.1 Environments (REQUIRED)
- `dev`: local filesystem / local object store emulator
- `staging`: cloud workspace, smaller backfill window, daily runs
- `prod`: full historical + nightly

### 3.2 Naming conventions (REQUIRED)
- All tables: `gbdp_<layer>.<table_name>` (if using catalogs/schemas)
- All paths: `/{layer}/{domain}/{table}/dt=YYYY-MM-DD/part-*.parquet`
- Snake_case column names, lowercase table names

### 3.3 Time model (REQUIRED)
- All timestamps stored in **UTC**.
- `game_date` stored as local-date of league (also store `game_start_ts_utc` when available).
- All Gold facts must include:
  - `dt` (snapshot date)
  - `source` (mlb_statcast / mlb_statsapi / npb_spaia / etc.)
  - `ingested_at_utc` (pipeline ingestion time)

---

## 4) Data Layers and Contracts

### 4.1 Bronze (REQUIRED)
**Purpose:** Raw source truth + replayability.

Bronze stores:
- Raw JSON payloads (exact responses) compressed (`.json.gz`)
- “Raw-parsed” parquet tables with minimal normalization (one-to-one with source entities)

**Bronze invariants (MUST):**
- Never mutate historical files for the same `dt` unless running a backfill with an explicit `--force` marker.
- Include `source_request` metadata:
  - endpoint URL
  - query params
  - response status
  - checksum/hash
  - fetched_at_utc

### 4.2 Silver (REQUIRED)
**Purpose:** Cleaned, normalized, deduped tables that map closely to domain entities.

Silver includes:
- Unified schema per entity (games/players/teams/rosters)
- Standardized enums
- Deduplication + canonical keys present (even before identity resolution is perfect)

### 4.3 Gold (REQUIRED)
**Purpose:** Analytics + ML-ready tables, stable keys, and derived metrics.

Gold includes:
- Canonical dimensions (SCD2 where needed)
- Event facts (pitch / play / PA depending on league)
- League-adjusted aggregates (rolling windows, park factors when possible, league translations when feasible)
- Feature tables and label tables for ML

---

## 5) Canonical IDs and Identity Resolution

### 5.1 Canonical ID strategy (REQUIRED)
Use **UUIDv7** (or ULID) as canonical IDs to preserve temporal locality.

Canonical IDs:
- `player_id` (UUIDv7)
- `team_id` (UUIDv7)
- `league_id` (static UUID per league)
- `game_id` (UUIDv7)
- `season_id` (UUIDv7)

### 5.2 Source ID mapping tables (REQUIRED)
Create bridge tables to map every source ID to canonical IDs.

**Gold table: `gbdp_gold.bridge_source_ids`**
- `entity_type` (player/team/game)
- `source` (mlb_statcast, npb_spaia, etc.)
- `source_id` (string)
- `canonical_id` (uuid)
- `first_seen_dt` (date)
- `last_seen_dt` (date)
- `confidence` (float 0..1)
- `match_method` (exact_id / exact_name / fuzzy_name / manual_override)
- `manual_override` (bool)
- `notes` (string)

### 5.3 Identity resolution pipeline (REQUIRED)
Nightly identity resolution MUST:
1. **Exact ID matches** (same source provides stable IDs across endpoints).
2. **Exact name + DOB** (where DOB exists).
3. **Fuzzy match name + team + season overlap** (where DOB missing).
4. **Manual override table** (authoritative):
   - `manual_entity_links.csv` committed to repo (or stored as a governed table)
   - overrides always win

Resolution must be deterministic and reproducible:
- same inputs → same canonical IDs
- merging canonical IDs must be explicit via `merge_events` table (audit trail)

---

## 6) Source Connectors and Ingestion Requirements

### 6.1 Connector interface (REQUIRED)
Every connector must implement:

- `list_partitions(start_date, end_date) -> list[Partition]`
- `fetch_partition(partition) -> RawPayload`
- `parse_payload(raw_payload) -> list[RawRecord]`
- `write_bronze(raw_payload, parsed_records)`
- `emit_watermark(partition, status)`

Partition definition:
- `dt`
- `entity` (schedule/roster/game/pitches/stats/etc.)
- `keys` (team_id, game_id, season, etc.)

All connectors must:
- Respect rate limits (token bucket + sleep)
- Cache responses by request hash
- Retry with exponential backoff
- Log request metadata

### 6.2 MLB ingestion (REQUIRED)
**Sources**
- Statcast pitch-level (and/or event-level) data
- MLB Stats API for schedules, rosters, transactions

**Bronze entities**
- `mlb_statcast/pitches`
- `mlb_statsapi/schedule`
- `mlb_statsapi/rosters`
- `mlb_statsapi/transactions`

**Partitioning**
- Statcast: by `game_date` (daily) and optionally by 7-day chunks for stability
- Stats API: daily for schedule updates, daily roster snapshot

### 6.3 NPB ingestion via SPAIA (REQUIRED)
The NPB connector MUST support the following entity families using the SPAIA endpoints you identified:
- schedules (season + monthly + date)
- rosters/directory (by team/year)
- standings (by league/year)
- player season/career stats (batting/pitching)
- per-game stats (both_batter_stats / both_pitcher_game_stats)
- play-by-play and pitch-by-pitch (game_text_pbp, flash_atbat_history / equivalent)

**Bronze entities**
- `npb_spaia/schedules`
- `npb_spaia/rosters`
- `npb_spaia/standings`
- `npb_spaia/player_stats`
- `npb_spaia/game_pbp`
- `npb_spaia/game_pitches` (pitch-by-pitch)

**Partitioning**
- schedules: `Year` (season partitions), stored daily snapshot
- games: by `game_date` (dt)
- pbp/pitches: by `game_id` inside the `dt` partition

### 6.4 Other leagues (KBO/LMB/Indy) ingestion (REQUIRED)
These leagues likely provide boxscore-level data:
- schedules/results
- rosters
- player season totals
- (maybe) play-by-play

The connector must be resilient to:
- HTML layout changes
- missing IDs
- missing DOB/handedness

Bronze must still store:
- raw HTML/JSON
- parsed minimal tables:
  - `games`
  - `teams`
  - `players` (best-effort)
  - `boxscores` (batting/pitching lines)

---

## 7) Canonical Data Model (Gold)

### 7.1 Dimensions (REQUIRED)

#### `gbdp_gold.dim_league`
- `league_id` (uuid, PK)
- `league_code` (MLB, MiLB, NPB, KBO, LMB, INDY)
- `country`
- `level` (major/minor/pro/independent)
- `season_start_month`, `season_end_month`

#### `gbdp_gold.dim_team` (SCD2)
- `team_id` (uuid, PK)
- `league_id` (uuid, FK)
- `team_name`
- `team_abbrev`
- `home_city`
- `valid_from_dt`, `valid_to_dt`, `is_current`

#### `gbdp_gold.dim_player` (SCD2)
- `player_id` (uuid, PK)
- `primary_name`
- `alternate_names` (array<string>)
- `dob` (date, nullable)
- `bats` (L/R/S/UNK)
- `throws` (L/R/UNK)
- `height_cm` (nullable)
- `weight_kg` (nullable)
- `nationality` (nullable)
- `primary_position` (nullable)
- `valid_from_dt`, `valid_to_dt`, `is_current`

#### `gbdp_gold.dim_season`
- `season_id` (uuid, PK)
- `league_id` (uuid, FK)
- `season_year` (int)
- `season_type` (regular/postseason)
- `season_start_dt`, `season_end_dt`

### 7.2 Facts (REQUIRED)

#### `gbdp_gold.fact_game`
- `game_id` (uuid, PK)
- `league_id`
- `season_id`
- `game_date`
- `game_start_ts_utc` (nullable)
- `home_team_id`, `away_team_id`
- `venue` (nullable)
- `status` (final/scheduled/in_progress/unknown)
- `home_score`, `away_score` (nullable until final)
- `dt` (snapshot date)

#### `gbdp_gold.fact_roster` (Type-2 intervals)
- `team_id`
- `player_id`
- `season_id`
- `role` (batter/pitcher/two_way/staff)
- `start_dt`, `end_dt` (nullable = still active)
- `dt` (snapshot date)

#### `gbdp_gold.fact_transaction`
- `transaction_id` (uuid, PK)
- `league_id`
- `team_id` (nullable)
- `player_id` (nullable)
- `transaction_type` (callup/demotion/trade/draft/signing/release/IL/etc.)
- `effective_dt`
- `details` (string/json)
- `dt`

#### `gbdp_gold.fact_contract` (where available)
- `contract_id` (uuid, PK)
- `player_id`
- `team_id`
- `start_season_year`, `end_season_year`
- `aav_usd` (nullable)
- `total_value_usd` (nullable)
- `contract_terms` (json)
- `source`
- `dt`

#### `gbdp_gold.fact_plate_appearance`
Canonical PA fact for any league that has at-bat outcomes.
- `pa_id` (uuid, PK)
- `game_id`
- `inning` (int)
- `is_top_inning` (bool)
- `batting_team_id`
- `fielding_team_id`
- `batter_id`
- `pitcher_id` (nullable for leagues without pitcher IDs)
- `event_type` (BB/HBP/K/HR/1B/2B/3B/OUT/ERROR/FC/etc.)
- `rbi` (nullable)
- `runs_scored_on_play` (int)
- `outs_on_play` (int)
- `base_state_before` (int 0..7)
- `outs_before` (0..2)
- `base_state_after` (int 0..7)
- `outs_after` (0..2)
- `dt`

#### `gbdp_gold.fact_pitch` (only for leagues with pitch-level)
- `pitch_id` (uuid, PK)
- `game_id`
- `pa_id` (FK)
- `pitch_number_in_pa`
- `pitcher_id`, `batter_id`
- `balls_before`, `strikes_before`
- `pitch_type` (nullable)
- `release_speed` (nullable)
- `plate_x`, `plate_z` (nullable)
- `result` (ball/strike/in_play/foul/etc.)
- `dt`

#### `gbdp_gold.fact_boxscore_batting` (for leagues lacking pitch/pa)
- `game_id`
- `team_id`
- `player_id`
- `ab`, `h`, `2b`, `3b`, `hr`, `bb`, `so`, `rbi`, `r`
- `dt`

#### `gbdp_gold.fact_boxscore_pitching`
- `game_id`
- `team_id`
- `player_id`
- `ip`, `h`, `r`, `er`, `bb`, `so`, `hr`
- `dt`

#### `gbdp_gold.fact_standings`
- `league_id`
- `season_id`
- `team_id`
- `w`, `l`, `t` (nullable)
- `pct` (nullable)
- `gb` (nullable)
- `dt`

### 7.3 Constraints and invariants (REQUIRED)
- All fact tables must have `dt`.
- Gold keys must be stable and not reused.
- `base_state_*` must always be in `0..7`, outs in `0..3`.
- `fact_pitch` must reference an existing `pa_id` and `game_id`.
- Referential integrity checks run nightly.

---

## 8) Transformations and Business Logic

### 8.1 Event normalization (REQUIRED)
Define a canonical event enum that all sources map into:
- `BB`, `HBP`, `K`, `HR`, `1B`, `2B`, `3B`, `OUT`, `ERROR`, `FC`, `SAC`, `DP`, `TP`, `UNKNOWN`

Rules:
- If a source provides finer-grained events, store raw in Silver and map to canonical in Gold.
- Preserve raw event text in a `raw_event` column for audit.

### 8.2 Base/outs reconstruction (REQUIRED)
If a source provides play-by-play but not explicit base/out states:
- reconstruct base state transitions deterministically from the sequence of plays
- store both before/after states in `fact_plate_appearance`
- validate inning outs never exceed 3

### 8.3 Snapshots vs intervals (REQUIRED)
- `dim_*` tables use SCD2 for fields that can change (team names, player primary name).
- `fact_roster` uses interval validity (start/end dates).
- Every record includes `dt` so queries can be “as-of dt”.

---

## 9) Orchestration: Nightly DAG (REQUIRED)

### 9.1 DAG stages (REQUIRED)
Nightly job executes in this order:

1. **Fetch schedules**
   - For each league/year window
   - Write bronze schedule payloads and parsed schedule tables

2. **Fetch rosters**
   - For each team in each league (where available)
   - Store full roster snapshots (dt)

3. **Fetch transactions + contracts (where available)**
   - MLB Stats API transactions
   - Other leagues best-effort

4. **Fetch game details**
   - For all games within the ingestion window (recent + backfill)
   - Includes boxscores, lineups, outcomes

5. **Fetch play-by-play / pitch-by-pitch**
   - MLB: Statcast / event tables
   - NPB: SPAIA pbp + pitch feed
   - Others: if available

6. **Silver normalize**
   - Dedup raw-parsed tables
   - Standardize ids as strings, normalize dates/times, fix encoding

7. **Identity resolution**
   - Update bridge table + canonical dim IDs
   - Emit audit trail for merges/splits

8. **Gold publish**
   - Upsert dims (SCD2)
   - Build gold facts (games, rosters, PAs, pitches, boxscores)
   - Build standings aggregates

9. **Quality checks**
   - Table-level assertions
   - Referential integrity
   - Duplicate keys
   - Completeness thresholds (see section 10)

10. **Artifacts + reporting**
   - Store run metrics, row counts, and anomalies for the run

### 9.2 Idempotency requirements (REQUIRED)
- Every stage writes outputs to deterministic paths keyed by `(source, entity, dt, partition_keys)`.
- Re-running the same stage without `--force` must not change outputs.
- Silver/Gold writes must use Delta `MERGE` with deterministic keys.

### 9.3 Backfills (REQUIRED)
Support:
- `backfill --league NPB --from 2018-03-01 --to 2018-10-31`
- partial backfill by entity type (schedule only, roster only, pbp only)

Backfills must:
- write to correct historical `dt` partitions
- re-run identity resolution and gold publish for affected ranges

---

## 10) Data Quality, Testing, and Observability

### 10.1 Data quality checks (REQUIRED)
Implement nightly checks:
1. **Row count sanity**
   - schedule rows ≥ expected games for league/season window
2. **Uniqueness**
   - `fact_game.game_id` unique
   - `(game_id, player_id, dt)` unique for boxscore lines
3. **Referential integrity**
   - every `fact_plate_appearance.game_id` exists in `fact_game`
   - every `fact_pitch.pa_id` exists in `fact_plate_appearance`
4. **State validity**
   - base states in `0..7`
   - outs in `0..3` with inning termination at 3
5. **Completeness thresholds**
   - For leagues with pitch data: percentage of games with ≥1 pitch record must exceed a threshold (configured per league)
6. **Schema drift detection**
   - bronze raw-parsed schema differences logged and flagged

### 10.2 Code-level testing (REQUIRED)
- Use `pytest`.
- Deterministic tests for:
  - event normalization mapping
  - base/out reconstruction
  - idempotent write paths
  - identity resolver deterministic output given fixed inputs

### 10.3 Observability (REQUIRED)
Persist run logs and metrics:
- `gbdp_gold.audit_pipeline_runs`
  - run_id, dt, start/end, status, row_counts_by_table, anomalies
- Store connector request logs (status codes, retries, latency histogram)

---

## 11) Security and Compliance

### 11.1 Secrets (REQUIRED)
- All secrets stored in:
  - Databricks Secret Scopes (cloud)
  - `.env` (local dev, never committed; `.env.example` required)
- No credentials in code or notebooks.

### 11.2 Source usage and throttling (REQUIRED)
- All connectors must:
  - set a descriptive User-Agent
  - throttle requests
  - cache responses
  - honor reasonable request rates to avoid disruption

---

## 12) Serving Layer (FastAPI)

### 12.1 API (REQUIRED)
The serving layer is read-only and refreshed nightly.

**Endpoints (REQUIRED)**
- `GET /health`
- `GET /leagues`
- `GET /teams?league=NPB&season=2023`
- `GET /players?query=ohtani&league=MLB`
- `GET /games?league=NPB&date=2023-10-18`
- `GET /games/{game_id}`
- `GET /players/{player_id}/stats?window=rolling_30d&as_of=YYYY-MM-DD`
- `GET /run-expectancy?league=MLB&season=2024&as_of=YYYY-MM-DD` (derived table output)
- `GET /breakout-candidates?league=MLB&season=2026&as_of=YYYY-MM-DD` (model output once trained)

**Backend access (REQUIRED)**
- API reads Gold tables via:
  - Databricks SQL endpoint (cloud), or
  - DuckDB over Parquet (local)

### 12.2 Caching (REQUIRED)
- Cache GET responses for 24h (nightly refresh).
- Cache keyed by request URL + `as_of`.

---

## 13) Storage, Partitioning, and Performance

### 13.1 Partitioning (REQUIRED)
- All facts partitioned by `dt` and often by `league_code`:
  - `dt=YYYY-MM-DD/league=MLB/`
- Large event facts partition additionally by `season_year` and/or `game_date` where helpful.

### 13.2 File format (REQUIRED)
- Parquet for files
- Delta for managed lakehouse tables (or Iceberg if not available)

### 13.3 Compute efficiency (REQUIRED)
- Avoid pandas in large transforms (use Spark SQL / Spark DataFrames).
- Keep ingestion parsing lightweight and write raw-parsed parquet early.

---

## 14) Repo Structure (Implementation Blueprint)

```
gbdp/
  README.md
  TRD.md
  PRD.md
  AGENTS.md
  pyproject.toml
  .env.example
  configs/
    pipelines.yaml
    sources.yaml
    leagues.yaml
    quality.yaml
  data/                      # local dev only (gitignored)
    bronze/
    silver/
    gold/
  src/gbdp/
    cli.py
    connectors/
      base.py
      mlb_statcast.py
      mlb_statsapi.py
      npb_spaia.py
      kbo.py
      lmb.py
      indy.py
    bronze/
      writer.py
      cache.py
    silver/
      normalize.py
      schemas.py
    gold/
      dims.py
      facts.py
      aggregates.py
      features.py
    identity/
      resolver.py
      rules.py
      manual_overrides.py
    quality/
      checks.py
      metrics.py
    serve/
      api.py
      db.py
    utils/
      logging.py
      time.py
      io.py
  tests/
    test_identity_resolution.py
    test_event_mapping.py
    test_base_outs_reconstruction.py
    test_idempotency.py
```

Rules for LLM contributors are enforced by `AGENTS.md`:
- type hints, Black, pytest, determinism.

---

## 15) Definition of Done (TRD)
This TRD is satisfied when:

1. A nightly job produces **bronze/silver/gold** snapshots for:
   - MLB (at least schedule + rosters + PA/pitch data)
   - NPB (at least schedule + rosters + PBPs/pitches where available)
   - at least one additional league at boxscore level

2. The system supports:
   - backfill by date range
   - idempotent reruns
   - reproducible “as-of dt” queries

3. Gold tables meet the quality checks in section 10.

4. FastAPI can serve canonical queries from Gold tables in both cloud and local modes.

---

## Appendix A — Practical ingestion window (REQUIRED)
Nightly job must ingest:
- `dt-1` full day (yesterday’s games + updates)
- rolling “correction window” of last **7 days** (some sources revise data)
- configurable full backfill ranges for historical runs

---

## Appendix B — Minimal schema for leagues without pitch data (REQUIRED)
If only boxscores exist:
- populate `fact_game`, `fact_roster`, `fact_boxscore_*`, `fact_standings`
- `fact_plate_appearance` and `fact_pitch` can be absent for that league
- identity resolution still applies via players/teams

