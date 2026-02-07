-- Unity Catalog table registration (Volumes paths, no scheme)
-- Run this in Databricks SQL Warehouse or SQL Editor.

CREATE SCHEMA IF NOT EXISTS gbdp.bronze_gbdp;
CREATE SCHEMA IF NOT EXISTS gbdp.silver_gbdp;
CREATE SCHEMA IF NOT EXISTS gbdp.gold_gbdp;

-- Gold tables (core)
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.dim_league USING DELTA LOCATION '/Volumes/gbdp/gold_gbdp/gold_vol/gbdp/gold/dim_league';
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.dim_team USING DELTA LOCATION '/Volumes/gbdp/gold_gbdp/gold_vol/gbdp/gold/dim_team';
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.dim_player USING DELTA LOCATION '/Volumes/gbdp/gold_gbdp/gold_vol/gbdp/gold/dim_player';
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.dim_season USING DELTA LOCATION '/Volumes/gbdp/gold_gbdp/gold_vol/gbdp/gold/dim_season';
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.bridge_source_ids USING DELTA LOCATION '/Volumes/gbdp/gold_gbdp/gold_vol/gbdp/gold/bridge_source_ids';
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.merge_events USING DELTA LOCATION '/Volumes/gbdp/gold_gbdp/gold_vol/gbdp/gold/merge_events';
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.fact_game USING DELTA LOCATION '/Volumes/gbdp/gold_gbdp/gold_vol/gbdp/gold/fact_game';
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.fact_roster USING DELTA LOCATION '/Volumes/gbdp/gold_gbdp/gold_vol/gbdp/gold/fact_roster';
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.fact_transaction USING DELTA LOCATION '/Volumes/gbdp/gold_gbdp/gold_vol/gbdp/gold/fact_transaction';
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.fact_contract USING DELTA LOCATION '/Volumes/gbdp/gold_gbdp/gold_vol/gbdp/gold/fact_contract';
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.fact_plate_appearance USING DELTA LOCATION '/Volumes/gbdp/gold_gbdp/gold_vol/gbdp/gold/fact_plate_appearance';
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.fact_pitch USING DELTA LOCATION '/Volumes/gbdp/gold_gbdp/gold_vol/gbdp/gold/fact_pitch';
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.fact_boxscore_batting USING DELTA LOCATION '/Volumes/gbdp/gold_gbdp/gold_vol/gbdp/gold/fact_boxscore_batting';
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.fact_boxscore_pitching USING DELTA LOCATION '/Volumes/gbdp/gold_gbdp/gold_vol/gbdp/gold/fact_boxscore_pitching';
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.fact_standings USING DELTA LOCATION '/Volumes/gbdp/gold_gbdp/gold_vol/gbdp/gold/fact_standings';
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.run_expectancy USING DELTA LOCATION '/Volumes/gbdp/gold_gbdp/gold_vol/gbdp/gold/run_expectancy';
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.breakout_candidates USING DELTA LOCATION '/Volumes/gbdp/gold_gbdp/gold_vol/gbdp/gold/breakout_candidates';
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.feature_player_rolling_30d USING DELTA LOCATION '/Volumes/gbdp/gold_gbdp/gold_vol/gbdp/gold/feature_player_rolling_30d';
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.audit_pipeline_runs USING DELTA LOCATION '/Volumes/gbdp/gold_gbdp/gold_vol/gbdp/gold/audit_pipeline_runs';
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.audit_stage_runs USING DELTA LOCATION '/Volumes/gbdp/gold_gbdp/gold_vol/gbdp/gold/audit_stage_runs';
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.audit_quality USING DELTA LOCATION '/Volumes/gbdp/gold_gbdp/gold_vol/gbdp/gold/audit_quality';
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.audit_schema USING DELTA LOCATION '/Volumes/gbdp/gold_gbdp/gold_vol/gbdp/gold/audit_schema';

-- Bronze requests (optional)
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.requests USING PARQUET LOCATION '/Volumes/gbdp/bronze_gbdp/bronze_vol/gbdp/bronze/requests';
