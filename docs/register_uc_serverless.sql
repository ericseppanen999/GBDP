-- Unity Catalog registration for SERVERLESS SQL Warehouse
-- IMPORTANT: Serverless does NOT allow LOCATION. These are managed tables.

CREATE SCHEMA IF NOT EXISTS gbdp.bronze_gbdp;
CREATE SCHEMA IF NOT EXISTS gbdp.silver_gbdp;
CREATE SCHEMA IF NOT EXISTS gbdp.gold_gbdp;

-- Bronze parsed tables (no LOCATION)
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.mlb_statsapi_schedule USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.mlb_statsapi_rosters USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.mlb_statsapi_transactions USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.mlb_statsapi_teams USING DELTA;

CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.mlb_statcast_pitches USING DELTA;

CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.npb_spaia_schedules USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.npb_spaia_game_schedule USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.npb_spaia_games_by_date USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.npb_spaia_weekly_schedule USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.npb_spaia_monthly_schedule USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.npb_spaia_directory USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.npb_spaia_player_info USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.npb_spaia_rosters_by_team USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.npb_spaia_batter_list USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.npb_spaia_pitcher_list USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.npb_spaia_staff_list USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.npb_spaia_standings USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.npb_spaia_hitting_stats_career USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.npb_spaia_hitting_stats_by_year USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.npb_spaia_hitting_stats_by_month USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.npb_spaia_hitting_stats_by_game USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.npb_spaia_player_pitching_detail_saber USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.npb_spaia_player_batting_detail_saber USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.npb_spaia_related_players USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.npb_spaia_same_draft_year_players USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.npb_spaia_live_games USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.npb_spaia_game_over_view USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.npb_spaia_prediction_game USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.npb_spaia_current_score USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.npb_spaia_starting_members_for_flash USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.npb_spaia_flash_atbat_history USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.npb_spaia_game_text_pbp USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.npb_spaia_both_batter_stats USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.npb_spaia_both_pitcher_game_stats USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.npb_spaia_player_hitting_career USING DELTA;

CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.indy_local_games USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.indy_local_rosters USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.indy_local_boxscore_batting USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.indy_local_boxscore_pitching USING DELTA;

CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.kbo_local_games USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.kbo_local_rosters USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.kbo_local_boxscore_batting USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.kbo_local_boxscore_pitching USING DELTA;

CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.lmb_local_games USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.lmb_local_rosters USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.lmb_local_boxscore_batting USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.lmb_local_boxscore_pitching USING DELTA;

CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.retrosheet_local_allplayers USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.retrosheet_local_gameinfo USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.retrosheet_local_teamstats USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.retrosheet_local_batting USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.retrosheet_local_pitching USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.retrosheet_local_fielding USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.retrosheet_local_plays USING DELTA;

CREATE TABLE IF NOT EXISTS gbdp.bronze_gbdp.requests USING DELTA;

-- Silver tables
CREATE TABLE IF NOT EXISTS gbdp.silver_gbdp.mlb_statsapi_games USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.silver_gbdp.mlb_statsapi_rosters USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.silver_gbdp.mlb_statsapi_transactions USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.silver_gbdp.mlb_statcast_pitches USING DELTA;

CREATE TABLE IF NOT EXISTS gbdp.silver_gbdp.npb_spaia_games USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.silver_gbdp.npb_spaia_rosters USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.silver_gbdp.npb_spaia_game_pbp USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.silver_gbdp.npb_spaia_standings USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.silver_gbdp.npb_spaia_game_batter_stats USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.silver_gbdp.npb_spaia_game_pitcher_stats USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.silver_gbdp.npb_spaia_player_batting_saber USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.silver_gbdp.npb_spaia_player_pitching_saber USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.silver_gbdp.npb_spaia_player_stats_by_year USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.silver_gbdp.npb_spaia_player_stats_by_month USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.silver_gbdp.npb_spaia_player_stats_by_game USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.silver_gbdp.npb_spaia_player_hitting_career USING DELTA;

CREATE TABLE IF NOT EXISTS gbdp.silver_gbdp.indy_local_games USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.silver_gbdp.indy_local_rosters USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.silver_gbdp.indy_local_boxscore_batting USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.silver_gbdp.indy_local_boxscore_pitching USING DELTA;

CREATE TABLE IF NOT EXISTS gbdp.silver_gbdp.kbo_local_games USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.silver_gbdp.kbo_local_rosters USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.silver_gbdp.kbo_local_boxscore_batting USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.silver_gbdp.kbo_local_boxscore_pitching USING DELTA;

CREATE TABLE IF NOT EXISTS gbdp.silver_gbdp.lmb_local_games USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.silver_gbdp.lmb_local_rosters USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.silver_gbdp.lmb_local_boxscore_batting USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.silver_gbdp.lmb_local_boxscore_pitching USING DELTA;

CREATE TABLE IF NOT EXISTS gbdp.silver_gbdp.retrosheet_local_games USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.silver_gbdp.retrosheet_local_rosters USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.silver_gbdp.retrosheet_local_teamstats USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.silver_gbdp.retrosheet_local_fielding USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.silver_gbdp.retrosheet_local_boxscore_batting USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.silver_gbdp.retrosheet_local_boxscore_pitching USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.silver_gbdp.retrosheet_local_game_pbp USING DELTA;

-- Gold tables
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.dim_league USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.dim_team USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.dim_player USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.dim_season USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.bridge_source_ids USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.merge_events USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.fact_game USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.fact_roster USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.fact_transaction USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.fact_contract USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.fact_plate_appearance USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.fact_pitch USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.fact_boxscore_batting USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.fact_boxscore_pitching USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.fact_standings USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.run_expectancy USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.breakout_candidates USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.feature_player_rolling_30d USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.audit_pipeline_runs USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.audit_stage_runs USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.audit_quality USING DELTA;
CREATE TABLE IF NOT EXISTS gbdp.gold_gbdp.audit_schema USING DELTA;
