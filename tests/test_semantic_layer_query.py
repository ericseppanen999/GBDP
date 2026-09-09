"""
Tests for the read-only SQL query engine (serve/db.py) and semantic layer
that back both the /query REST endpoint and the MCP server tools.
"""
from __future__ import annotations

import pytest

from gbdp.serve.db import UnsafeSqlError, assert_read_only_sql, known_table_names, load_semantic_layer


@pytest.mark.parametrize(
    "sql",
    [
        "DROP TABLE fact_game",
        "DELETE FROM fact_game",
        "INSERT INTO fact_game VALUES (1)",
        "UPDATE fact_game SET dt = '2025-01-01'",
        "TRUNCATE TABLE fact_game",
        "SELECT * FROM fact_game; DROP TABLE fact_game",
        "ALTER TABLE fact_game ADD COLUMN x INT",
        "CREATE TABLE evil AS SELECT * FROM fact_game",
    ],
)
def test_assert_read_only_sql_blocks_writes(sql):
    with pytest.raises(UnsafeSqlError):
        assert_read_only_sql(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM fact_game",
        "select * from fact_game where dt = '2026-09-08'",
        "WITH x AS (SELECT 1 AS a) SELECT * FROM x",
        "  \n  SELECT count(*) FROM fact_game  ",
    ],
)
def test_assert_read_only_sql_allows_selects(sql):
    assert_read_only_sql(sql)  # must not raise


def test_semantic_layer_loads_and_covers_known_gold_tables():
    layer = load_semantic_layer()
    assert "tables" in layer
    # Spot-check a few tables that gold/publish.py actually writes are
    # documented -- catches the semantic layer silently drifting out of
    # sync with the real schema as new tables get added.
    for expected in ["fact_game", "fact_boxscore_batting", "fact_boxscore_pitching", "dim_player", "dim_team"]:
        assert expected in layer["tables"], f"{expected} missing from semantic_layer.yaml"


def test_semantic_layer_documents_known_data_gotchas():
    layer = load_semantic_layer()
    # These are real, confirmed-live caveats (found earlier this session) --
    # if they get scrubbed from the yaml during an edit, a client loses the
    # exact warnings that prevent it from misusing the data.
    boxscore_pitching_note = str(layer["tables"]["fact_boxscore_pitching"]["columns"].get("ip", ""))
    assert "STRING" in boxscore_pitching_note.upper() or "string" in boxscore_pitching_note


def test_known_table_names_nonempty():
    names = known_table_names()
    assert "fact_game" in names
    assert len(names) > 5
