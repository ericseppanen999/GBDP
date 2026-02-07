from gbdp.identity.rules import player_match_key, team_match_key


def test_player_match_key_exact_id():
    res = player_match_key({"source": "mlb_statsapi", "source_player_id": "123"})
    assert res is not None
    assert res.method == "exact_id"


def test_team_match_key_name_league():
    res = team_match_key({"name": "Tigers", "league_code": "MLB"})
    assert res is not None
    assert "tigers" in res.canonical_key
