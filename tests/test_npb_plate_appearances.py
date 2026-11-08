"""NPB plate appearances from SPAIA play-by-play (one row per pitch or game event).

The rules here were checked against the real feed: over every May-June 2026
game (156), hits per team equal the feed's own totals in all of them, and runs
equal the final score in all but one (a walk-off run on a wild pitch, with no
PA result to carry it).
"""
from __future__ import annotations

import json
from datetime import date

import pytest

from gbdp.gold import publish


@pytest.mark.parametrize("header,expected", [
    ("＜1番：名原＞無死走者なし", (0, 0)),
    ("＜3番：小園＞二死一塁", (2, 1)),
    ("＜4番：坂倉＞一死一三塁", (1, 5)),
    ("＜5番：末包＞一死二三塁", (1, 6)),
    ("＜6番：大盛＞二死満塁", (2, 7)),
    ("試合開始", (None, None)),
])
def test_header_gives_outs_and_runners(header, expected):
    assert publish._npb_state(header) == expected


@pytest.mark.parametrize("text,event", [
    ("6球目:サードゴロ 1アウト", "OUT"),
    ("4球目:見逃し三振 2アウト", "K"),
    ("5球目:暴投による振り逃げ 一塁", "K"),
    ("2球目:1アウト二塁の1-1からレフトへタイムリーツーベース！ 日 3-0 デ 二塁", "2B"),
    ("3球目:カウント1-1から左中間への2ランホームラン 西 3-2 広 ", "HR"),
    ("1球目:ショートへの内野安打 一塁", "1B"),
    ("2球目:三塁側へ意表をついたセーフティバントを決める 一二塁", "1B"),
    ("3球目:ピッチャーの左をつくプッシュバント！一塁へ出塁する 一塁", "1B"),
    ("1球目:セーフティバントを試みるもピッチャーゴロとなる 1アウト", "OUT"),
    ("6球目:送りバント失敗 1アウト一塁", "OUT"),
    ("3球目:見事送りバントを成功させる 1アウト二塁", "SAC"),
    ("4球目:渡部(三)のファンブルにより出塁する 一二塁", "ERROR"),
    ("5球目:中野(二)の後逸により出塁する", "ERROR"),
    ("4球目:内山(二)の悪送球により出塁する 一塁", "ERROR"),
    ("4球目:フォアボールを選ぶ 一塁", "BB"),
    ("満塁策で敬遠される 満塁", "BB"),
    ("4球目:デッドボールを受ける 一塁", "HBP"),
    ("5球目:6-4-3のダブルプレー 3アウト", "DP"),
    ("3球目:山崎(二)のフィルダースチョイスにより出塁する", "FC"),
    ("5球目:打撃妨害 一塁", "INT"),
])
def test_result_text_maps_to_event(text, event):
    assert publish._npb_event_type(text) == event


def test_score_only_matches_this_games_teams():
    teams = ("西", "広")
    assert publish._npb_score("1球目:先制のタイムリーツーベース！ 西 2-0 広 二塁", teams) == [2, 0]
    assert publish._npb_score("3回裏 西3-2広　西武は2番滝澤からの攻撃", teams) == [3, 2]
    # A pitch count looks like a score; it must not be read as one.
    assert publish._npb_score("3球目:カウント1-1から左中間への2ランホームラン", teams) is None


def _ev(event_id, ab, sec, text, inning=1, top="1"):
    return {"game_id": "g", "inning": inning, "top_bottom": top, "play_ab": ab, "play_seq_no": ab,
            "event_id": str(event_id), "play_player_id": "p", "text_info_text": text,
            "raw_event_json": json.dumps({"TextInfo_Bat_Text_Sec": sec})}


def test_a_half_inning_becomes_plate_appearances_with_state_and_runs():
    rows = [
        _ev(1, 1, "14", "試合開始 1回表 西0-0広　広島は1番名原からの攻撃"),
        _ev(2, 1, "6", "＜1番：名原＞無死走者なし"),
        _ev(3, 1, "0", "1球目:外角のストレートを見送ってボール 1-0"),
        _ev(4, 1, "1", "2球目:レフトへのヒットを放つ 一塁"),
        _ev(5, 2, "6", "＜2番：菊池＞無死一塁"),
        _ev(6, 2, "1", "1球目:カウント0-0から左中間への2ランホームラン 西 0-2 広 "),
        _ev(7, 3, "6", "＜3番：小園＞無死走者なし"),
        _ev(8, 3, "1", "1球目:6-4-3のダブルプレー 2アウト"),  # odd text; the next header wins
        _ev(9, 4, "6", "＜4番：坂倉＞二死走者なし"),
        _ev(10, 4, "1", "3球目:サードゴロ 3アウト"),
    ]
    # Shuffled: order comes from event_id, not row order or play_seq_no.
    pas = publish._reconstruct_npb_pa(list(reversed(rows)), date(2026, 6, 10), {})

    assert [p["event_type"] for p in pas] == ["1B", "HR", "DP", "OUT"]
    assert [(p["outs_before"], p["base_state_before"]) for p in pas] == [(0, 0), (0, 1), (0, 0), (2, 0)]
    assert [(p["outs_after"], p["base_state_after"]) for p in pas] == [(0, 1), (0, 0), (2, 0), (3, 0)]
    assert [p["runs_scored_on_play"] for p in pas] == [0, 2, 0, 0]
    assert all(p["is_top_inning"] for p in pas)


def test_an_inning_ending_on_the_bases_is_a_truncated_row_with_the_third_out():
    rows = [
        _ev(1, 1, "6", "＜1番：名原＞二死一塁"),
        _ev(2, 1, "3", "一塁走者名原:盗塁失敗 3アウト"),
    ]
    pa, = publish._reconstruct_npb_pa(rows, date(2026, 6, 10), {})
    assert (pa["event_type"], pa["outs_before"], pa["outs_after"], pa["base_state_after"]) == ("TRUNCATED", 2, 3, 0)


def test_a_result_less_header_mid_inning_is_dropped():
    rows = [
        _ev(1, 1, "6", "＜1番：名原＞無死走者なし"),       # batter replaced mid-count
        _ev(2, 2, "6", "＜1番：代打＞無死走者なし"),
        _ev(3, 2, "1", "1球目:サードゴロ 1アウト"),
        _ev(4, 3, "6", "＜2番：菊池＞一死走者なし"),
        _ev(5, 3, "1", "1球目:サードゴロ 2アウト"),
    ]
    assert [p["event_type"] for p in publish._reconstruct_npb_pa(rows, date(2026, 6, 10), {})] == ["OUT", "OUT"]


def test_runs_from_a_walk_off_row_go_to_the_pa_in_progress():
    rows = [
        _ev(1, 1, "15", "9回裏 デ7-7楽　DeNAは5番度会からの攻撃", inning=9, top="2"),
        _ev(2, 1, "6", "＜5番：度会＞一死三塁", inning=9, top="2"),
        _ev(3, 1, "1", "2球目:センターへのヒット 一塁", inning=9, top="2"),
        _ev(4, 2, "4", "DeNAサヨナラ勝ち！ デ 8-7 楽", inning=9, top="2"),
    ]
    pa, = publish._reconstruct_npb_pa(rows, date(2026, 6, 10), {})
    assert pa["runs_scored_on_play"] == 1
    assert pa["is_top_inning"] is False
