from datetime import date

import gbdp.pipeline.runner as runner

CFG = {k: {} for k in ("mlb_statsapi", "mlb_statcast", "npb_spaia", "indy_local", "kbo_local", "kbo_api", "lmb_local", "retrosheet_local")}


def _silver_steps(monkeypatch, leagues):
    steps = []
    monkeypatch.setattr(runner, "_run_step", lambda name, fn: steps.append(name))
    runner._stage_silver(date(2026, 5, 13), date(2026, 5, 13), CFG, None, None, False, leagues)
    return {s.split(":")[1] for s in steps}


def test_kbo_only_normalizes_kbo_sources(monkeypatch):
    # leagues=kbo used to normalize indy and retrosheet too.
    assert _silver_steps(monkeypatch, ["kbo"]) == {"kbo_local", "kbo_api"}


def test_retrosheet_runs_with_mlb_or_by_name(monkeypatch):
    assert "retrosheet_local" in _silver_steps(monkeypatch, ["mlb"])
    assert _silver_steps(monkeypatch, ["retrosheet"]) == {"retrosheet_local"}


def test_indy_runs_only_when_asked(monkeypatch):
    assert _silver_steps(monkeypatch, ["indy"]) == {"indy"}
    assert "indy" in _silver_steps(monkeypatch, None)
