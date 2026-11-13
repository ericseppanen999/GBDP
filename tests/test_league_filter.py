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


def test_register_uc_gold_publishes_gold_only(monkeypatch):
    import gbdp.catalog.managed_publish as mp

    calls = []
    monkeypatch.setenv("GBDP_UC_CATALOG", "gbdp")
    monkeypatch.setenv("GBDP_UC_MODE", "managed")
    monkeypatch.setattr(mp, "_spark_session", lambda: object())
    monkeypatch.setattr(mp, "_ensure_schema", lambda *a: None)
    for layer in ("gold", "silver", "bronze_parsed", "bronze_requests"):
        monkeypatch.setattr(mp, f"_publish_{layer}", lambda spark, d, c, s, e, layer=layer: calls.append(layer))
    monkeypatch.setattr(runner, "_run_step", lambda name, fn: fn())
    runner._stage_register_uc_gold("2026-05-13", "2026-05-14", {}, None, None, False)
    assert calls == ["gold", "gold"]
    calls.clear()
    runner._stage_register_uc("2026-05-13", "2026-05-13", {}, None, None, False)
    assert calls == ["gold", "silver", "bronze_parsed", "bronze_requests"]
    assert "register_uc_gold" in runner._pipeline_stages() and "register_uc_gold" not in runner._OPT_IN_STAGES
    # The nightly (no --stages) publishes gold only; bronze/silver UC copies are by hand.
    assert "register_uc" in runner._OPT_IN_STAGES
