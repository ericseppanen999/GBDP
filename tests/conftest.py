from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

# Test this checkout's code. An editable install of another checkout (a git
# worktree's parent, say) would otherwise be imported instead.
_SRC = str(Path(__file__).resolve().parent.parent / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)
for _name in [m for m in sys.modules if m == "gbdp" or m.startswith("gbdp.")]:
    del sys.modules[_name]


@pytest.fixture(scope="session")
def _semantic_gold_dirs(tmp_path_factory):
    """Fixture silver run through the real identity and gold code, once per session."""
    import semantic_fixture

    tmp = tmp_path_factory.mktemp("semantic")
    env = {"GBDP_SILVER_ROOT": str(tmp / "silver"), "GBDP_GOLD_ROOT": str(tmp / "gold"),
           "GBDP_STORAGE_FORMAT": "parquet", "GBDP_DATA_ROOT": str(tmp),
           "GBDP_ML_CONFIG": str(tmp / "ml.yaml")}
    saved = {k: os.environ.get(k) for k in env}
    os.environ.update(env)
    try:
        semantic_fixture.build_gold(tmp)
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    return tmp


@pytest.fixture
def semantic_gold(_semantic_gold_dirs, monkeypatch) -> Path:
    """The fixture gold directory, with the env pointing at it for this test."""
    tmp = _semantic_gold_dirs
    monkeypatch.setenv("GBDP_SILVER_ROOT", str(tmp / "silver"))
    monkeypatch.setenv("GBDP_GOLD_ROOT", str(tmp / "gold"))
    monkeypatch.setenv("GBDP_STORAGE_FORMAT", "parquet")
    monkeypatch.setenv("GBDP_DATA_ROOT", str(tmp))
    monkeypatch.delenv("GBDP_API_TOKEN", raising=False)
    from gbdp.serve import db

    db._CACHE.clear()
    return tmp / "gold"
