"""0.4.3 units: shadow clone time, status idle fields."""

import json
import time
from pathlib import Path

from local_spark_mcp.engine import _shadow_state


def _log(dir_: Path, commits: list[dict]) -> None:
    log = dir_ / "_delta_log"; log.mkdir(parents=True)
    for i, ci in enumerate(commits):
        (log / f"{i:020d}.json").write_text(json.dumps({"commitInfo": ci}) + "\n", encoding="utf-8")


def test_shadow_state_reports_clone_time(tmp_path):
    _log(tmp_path / "read", [{"operation": "CLONE", "timestamp": 1_759_800_000_000}])
    assert _shadow_state(tmp_path / "read") == ("read", 0, "2025-10-07T01:20:00+00:00")
    _log(tmp_path / "written", [{"operation": "CLONE", "timestamp": 1_759_800_000_000}, {"operation": "WRITE"}])
    state, version, cloned_at = _shadow_state(tmp_path / "written")
    assert (state, version, cloned_at) == ("written", 1, "2025-10-07T01:20:00+00:00")
    # no timestamp in the commit: the file's mtime stands in
    _log(tmp_path / "old", [{"operation": "CLONE"}])
    state, version, cloned_at = _shadow_state(tmp_path / "old")
    assert state == "read" and cloned_at is not None and cloned_at.startswith(time.strftime("%Y"))
    (tmp_path / "empty" / "_delta_log").mkdir(parents=True)
    assert _shadow_state(tmp_path / "empty") == ("unknown", -1, None)
