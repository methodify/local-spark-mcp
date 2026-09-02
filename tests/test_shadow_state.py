"""Shadow state (read vs written) from a shadow's _delta_log, the shadow_status
rendering, and the package version reported to MCP clients."""

import json
import tomllib
from pathlib import Path

from local_spark_mcp import __version__
from local_spark_mcp.engine import _shadow_state
from local_spark_mcp.server import ServerState, build_server, format_shadow


def _log(table_dir: Path, commits: list[str]):
    log = table_dir / "_delta_log"
    log.mkdir(parents=True)
    for v, op in enumerate(commits):
        lines = [json.dumps({"commitInfo": {"operation": op}}), json.dumps({"add": {"path": f"p{v}.parquet"}})]
        (log / f"{v:020d}.json").write_text("\n".join(lines))


def test_clone_only_is_read(tmp_path):
    _log(tmp_path / "t", ["CLONE"])
    assert _shadow_state(tmp_path / "t") == ("read", 0)


def test_clone_then_write_is_written(tmp_path):
    _log(tmp_path / "t", ["CLONE", "WRITE"])
    assert _shadow_state(tmp_path / "t") == ("written", 1)


def test_new_table_is_written_at_version_zero(tmp_path):
    _log(tmp_path / "t", ["CREATE TABLE AS SELECT"])
    assert _shadow_state(tmp_path / "t") == ("written", 0)


def test_clone_commit_with_non_ascii_metadata_is_read(tmp_path):
    # Delta commit JSON carries non-ASCII bytes; the read must not depend on the platform codec
    log = tmp_path / "t" / "_delta_log"
    log.mkdir(parents=True)
    (log / f"{0:020d}.json").write_text(
        json.dumps({"commitInfo": {"operation": "CLONE", "userName": "Bryon \u00e9\u2014\u00e5"}}, ensure_ascii=False),
        encoding="utf-8",
    )
    assert _shadow_state(tmp_path / "t") == ("read", 0)


def test_empty_log_is_unknown(tmp_path):
    (tmp_path / "t" / "_delta_log").mkdir(parents=True)
    assert _shadow_state(tmp_path / "t") == ("unknown", -1)


def test_format_shadow_shows_state():
    out = format_shadow({
        "write_mode": "sandbox", "shadow_root": "/s", "persistent": False,
        "tables": [
            {"lakehouse": "dataverse", "table": "custtable", "path": "/s/a/custtable", "state": "read", "version": 0},
            {"lakehouse": "silver", "table": "x", "path": "/s/b/x", "state": "written", "version": 3},
        ],
    })
    assert "dataverse.custtable  [read, v0]" in out and "silver.x  [written, v3]" in out
    assert "(none)" in format_shadow({"write_mode": "sandbox", "shadow_root": "/s", "persistent": False, "tables": []})


def test_version_matches_pyproject_and_is_reported_to_clients():
    pyproject = tomllib.loads((Path(__file__).resolve().parents[1] / "pyproject.toml").read_text())
    assert __version__ == pyproject["project"]["version"]
    mcp = build_server(ServerState())
    assert mcp._mcp_server.create_initialization_options().server_version == __version__
