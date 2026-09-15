"""REQUEST-006 units: shadow log truncation (restore_shadow), fatal-error
detection, hardened formatters, profile session confs, long-call timeouts."""

import json
from pathlib import Path

import pytest

from local_spark_mcp.engine import _shadow_state, _truncate_delta_log
from local_spark_mcp.profiles import PROFILES
from local_spark_mcp.server import format_exec_result, format_sync
from local_spark_mcp.worker import _is_fatal
from local_spark_mcp.worker_client import LONG_METHODS, WorkerError


def _table(tmp_path, commits):
    """commits: list of (operation, [added paths])."""
    t = tmp_path / "t"
    log = t / "_delta_log"
    log.mkdir(parents=True)
    for v, (op, adds) in enumerate(commits):
        lines = [json.dumps({"commitInfo": {"operation": op}})]
        for a in adds:
            (t / a).parent.mkdir(parents=True, exist_ok=True)
            (t / a).write_bytes(b"x")
            lines.append(json.dumps({"add": {"path": a}}))
        (log / f"{v:020d}.json").write_text("\n".join(lines), encoding="utf-8")
    (log / "_last_checkpoint").write_text("{}")
    (log / f"{len(commits) - 1:020d}.checkpoint.parquet").write_bytes(b"")
    return t


def test_truncate_to_clone_commit_removes_later_commits_and_their_files(tmp_path):
    t = _table(tmp_path, [("CLONE", []), ("WRITE", ["part-1.parquet"]), ("MERGE", ["p/part-2.parquet"])])
    assert _shadow_state(t) == ("written", 2)
    removed = _truncate_delta_log(t, 0)
    assert removed == {"commits": 2, "files": 2}
    assert _shadow_state(t) == ("read", 0)
    assert not (t / "part-1.parquet").exists() and not (t / "p" / "part-2.parquet").exists()
    assert not (t / "_delta_log" / "_last_checkpoint").exists()
    assert not list((t / "_delta_log").glob("*.checkpoint*.parquet"))


def test_truncate_keeps_files_of_kept_commits_and_rejects_bad_versions(tmp_path):
    t = _table(tmp_path, [("CLONE", []), ("WRITE", ["a.parquet"]), ("WRITE", ["b.parquet"])])
    assert _truncate_delta_log(t, 1) == {"commits": 1, "files": 1}
    assert (t / "a.parquet").exists() and not (t / "b.parquet").exists()
    with pytest.raises(ValueError):
        _truncate_delta_log(t, 5)


def test_result_level_fatal_detection():
    from local_spark_mcp.worker import _result_is_fatal

    assert _result_is_fatal({"ok": False, "error": "Py4JNetworkError: An error occurred while trying to connect"})
    assert _result_is_fatal({"ok": False, "error": "RuntimeError", "traceback": "... Java gateway process exited ..."})
    assert not _result_is_fatal({"ok": False, "error": "AnalysisException: column x not found"})
    assert not _result_is_fatal({"ok": True, "stdout": "fine"})


def test_fatal_detection():
    class Py4JNetworkError(Exception):
        pass

    inner = Py4JNetworkError("An error occurred while trying to connect to the Java server")
    outer = RuntimeError("wrapped")
    outer.__cause__ = inner
    assert _is_fatal(outer)
    assert _is_fatal(ConnectionRefusedError())
    assert _is_fatal(RuntimeError("Java gateway process exited before sending its port number"))
    assert not _is_fatal(ValueError("bad column"))


def test_worker_error_carries_fatal_and_long_methods():
    assert WorkerError("x", fatal=True).fatal and not WorkerError("x").fatal
    assert {"run_code", "run_sql", "run_notebook"} <= LONG_METHODS and "get_info" not in LONG_METHODS


def test_formatters_never_keyerror():
    assert "?" in format_sync({"errors": ["boom"]})
    out = format_exec_result({"ok": False})
    assert "no detail" in out


def test_profile_session_confs():
    assert PROFILES["fabric-2.0"].session_confs == {"spark.sql.ansi.enabled": "false"}
    assert PROFILES["fabric-1.3"].session_confs == {}
