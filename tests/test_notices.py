"""Notices (REQUEST-007): first-touch mounts in cell/SQL/notebook results, and a
late-noticed worker death reported at the top of the next tool result."""

from local_spark_mcp.server import _exit_reason, format_exec_result, format_notebook_result, format_sql_result


def test_exec_and_sql_results_lead_with_notices():
    out = format_exec_result({"ok": True, "stdout": "42\n", "notices": ["mounted dataverse.custtable in 11.2 s (shallow clone)"]})
    assert out.startswith("notice: mounted dataverse.custtable in 11.2 s (shallow clone)\n42")
    out = format_sql_result({"columns": ["c"], "rows": [[1]], "row_count": 1, "truncated": False, "limit": 100,
                             "notices": ["mounted silver.x in 0.9 s (existing shadow)"]})
    assert out.startswith("notice: mounted silver.x in 0.9 s (existing shadow)\n")
    assert format_exec_result({"ok": True, "stdout": "x\n"}) == "x"  # no notices, no prefix


def test_notebook_cells_show_notices():
    out = format_notebook_result({"path": "nb.py", "status": "ok", "cells_total": 1, "cells": [
        {"index": 0, "kind": "code", "language": "python", "line": 3, "status": "ok", "stdout": "", "notices": ["mounted a.b in 2.0 s (shallow clone)"]}
    ], "warnings": []})
    assert "    notice: mounted a.b in 2.0 s (shallow clone)" in out


def test_exit_reason_names_signals():
    assert _exit_reason(None) == "exit status unknown"
    assert _exit_reason(1) == "exit code 1"
    assert "SIGKILL" in _exit_reason(-9) and "out-of-memory" in _exit_reason(-9)
    assert "SIGTERM" in _exit_reason(-15) and "out-of-memory" not in _exit_reason(-15)
