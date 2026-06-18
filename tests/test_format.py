from local_spark_mcp.server import (
    format_exec_result,
    format_info,
    format_sql_result,
)


def test_exec_ok_with_output():
    out = format_exec_result({"ok": True, "stdout": "hello\n", "stderr": "", "error": None})
    assert out == "hello"


def test_exec_ok_no_output():
    assert format_exec_result({"ok": True, "stdout": "", "error": None}) == "(ok — no output)"


def test_exec_error_prefers_traceback():
    res = {
        "ok": False,
        "stdout": "partial\n",
        "error": "ZeroDivisionError: division by zero",
        "traceback": "Traceback ...\nZeroDivisionError: division by zero",
    }
    out = format_exec_result(res)
    assert "partial" in out
    assert "Traceback ..." in out


def test_exec_error_falls_back_to_summary():
    res = {"ok": False, "stdout": "", "error": "SyntaxError: bad", "traceback": None}
    assert "SyntaxError: bad" in format_exec_result(res)


def test_sql_table_render():
    res = {
        "columns": ["id", "name"],
        "rows": [[1, "Alice"], [2, None]],
        "row_count": 2,
        "truncated": False,
        "limit": 100,
    }
    out = format_sql_result(res)
    lines = out.splitlines()
    assert lines[0] == "id | name "
    assert set(lines[1]) <= {"-", "+"}
    assert "1  | Alice" in out
    assert "2  | NULL" in out  # None renders as NULL
    assert "[2 row(s)]" in out


def test_sql_truncation_note():
    res = {
        "columns": ["x"],
        "rows": [[1], [2]],
        "row_count": 2,
        "truncated": True,
        "limit": 2,
    }
    assert "truncated at limit 2" in format_sql_result(res)


def test_sql_no_result_set():
    res = {"columns": [], "rows": [], "row_count": 0, "truncated": False, "limit": 100}
    assert "no result set" in format_sql_result(res)


def test_info_render():
    info = {
        "spark_version": "3.5.0",
        "master": "local[*]",
        "app_id": "local-123",
        "current_database": "default",
        "execution_count": 4,
        "default_sql_limit": 100,
        "databases": ["default", "bronze"],
    }
    out = format_info(info)
    assert "spark_version: 3.5.0" in out
    assert "databases (2): default, bronze" in out
