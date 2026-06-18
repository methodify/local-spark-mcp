"""Unit tests for run_sql's auto-mount logic (no Spark): _resolve_lakehouse and
_automount_missing parse/decide correctly. The live end-to-end behavior is
exercised against real OneLake separately."""

from local_spark_mcp.discovery import LakehouseInfo
from local_spark_mcp.engine import SparkEngine


def make_engine(lakehouses):
    eng = SparkEngine.__new__(SparkEngine)  # bypass __init__ (no Spark)
    eng.lakehouses = {n: LakehouseInfo(n, f"id-{n}", "ws") for n in lakehouses}
    eng._mounted = {}
    eng.calls = []

    def fake_mount(lh, table):
        eng._mounted.setdefault(lh, set()).add(table)
        eng.calls.append((lh, table))
        return {"lakehouse": lh, "table": table}

    eng.mount_table = fake_mount
    return eng


class FakeExc(Exception):
    pass


NOT_FOUND = "[TABLE_OR_VIEW_NOT_FOUND] The table or view `{}`.`{}` cannot be found."


def test_mounts_known_lakehouse_table():
    eng = make_engine(["customer", "silver"])
    assert eng._automount_missing(FakeExc(NOT_FOUND.format("customer", "sources_name"))) is True
    assert eng.calls == [("customer", "sources_name")]


def test_ignores_unknown_lakehouse():
    eng = make_engine(["customer"])
    assert eng._automount_missing(FakeExc(NOT_FOUND.format("randomdb", "t"))) is False
    assert eng.calls == []


def test_case_insensitive_resolves_to_registered_name():
    eng = make_engine(["Customer"])
    assert eng._automount_missing(FakeExc(NOT_FOUND.format("customer", "x"))) is True
    assert eng.calls == [("Customer", "x")]  # mounts under the registered name


def test_guard_prevents_remount_loop():
    eng = make_engine(["customer"])
    eng._mounted = {"customer": {"sources_name"}}
    # already mounted but still reported missing -> don't remount -> let it raise
    assert eng._automount_missing(FakeExc(NOT_FOUND.format("customer", "sources_name"))) is False
    assert eng.calls == []


def test_non_table_error_is_ignored():
    eng = make_engine(["customer"])
    assert eng._automount_missing(FakeExc("AMBIGUOUS_REFERENCE or a syntax error")) is False


def test_resolve_lakehouse_exact_and_ci():
    eng = make_engine(["customer", "Mosaic_History"])
    assert eng._resolve_lakehouse("customer").name == "customer"
    assert eng._resolve_lakehouse("mosaic_history").name == "Mosaic_History"
    assert eng._resolve_lakehouse("nope") is None
