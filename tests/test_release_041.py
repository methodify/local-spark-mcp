"""0.4.1 units: schema-qualified naming, explicit preload parsing, shadow naming."""

from local_spark_mcp.engine import SparkEngine


def test_qualified_entries():
    assert SparkEngine._qualified("test", "dbo/holidays") == ("test__dbo", "holidays")
    assert SparkEngine._qualified("test", "plain") == ("test", "plain")


def test_preload_tool_parses_explicit_tables():
    # mirrors the parsing in server.preload_lakehouses
    parts = ["silver.company", "test.dbo.holidays", "gold"]
    names = {}
    for n in parts:
        lh, _, rest = n.partition(".")
        names.setdefault(lh, []).append(rest.replace(".", "/") if rest else None)
    names = {lh: [t for t in ts if t] or None for lh, ts in names.items()}
    assert names == {"silver": ["company"], "test": ["dbo/holidays"], "gold": None}


def test_healthcheck_reports_protocol_version():
    from local_spark_mcp.healthcheck import healthcheck
    from local_spark_mcp.protocol import PROTOCOL_VERSION

    assert healthcheck(None)["protocol_version"] == PROTOCOL_VERSION == 2


def test_write_target_covers_dml_and_ctas():
    from local_spark_mcp.engine import _sql_write_target

    assert _sql_write_target("INSERT INTO lh.t SELECT 1") == "lh.t"
    assert _sql_write_target("insert overwrite table lh.t select 1") == "lh.t"
    assert _sql_write_target("MERGE INTO lh.t t USING s ON 1=1 WHEN MATCHED THEN DELETE") == "lh.t"
    assert _sql_write_target("UPDATE lh.t SET a = 1") == "lh.t" and _sql_write_target("DELETE FROM `lh`.`t` WHERE 1") == "`lh`.`t`"
    assert _sql_write_target("CREATE TABLE IF NOT EXISTS lh.c AS SELECT 1") == "lh.c"
    assert _sql_write_target("CREATE OR REPLACE TABLE lh.c AS SELECT 1") == "lh.c"
    assert _sql_write_target("SELECT * FROM lh.t") is None

