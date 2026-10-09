"""Live: SHOW TABLES / listTables on a lakehouse answers from OneLake (every
table the lakehouse has), merged with what the session catalog holds, with
nothing mounted; a touched table is listed once.

    LOCAL_SPARK_LIVE=1 LOCAL_SPARK_LIVE_WORKSPACE_ID=<guid> .venv/bin/python -m pytest tests/test_catalog_listing_live.py -v
"""

import os

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("LOCAL_SPARK_LIVE") != "1" or not os.environ.get("LOCAL_SPARK_LIVE_WORKSPACE_ID"),
    reason="set LOCAL_SPARK_LIVE=1 and LOCAL_SPARK_LIVE_WORKSPACE_ID to run against a real workspace",
)

WS = os.environ.get("LOCAL_SPARK_LIVE_WORKSPACE_ID", "")
LH = os.environ.get("LOCAL_SPARK_LIVE_LAKEHOUSE", "customer")
TABLE = os.environ.get("LOCAL_SPARK_LIVE_TABLE", "sources_name")


def test_show_tables_lists_onelake(tmp_path):
    from local_spark_mcp.discovery import FabricAPIClient
    from local_spark_mcp.engine import SparkEngine
    from local_spark_mcp.fabric import default_jar_path
    from local_spark_mcp.token_server import TokenServer

    entries = [{"name": lh.name, "id": lh.id, "workspace_id": lh.workspace_id, "detect_schemas": False}
               for lh in FabricAPIClient().list_lakehouses(WS)]
    srv = TokenServer(); srv.start()
    eng = SparkEngine(driver_memory="4g", onelake={"endpoint": srv.url, "secret": srv.secret, "jar_path": default_jar_path()},
                      lakehouses=entries, write_mode="sandbox", state_root=str(tmp_path / "state"), default_lakehouse=LH)
    try:
        remote = {e.lower() for e in eng.list_tables(LH) if "/" not in e}
        assert len(remote) > 10
        assert eng.shadow_status()["tables"] == []
        shown = {r[1].lower() for r in eng.run_sql(f"SHOW TABLES IN {LH}", limit=10000).rows}
        assert shown >= remote, remote - shown                      # everything the lakehouse has, with nothing mounted
        assert eng.shadow_status()["tables"] == []                  # listing mounted nothing
        assert eng.info()["shadows"] == []                          # and neither does the session snapshot
        # (spark.catalog.listTables() is not checked here: Spark's CatalogImpl loads every
        # table it lists, which materializes all of them; SHOW TABLES does not.)
        # a touched table (now a clone in the session catalog) is listed once, and a new local table shows up too
        assert eng.run_sql(f"SELECT COUNT(*) FROM {LH}.{TABLE}").rows[0][0] > 0
        eng.run_code(f"spark.range(2).write.saveAsTable('{LH}.listing_probe_tmp')")
        names = [r[1].lower() for r in eng.run_sql(f"SHOW TABLES IN {LH}", limit=10000).rows]
        assert names.count(TABLE) == 1 and "listing_probe_tmp" in names
        # unqualified SHOW TABLES with the default lakehouse current
        assert {r[1].lower() for r in eng.run_sql("SHOW TABLES", limit=10000).rows} >= remote
    finally:
        eng.stop(); srv.stop()
