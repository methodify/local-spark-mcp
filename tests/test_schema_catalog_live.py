"""Live: a lakehouse registered with a schema catalog (`spark.sql.catalog.<lh>`)
must still materialize OneLake tables while that V2 catalog is the session's
current catalog (`USE <lh>`), which is what Cobalt's schema-enabled lakehouse hit
in 0.4.1. `customer` has no schemas on OneLake; the host-declared `schemas` list
registers the catalog anyway, and ns [] maps to the lakehouse's top-level tables.

    LOCAL_SPARK_LIVE=1 LOCAL_SPARK_LIVE_WORKSPACE_ID=<guid> .venv/bin/python -m pytest tests/test_schema_catalog_live.py -v
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


def test_materialization_with_a_v2_catalog_current(tmp_path):
    from local_spark_mcp.discovery import FabricAPIClient
    from local_spark_mcp.engine import SparkEngine
    from local_spark_mcp.fabric import default_jar_path
    from local_spark_mcp.token_server import TokenServer

    lakehouses = []
    for lh in FabricAPIClient().list_lakehouses(WS):
        entry = {"name": lh.name, "id": lh.id, "workspace_id": lh.workspace_id, "detect_schemas": False}
        if lh.name == LH:
            entry.update(schemas=["dbo"], default_schema="dbo")  # host-declared, as Cobalt does
        lakehouses.append(entry)
    srv = TokenServer(); srv.start()
    eng = SparkEngine(driver_memory="4g", onelake={"endpoint": srv.url, "secret": srv.secret, "jar_path": default_jar_path()},
                      lakehouses=lakehouses, write_mode="sandbox", state_root=str(tmp_path / "state"), default_lakehouse=LH)
    try:
        spark = eng.spark
        info = eng.info()
        assert info["lakehouse_schemas"] == {LH: ["dbo"]}
        # the default lakehouse selects its default schema's database, in spark_catalog
        assert info["current_catalog"] == "spark_catalog" and info["current_database"] == f"{LH}__dbo"
        assert spark.conf.get(f"spark.sql.catalog.{LH}") == "ch.fs.OneLakeSchemaCatalog"
        top = [t for t in eng.list_tables(LH) if "/" not in t and t != TABLE]
        assert len(top) >= 3, top
        t_sql, t_mount, t_py = top[:3]

        # first touch through the V2 catalog while spark_catalog is current
        assert spark.table(f"{LH}.{TABLE}").count() > 0
        # now the trap: the V2 catalog is current
        spark.sql(f"USE {LH}")
        assert spark.sql("SELECT current_catalog(), current_schema()").first()[:2] == (LH, "dbo")
        assert eng.run_sql(f"SELECT COUNT(*) AS n FROM {LH}.{t_sql}").rows[0][0] >= 0          # SQL first touch
        mounted = eng.mount_table(LH, t_mount)                                                   # explicit mount
        assert mounted["table"] == t_mount, mounted
        assert spark.table(f"{LH}.{t_py}").columns                                               # DataFrame first touch
        assert spark.table(f"spark_catalog.{LH}.{t_py}").columns
        names = {s["table"] for s in eng.shadow_status()["tables"]}
        assert {TABLE, t_sql, t_mount, t_py} <= names, names
        assert eng.run_sql(f"SHOW TABLES IN {LH}").row_count >= 4
        assert eng.preload_status()["state"] in ("idle", "done")
        spark.sql("USE spark_catalog.default")
    finally:
        eng.stop(); srv.stop()
