"""Live: a lakehouse registered after init resolves on first touch like one the
session started with, and unregistering drops it from the catalog.

    LOCAL_SPARK_LIVE=1 LOCAL_SPARK_LIVE_WORKSPACE_ID=<guid> .venv/bin/python -m pytest tests/test_register_lakehouse_live.py -v
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


def test_register_after_init(tmp_path):
    from local_spark_mcp.discovery import FabricAPIClient
    from local_spark_mcp.engine import SparkEngine
    from local_spark_mcp.fabric import default_jar_path
    from local_spark_mcp.token_server import TokenServer

    target = next(lh for lh in FabricAPIClient().list_lakehouses(WS) if lh.name == LH)
    srv = TokenServer(); srv.start()
    # a Fabric session that knows no lakehouse yet
    eng = SparkEngine(driver_memory="4g", onelake={"endpoint": srv.url, "secret": srv.secret, "jar_path": default_jar_path()},
                      lakehouses=[], write_mode="sandbox", state_root=str(tmp_path / "state"))
    try:
        assert eng.info()["lakehouses"] == []
        r = eng.register_lakehouse({"name": target.name, "id": target.id, "workspace_id": target.workspace_id})
        assert r["name"] == LH and r["lakehouses"] == [LH]
        assert eng.spark.table(f"{LH}.{TABLE}").count() > 0            # first touch through the registered lakehouse
        assert eng.run_sql(f"SELECT COUNT(*) AS n FROM {LH}.{TABLE}").rows[0][0] > 0
        shadows = eng.shadow_status()["tables"]
        assert [s for s in shadows if s["table"] == TABLE and s["lakehouse"] == LH and s["state"] == "read" and s["cloned_at"]]
        assert all(s["registered"] is True for s in shadows if s["table"] == TABLE)
        assert sorted(e for e in eng.list_tables(LH) if e == TABLE) == [TABLE]
        r = eng.unregister_lakehouse(LH)
        assert r["lakehouses"] == [] and LH in r["dropped_databases"]
        with pytest.raises(Exception):
            eng.spark.table(f"{LH}.{TABLE}").count()
        # the shadow files stayed; re-registering lists them again, untouched by this catalog
        eng.register_lakehouse({"name": target.name, "id": target.id, "workspace_id": target.workspace_id})
        again = [s for s in eng.shadow_status()["tables"] if s["table"] == TABLE]
        assert again and again[0]["registered"] is False
        assert eng.spark.table(f"{LH}.{TABLE}").count() > 0           # re-links the existing shadow
    finally:
        eng.stop(); srv.stop()
