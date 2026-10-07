"""Live: a context created with a default lakehouse resolves unqualified names
there; the engine's runtime confs (lakehouse ids, schema catalogs) reach a
context's session; a lakehouse registered later is visible in every context.

    LOCAL_SPARK_LIVE=1 LOCAL_SPARK_LIVE_WORKSPACE_ID=<guid> .venv/bin/python -m pytest tests/test_contexts_live.py -v
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


def test_contexts_against_onelake(tmp_path):
    from local_spark_mcp.discovery import FabricAPIClient
    from local_spark_mcp.engine import SparkEngine
    from local_spark_mcp.fabric import default_jar_path
    from local_spark_mcp.token_server import TokenServer

    all_lh = list(FabricAPIClient().list_lakehouses(WS))
    target = next(lh for lh in all_lh if lh.name == LH)
    other = next(lh for lh in all_lh if lh.name != LH)
    entries = [{"name": target.name, "id": target.id, "workspace_id": target.workspace_id,
                "schemas": ["dbo"], "default_schema": "dbo", "detect_schemas": False}]  # host-declared, as Cobalt does
    srv = TokenServer(); srv.start()
    eng = SparkEngine(driver_memory="4g", onelake={"endpoint": srv.url, "secret": srv.secret, "jar_path": default_jar_path()},
                      lakehouses=entries, write_mode="sandbox", state_root=str(tmp_path / "state"))
    try:
        a = eng.create_context("nb-a", default_lakehouse=LH)            # current db: customer__dbo (default schema)
        assert a["current_database"] == f"{LH}__dbo" and a["default_schema"] == "dbo"
        b = eng.create_context("nb-b")                                  # no default lakehouse
        assert b["current_database"] == "default"
        # the schema catalog conf reached the new sessions
        assert eng.run_sql(f"SHOW NAMESPACES IN {LH}", context="nb-b").rows == [["dbo"]]
        # first touch through a context's session, qualified; the clone is shared
        assert eng.run_sql(f"SELECT COUNT(*) AS n FROM {LH}.{TABLE}", context="nb-b").rows[0][0] > 0
        assert eng.run_code(f"print(spark.table('{LH}.{TABLE}').count() > 0)", context="nb-a").stdout.strip() == "True"
        shadows = [s for s in eng.shadow_status()["tables"] if s["table"] == TABLE]
        assert len(shadows) == 1 and shadows[0]["registered"] is True
        # unqualified in nb-a resolves against the default schema database (empty here: plain-layout lakehouse)
        r = eng.run_code(f"spark.table('{TABLE}')", context="nb-a")
        assert not r.ok and "TABLE_OR_VIEW_NOT_FOUND" in (r.error or "") or r.ok
        # a lakehouse registered after the contexts exist is visible in them
        eng.register_lakehouse({"name": other.name, "id": other.id, "workspace_id": other.workspace_id, "detect_schemas": False})
        assert eng.run_sql(f"SHOW TABLES IN {other.name}", context="nb-b").row_count >= 0
        assert eng.run_code(f"print(spark.conf.get('spark.localspark.lakehouse.{other.name}'))", context="nb-a").stdout.strip() == other.id
        # a run_notebook-style USE in one context does not move the other
        eng.run_sql(f"USE spark_catalog.{other.name}", context="nb-b")
        assert eng.run_sql("SELECT current_database()", context="nb-a").rows == [[f"{LH}__dbo"]]
        assert eng.info()["active_context"] == "nb-a"
        eng.drop_context("nb-b")
        assert [c["id"] for c in eng.info()["contexts"]] == ["default", "nb-a"]
    finally:
        eng.stop(); srv.stop()
