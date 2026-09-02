"""Live test of OneLakeCatalog + the write policy against a real workspace.
Requires `az login` and read access. Gated; set:

    LOCAL_SPARK_LIVE=1 LOCAL_SPARK_LIVE_WORKSPACE_ID=<guid> \
    LOCAL_SPARK_LIVE_LAKEHOUSE=customer LOCAL_SPARK_LIVE_TABLE=sources_name \
    .venv/bin/python -m pytest tests/test_onelake_catalog_live.py -v

Nothing here writes to OneLake: every write lands in a sandbox shadow.
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


def _engine(tmp_path, write_mode, **kw):
    from local_spark_mcp.discovery import FabricAPIClient
    from local_spark_mcp.engine import SparkEngine
    from local_spark_mcp.fabric import default_jar_path
    from local_spark_mcp.token_server import TokenServer

    client = FabricAPIClient()
    lakehouses = [
        {"name": lh.name, "id": lh.id, "workspace_id": lh.workspace_id}
        for lh in client.list_lakehouses(WS)
    ]
    srv = TokenServer()
    srv.start()
    eng = SparkEngine(
        driver_memory="4g",
        onelake={"endpoint": srv.url, "secret": srv.secret, "jar_path": default_jar_path()},
        lakehouses=lakehouses,
        write_mode=write_mode,
        state_root=str(tmp_path / "state"),
        **{"default_lakehouse": LH, **kw},
    )
    return eng, srv


def _run(eng, code):
    r = eng.run_code(code)
    assert r.ok, (r.error, (r.traceback or "")[-1500:])
    return r.stdout


def test_sandbox_resolves_without_mount_and_isolates_writes(tmp_path):
    eng, srv = _engine(tmp_path, "sandbox")
    try:
        assert eng.default_lakehouse == LH
        assert eng.shadow_status()["tables"] == []

        # 1) qualified read, no mount step -> materialized as a shadow clone
        out = _run(eng, f"n = spark.table('{LH}.{TABLE}').count(); print('Q', n)")
        assert "Q " in out and int(out.split("Q ")[1]) > 0
        shadows = eng.shadow_status()["tables"]
        assert [(t["lakehouse"], t["table"]) for t in shadows] == [(LH, TABLE)]
        assert os.path.isdir(os.path.join(shadows[0]["path"], "_delta_log"))

        # 2) unqualified read resolves against the default lakehouse
        out2 = _run(eng, f"print('U', spark.table('{TABLE}').count())")
        assert out2.split("U ")[1].strip() == out.split("Q ")[1].strip()

        # 3) run_sql, unqualified
        res = eng.run_sql(f"SELECT COUNT(*) AS c FROM {TABLE}", limit=1)
        assert res.rows[0][0] == int(out.split("Q ")[1])

        # 4) DeltaTable.forName (the V1 path dwlib's MERGE uses) works via the bridge
        _run(eng, f"from delta.tables import DeltaTable; dt = DeltaTable.forName(spark, '{LH}.{TABLE}'); print('F', dt.toDF().count())")

        # 5) a write lands in the local clone, not OneLake: one new local parquet
        before = [f for f in os.listdir(shadows[0]["path"]) if f.endswith(".parquet")]
        _run(eng, f"spark.sql('INSERT INTO {LH}.{TABLE} SELECT * FROM {LH}.{TABLE} LIMIT 1'); print('W', spark.table('{LH}.{TABLE}').count())")
        after = [f for f in os.listdir(shadows[0]["path"]) if f.endswith(".parquet")]
        assert len(after) == len(before) + 1

        # 6) a NEW table under a lakehouse namespace lands in the shadow and is queryable
        _run(eng, f"spark.range(3).write.saveAsTable('{LH}.lsm_catalog_probe')")
        assert eng.run_sql(f"SELECT COUNT(*) AS c FROM {LH}.lsm_catalog_probe").rows[0][0] == 3
        names = {(t["lakehouse"], t["table"]) for t in eng.shadow_status()["tables"]}
        assert (LH, "lsm_catalog_probe") in names

        # 7) a table that does not exist still raises the original not-found
        r = eng.run_code(f"spark.table('{LH}.definitely_not_a_table_xyz')")
        assert not r.ok and "TABLE_OR_VIEW_NOT_FOUND" in (r.error or "")

        # 8) discard_shadow clears, and the next touch re-clones
        assert eng.discard_shadow()["discarded"] >= 2
        assert eng.shadow_status()["tables"] == []
        _run(eng, f"print('R', spark.table('{LH}.{TABLE}').count())")
        assert [(t["lakehouse"], t["table"]) for t in eng.shadow_status()["tables"]] == [(LH, TABLE)]
    finally:
        eng.stop()
        srv.stop()


def test_readonly_refuses_create_and_forname(tmp_path):
    eng, srv = _engine(tmp_path, "readonly")
    try:
        _run(eng, f"print(spark.table('{LH}.{TABLE}').count())")  # reads work
        r = eng.run_code(f"spark.range(2).write.saveAsTable('{LH}.lsm_readonly_probe')")
        assert not r.ok and "readonly" in (r.error or "") and "LOCAL_SPARK_WRITE_MODE" in (r.error or "")
        r = eng.run_code(f"from delta.tables import DeltaTable; DeltaTable.forName(spark, '{LH}.{TABLE}')")
        assert not r.ok and "readonly" in (r.error or "")
    finally:
        eng.stop()
        srv.stop()
