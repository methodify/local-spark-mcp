"""Tables whose Delta protocol declares deletionVectors (Link-to-Fabric mirrors)
resolve as live read-only views in sandbox/readonly, since Delta 3.2 cannot
shallow-clone them; writes are refused with an explanation (REQUEST-004).

    LOCAL_SPARK_LIVE=1 LOCAL_SPARK_LIVE_WORKSPACE_ID=<guid> .venv/bin/python -m pytest tests/test_deletion_vectors_live.py -v
"""

import os

import pytest

from tests.test_onelake_catalog_live import _engine, _run

pytestmark = pytest.mark.skipif(os.environ.get("LOCAL_SPARK_LIVE") != "1", reason="set LOCAL_SPARK_LIVE=1")

LH = os.environ.get("LOCAL_SPARK_LIVE_DV_LAKEHOUSE", "dataverse_l2f")
TABLE = os.environ.get("LOCAL_SPARK_LIVE_DV_TABLE", "custtable")


def test_deletion_vector_table_reads_as_live_view_and_refuses_writes(tmp_path):
    eng, srv = _engine(tmp_path, "sandbox", default_lakehouse=LH)
    try:
        out = _run(eng, f"n = spark.table('{LH}.{TABLE}').count(); print('Q', n)")
        assert "Q " in out and int(out.split("Q ")[1]) > 0, out
        assert eng.run_sql(f"SELECT COUNT(*) AS c FROM {TABLE}", limit=1).rows[0][0] > 0  # unqualified, default lakehouse

        status = eng.shadow_status()
        info = eng.info()
        if info["dv_strategy"] == "clone":  # Delta >= 3.3: a real shallow clone, full sandbox
            assert [(t["lakehouse"], t["table"], t["state"]) for t in status["tables"]] == [(LH, TABLE, "read")]
            assert status["deletion_vector_tables"] == []
            r = eng.run_code(f"spark.sql(\"INSERT INTO {LH}.{TABLE} SELECT * FROM {LH}.{TABLE} LIMIT 1\"); print('W', spark.table('{LH}.{TABLE}').count())")
            assert r.ok and "W " in r.stdout, r.stdout
            assert eng.shadow_status()["tables"][0]["state"] == "written"
            r = eng.run_code(f"from delta.tables import DeltaTable; print(DeltaTable.forName(spark, '{LH}.{TABLE}').toDF().count())")
            assert r.ok, r.stdout
        else:  # Delta 3.2: live read-only view
            assert [(t["lakehouse"], t["table"]) for t in status["deletion_vector_tables"]] == [(LH, TABLE)]
            assert status["tables"] == []  # no clone, no shadow dir
            assert f"{LH}.{TABLE}" in info["deletion_vector_tables"]

            # DeltaTable.forName (dwlib's MERGE gateway) is refused with the explanation
            r = eng.run_code(f"from delta.tables import DeltaTable; DeltaTable.forName(spark, '{LH}.{TABLE}')")
            assert not r.ok and "deletion vectors" in r.stdout and "read-only" in r.stdout, r.stdout

            # SQL writes: run_sql refuses up front; a spark.sql in a cell fails and is annotated
            with pytest.raises(Exception, match="deletion vectors"):
                eng.run_sql(f"MERGE INTO {TABLE} t USING (SELECT * FROM {TABLE} LIMIT 1) s ON t.Id = s.Id WHEN MATCHED THEN UPDATE SET *")
            r = eng.run_code(f"spark.sql(\"INSERT INTO {LH}.{TABLE} SELECT * FROM {LH}.{TABLE} LIMIT 1\")")
            assert not r.ok and "deletion vectors" in r.stdout, r.stdout
            r = eng.run_code(f"spark.range(1).write.mode('append').saveAsTable('{LH}.{TABLE}')")
            assert not r.ok and "deletion vectors" in r.stdout, r.stdout

        # protocol scan for the doctor
        feats = eng.table_features(LH, [TABLE])
        assert feats[TABLE]["deletion_vectors"] is True and "deletionVectors" in feats[TABLE]["features"]

        # copying it out is the documented escape hatch: a plain local table in the shadow
        _run(eng, f"spark.table('{LH}.{TABLE}').limit(5).write.saveAsTable('{LH}.lsm_dv_copy'); print('C', spark.table('{LH}.lsm_dv_copy').count())")
        assert any(t["table"] == "lsm_dv_copy" for t in eng.shadow_status()["tables"])
    finally:
        eng.stop()
        srv.stop()
