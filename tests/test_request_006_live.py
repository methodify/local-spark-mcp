"""REQUEST-006 live checks against the real workspace (sandbox; nothing reaches
OneLake): mixed-case table names, relative Files/ paths, DeltaTable builder on
an untouched table, restore_shadow, and ANSI parity under fabric-2.0.

    LOCAL_SPARK_LIVE=1 LOCAL_SPARK_LIVE_WORKSPACE_ID=<guid> .venv/bin/python -m pytest tests/test_request_006_live.py -v
"""

import os
from pathlib import Path

import pytest

from local_spark_mcp.profiles import current_profile
from tests.test_onelake_catalog_live import LH, TABLE, _engine, _run

pytestmark = pytest.mark.skipif(os.environ.get("LOCAL_SPARK_LIVE") != "1", reason="set LOCAL_SPARK_LIVE=1")

MIXED_LH = os.environ.get("LOCAL_SPARK_LIVE_MIXED_LAKEHOUSE", "dataverse")
MIXED_TABLE = os.environ.get("LOCAL_SPARK_LIVE_MIXED_TABLE", "GlobalOptionsetMetadata")
WHEEL = "lib/dwlib_noop-0.0-py3-none-any.whl"


def test_request_006(tmp_path):
    eng, srv = _engine(tmp_path, "sandbox", files_sync=[WHEEL], mirror_root=str(tmp_path / "mirror"))
    try:
        # (1) session confs follow the runtime: Runtime 2.0 runs ANSI off
        ansi = eng.spark.conf.get("spark.sql.ansi.enabled")
        assert ansi == "false", ansi
        out = _run(eng, "print('CAST', spark.sql(\"SELECT CAST('' AS BIGINT) AS v\").collect()[0][0])")
        assert "CAST None" in out, out

        # (2) mixed-case table names resolve, any casing, no mount step
        for spelled in (MIXED_TABLE, MIXED_TABLE.lower(), MIXED_TABLE.upper()):
            out = _run(eng, f"print('N', spark.table('{MIXED_LH}.{spelled}').count())")
            assert "N " in out, out

        # (3) relative Files/ resolves against the default lakehouse's mirror
        assert eng.spark_working_dir and Path(eng.spark_working_dir).name  # the lakehouse dir
        out = _run(eng, f"print('F', spark.read.format('binaryFile').load('Files/{WHEEL}').count())")
        assert "F 1" in out, out
        out = _run(eng, "spark.range(3).write.mode('overwrite').csv('Files/lsm_006_out'); import os; print('W', os.path.isdir('/lakehouse/default/Files/lsm_006_out'))")
        assert "W True" in out, out

        # (5) DeltaTable.createIfNotExists(...).tableName(<untouched table>).execute()
        out = _run(eng, f"from delta.tables import DeltaTable; dt = DeltaTable.createIfNotExists(spark).tableName('{LH}.{TABLE}').execute(); print('B', dt.toDF().count())")
        assert "B " in out and int(out.split("B ")[1]) > 0, out
        assert any(t["table"] == TABLE for t in eng.shadow_status()["tables"])

        # (6) restore_shadow rewinds to the clone commit without OneLake
        before = int(out.split("B ")[1])
        _run(eng, f"spark.sql('INSERT INTO {LH}.{TABLE} SELECT * FROM {LH}.{TABLE} LIMIT 2')")
        entry = next(t for t in eng.shadow_status()["tables"] if t["table"] == TABLE)
        assert entry["state"] == "written", entry
        res = eng.restore_shadow(f"{LH}.{TABLE}")
        assert res["state"] == "read" and res["removed_commits"] == 1, res
        out = _run(eng, f"print('R', spark.table('{LH}.{TABLE}').count())")
        assert int(out.split("R ")[1]) == before, (out, before)
        _run(eng, f"spark.sql('INSERT INTO {LH}.{TABLE} SELECT * FROM {LH}.{TABLE} LIMIT 1'); print('again ok')")
    finally:
        eng.stop()
        srv.stop()
