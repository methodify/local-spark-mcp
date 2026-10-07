"""Live: files_mode = "lazy" makes a session's relative `Files/` the default
lakehouse's OneLake Files/, per context; sandbox writes there are refused;
shadows and the mirror link keep working.

    LOCAL_SPARK_LIVE=1 LOCAL_SPARK_LIVE_WORKSPACE_ID=<guid> .venv/bin/python -m pytest tests/test_lazy_files_live.py -v
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
FILES_DIR = os.environ.get("LOCAL_SPARK_LIVE_FILES_DIR", "Files/lib")  # a Files/ subtree with at least one file


def test_lazy_files(tmp_path):
    from local_spark_mcp.discovery import FabricAPIClient
    from local_spark_mcp.engine import SparkEngine
    from local_spark_mcp.fabric import default_jar_path
    from local_spark_mcp.token_server import TokenServer

    all_lh = list(FabricAPIClient().list_lakehouses(WS))
    other = next(lh for lh in all_lh if lh.name != LH)
    entries = [{"name": lh.name, "id": lh.id, "workspace_id": lh.workspace_id, "detect_schemas": False} for lh in all_lh]
    srv = TokenServer(); srv.start()
    eng = SparkEngine(driver_memory="4g", onelake={"endpoint": srv.url, "secret": srv.secret, "jar_path": default_jar_path()},
                      lakehouses=entries, write_mode="sandbox", state_root=str(tmp_path / "state dir"),  # a space, like an app-data folder
                      default_lakehouse=LH, files_mode="lazy")
    try:
        info = eng.info()
        assert info["files_mode"] == "lazy" and "files_lazy" in info["features"]
        default_fs = eng.spark.conf.get("fs.defaultFS")
        assert default_fs.startswith("lakehouse://") and default_fs.endswith("onelake.dfs.fabric.microsoft.com"), default_fs
        # relative Files/ reads OneLake directly (nothing mirrored)
        n = eng.run_sql(f"SELECT COUNT(*) AS n FROM binaryFile.`{FILES_DIR}`").rows[0][0]
        assert n >= 1
        r = eng.run_code(f"paths = [x.path for x in spark.read.format('binaryFile').load('{FILES_DIR}').select('path').collect()]\nprint(paths[0])")
        assert r.ok and r.stdout.startswith(default_fs + "/"), r.stdout
        # shadows (file: URIs) and new tables still work with the lakehouse filesystem current
        assert eng.run_sql(f"SELECT COUNT(*) AS n FROM {LH}.{TABLE}").rows[0][0] > 0
        assert eng.run_code(f"spark.range(3).write.mode('overwrite').saveAsTable('{LH}.lazy_probe_tmp'); print(spark.table('{LH}.lazy_probe_tmp').count())").stdout.strip() == "3"
        # the clones are where the lister looks, with a space in the state path (0.6.2 regression)
        states = {(t["lakehouse"], t["table"]): t["state"] for t in eng.shadow_status()["tables"]}
        assert states.get((LH, TABLE)) == "read" and states.get((LH, "lazy_probe_tmp")) == "written", states
        assert eng.mount_table(LH, TABLE)["database"] == LH
        # sandbox: a Spark write under Files/ is refused before anything reaches OneLake
        r = eng.run_code("spark.range(1).write.mode('overwrite').csv('Files/_localspark_probe_out')")
        assert not r.ok and "write_mode=sandbox" in (r.error or "") + (r.traceback or ""), (r.error, (r.traceback or "")[-500:])
        # per context: another default lakehouse means another Files/
        eng.create_context("other", default_lakehouse=other.name)
        other_fs = eng.contexts["other"].spark.conf.get("fs.defaultFS")
        assert other_fs != default_fs and other.id in other_fs
        r = eng.run_code(f"print(spark.read.format('binaryFile').load('{FILES_DIR}').count())", context="other")
        assert (not r.ok) or r.stdout.strip() != str(n)  # not the same tree (usually: PATH_NOT_FOUND)
        assert eng.run_code(f"print(spark.read.format('binaryFile').load('{FILES_DIR}').count())").stdout.strip() == str(n)
        # the mirror link for Python IO is still there
        assert info["files_link"] and info["files_root"]
    finally:
        eng.stop(); srv.stop()
