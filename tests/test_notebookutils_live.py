"""notebookutils members that need Fabric: variableLibrary.getLibrary and
fs.ls/exists over OneLake. Gated like test_onelake_catalog_live. Prints only
variable NAMES, never values."""

import os

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("LOCAL_SPARK_LIVE") != "1" or not os.environ.get("LOCAL_SPARK_LIVE_WORKSPACE_ID"),
    reason="set LOCAL_SPARK_LIVE=1 and LOCAL_SPARK_LIVE_WORKSPACE_ID to run against a real workspace",
)
WS = os.environ.get("LOCAL_SPARK_LIVE_WORKSPACE_ID", "")
LH = os.environ.get("LOCAL_SPARK_LIVE_LAKEHOUSE", "customer")
TABLE = os.environ.get("LOCAL_SPARK_LIVE_TABLE", "sources_name")
VARLIB = os.environ.get("LOCAL_SPARK_LIVE_VARLIB", "Variables_AIServices")


def test_variable_library_and_onelake_fs(tmp_path):
    from tests.test_onelake_catalog_live import _engine

    eng, srv = _engine(tmp_path, "sandbox")
    try:
        r = eng.run_code(f"lib = notebookutils.variableLibrary.getLibrary({VARLIB!r}); names = sorted(vars(lib)); print('VARS', len(names))")
        assert r.ok, r.error
        assert int(r.stdout.split("VARS ")[1]) >= 1
        r = eng.run_code("notebookutils.variableLibrary.getLibrary('no-such-library-xyz')")
        assert not r.ok and "not found" in r.error

        lh = eng.lakehouses[LH]
        tables = f"abfss://{WS}@onelake.dfs.fabric.microsoft.com/{lh.id}/Tables"
        r = eng.run_code(f"e = mssparkutils.fs.ls({tables!r}); print('LS', any(x.name == {TABLE!r} and x.isDir for x in e), len(e))")
        assert r.ok and "LS True" in r.stdout
        r = eng.run_code(f"print('EX', mssparkutils.fs.exists({tables + '/' + TABLE!r}), mssparkutils.fs.exists({tables + '/nope_xyz'!r}))")
        assert r.ok and "EX True False" in r.stdout
        r = eng.run_code("print('CTX', notebookutils.runtime.context['defaultLakehouseName'])")
        assert r.ok and f"CTX {LH}" in r.stdout
    finally:
        eng.stop()
        srv.stop()
