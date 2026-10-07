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


def test_fs_members_on_the_mirror_and_refusal_on_onelake(tmp_path):
    """mkdirs/put/head/append/cp/mv/rm on /lakehouse paths (local mirror) and the
    sandbox refusal for the same calls on abfss:// paths."""
    from local_spark_mcp.discovery import FabricAPIClient
    from local_spark_mcp.engine import SparkEngine
    from local_spark_mcp.fabric import default_jar_path
    from local_spark_mcp.token_server import TokenServer

    client = FabricAPIClient()
    lakehouses = [{"name": lh.name, "id": lh.id, "workspace_id": lh.workspace_id, "detect_schemas": False} for lh in client.list_lakehouses(WS)]
    target = next(lh for lh in lakehouses if lh["name"] == LH)
    srv = TokenServer(); srv.start()
    eng = SparkEngine(driver_memory="4g", onelake={"endpoint": srv.url, "secret": srv.secret, "jar_path": default_jar_path()},
                      lakehouses=lakehouses, write_mode="sandbox", state_root=str(tmp_path / "state"),
                      mirror_root=str(tmp_path / "mirror"), default_lakehouse=LH, files_mode="lazy")

    def run(code):
        r = eng.run_code(code)
        assert r.ok, (r.error, (r.traceback or "")[-1200:])
        return r.stdout.strip()

    try:
        base = "/lakehouse/default/Files/_lsm_fs_probe"
        out = run(f"""
fs = notebookutils.fs
fs.mkdirs({base!r} + "/d")
fs.put({base!r} + "/a.txt", "hello")
fs.append({base!r} + "/a.txt", " world")
fs.cp({base!r} + "/a.txt", {base!r} + "/d/b.txt")
fs.mv({base!r} + "/d/b.txt", {base!r} + "/c.txt")
print(fs.head({base!r} + "/a.txt"), "|", sorted(e.name for e in fs.ls({base!r})), "|", fs.exists({base!r} + "/d/b.txt"))
fs.rm({base!r}, recurse=True)
print(fs.exists({base!r}))
""")
        assert out.splitlines() == ["hello world | ['a.txt', 'c.txt', 'd'] | False", "False"]
        abfss = f"abfss://{target['workspace_id']}@onelake.dfs.fabric.microsoft.com/{target['id']}/Files/_lsm_fs_probe.txt"
        r = eng.run_code(f"notebookutils.fs.put({abfss!r}, 'x')")
        assert not r.ok and "write_mode is 'sandbox'" in (r.error or ""), r.error
        assert eng.onelake_is_dir(abfss) is None  # OneLake untouched
        assert "lib" in run(f"print([e.name for e in notebookutils.fs.ls('abfss://{target['workspace_id']}@onelake.dfs.fabric.microsoft.com/{target['id']}/Files')])")
    finally:
        eng.stop(); srv.stop()

