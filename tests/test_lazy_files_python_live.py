"""Live: under files_mode = "lazy", Python IO on /lakehouse/default/Files/... works
against OneLake with nothing synced: listings and predicates from OneLake
metadata, a file fetched on first open, writes kept local in sandbox, and
mirror_status / clear_mirror reporting and undoing it. Runs through a worker
cell so IPython's own `open` is covered.

    LOCAL_SPARK_LIVE=1 LOCAL_SPARK_LIVE_WORKSPACE_ID=<guid> .venv/bin/python -m pytest tests/test_lazy_files_python_live.py -v
"""

import os

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("LOCAL_SPARK_LIVE") != "1" or not os.environ.get("LOCAL_SPARK_LIVE_WORKSPACE_ID"),
    reason="set LOCAL_SPARK_LIVE=1 and LOCAL_SPARK_LIVE_WORKSPACE_ID to run against a real workspace",
)

WS = os.environ.get("LOCAL_SPARK_LIVE_WORKSPACE_ID", "")
LH = os.environ.get("LOCAL_SPARK_LIVE_LAKEHOUSE", "customer")
FILES_DIR = os.environ.get("LOCAL_SPARK_LIVE_FILES_SUBDIR", "lib")  # a Files/ subdirectory holding at least one file


def test_python_io_without_sync(tmp_path):
    from local_spark_mcp.discovery import FabricAPIClient
    from local_spark_mcp.engine import SparkEngine
    from local_spark_mcp.fabric import default_jar_path
    from local_spark_mcp.token_server import TokenServer

    entries = [{"name": lh.name, "id": lh.id, "workspace_id": lh.workspace_id, "detect_schemas": False}
               for lh in FabricAPIClient().list_lakehouses(WS)]
    srv = TokenServer(); srv.start()
    eng = SparkEngine(driver_memory="4g", onelake={"endpoint": srv.url, "secret": srv.secret, "jar_path": default_jar_path()},
                      lakehouses=entries, write_mode="sandbox", state_root=str(tmp_path / "state"),
                      mirror_root=str(tmp_path / "mirror"), default_lakehouse=LH, files_mode="lazy")

    def run(code):
        r = eng.run_code(code)
        assert r.ok, (r.error, (r.traceback or "")[-1200:])
        return r.stdout.strip()

    try:
        assert eng.info()["files_hooks"] is True
        mirror_dir = eng.files.mirror_dir(LH)
        assert not (mirror_dir / FILES_DIR).exists()  # nothing synced
        # listing and predicates come from OneLake metadata
        names = run(f"import os; print(sorted(os.listdir('/lakehouse/default/Files')))")
        assert FILES_DIR in names
        assert run(f"import os; print(os.path.isdir('/lakehouse/default/Files/{FILES_DIR}'), os.path.exists('/lakehouse/default/Files/__nope__'))") == "True False"
        files = eng.files.remote_list(LH, FILES_DIR)
        first = next(e for e in files if not e["is_dir"])
        assert not (mirror_dir / FILES_DIR).exists()
        # first open fetches exactly that file
        size = run(f"from pathlib import Path\np = Path('/lakehouse/default/Files/{FILES_DIR}/{first['name']}')\nprint(p.stat().st_size, len(p.read_bytes()))")
        assert size == f"{first['size']} {first['size']}"
        assert (mirror_dir / FILES_DIR / first["name"]).is_file()
        assert len(list((mirror_dir / FILES_DIR).iterdir())) == 1
        st = eng.mirror_status()["lakehouses"][LH]
        assert st["fetched_files"] == 1 and st["fetched_bytes"] == first["size"] and st["fetched"] == [f"{FILES_DIR}/{first['name']}"]
        # a sandbox write stays local
        run("with open('/lakehouse/default/Files/_localspark_probe/hello.txt', 'w') as f:\n    f.write('hi')\nprint(open('/lakehouse/default/Files/_localspark_probe/hello.txt').read())")
        assert (mirror_dir / "_localspark_probe" / "hello.txt").read_text() == "hi"
        assert eng.files.remote_stat(LH, "_localspark_probe/hello.txt") is None  # OneLake untouched
        assert "_localspark_probe" in run("import os; print(sorted(os.listdir('/lakehouse/default/Files')))")
        # a missing file names the sync_files call
        r = eng.run_code(f"open('/lakehouse/default/Files/{FILES_DIR}/__missing__.bin')")
        assert not r.ok and "sync_files(paths=['" + FILES_DIR + "']" in (r.error or "") + (r.traceback or "")
        # clear_mirror undoes the fetch; the next read fetches again
        eng.clear_mirror(LH, [FILES_DIR])
        assert not (mirror_dir / FILES_DIR).exists() and eng.mirror_status()["lakehouses"][LH]["fetched_files"] == 0
        assert run(f"print(len(open('/lakehouse/default/Files/{FILES_DIR}/{first['name']}', 'rb').read()))") == str(first["size"])
        # a context with another default lakehouse sees its own Files/
        other = next(e["name"] for e in entries if e["name"] != LH)
        eng.create_context("other", default_lakehouse=other)
        r = eng.run_code(f"import os; print(sorted(os.listdir('/lakehouse/default/Files')))", context="other")
        assert r.ok and r.stdout.strip() != names
    finally:
        eng.stop(); srv.stop()
