"""Files mirror end to end against a real workspace (sandbox mode, so nothing is
written to OneLake). Pulls one tiny wheel from the `customer` lakehouse.

    LOCAL_SPARK_LIVE=1 LOCAL_SPARK_LIVE_WORKSPACE_ID=<guid> .venv/bin/python -m pytest tests/test_files_mirror_live.py -v
"""

import os
from pathlib import Path

import pytest

from tests.test_onelake_catalog_live import _engine  # noqa: E402

pytestmark = pytest.mark.skipif(os.environ.get("LOCAL_SPARK_LIVE") != "1", reason="set LOCAL_SPARK_LIVE=1")

LAKEHOUSE = os.environ.get("LOCAL_SPARK_LIVE_LAKEHOUSE", "customer")
WHEEL = os.environ.get("LOCAL_SPARK_LIVE_FILE", "lib/dwlib_noop-0.0-py3-none-any.whl")


def test_files_mirror_end_to_end(tmp_path):
    eng, srv = _engine(tmp_path, "sandbox", default_lakehouse=LAKEHOUSE, files_sync=[WHEEL],
                       mirror_root=str(tmp_path / "mirror"))
    marker = None
    try:
        info = eng.info()
        link = info["files_link"]
        mirror = Path(link["files_root"])
        assert mirror.is_dir() and mirror.parts[-1] == "Files"
        assert link["linked"], link  # this box: /lakehouse -> ~/src/mosaic/lakehouse (created on demand)
        rep = info["files_sync"][0]
        assert rep["transferred"] + rep["skipped"] >= 1 and not rep["errors"], rep
        assert (mirror / WHEEL).stat().st_size > 0

        # plain Python IO through the literal path, and the env var for code that avoids it
        r = eng.run_code(
            "import os\n"
            f"print(sorted(os.listdir('/lakehouse/default/Files/{Path(WHEEL).parent.as_posix()}')))\n"
            "print(os.environ['LOCAL_SPARK_FILES_ROOT'])"
        )
        assert Path(WHEEL).name in r.stdout and str(mirror) in r.stdout, r.stdout

        # sandbox: a marker-file write lands locally, never on OneLake
        r = eng.run_code(
            "open('/lakehouse/default/Files/_lsm_test_marker', 'w').write('x')\n"
            "print(mssparkutils.fs.exists('/lakehouse/default/Files/_lsm_test_marker'))\n"
            "print([f.name for f in mssparkutils.fs.ls('/lakehouse/default/Files/lib')])"
        )
        marker = mirror / "_lsm_test_marker"
        assert marker.is_file() and "True" in r.stdout and Path(WHEEL).name in r.stdout, r.stdout
        with pytest.raises(PermissionError, match="writethrough"):
            eng.sync_files(direction="push")

        again = eng.sync_files()  # second pull: cached
        assert again["transferred"] == 0 and again["skipped"] >= 1, again
    finally:
        if marker and marker.exists():
            marker.unlink()
        eng.stop()
        srv.stop()
