"""Concurrent first touch of one table is serialized: N threads, one clone, every
read succeeds (REQUEST-008, ADO #301)."""

import os

import pytest

from tests.test_onelake_catalog_live import LH, TABLE, _engine

pytestmark = pytest.mark.skipif(os.environ.get("LOCAL_SPARK_LIVE") != "1", reason="set LOCAL_SPARK_LIVE=1")

CELL = """
from concurrent.futures import ThreadPoolExecutor
def touch(i):
    return spark.read.table('{t}').count()
with ThreadPoolExecutor(max_workers=6) as ex:
    counts = list(ex.map(touch, range(6)))
print('COUNTS', counts)
"""


def test_six_threads_one_clone(tmp_path):
    eng, srv = _engine(tmp_path, "sandbox")
    try:
        for table in (f"{LH}.{TABLE}", "dataverse.GlobalOptionsetMetadata", "silver.company"):
            res = eng.run_code(CELL.format(t=table))
            assert res.ok, (table, res.stdout[-1500:])
            counts = eval(res.stdout.split("COUNTS ")[1].splitlines()[0])
            assert len(set(counts)) == 1 and counts[0] > 0, counts
            mounts = [n for n in res.notices if n.startswith("mounted")]
            assert len(mounts) == 1, res.notices  # one clone, not six
        shadows = eng.shadow_status()["tables"]
        assert all(t["state"] == "read" for t in shadows), shadows
    finally:
        eng.stop()
        srv.stop()
