"""Regression guard for SPARK-45592 / SPARK-45282 (ADO #282): joins over
persisted, key-repartitioned frames lost rows on Spark 3.5.0 because an
AQE-coalesced cached read claimed a plain HashPartitioning and the join skipped
its exchange. Runs against the real local session (no Fabric).

    LOCAL_SPARK_RUN_INTEGRATION=1 .venv/bin/python -m pytest tests/test_cached_join_correctness.py -v
"""

import os

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("LOCAL_SPARK_RUN_INTEGRATION") != "1",
    reason="set LOCAL_SPARK_RUN_INTEGRATION=1 to run (starts a real Spark session)",
)

CELL = """
from pyspark import StorageLevel
n = 400000
results = {}
for thr in ("10485760", "-1"):
    for ccp in ("true", "false"):
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", thr)
        spark.conf.set("spark.sql.optimizer.canChangeCachedPlanOutputPartitioning", ccp)
        spark.catalog.clearCache()
        keys = spark.range(n).select(F.sha1(F.col("id").cast("string")).alias("key"), F.col("id"))
        left = keys.select("key", F.lit(1).alias("v1")).repartition(F.col("key")).persist(StorageLevel.MEMORY_AND_DISK); left.count()
        right = keys.where(F.col("id") % 7 == 0).select("key", F.col("id").alias("v2")).repartition(F.col("key")).persist(StorageLevel.MEMORY_AND_DISK); right.count()
        agg = left.groupBy("key").agg(F.count("*").alias("c")).persist(StorageLevel.MEMORY_AND_DISK); agg.count()
        u = left.join(agg, "key").unionByName(left.join(right, "key").select("key", "v1").join(agg, "key"))
        results[(thr, ccp)] = (left.join(right, "key").count(), u.count())
expect = (n // 7 + (1 if n % 7 else 0), n + n // 7 + (1 if n % 7 else 0))
print("RESULTS", {k: v for k, v in results.items()}, "EXPECT", expect)
print("OK" if all(v == expect for v in results.values()) else "WRONG")
"""


def test_joins_over_persisted_frames_keep_every_row():
    from local_spark_mcp.engine import SparkEngine

    eng = SparkEngine(driver_memory="3g")
    try:
        r = eng.run_code(CELL)
        assert r.ok, r.stdout
        assert "\nOK" in r.stdout, r.stdout
    finally:
        eng.stop()
