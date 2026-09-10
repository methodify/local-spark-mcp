"""End-to-end test of the worker subprocess + IPC. Spins up a real Spark
session, so it is slow and gated. Run with:

    LOCAL_SPARK_RUN_INTEGRATION=1 .venv/bin/python -m pytest tests/test_worker_integration.py -v
"""

import os
import tempfile
from pathlib import Path

import pytest

from local_spark_mcp.worker_client import WorkerError, WorkerProcess

pytestmark = pytest.mark.skipif(
    os.environ.get("LOCAL_SPARK_RUN_INTEGRATION") != "1",
    reason="set LOCAL_SPARK_RUN_INTEGRATION=1 to run (starts a real Spark session)",
)


@pytest.fixture(scope="module")
def worker():
    w = WorkerProcess(engine_kwargs={"driver_memory": "2g", "default_sql_limit": 5})
    info = w.start()
    from local_spark_mcp.profiles import current_profile

    assert info["spark_version"].startswith(current_profile().pyspark.rsplit(".", 1)[0])
    yield w
    w.stop()


def test_persistent_state_across_calls(worker):
    r1 = worker.run_code("x = 21")
    assert r1["ok"], r1
    r2 = worker.run_code("print(x * 2)")
    assert r2["ok"], r2
    assert "42" in r2["stdout"]


def test_error_is_captured_not_fatal(worker):
    r = worker.run_code("1/0")
    assert r["ok"] is False
    assert "ZeroDivisionError" in r["error"]
    # worker still alive and stateful afterwards
    assert worker.ping()["pong"] is True
    assert worker.run_code("print('still here')")["ok"]


def test_sql_roundtrip_and_truncation(worker):
    tmp = Path(tempfile.mkdtemp(prefix="worker-it-"))
    code = f"""
df = spark.createDataFrame([(i, f"n{{i}}") for i in range(10)], ["id", "name"])
df.write.format("delta").mode("overwrite").save("{(tmp / 'nums').as_posix()}")
spark.sql("CREATE TABLE IF NOT EXISTS nums USING DELTA LOCATION '{(tmp / 'nums').as_posix()}'")
"""
    assert worker.run_code(code)["ok"]
    res = worker.run_sql("SELECT * FROM nums ORDER BY id", limit=3)
    assert res["columns"] == ["id", "name"]
    assert res["row_count"] == 3
    assert res["truncated"] is True
    assert res["rows"][0] == [0, "n0"]


def test_sql_error_surfaces_as_worker_error(worker):
    with pytest.raises(WorkerError) as exc:
        worker.run_sql("SELECT * FROM does_not_exist")
    assert "AnalysisException" in str(exc.value) or "TABLE_OR_VIEW" in str(exc.value)


def test_spark_env_reaches_driver_and_workers(tmp_path):
    """[spark.env] vars reach the driver process AND Spark Python workers; a
    PYTHONPATH entry lets workers import a path-based lib (the native-lib gap)."""
    libdir = tmp_path / "lib"
    libdir.mkdir()
    (libdir / "lsm_fake.py").write_text("MARK = 'lsm-fake-ok'\n")

    w = WorkerProcess(
        engine_kwargs={
            "driver_memory": "2g",
            "env": {"LSM_TEST_VAR": "hello-env", "PYTHONPATH": str(libdir)},
        }
    )
    w.start()
    try:
        # Driver process sees the env vars (set before the JVM launched).
        r = w.run_code("import os; print('D', os.environ.get('LSM_TEST_VAR'), str(os.environ.get('PYTHONPATH','')).find('lib') >= 0)")
        assert r["ok"], r
        assert "D hello-env True" in r["stdout"]

        # Workers see the env var AND can import the path lib via executorEnv PYTHONPATH.
        r2 = w.run_code(
            """
def probe(rows):
    import os
    try:
        import lsm_fake
        m = lsm_fake.MARK
    except Exception as e:
        m = f"FAIL {type(e).__name__}"
    for _ in rows:
        yield (os.environ.get("LSM_TEST_VAR"), m)
        break
out = spark.range(2).repartition(1).rdd.mapPartitions(probe).collect()
print("W", out[0][0], out[0][1])
"""
        )
        assert r2["ok"], r2
        assert "W hello-env lsm-fake-ok" in r2["stdout"]
    finally:
        w.stop()


def test_restart_clears_state():
    w = WorkerProcess(engine_kwargs={"driver_memory": "2g"})
    w.start()
    try:
        assert w.run_code("secret = 99")["ok"]
        assert w.run_code("print(secret)")["ok"]
        w.restart()
        # fresh namespace: the name is gone
        r = w.run_code("print(secret)")
        assert r["ok"] is False
        assert "NameError" in r["error"]
    finally:
        w.stop()
