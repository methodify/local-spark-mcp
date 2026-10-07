"""0.4.3 over a real worker (no OneLake): job_description reaches Spark's job
list, status reports idle_s, lakehouses can be registered and unregistered
after init.

    LOCAL_SPARK_RUN_INTEGRATION=1 .venv/bin/python -m pytest tests/test_release_043_integration.py -v
"""

import os
import threading
import time

import pytest

from local_spark_mcp.worker_client import WorkerProcess

pytestmark = pytest.mark.skipif(
    os.environ.get("LOCAL_SPARK_RUN_INTEGRATION") != "1",
    reason="set LOCAL_SPARK_RUN_INTEGRATION=1 to run (starts a real Spark session)",
)


@pytest.fixture(scope="module")
def worker():
    from local_spark_mcp.fabric import default_jar_path

    # the catalog jar, so a schema-enabled registration can load ch.fs.OneLakeSchemaCatalog
    w = WorkerProcess(engine_kwargs={"driver_memory": "2g", "extra_jars": [default_jar_path()]})
    w.start()
    yield w
    w.stop()


def test_job_description_and_idle(worker):
    time.sleep(1.2)
    st = worker.status()
    assert st["cell_running"] is False and st["idle_s"] >= 1.0 and st["cell"] is None
    out = {}

    def run():
        out["res"] = worker.run_code("spark.range(10**14).selectExpr('sum(id % 7)').collect()", job_description="cell 3: big sum")

    t = threading.Thread(target=run); t.start()
    time.sleep(4)
    st = worker.status()
    assert st["idle_s"] is None and st["cell"]["jobs"], st
    assert st["cell"]["jobs"][0]["description"] == "cell 3: big sum", st["cell"]["jobs"]
    worker.interrupt(); t.join(timeout=60)
    assert not t.is_alive()
    # the description does not leak into the next cell's jobs
    res = worker.run_code("print(spark.sparkContext.getLocalProperty('spark.job.description'))")
    assert res["stdout"].strip() == "None"
    assert worker.run_sql("SELECT 1 AS one", job_description="q")["rows"] == [[1]]
    time.sleep(0.6)
    assert worker.status()["idle_s"] is not None and worker.status()["idle_s"] < 10


def test_register_and_unregister_lakehouse(worker):
    info = worker.get_info()
    assert "late" not in info["lakehouses"]
    r = worker.register_lakehouse({"name": "late", "id": "00000000-0000-0000-0000-00000000aaaa",
                                   "workspace_id": "00000000-0000-0000-0000-00000000bbbb", "schemas": ["dbo"], "detect_schemas": False})
    assert r["name"] == "late" and r["schemas"] == ["dbo"] and "late" in r["lakehouses"]
    info = worker.get_info()
    assert "late" in info["lakehouses"] and info["lakehouse_schemas"] == {"late": ["dbo"]}
    assert {"late", "late__dbo"} <= set(info["databases"])
    assert worker.run_sql("SHOW TABLES IN late.dbo")["row_count"] == 0  # the schema catalog is live
    with pytest.raises(Exception, match="already registered"):
        worker.register_lakehouse({"name": "late", "id": "other", "workspace_id": "x"})
    r = worker.unregister_lakehouse("late")
    assert "late" not in r["lakehouses"] and set(r["dropped_databases"]) == {"late", "late__dbo"}
    info = worker.get_info()
    assert "late" not in info["lakehouses"] and "late" not in info["databases"] and "late__dbo" not in info["databases"]
    with pytest.raises(Exception, match="unknown lakehouse"):
        worker.unregister_lakehouse("late")
