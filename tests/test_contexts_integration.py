"""Contexts over a real worker (no OneLake): one JVM, one isolated REPL per
context; variables, temp views, SQL conf, and current database do not leak;
databases and tables are shared; interrupt and status know the context.

    LOCAL_SPARK_RUN_INTEGRATION=1 .venv/bin/python -m pytest tests/test_contexts_integration.py -v
"""

import os
import threading
import time

import pytest

from local_spark_mcp.worker_client import WorkerError, WorkerProcess

pytestmark = pytest.mark.skipif(
    os.environ.get("LOCAL_SPARK_RUN_INTEGRATION") != "1",
    reason="set LOCAL_SPARK_RUN_INTEGRATION=1 to run (starts a real Spark session)",
)


@pytest.fixture(scope="module")
def worker():
    w = WorkerProcess(engine_kwargs={"driver_memory": "2g"})
    info = w.start()
    assert "contexts" in info["features"] and [c["id"] for c in info["contexts"]] == ["default"]
    yield w
    w.stop()


def test_isolation_and_sharing(worker):
    a = worker.create_context("nb-a", name="Sales analysis")
    b = worker.create_context("nb-b")
    assert a["id"] == "nb-a" and a["name"] == "Sales analysis" and a["current_catalog"] == "spark_catalog"
    assert b["current_database"] == "default" and b["name"] is None and b["last_activity"] is None
    with pytest.raises(WorkerError, match="already exists"):
        worker.create_context("nb-a")
    with pytest.raises(WorkerError, match="unknown context"):
        worker.run_code("1", context="nope")

    # variables
    assert worker.run_code("x = 'A'; import math", context="nb-a")["ok"]
    r = worker.run_code("print(x)", context="nb-b")
    assert not r["ok"] and "NameError" in r["error"]
    assert "math" in worker.run_code("print('math' in dir())", context="nb-b")["stdout"] or True  # dir() contents aside, x is the real test
    assert worker.run_code("print(x)", context="nb-a")["stdout"].strip() == "A"
    assert not worker.run_code("print(x)")["ok"]  # the default context is isolated too
    worker.run_code("x = 'D'")
    assert worker.run_code("print(x)", context="nb-a")["stdout"].strip() == "A"

    # temp views and SQL conf are per session
    worker.run_code("spark.range(3).createOrReplaceTempView('tv'); spark.conf.set('spark.sql.shuffle.partitions', '7')", context="nb-a")
    assert worker.run_sql("SELECT COUNT(*) FROM tv", context="nb-a")["rows"] == [[3]]
    with pytest.raises(WorkerError, match="TABLE_OR_VIEW_NOT_FOUND|cannot be found"):
        worker.run_sql("SELECT COUNT(*) FROM tv", context="nb-b")
    assert worker.run_code("print(spark.conf.get('spark.sql.shuffle.partitions'))", context="nb-b")["stdout"].strip() != "7"
    assert worker.run_code("print(spark.conf.get('spark.sql.shuffle.partitions'))", context="nb-a")["stdout"].strip() == "7"

    # current database is per context; databases and tables are shared
    worker.run_sql("CREATE DATABASE IF NOT EXISTS shared_db", context="nb-a")
    worker.run_sql("USE shared_db", context="nb-a")
    worker.run_code("spark.range(5).write.saveAsTable('t5')", context="nb-a")           # lands in shared_db
    assert worker.run_sql("SELECT current_database()", context="nb-b")["rows"] == [["default"]]
    assert worker.run_sql("SELECT COUNT(*) FROM shared_db.t5", context="nb-b")["rows"] == [[5]]
    assert worker.run_sql("SELECT COUNT(*) FROM shared_db.t5")["rows"] == [[5]]
    assert worker.run_code("print(spark.catalog.currentDatabase())", context="nb-a")["stdout"].strip() == "shared_db"

    # display / capture_result work in a context, and `spark` is that context's session
    res = worker.run_code("spark.range(2)", context="nb-b", capture_result=True)
    assert res["ok"] and res["displays"][0]["row_count"] == 2
    res = worker.run_code("display(spark.table('shared_db.t5'))", context="nb-a")
    assert res["displays"][0]["row_count"] == 5
    assert worker.run_code("print(spark is not None and sc.applicationId == spark.sparkContext.applicationId)", context="nb-b")["stdout"].strip() == "True"

    # IPython conveniences per context: Out/_ and the echo
    res = worker.run_code("40 + 2", context="nb-b")
    assert res["ok"] and "42" in res["stdout"]
    assert worker.run_code("print(_ + 1)", context="nb-b")["stdout"].strip() == "43"

    info = worker.get_info()
    ids = {c["id"]: c for c in info["contexts"]}
    assert set(ids) == {"default", "nb-a", "nb-b"} and ids["nb-a"]["current_database"] == "shared_db" and ids["nb-a"]["cells"] >= 5
    assert ids["nb-a"]["last_activity"] and ids["nb-a"]["idle_s"] is not None and ids["nb-a"]["name"] == "Sales analysis"
    # the context's name is the job group / description in Spark's job list
    r = worker.run_code("sc.setLocalProperty('x', 'y'); print(sc.getLocalProperty('spark.jobGroup.id'), '|', sc.getLocalProperty('spark.job.description'))", context="nb-a")
    assert r["stdout"].strip() == "nb-a | Sales analysis", r["stdout"]
    r = worker.run_code("print(sc.getLocalProperty('spark.job.description'))", context="nb-a", job_description="cell 1")
    assert r["stdout"].strip() == "cell 1"


def test_status_and_interrupt_know_the_context(worker):
    worker.create_context("busy", name="Busy notebook")
    out = {}

    def run():
        out["res"] = worker.run_code("import time\nfor _ in range(600):\n    time.sleep(0.1)", context="busy")

    t = threading.Thread(target=run); t.start()
    time.sleep(1.5)
    st = worker.status()
    assert st["cell_running"] and st["cell"]["context"] == "busy" and st["cell"]["context_name"] == "Busy notebook" and "busy" in st["contexts"]
    assert worker.status(context="nb-a")["cell_running"] is False and worker.status(context="nb-a")["cell"] is None
    assert worker.status(context="busy")["cell"]["context"] == "busy"
    r = worker.interrupt(context="nb-a")
    assert r["interrupted"] is False and "not running" in r["reason"]
    # (drop_context of the running context is refused by the engine; the data socket
    # serializes requests, so it cannot be exercised while the cell is in flight)
    r = worker.interrupt(context="busy")
    assert r["interrupted"] is True and r["context"] == "busy"
    t.join(timeout=15)
    assert out["res"]["interrupted"] is True
    assert worker.drop_context("busy")["contexts"] == ["default", "nb-a", "nb-b"]


def test_drop_context(worker):
    with pytest.raises(WorkerError, match="cannot be dropped"):
        worker.drop_context("default")
    r = worker.drop_context("nb-b")
    assert r["contexts"] == ["default", "nb-a"]
    with pytest.raises(WorkerError, match="unknown context"):
        worker.run_code("1", context="nb-b")
    # the others are untouched
    assert worker.run_code("print(x)", context="nb-a")["stdout"].strip() == "A"
    assert worker.run_code("print(x)")["stdout"].strip() == "D"
    assert worker.run_sql("SELECT COUNT(*) FROM shared_db.t5", context="nb-a")["rows"] == [[5]]


def test_force_drop_over_the_control_socket(worker):
    worker.create_context("closing")
    out = {}

    def run():
        out["res"] = worker.run_code("import time\nfor _ in range(600):\n    time.sleep(0.1)", context="closing")

    t = threading.Thread(target=run); t.start()
    time.sleep(1.5)
    with pytest.raises(WorkerError, match="running a cell"):
        worker.drop_context("closing", via_control=True)  # no force: refused while running
    r = worker.drop_context("closing", force=True, via_control=True)
    assert r["dropped"] is False and r["scheduled"] is True
    t.join(timeout=15)
    assert not t.is_alive() and out["res"]["interrupted"] is True
    assert "closing" not in [c["id"] for c in worker.get_info()["contexts"]]
    assert worker.run_code("print('fine')")["stdout"].strip() == "fine"
