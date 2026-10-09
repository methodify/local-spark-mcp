"""Protocol v2 over a real worker: streaming events, interrupt of a sleeping
cell and of a Spark job, Arrow run_sql, display(df), idle interrupt.

    LOCAL_SPARK_RUN_INTEGRATION=1 .venv/bin/python -m pytest tests/test_protocol_v2_integration.py -v
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
    w = WorkerProcess(engine_kwargs={"driver_memory": "2g"})
    info = w.start()
    assert info["protocol_version"] == 2 and info.get("control") is True
    yield w
    w.stop()


def test_streaming_events_arrive_before_the_reply(worker):
    events = []
    res = worker.run_code("import time\nfor i in range(3):\n    print('tick', i)\n    time.sleep(0.3)\nprint('done')",
                          stream=True, on_event=events.append)
    assert res["ok"] and "done" in res["stdout"]
    texts = "".join(e["text"] for e in events if e["event"] == "stdout")
    assert "tick 0" in texts and "tick 2" in texts and "done" in texts
    assert len(events) >= 3  # one frame per line, not one at the end


def test_interrupt_sleeping_cell(worker):
    assert worker.interrupt() == {"interrupted": False, "state": "idle", "reason": "idle: no cell is running"}
    out = {}

    def run():
        # short sleeps: on Windows a queued interrupt is noticed between calls, not inside one
        out["res"] = worker.run_code("import time\nx = 'before'\nfor _ in range(600):\n    time.sleep(0.1)\nx = 'after'")

    t = threading.Thread(target=run); t.start()
    time.sleep(1.5)
    st = worker.status()
    assert st["cell_running"] is True and st["cell"]["method"] == "run_code" and st["cell"]["elapsed_s"] >= 1.0
    assert st["cell"]["active_jobs"] == 0  # sleeping, not in Spark
    r = worker.interrupt()
    assert r["interrupted"] is True and r["state"] == "interrupting"
    t.join(timeout=15)
    assert not t.is_alive(), "cell did not return after interrupt"
    res = out["res"]
    assert not res["ok"] and res["interrupted"] is True and "KeyboardInterrupt" in (res["error"] or "")
    after = worker.run_code("print(x)")  # state kept, cell stopped before reassigning
    assert "before" in after["stdout"]


def test_interrupt_spark_job(worker):
    out = {}

    def run():
        try:
            out["res"] = worker.run_code("n = spark.range(10**14).selectExpr('sum(id % 7)').collect()")  # ~hours uninterrupted
        except Exception as exc:  # a worker-level error instead of a cell result: report it, do not hide it in a KeyError
            out["exc"] = f"{type(exc).__name__}: {exc} fatal={getattr(exc, 'fatal', None)} tb={getattr(exc, 'traceback_str', '')}"

    t = threading.Thread(target=run); t.start()
    time.sleep(3)
    st = worker.status()
    assert st["cell_running"] and st["cell"]["active_jobs"] >= 1, st  # job count read from the control thread while the cell is blocked
    assert st["cell"]["jobs"] and isinstance(st["cell"]["jobs"][0]["name"], str) and "id" in st["cell"]["jobs"][0], st
    t0 = time.time()
    r = worker.interrupt()
    reply_s = time.time() - t0
    assert r["interrupted"] is True
    assert reply_s < 5, f"interrupt acknowledged after {reply_s:.1f}s"  # Cobalt saw 10 s+ with pyspark's SIGINT handler in place
    t.join(timeout=60)
    assert not t.is_alive(), "Spark job did not cancel"
    assert "exc" not in out, out["exc"]
    assert not out["res"]["ok"] and out["res"]["interrupted"], out["res"]
    assert out["res"]["error"].startswith("KeyboardInterrupt"), out["res"]["error"]
    # the cancel's own py4j noise ("reentrant call", "while sending command") stays off the cell's stderr
    assert "while sending command" not in out["res"]["stderr"] and "reentrant" not in out["res"]["stderr"], out["res"]["stderr"]
    assert worker.run_code("print(spark.range(5).count())")["stdout"].strip() == "5"  # session healthy


def test_run_sql_arrow_and_display(worker):
    import pyarrow as pa

    res = worker.run_sql("SELECT id, CAST(id * 1.5 AS DOUBLE) AS v, CAST(id AS STRING) AS s FROM range(250)", limit=100, arrow=True)
    assert res["arrow"]["row_count"] == 100 and res["arrow"]["truncated"] is True and res["rows"] == []
    table = pa.ipc.open_stream(res["blobs"][0]).read_all()
    assert table.num_rows == 100 and table.schema.names == ["id", "v", "s"]
    assert str(table.schema.field("v").type) == "double" and table.column("id").to_pylist()[:3] == [0, 1, 2]

    res = worker.run_code("df = spark.range(7).withColumn('sq', F.col('id') * F.col('id'))\ndisplay(df)\ndisplay('not a frame')\nprint('after')")
    assert res["ok"] and "after" in res["stdout"] and "'not a frame'" in res["stdout"]
    assert len(res["displays"]) == 1 and res["displays"][0]["row_count"] == 7 and len(res["blobs"]) == 1
    t2 = pa.ipc.open_stream(res["blobs"][0]).read_all()
    assert t2.column("sq").to_pylist() == [0, 1, 4, 9, 16, 25, 36]


def test_interrupt_run_sql(worker):
    from local_spark_mcp.worker_client import WorkerError

    out = {}

    def run():
        try:
            out["res"] = worker.run_sql("SELECT sum(id % 7) FROM range(100000000000000)")
        except WorkerError as exc:
            out["err"] = exc

    t = threading.Thread(target=run); t.start()
    time.sleep(3)
    assert worker.status()["cell"]["method"] == "run_sql"
    assert worker.interrupt()["interrupted"] is True
    t.join(timeout=60)
    assert not t.is_alive(), "query did not cancel"
    err = out.get("err")
    assert err is not None and err.interrupted and not err.fatal and str(err).startswith("KeyboardInterrupt"), out
    assert worker.run_sql("SELECT 1 AS one")["rows"] == [[1]]


def test_capture_result_bare_dataframe(worker):
    import pyarrow as pa

    # not captured unless asked
    res = worker.run_code("spark.range(3)")
    assert res["ok"] and res["displays"] == [] and "DataFrame" in res["stdout"]
    # Spark DataFrame as the last expression
    res = worker.run_code("spark.range(5).withColumn('d', F.col('id') * 2)", capture_result=True)
    assert res["ok"] and len(res["displays"]) == 1 and res["displays"][0]["source"] == "result"
    assert pa.ipc.open_stream(res["blobs"][0]).read_all().column("d").to_pylist() == [0, 2, 4, 6, 8]
    # pandas DataFrame, truncated to the row cap
    res = worker.run_code("import pandas as pd\npd.DataFrame({'a': range(150)})", capture_result=True)
    d = res["displays"][0]
    assert d["source"] == "result" and d["truncated"] is True and d["row_count"] == 100 and d["columns"] == ["a"]
    # display() entries and the captured result both arrive, in order
    res = worker.run_code("display(spark.range(2))\nspark.range(4)", capture_result=True)
    assert [d["source"] for d in res["displays"]] == ["display", "result"] and len(res["blobs"]) == 2
    # a non-frame result or a statement adds nothing
    res = worker.run_code("x = 1\nx + 1", capture_result=True)
    assert res["ok"] and res["displays"] == [] and "2" in res["stdout"]


def test_run_sql_streams_arrow_batches(worker):
    import pyarrow as pa

    events = []
    res = worker.run_sql("SELECT id, CAST(id * 2 AS DOUBLE) AS d FROM range(25000) ORDER BY id", batch_rows=5000, on_batch=events.append)
    assert res["rows"] == [] and res["row_count"] == 25000 and res["batches"] == len(events) >= 5, (res, len(events))
    assert res["arrow"]["streamed"] is True and res["elapsed_s"] is not None and res["truncated"] is False
    tables = [pa.ipc.open_stream(e["blobs"][0]).read_all() for e in events]
    assert all(e["event"] == "batch" and e["rows"] == t.num_rows and e["arrow_bytes"] == len(e["blobs"][0]) for e, t in zip(events, tables))
    ids = [i for t in tables for i in t.column("id").to_pylist()]
    assert ids == list(range(25000))  # ordered, nothing lost, nothing twice
    assert [e["batch"] for e in events] == list(range(len(events)))
    # an explicit limit still applies and reports the cut; a limit that is not reached does not
    events.clear()
    res = worker.run_sql("SELECT id FROM range(100)", limit=7, batch_rows=3, on_batch=events.append)
    assert res["row_count"] == 7 and sum(e["rows"] for e in events) == 7 and res["truncated"] is True and res["arrow"]["truncated"] is True
    events.clear()
    res = worker.run_sql("SELECT id FROM range(100)", limit=200, batch_rows=50, on_batch=events.append)
    assert res["row_count"] == 100 and res["truncated"] is False and sum(e["rows"] for e in events) == 100
    events.clear()
    res = worker.run_sql("SELECT id FROM range(10)", limit=10, batch_rows=4, on_batch=events.append)
    assert res["row_count"] == 10 and res["truncated"] is False  # exactly limit rows: not cut
    # the plain path is unchanged
    assert worker.run_sql("SELECT 1 AS one")["rows"] == [[1]]


def test_dml_metrics(worker, tmp_path):
    loc = (tmp_path / "dml").as_posix()
    worker.run_sql(f"CREATE TABLE dml_t (id BIGINT, v STRING) USING delta LOCATION '{loc}'")
    r = worker.run_sql("INSERT INTO dml_t SELECT id, 'a' FROM range(10)")
    assert r["metrics"]["affected_rows"] == 10 and r["metrics"]["source"] == "history" and r["metrics"]["operation"] == "WRITE", r["metrics"]
    r = worker.run_sql("UPDATE dml_t SET v = 'b' WHERE id < 3")
    assert r["metrics"]["affected_rows"] == 3 and r["metrics"]["source"] == "result"
    r = worker.run_sql("DELETE FROM dml_t WHERE id >= 8")
    assert r["metrics"]["affected_rows"] == 2
    worker.run_code("spark.range(5, 12).selectExpr('id', \"'m' v\").createOrReplaceTempView('dml_src')")
    r = worker.run_sql("MERGE INTO dml_t t USING dml_src s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v WHEN NOT MATCHED THEN INSERT *")
    assert r["metrics"]["inserted"] == 4 and r["metrics"]["updated"] == 3 and r["metrics"]["affected_rows"] == 7
    r = worker.run_sql(f"CREATE TABLE dml_ctas USING delta LOCATION '{(tmp_path / 'ctas').as_posix()}' AS SELECT * FROM dml_t")
    assert r["metrics"]["affected_rows"] == 12 and r["metrics"]["source"] == "history", r["metrics"]
    assert worker.run_sql("SELECT COUNT(*) FROM dml_t")["metrics"] is None
    # streamed DML carries the same metrics
    events = []
    r = worker.run_sql("DELETE FROM dml_t WHERE id = 0", batch_rows=10, on_batch=events.append)
    assert r["metrics"]["affected_rows"] == 1 and r["row_count"] == 1  # Delta's num_affected_rows frame, streamed as one batch

