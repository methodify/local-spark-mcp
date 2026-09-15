"""A dead JVM (driver OOM, kill) must not leave the server returning
ConnectionRefused forever: the failing call is reported as fatal, the worker is
dropped, and the next call starts a fresh session (REQUEST-006 #7c)."""

import asyncio
import os

import pytest

from local_spark_mcp.config import Config
from local_spark_mcp.server import ServerState
from local_spark_mcp.worker_client import WorkerError

pytestmark = pytest.mark.skipif(
    os.environ.get("LOCAL_SPARK_RUN_INTEGRATION") != "1",
    reason="set LOCAL_SPARK_RUN_INTEGRATION=1 to run (starts a real Spark session)",
)

KILL_JVM = """
import os, signal
pid = int(spark._jvm.java.lang.ProcessHandle.current().pid())
os.kill(pid, signal.SIGKILL if hasattr(signal, "SIGKILL") else signal.SIGTERM)
import time; time.sleep(2)
print(spark.range(1).count())  # reaches the dead gateway
"""


def test_dead_driver_is_detected_and_replaced():
    state = ServerState(Config())

    async def scenario():
        first = await state.call("run_code", "x = 41; print(x + 1)")
        assert "42" in first["stdout"]
        with pytest.raises(WorkerError, match="fresh session"):
            await state.call("run_code", KILL_JVM)  # the cell's own failure says the JVM is gone
        assert state._worker is None
        info = await state.call("get_info")  # fresh worker, state gone
        assert info["spark_version"]
        again = await state.call("run_code", "print('x' in dir())")
        assert "False" in again["stdout"]

    try:
        asyncio.run(scenario())
    finally:
        state.shutdown()
