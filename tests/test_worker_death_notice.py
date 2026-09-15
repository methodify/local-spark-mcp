"""A worker that dies between calls (no call saw it fail) is reported at the top
of the next result, not replaced silently (REQUEST-007, ADO #299)."""

import asyncio
import os
import signal

import pytest

from local_spark_mcp.config import Config
from local_spark_mcp.server import ServerState

pytestmark = pytest.mark.skipif(
    os.environ.get("LOCAL_SPARK_RUN_INTEGRATION") != "1",
    reason="set LOCAL_SPARK_RUN_INTEGRATION=1 to run (starts a real Spark session)",
)


def test_worker_death_between_calls_is_reported():
    state = ServerState(Config())

    async def scenario():
        first = await state.call("run_code", "x = 7; print(x)")
        assert "7" in first["stdout"]
        pid = state._worker._proc.pid
        os.kill(pid, signal.SIGKILL if hasattr(signal, "SIGKILL") else signal.SIGTERM)
        for _ in range(50):  # let the process reap
            if not state._worker.running:
                break
            await asyncio.sleep(0.2)
        assert not state._worker.running
        res = await state.call("run_code", "print('x' in dir())")  # fresh worker, x is gone
        assert "False" in res["stdout"]
        text = state.with_notices("body")
        assert text.startswith("notice: runtime restarted: the previous session's worker process exited"), text
        assert "no idle timeout" in text and "body" in text
        assert state.with_notices("again") == "again"  # consumed

    try:
        asyncio.run(scenario())
    finally:
        state.shutdown()
