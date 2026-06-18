"""Full-stack end-to-end test: a real MCP client talks to the server over stdio,
which spawns a real Spark worker. Slow + gated. Run with:

    LOCAL_SPARK_RUN_INTEGRATION=1 .venv/bin/python -m pytest tests/test_server_e2e.py -v
"""

import os
import sys
from datetime import timedelta

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("LOCAL_SPARK_RUN_INTEGRATION") != "1",
    reason="set LOCAL_SPARK_RUN_INTEGRATION=1 to run (starts a real Spark session)",
)


def _text(result) -> str:
    return "\n".join(block.text for block in result.content if block.type == "text")


@pytest.mark.anyio
async def test_full_mcp_roundtrip(tmp_path):
    from mcp import ClientSession
    from mcp.client.stdio import StdioServerParameters, stdio_client

    params = StdioServerParameters(
        command=sys.executable,
        args=["-m", "local_spark_mcp.server"],
        # Run in an isolated cwd with a minimal local-only config.
        cwd=str(tmp_path),
        env={**os.environ, "LOCAL_SPARK_DRIVER_MEMORY": "2g", "LOCAL_SPARK_SQL_LIMIT": "5"},
    )
    (tmp_path / "local-spark.toml").write_text("[spark]\ndriver_memory = '2g'\n")
    long = timedelta(seconds=240)

    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            tools = {t.name for t in (await session.list_tools()).tools}
            assert {"run_code", "run_sql", "session_info", "reset_runtime"} <= tools

            # persistent state across two run_code calls (first pays Spark startup)
            r = await session.call_tool("run_code", {"code": "y = 7"}, read_timeout_seconds=long)
            assert not r.isError, _text(r)
            r = await session.call_tool("run_code", {"code": "print(y * 6)"}, read_timeout_seconds=long)
            assert "42" in _text(r)

            # create + query a Delta table through SQL
            path = tmp_path / "t"
            await session.call_tool(
                "run_code",
                {"code": f"spark.range(3).write.format('delta').mode('overwrite').save('{path}')"},
                read_timeout_seconds=long,
            )
            await session.call_tool(
                "run_code",
                {"code": f"spark.sql(\"CREATE TABLE t USING DELTA LOCATION '{path}'\")"},
                read_timeout_seconds=long,
            )
            r = await session.call_tool("run_sql", {"sql": "SELECT * FROM t ORDER BY id"}, read_timeout_seconds=long)
            body = _text(r)
            assert "id" in body and "row(s)" in body

            # error is reported, session survives
            r = await session.call_tool("run_code", {"code": "1/0"}, read_timeout_seconds=long)
            assert "ZeroDivisionError" in _text(r)

            # SQL error is a clean message, not a crash
            r = await session.call_tool("run_sql", {"sql": "SELECT * FROM nope"}, read_timeout_seconds=long)
            assert "SQL error" in _text(r)

            # reset wipes state
            await session.call_tool("reset_runtime", {}, read_timeout_seconds=long)
            r = await session.call_tool("run_code", {"code": "print(y)"}, read_timeout_seconds=long)
            assert "NameError" in _text(r)


@pytest.fixture
def anyio_backend():
    return "asyncio"
