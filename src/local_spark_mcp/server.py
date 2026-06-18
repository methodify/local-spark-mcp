"""The MCP server: exposes the stateful Spark session as tools.

A single worker process (one Spark session + one IPython namespace) backs the
whole server. Calls are serialized — notebook semantics are sequential — and the
blocking worker IPC runs in a thread so the asyncio event loop stays responsive.
"""

from __future__ import annotations

import asyncio

from mcp.server.fastmcp import FastMCP

from .config import Config, load_config
from .worker_client import WorkerError, WorkerProcess

INSTRUCTIONS = """\
A stateful local Apache Spark session for prototyping PySpark that will later run
on Microsoft Fabric. State persists across calls like cells in one notebook
kernel: variables, imports, and the SparkSession (`spark`) survive between
run_code calls. `spark`, `sc`, `F` (functions), `T` (types), and `Window` are
pre-imported. Use run_code to explore/transform and run_sql for quick queries.
reset_runtime wipes all state by restarting the session.
"""

# Max width of a single SQL table cell before it is elided.
MAX_SQL_CELL_WIDTH = 60


def _engine_kwargs(config: Config) -> dict:
    return {
        "driver_memory": config.spark.driver_memory,
        "extra_configs": config.spark.extra_configs,
        "java_home": config.runtime.java_home,
        "default_sql_limit": config.runtime.default_sql_limit,
    }


class ServerState:
    """Holds the worker and serializes access to it."""

    def __init__(self, config: Config | None = None):
        self.config = config or load_config()
        self.worker = WorkerProcess(engine_kwargs=_engine_kwargs(self.config))
        self.lock = asyncio.Lock()

    async def _ensure_started(self) -> None:
        if not self.worker.running:
            await asyncio.to_thread(self.worker.start)

    async def call(self, method: str, *args):
        """Serialize, lazily start the worker, run the blocking call in a thread."""
        async with self.lock:
            await self._ensure_started()
            fn = getattr(self.worker, method)
            return await asyncio.to_thread(fn, *args)

    async def restart(self) -> dict:
        async with self.lock:
            if self.worker.running:
                return await asyncio.to_thread(self.worker.restart)
            return await asyncio.to_thread(self.worker.start)

    def shutdown(self) -> None:
        self.worker.stop()


# ---------- formatting (pure, unit-tested) ----------

def _cell(value) -> str:
    if value is None:
        return "NULL"
    text = str(value)
    if len(text) > MAX_SQL_CELL_WIDTH:
        return text[: MAX_SQL_CELL_WIDTH - 1] + "…"
    return text


def format_exec_result(res: dict) -> str:
    parts: list[str] = []
    stdout = (res.get("stdout") or "").rstrip("\n")
    if stdout:
        parts.append(stdout)
    if res.get("ok"):
        return "\n".join(parts) if parts else "(ok — no output)"
    # error path: prefer the full traceback, fall back to the one-line summary
    detail = (res.get("traceback") or "").rstrip("\n") or res.get("error") or "error"
    parts.append(detail)
    return "\n".join(parts)


def format_sql_result(res: dict) -> str:
    columns = res.get("columns") or []
    rows = res.get("rows") or []
    if not columns:
        return "(statement executed — no result set)"

    str_rows = [[_cell(v) for v in row] for row in rows]
    widths = [len(c) for c in columns]
    for row in str_rows:
        for i, val in enumerate(row):
            widths[i] = max(widths[i], len(val))

    def fmt_row(cells: list[str]) -> str:
        return " | ".join(c.ljust(widths[i]) for i, c in enumerate(cells))

    lines = [fmt_row(list(columns)), "-+-".join("-" * w for w in widths)]
    lines += [fmt_row(r) for r in str_rows]

    footer = f"[{res.get('row_count', len(rows))} row(s)"
    if res.get("truncated"):
        footer += f"; truncated at limit {res.get('limit')}"
    footer += "]"
    lines.append(footer)
    return "\n".join(lines)


def format_info(info: dict) -> str:
    lines = ["Spark session:"]
    for key in (
        "spark_version",
        "master",
        "app_id",
        "current_database",
        "execution_count",
        "default_sql_limit",
    ):
        if key in info:
            lines.append(f"  {key}: {info[key]}")
    dbs = info.get("databases") or []
    lines.append(f"  databases ({len(dbs)}): {', '.join(dbs) if dbs else '(none)'}")
    return "\n".join(lines)


# ---------- server / tools ----------

def build_server(state: ServerState | None = None) -> FastMCP:
    state = state or ServerState()
    mcp = FastMCP("local-spark", instructions=INSTRUCTIONS)

    @mcp.tool()
    async def run_code(code: str) -> str:
        """Run a cell of Python/PySpark against the persistent session.

        State persists across calls. `spark`, `sc`, `F`, `T`, `Window` are
        available. Returns captured stdout plus the last-expression echo, or the
        traceback if the cell raised.
        """
        res = await state.call("run_code", code)
        return format_exec_result(res)

    @mcp.tool()
    async def run_sql(sql: str, limit: int | None = None) -> str:
        """Run a Spark SQL statement and return rows as a text table.

        `limit` caps returned rows (default from config, ~100); the result notes
        when output was truncated. Tables created/registered in the session are
        queryable here.
        """
        try:
            res = await state.call("run_sql", sql, limit)
        except WorkerError as exc:
            return f"SQL error: {exc}"
        return format_sql_result(res)

    @mcp.tool()
    async def session_info() -> str:
        """Show the live Spark session: version, master, current database, and
        the databases registered in the catalog."""
        info = await state.call("get_info")
        return format_info(info)

    @mcp.tool()
    async def reset_runtime() -> str:
        """Reset the runtime: restart the Spark session and wipe all Python
        state (variables, imports, registered tables). Use to start clean."""
        info = await state.restart()
        return "Runtime reset — fresh Spark session.\n\n" + format_info(info)

    return mcp


def main() -> None:
    state = ServerState()
    mcp = build_server(state)
    try:
        mcp.run(transport="stdio")
    finally:
        state.shutdown()


if __name__ == "__main__":
    main()
