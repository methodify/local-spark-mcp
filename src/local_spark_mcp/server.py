"""The MCP server: exposes the stateful Spark session as tools.

A single worker process (one Spark session + one IPython namespace) backs the
whole server. Calls are serialized — notebook semantics are sequential — and the
blocking worker IPC runs in a thread so the asyncio event loop stays responsive.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from mcp.server.fastmcp import FastMCP

from .config import Config, ConfigError, load_config
from .worker_client import WorkerError, WorkerProcess

INSTRUCTIONS = """\
A stateful local Apache Spark session for prototyping PySpark that will run on
Microsoft Fabric. State persists across calls like cells in one notebook kernel:
variables, imports, and the SparkSession (`spark`) survive between run_code
calls. `spark`, `sc`, `F` (functions), `T` (types), and `Window` are pre-imported.
Use run_code to explore and transform; use run_sql for quick queries.
reset_runtime wipes all state by restarting the session.

When a Fabric workspace is configured, its lakehouses appear as Spark databases
and their Delta tables as `<lakehouse>`.`<table>` — the same catalog shape as the
Fabric runtime. Explore with list_lakehouses / list_tables, mount tables on
demand with mount_table / mount_lakehouse, then reference them by name via
run_sql (or spark.sql / spark.table in run_code). Work entirely through named
tables and databases, exactly as you would in a Fabric notebook, so the code you
arrive at transfers to Fabric with a similar outcome.
"""

# Max width of a single SQL table cell before it is elided.
MAX_SQL_CELL_WIDTH = 60


def _base_engine_kwargs(config: Config) -> dict:
    return {
        "driver_memory": config.spark.driver_memory,
        "extra_configs": config.spark.extra_configs,
        "java_home": config.runtime.java_home,
        "default_sql_limit": config.runtime.default_sql_limit,
    }


class ServerState:
    """Owns the worker and, in Fabric mode, the OneLake token server + REST
    discovery. Serializes access. Discovery (cheap REST) is separable from worker
    startup (slow Spark) so lakehouse/table listing doesn't pay for a session."""

    def __init__(self, config: Config | None = None):
        self.config = config or load_config()
        self.lock = asyncio.Lock()
        self._worker: WorkerProcess | None = None
        self._token_server = None  # TokenServer | None
        self._fabric_client = None  # FabricAPIClient | None
        self._cred = None
        self._workspace_id: str | None = None
        self._lakehouses = None  # list[LakehouseInfo] | None (None = not discovered)

    def _fabric_enabled(self) -> bool:
        return bool(self.config.workspace.name or self.config.workspace.id)

    def _resolve_jar(self) -> str:
        from .fabric import default_jar_path

        jar = self.config.runtime.token_jar_path or default_jar_path()
        if not jar or not Path(jar).is_file():
            raise ConfigError(
                "OneLake token provider jar not found. Build it "
                "(cd token-provider && sbt package) or set runtime.token_jar_path "
                "in local-spark.toml."
            )
        return jar

    def _discover(self) -> None:
        """Resolve the workspace and list its (non-excluded) lakehouses (REST)."""
        from azure.identity import DefaultAzureCredential

        from .discovery import FabricAPIClient

        if self._cred is None:
            self._cred = DefaultAzureCredential()
        if self._fabric_client is None:
            self._fabric_client = FabricAPIClient(credential=self._cred)

        ws = self.config.require_workspace()
        self._workspace_id = self._fabric_client.resolve_workspace(name=ws.name, id=ws.id)
        excluded = {e.lower() for e in self.config.lakehouses.exclude}
        self._lakehouses = [
            lh
            for lh in self._fabric_client.list_lakehouses(self._workspace_id)
            if lh.name.lower() not in excluded
        ]

    def _find_lakehouse(self, name: str):
        for lh in self._lakehouses or []:
            if lh.name == name:
                return lh
        known = sorted(lh.name for lh in (self._lakehouses or []))
        raise ConfigError(f"Unknown lakehouse {name!r}; available: {known}")

    def _engine_kwargs(self) -> dict:
        kwargs = _base_engine_kwargs(self.config)
        if self._fabric_enabled():
            assert self._token_server is not None  # started in _ensure_started
            kwargs["onelake"] = {
                "endpoint": self._token_server.url,
                "secret": self._token_server.secret,
                "jar_path": self._resolve_jar(),
            }
            kwargs["lakehouses"] = [
                {"name": lh.name, "id": lh.id, "workspace_id": lh.workspace_id}
                for lh in (self._lakehouses or [])
            ]
        return kwargs

    async def _ensure_discovered(self) -> None:
        if self._fabric_enabled() and self._lakehouses is None:
            await asyncio.to_thread(self._discover)

    async def _ensure_started(self) -> None:
        if self._fabric_enabled():
            self._resolve_jar()  # fail fast on a missing jar
            await self._ensure_discovered()
            if self._token_server is None:
                from .token_server import TokenServer

                self._token_server = TokenServer(credential=self._cred)
                await asyncio.to_thread(self._token_server.start)
        if self._worker is None:
            self._worker = WorkerProcess(engine_kwargs=self._engine_kwargs())
        if not self._worker.running:
            await asyncio.to_thread(self._worker.start)

    async def call(self, method: str, *args):
        """Serialize, lazily start the worker, run the blocking call in a thread."""
        async with self.lock:
            await self._ensure_started()
            fn = getattr(self._worker, method)
            return await asyncio.to_thread(fn, *args)

    async def list_lakehouses(self) -> list:
        async with self.lock:
            await self._ensure_discovered()
            return list(self._lakehouses or [])

    async def list_tables(self, lakehouse: str) -> list[str]:
        async with self.lock:
            await self._ensure_discovered()
            lh = self._find_lakehouse(lakehouse)
            return await asyncio.to_thread(
                self._fabric_client.list_tables, self._workspace_id, lh.id
            )

    async def mount(self, lakehouse: str, tables: list[str] | None) -> dict:
        async with self.lock:
            await self._ensure_started()
            lh = self._find_lakehouse(lakehouse)
            if tables is None:
                tables = await asyncio.to_thread(
                    self._fabric_client.list_tables, self._workspace_id, lh.id
                )
            return await asyncio.to_thread(self._worker.mount_tables, lakehouse, tables)

    async def restart(self) -> dict:
        async with self.lock:
            if self._worker is not None:
                await asyncio.to_thread(self._worker.stop)
                self._worker = None
            await self._ensure_started()
            return self._worker.info  # set by WorkerProcess.start()

    def shutdown(self) -> None:
        if self._worker is not None:
            self._worker.stop()
        if self._token_server is not None:
            self._token_server.stop()


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
    lhs = info.get("lakehouses") or []
    if lhs:
        lines.append(f"  fabric lakehouses ({len(lhs)}): {', '.join(lhs)}")
    return "\n".join(lines)


def format_mount(res: dict, *, max_listed: int = 50) -> str:
    lakehouse = res["lakehouse"]
    mounted = res.get("mounted", [])
    failed = res.get("failed", [])
    shown = ", ".join(mounted[:max_listed]) + (
        f", … (+{len(mounted) - max_listed} more)" if len(mounted) > max_listed else ""
    )
    lines = [f"Mounted {len(mounted)} table(s) in {lakehouse}: {shown or '(none)'}"]
    if failed:
        lines.append(f"{len(failed)} failed:")
        lines += [f"  {f['table']}: {f['error']}" for f in failed[:10]]
    return "\n".join(lines)


# ---------- server / tools ----------

def build_server(state: ServerState | None = None) -> FastMCP:
    state = state or ServerState()
    mcp = FastMCP("local-spark", instructions=INSTRUCTIONS)

    @mcp.tool()
    async def run_code(code: str) -> str:
        """Run a cell of Python/PySpark against the persistent session. State persists across calls; `spark`, `sc`, `F`, `T`, `Window` are pre-imported. Returns captured stdout and the last-expression echo, or the traceback if the cell raised."""
        res = await state.call("run_code", code)
        return format_exec_result(res)

    @mcp.tool()
    async def run_sql(sql: str, limit: int | None = None) -> str:
        """Run a Spark SQL statement and return rows as a text table. `limit` caps returned rows (default from config, ~100) and the result flags truncation. Reference mounted Fabric tables by name (`lakehouse`.`table`), or any table/view registered in the session."""
        try:
            res = await state.call("run_sql", sql, limit)
        except WorkerError as exc:
            return f"SQL error: {exc}"
        return format_sql_result(res)

    @mcp.tool()
    async def session_info() -> str:
        """Show the live Spark session: version, master, current database, the catalog databases, and any Fabric lakehouses registered this session."""
        info = await state.call("get_info")
        return format_info(info)

    @mcp.tool()
    async def reset_runtime() -> str:
        """Reset the runtime: restart the Spark session and wipe all state — variables, imports, and mounted tables. Use to start from a clean slate."""
        info = await state.restart()
        return "Runtime reset — fresh Spark session.\n\n" + format_info(info)

    def _local_only_note() -> str | None:
        if not state._fabric_enabled():
            return (
                "Local-only mode: no Fabric workspace configured "
                "(set [workspace] in local-spark.toml to enable OneLake)."
            )
        return None

    @mcp.tool()
    async def list_lakehouses() -> str:
        """List the Fabric lakehouses available in this session. Each is registered as a Spark database; its tables are mounted on demand."""
        if note := _local_only_note():
            return note
        lhs = await state.list_lakehouses()
        if not lhs:
            return "No lakehouses found in the workspace (after exclusions)."
        return "Lakehouses (Spark databases):\n" + "\n".join(f"  {lh.name}" for lh in lhs)

    @mcp.tool()
    async def list_tables(lakehouse: str) -> str:
        """List the Delta tables in a Fabric lakehouse. Tables are not queryable via SQL until you mount them with mount_table or mount_lakehouse."""
        if note := _local_only_note():
            return note
        try:
            tables = await state.list_tables(lakehouse)
        except ConfigError as exc:
            return str(exc)
        if not tables:
            return f"{lakehouse}: no tables."
        return f"{lakehouse} ({len(tables)} tables):\n" + "\n".join(f"  {t}" for t in tables)

    @mcp.tool()
    async def mount_table(lakehouse: str, table: str) -> str:
        """Register one Fabric Delta table as `<lakehouse>`.`<table>` so it's queryable by name via run_sql (and spark.sql / spark.table in run_code)."""
        if note := _local_only_note():
            return note
        try:
            res = await state.mount(lakehouse, [table])
        except ConfigError as exc:
            return str(exc)
        return format_mount(res)

    @mcp.tool()
    async def mount_lakehouse(lakehouse: str) -> str:
        """Mount ALL tables in a lakehouse as `<lakehouse>`.`<table>`. Convenient, but can register many tables at once."""
        if note := _local_only_note():
            return note
        try:
            res = await state.mount(lakehouse, None)
        except ConfigError as exc:
            return str(exc)
        return format_mount(res)

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
