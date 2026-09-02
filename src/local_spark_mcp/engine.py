"""The Spark engine: an IPython InteractiveShell with a persistent namespace and
a live SparkSession injected in. This is the in-process core that the worker
process wraps with IPC; it has no knowledge of MCP or process boundaries, so it
can be unit-tested directly.
"""

from __future__ import annotations

import datetime
import decimal
import os
import re
import shutil
import time
import traceback as _tb
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from pathlib import Path

from .spark_session import build_spark

# Truncate over-long reprs/values so a single cell can't flood the transport.
MAX_VALUE_LEN = 4000


@dataclass
class ExecResult:
    """Result of running a code cell."""

    ok: bool
    stdout: str = ""
    stderr: str = ""
    error: str | None = None  # "ExceptionType: message" when the cell raised
    traceback: str | None = None  # full formatted traceback when available
    execution_count: int | None = None

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class SqlResult:
    """Result of running a SQL query."""

    columns: list[str] = field(default_factory=list)
    rows: list[list] = field(default_factory=list)
    row_count: int = 0
    truncated: bool = False
    limit: int = 0

    def to_dict(self) -> dict:
        return asdict(self)


def _jsonify(value):
    """Coerce a Spark cell value into something JSON-serializable."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    if isinstance(value, (list, tuple)):
        return [_jsonify(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _jsonify(v) for k, v in value.items()}
    text = str(value)
    return text[:MAX_VALUE_LEN] + "…" if len(text) > MAX_VALUE_LEN else text


class SparkEngine:
    """Holds the persistent IPython shell and SparkSession."""

    def __init__(
        self,
        *,
        driver_memory: str = "8g",
        extra_configs: dict[str, str] | None = None,
        java_home: str | None = None,
        default_sql_limit: int = 100,
        app_name: str = "local-spark-mcp",
        onelake: dict | None = None,
        lakehouses: list[dict] | None = None,
        env: dict[str, str] | None = None,
        hadoop_home: str | None = None,
        default_lakehouse: str | None = None,
        write_mode: str = "sandbox",
        persist_shadow: bool = False,
        state_root: str | None = None,
    ):
        self.default_sql_limit = default_sql_limit
        self.write_mode = write_mode
        self.persist_shadow = persist_shadow
        self.default_lakehouse: str | None = None  # resolved in _register_lakehouses
        lakehouses = lakehouses or []
        workspace_id = lakehouses[0]["workspace_id"] if lakehouses else None

        # Per-session state: the managed-table warehouse (so nothing lands in the
        # project cwd) and, unless persisted, the write-policy shadow.
        self._state_root = Path(state_root or "~/.local-spark").expanduser()
        self._session_dir = self._state_root / "sessions" / f"{os.getpid()}-{int(time.time())}"
        self._session_dir.mkdir(parents=True, exist_ok=True)
        _purge_stale_sessions(self._state_root / "sessions")
        if persist_shadow and workspace_id:
            self.shadow_root = self._state_root / "lakehouses" / workspace_id / "shadow"
        else:
            self.shadow_root = self._session_dir / "shadow"
        self.shadow_root.mkdir(parents=True, exist_ok=True)

        catalog = None
        if onelake and lakehouses:
            catalog = {
                "workspace_id": workspace_id,
                "lakehouses": {lh["name"]: lh["id"] for lh in lakehouses},
                "write_mode": write_mode,
                "shadow_root": self.shadow_root.as_posix(),
            }
        self.spark = build_spark(
            driver_memory=driver_memory,
            extra_configs=extra_configs,
            java_home=java_home,
            app_name=app_name,
            onelake=onelake,
            env=env,
            hadoop_home=hadoop_home,
            catalog=catalog,
            warehouse_dir=(self._session_dir / "warehouse").as_posix(),
        )
        self.shell = self._make_shell()
        self._register_lakehouses(lakehouses, default_lakehouse)
        self._bootstrap_namespace()

    def _make_shell(self):
        from IPython.core.interactiveshell import InteractiveShell

        shell = InteractiveShell.instance()
        # Plain (non-ANSI) tracebacks — the transport is text, not a terminal.
        shell.run_line_magic("colors", "nocolor")
        return shell

    def _bootstrap_namespace(self):
        """Seed the namespace with the things a Fabric notebook would have."""
        import pyspark.sql.functions as F
        import pyspark.sql.types as T
        from pyspark.sql import Window

        self.shell.user_ns.update(
            {
                "spark": self.spark,
                "sc": self.spark.sparkContext,
                "F": F,
                "T": T,
                "Window": Window,
            }
        )
        self._install_delta_forname_bridge()

    def _install_delta_forname_bridge(self) -> None:
        """Bridge ``DeltaTable.forName`` to OneLakeCatalog.

        ``forName`` resolves through Spark's V1 session catalog, which bypasses V2
        catalog plugins — so an unregistered lakehouse table would fail there even
        though ``spark.table`` resolves it. Resolve via ``spark.table`` first
        (materializing on first touch), then call the original. ``forName`` is also
        the write gateway ``dwlib`` uses for MERGE, so readonly refuses here.
        Installed before any user import, so a library that patches
        ``DataFrameReader.table`` (as dwlib does) still chains through this.
        """
        try:
            from delta.tables import DeltaTable
        except ImportError:  # delta python API not installed — nothing to bridge
            return
        engine = self
        original = DeltaTable.forName

        def forName(cls, sparkSession, tableOrViewName):
            engine._refuse_if_readonly(tableOrViewName)
            sparkSession.table(tableOrViewName)
            return original(sparkSession, tableOrViewName)

        DeltaTable.forName = classmethod(forName)

    def _refuse_if_readonly(self, table_name: str) -> None:
        if self.write_mode != "readonly" or not getattr(self, "lakehouses", None):
            return
        parts = [part.strip("`") for part in table_name.split(".")]
        if len(parts) == 2:
            lakehouse = parts[0]
        elif len(parts) == 1:
            lakehouse = self.default_lakehouse
        else:
            return
        if lakehouse and self._resolve_lakehouse(lakehouse) is not None:
            raise PermissionError(
                f"write_mode is 'readonly': DeltaTable.forName({table_name!r}) is a "
                "write gateway (MERGE/UPDATE/DELETE). Set LOCAL_SPARK_WRITE_MODE (or "
                "[runtime] write_mode in local-spark.toml) to 'sandbox' to write "
                "locally, or 'writethrough' to write to OneLake."
            )

    @staticmethod
    def _q(identifier: str) -> str:
        """Backtick-quote a Spark SQL identifier."""
        return "`" + identifier.replace("`", "``") + "`"

    def _register_lakehouses(self, lakehouses: list[dict], default_lakehouse: str | None = None) -> None:
        """Register each (non-excluded) lakehouse as a Spark database and select
        the default. Tables are NOT mounted here: OneLakeCatalog resolves them on
        first touch (and mount_table/mount_tables force it explicitly)."""
        from .discovery import LakehouseInfo

        self.lakehouses: dict[str, LakehouseInfo] = {}
        self._mounted: dict[str, set] = {}  # lakehouse name -> mounted table names
        for lh in lakehouses:
            info = LakehouseInfo(name=lh["name"], id=lh["id"], workspace_id=lh["workspace_id"])
            self.lakehouses[info.name] = info
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self._q(info.name)}")
        if default_lakehouse and self.lakehouses:
            info = self._resolve_lakehouse(default_lakehouse)
            if info is None:
                raise ValueError(
                    f"default lakehouse {default_lakehouse!r} is not in the workspace "
                    f"(or is excluded); known: {sorted(self.lakehouses)}"
                )
            # Unqualified names now resolve here, like a Fabric notebook's default lakehouse.
            self.spark.sql(f"USE {self._q(info.name)}")
            self.default_lakehouse = info.name

    def _resolve_lakehouse(self, name: str):
        """Look up a lakehouse by name (case-insensitively, since Spark
        normalizes catalog identifiers)."""
        info = self.lakehouses.get(name)
        if info is not None:
            return info
        lowered = name.lower()
        for known, lh in self.lakehouses.items():
            if known.lower() == lowered:
                return lh
        return None

    def mount_table(self, lakehouse: str, table: str) -> dict:
        """Force OneLakeCatalog to materialize <lakehouse>.<table> now.

        Resolution goes through the catalog so the write policy applies: a
        shallow clone under the shadow root in sandbox/readonly, an external
        OneLake table in writethrough. Raises AnalysisException if the table
        does not exist in OneLake.
        """
        info = self._resolve_lakehouse(lakehouse)
        if info is None:
            raise ValueError(
                f"unknown lakehouse {lakehouse!r}; known: {sorted(self.lakehouses)}"
            )
        self.spark.table(f"{self._q(info.name)}.{self._q(table)}")
        self._mounted.setdefault(info.name, set()).add(table)
        return {
            "lakehouse": info.name,
            "table": table,
            "path": info.table_path(table),
            "write_mode": self.write_mode,
        }

    def mount_tables(self, lakehouse: str, tables: list[str], workers: int = 8) -> dict:
        """Materialize several tables in parallel; each is an independent Delta
        log read from OneLake. Per-table errors are captured, not fatal."""

        def one(table: str):
            try:
                self.mount_table(lakehouse, table)
                return table, None
            except Exception as exc:  # keep going; report per-table
                return table, f"{type(exc).__name__}: {exc}"

        mounted, failed = [], []
        with ThreadPoolExecutor(max_workers=max(1, min(workers, len(tables) or 1))) as pool:
            for table, error in pool.map(one, tables):
                if error is None:
                    mounted.append(table)
                else:
                    failed.append({"table": table, "error": error})
        return {"lakehouse": lakehouse, "mounted": mounted, "failed": failed}

    def run_code(self, code: str) -> ExecResult:
        """Run a cell of Python against the persistent namespace."""
        from IPython.utils.capture import capture_output

        with capture_output() as cap:
            result = self.shell.run_cell(code, store_history=True)

        error = None
        tb = None
        exc = result.error_before_exec or result.error_in_exec
        if exc is not None:
            error = f"{type(exc).__name__}: {exc}"
            # IPython's captured stderr is unreliable for tracebacks; format from
            # the exception object directly so the agent always sees the detail.
            if result.error_in_exec is not None:
                tb = "".join(
                    _tb.format_exception(type(exc), exc, exc.__traceback__)
                )

        stdout = cap.stdout
        # Rich display outputs (e.g. displayhook) land in cap.outputs; fold their
        # text/plain representation into stdout so nothing is silently dropped.
        for out in cap.outputs:
            text = out.data.get("text/plain") if hasattr(out, "data") else None
            if text:
                stdout += text + "\n"

        return ExecResult(
            ok=bool(result.success),
            stdout=_truncate(stdout),
            stderr=_truncate(cap.stderr),
            error=error,
            traceback=_truncate(tb) if tb else None,
            execution_count=self.shell.execution_count,
        )

    # "[TABLE_OR_VIEW_NOT_FOUND] ... `db`.`table` cannot be found"
    _MISSING_TABLE_RE = re.compile(r"`([^`]+)`\.`([^`]+)`")

    def _automount_missing(self, exc) -> bool:
        """If ``exc`` is a table-not-found for a known Fabric lakehouse table,
        mount it and return True (so the caller can retry). Mirrors the Fabric
        runtime, where a lakehouse's tables are queryable by name without an
        explicit mount step."""
        message = str(exc)
        if "TABLE_OR_VIEW_NOT_FOUND" not in message and "cannot be found" not in message:
            return False
        for db, table in self._MISSING_TABLE_RE.findall(message):
            info = self._resolve_lakehouse(db)
            if info is not None and table not in self._mounted.get(info.name, set()):
                self.mount_table(info.name, table)  # records into self._mounted
                return True
        return False

    def _sql_with_automount(self, sql: str):
        """spark.sql, transparently mounting referenced Fabric tables on first
        use. Each iteration mounts one newly-referenced table; the per-table
        guard prevents loops if a mount doesn't resolve the reference."""
        from pyspark.errors import AnalysisException

        while True:
            try:
                return self.spark.sql(sql)
            except AnalysisException as exc:
                if not self._automount_missing(exc):
                    raise

    def run_sql(self, sql: str, limit: int | None = None) -> SqlResult:
        """Run a SQL statement and return up to ``limit`` rows."""
        if limit is None:
            limit = self.default_sql_limit
        df = self._sql_with_automount(sql)
        columns = list(df.columns)
        # Pull one extra row to detect truncation without a full count.
        collected = df.limit(limit + 1).collect()
        truncated = len(collected) > limit
        collected = collected[:limit]
        rows = [[_jsonify(v) for v in row] for row in collected]
        return SqlResult(
            columns=columns,
            rows=rows,
            row_count=len(rows),
            truncated=truncated,
            limit=limit,
        )

    def info(self) -> dict:
        """Snapshot of the live session for the agent."""
        sc = self.spark.sparkContext
        catalog = self.spark.catalog
        try:
            databases = [db.name for db in catalog.listDatabases()]
        except Exception:  # pragma: no cover - defensive
            databases = []
        # User-defined names only (skip the bootstrap + IPython internals).
        return {
            "spark_version": self.spark.version,
            "app_id": sc.applicationId,
            "master": sc.master,
            "current_database": catalog.currentDatabase(),
            "databases": databases,
            "lakehouses": sorted(getattr(self, "lakehouses", {})),
            "default_lakehouse": self.default_lakehouse,
            "write_mode": self.write_mode,
            "shadow_root": self.shadow_root.as_posix(),
            "shadows": [f"{t['lakehouse']}.{t['table']}" for t in self._shadow_tables()],
            "execution_count": self.shell.execution_count,
            "default_sql_limit": self.default_sql_limit,
        }

    # ---- write-policy shadow ----

    def _shadow_tables(self) -> list[dict]:
        """Shadowed tables on disk: <shadow_root>/<lakehouse-id>/<table>/_delta_log."""
        id_to_name = {info.id: name for name, info in getattr(self, "lakehouses", {}).items()}
        found: list[dict] = []
        if not self.shadow_root.is_dir():
            return found
        for lh_dir in sorted(self.shadow_root.iterdir()):
            if not lh_dir.is_dir():
                continue
            for table_dir in sorted(lh_dir.iterdir()):
                if (table_dir / "_delta_log").is_dir():
                    found.append({
                        "lakehouse": id_to_name.get(lh_dir.name, lh_dir.name),
                        "table": table_dir.name,
                        "path": table_dir.as_posix(),
                    })
        return found

    def shadow_status(self) -> dict:
        return {
            "write_mode": self.write_mode,
            "shadow_root": self.shadow_root.as_posix(),
            "persistent": self.persist_shadow,
            "tables": self._shadow_tables(),
        }

    def discard_shadow(self) -> dict:
        """Drop every shadowed table from the catalog and delete the shadow files,
        so the next touch re-clones from OneLake. OneLake is not affected."""
        tables = self._shadow_tables()
        for t in tables:
            try:
                self.spark.sql(f"DROP TABLE IF EXISTS {self._q(t['lakehouse'])}.{self._q(t['table'])}")
            except Exception:  # external table; best effort — files go next
                pass
        shutil.rmtree(self.shadow_root, ignore_errors=True)
        self.shadow_root.mkdir(parents=True, exist_ok=True)
        self._mounted = {}
        return {"discarded": len(tables), "tables": tables}

    def stop(self):
        try:
            self.spark.stop()
        except Exception:  # pragma: no cover - best effort on shutdown
            pass
        # Session-scoped state (warehouse + non-persistent shadow) goes with the session.
        shutil.rmtree(self._session_dir, ignore_errors=True)


def _purge_stale_sessions(sessions_dir: Path, max_age_days: int = 7) -> None:
    """Best-effort cleanup of session dirs a crashed worker never removed."""
    if not sessions_dir.is_dir():
        return
    cutoff = time.time() - max_age_days * 86400
    for entry in sessions_dir.iterdir():
        try:
            if entry.is_dir() and entry.stat().st_mtime < cutoff:
                shutil.rmtree(entry, ignore_errors=True)
        except OSError:
            pass


def _truncate(text: str) -> str:
    if len(text) > MAX_VALUE_LEN:
        return text[:MAX_VALUE_LEN] + f"\n… [truncated, {len(text)} chars total]"
    return text
