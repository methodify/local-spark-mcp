"""The Spark engine: an IPython InteractiveShell with a persistent namespace and
a live SparkSession injected in. This is the in-process core that the worker
process wraps with IPC; it has no knowledge of MCP or process boundaries, so it
can be unit-tested directly.
"""

from __future__ import annotations

import datetime
import decimal
import traceback as _tb
from dataclasses import asdict, dataclass, field

from .spark_session import build_local_spark

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
    ):
        self.default_sql_limit = default_sql_limit
        self.spark = build_local_spark(
            driver_memory=driver_memory,
            extra_configs=extra_configs,
            java_home=java_home,
            app_name=app_name,
        )
        self.shell = self._make_shell()
        self._bootstrap_namespace()

    def _make_shell(self):
        from IPython.core.interactiveshell import InteractiveShell

        shell = InteractiveShell.instance()
        # Plain (non-ANSI) tracebacks — the transport is text, not a terminal.
        shell.run_line_magic("colors", "NoColor")
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

    def run_sql(self, sql: str, limit: int | None = None) -> SqlResult:
        """Run a SQL statement and return up to ``limit`` rows."""
        if limit is None:
            limit = self.default_sql_limit
        df = self.spark.sql(sql)
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
            "execution_count": self.shell.execution_count,
            "default_sql_limit": self.default_sql_limit,
        }

    def stop(self):
        try:
            self.spark.stop()
        except Exception:  # pragma: no cover - best effort on shutdown
            pass


def _truncate(text: str) -> str:
    if len(text) > MAX_VALUE_LEN:
        return text[:MAX_VALUE_LEN] + f"\n… [truncated, {len(text)} chars total]"
    return text
