"""The Spark engine: an IPython InteractiveShell with a persistent namespace and
a live SparkSession injected in. This is the in-process core that the worker
process wraps with IPC; it has no knowledge of MCP or process boundaries, so it
can be unit-tested directly.
"""

from __future__ import annotations

import datetime
import decimal
import io
import json
import os
import re
import shutil
import threading
import sys
import time
import traceback as _tb
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from pathlib import Path

from .protocol import FEATURES, PROTOCOL_VERSION  # noqa: F401  (re-exported; profiles.manifest imports it from here)
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
    notices: list[str] = field(default_factory=list)  # e.g. first-touch mounts during this cell
    interrupted: bool = False  # the cell was stopped by `interrupt`
    displays: list[dict] = field(default_factory=list)  # display(df) results: Arrow metadata; blobs ride alongside

    def to_dict(self) -> dict:
        return asdict(self)


def _hadoop_file_uri(path: Path) -> str:
    """`file:/…` for a local path, spelled the way Hadoop's Path expects: scheme plus the
    raw absolute path, no percent-encoding. `Path.as_uri()` encodes a space as `%20`,
    and Hadoop takes that string as already-raw, so the clone of a table under
    `C:/Users/x/App Data/...` landed in a sibling directory literally named `App%20Data`
    that the shadow lister never saw (Cobalt, 0.6.2)."""
    p = Path(path).resolve().as_posix()
    return "file:" + (p if p.startswith("/") else "/" + p)


def _safe(fn, default=None):
    try:
        return fn()
    except Exception:
        return default


@dataclass
class Context:
    """One notebook's REPL inside the shared JVM: its own Python namespace (a
    module whose dict IPython runs cells in) and its own SparkSession
    (`newSession()`: isolated temp views, SQL conf, current database, UDFs;
    shared SparkContext, catalog, clones, cache, jars)."""

    id: str
    spark: object
    module: object
    default_lakehouse: str | None = None
    default_schema: str | None = None
    created_at: float = field(default_factory=time.time)
    cells: int = 0
    seeded: bool = False  # IPython's hidden names (In, Out, get_ipython, …) added on first activation
    name: str | None = None  # display name (the notebook's title); job group in the Spark UI
    last_activity: float | None = None
    pending_drop: bool = False  # drop_context(force=True) on the control socket while a cell runs

    @property
    def ns(self) -> dict:
        return self.module.__dict__


class InterruptedQuery(RuntimeError):
    """run_sql was stopped by `interrupt`; the worker reports it with interrupted=true."""


@dataclass
class SqlResult:
    """Result of running a SQL query."""

    columns: list[str] = field(default_factory=list)
    rows: list[list] = field(default_factory=list)
    row_count: int = 0
    truncated: bool = False
    limit: int = 0
    notices: list[str] = field(default_factory=list)
    arrow: dict | None = None  # when requested: {"arrow_bytes", "row_count", "truncated"}; the IPC stream rides alongside
    metrics: dict | None = None  # DML commit metrics: affected_rows, inserted/updated/deleted, operation, source
    batches: int | None = None  # streamed run_sql: number of batch events sent before this reply
    elapsed_s: float | None = None

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
        notebooks_root: str | None = None,
        files_mode: str = "mirror",
        files_sync: list[str] | None = None,
        mirror_root: str | None = None,
        preload: list[str] | None = None,
        preload_workers: int = 32,
        extra_jars: list[str] | None = None,
        extra_packages: list[str] | None = None,
    ):
        self.default_sql_limit = default_sql_limit
        self.notebooks_root = notebooks_root
        self.files_sync = list(files_sync or [])
        if files_mode not in ("mirror", "lazy"):
            raise ValueError(f"files_mode must be 'mirror' or 'lazy', not {files_mode!r}")
        self.files_mode = files_mode
        self.files: "FilesMirror | None" = None
        self.files_link: dict | None = None
        self.files_sync_report: list[dict] = []
        self.spark_working_dir: str | None = None
        self.started_at = time.time()  # kernel start, for session_info (ADO #302)
        self._notice_lock = threading.Lock()
        self._stray_notices: list[str] = []  # mount notices that belong to a user cell, drained by a background thread
        self._claimed_mounts: set[str] = set()  # lowercased "lh.table" being mounted by preload / mount_tables: not the user's notice
        self._pending_notices: list[str] = []  # engine-level notices for the next result
        self._preload_thread: threading.Thread | None = None
        self._cell_running = False
        self._cell_started: float | None = None  # for control-socket `status`
        self._last_activity: float = time.time()  # last cell / query / preload end, for `status.idle_s`
        self._cell_method: str | None = None
        self._cell_gen = 0  # bumps per cell; an interrupt watchdog only acts on the cell it was started for
        self._interrupt_requested = False
        self._displays: list[dict] = []
        self._blobs: list[bytes] = []
        self.blobs_out: list[bytes] = []  # binary payloads for the worker to send after the next reply
        self.preload_state: dict = {"state": "idle", "lakehouses": {}, "started_at": None, "finished_at": None,
                                    "workers": preload_workers, "tables_total": 0, "tables_done": 0, "tables_failed": 0}
        self.preload_workers = preload_workers
        self.extra_jars = list(extra_jars or [])
        self.extra_packages = list(extra_packages or [])
        self._notebook_index: dict | None = None
        self._cred = None
        self._fabric_client = None
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

        env = dict(env or {})
        if onelake:  # Fabric mode, even with no lakehouse yet: register_lakehouse can add them later
            from .discovery import LakehouseInfo as _LI
            from .files import FilesMirror

            registry = {lh["name"]: _LI(name=lh["name"], id=lh["id"], workspace_id=lh["workspace_id"]) for lh in lakehouses}
            self.files = FilesMirror(
                root=Path(mirror_root).expanduser() if mirror_root else self._state_root / "lakehouses",
                workspace_id=workspace_id or "", lakehouses=registry, write_mode=write_mode,
                credential_factory=self.credential,
            )
            if default_lakehouse:
                try:
                    env["LOCAL_SPARK_FILES_ROOT"] = str(self.files.mirror_dir(default_lakehouse))
                except LookupError:
                    pass  # reported by _register_lakehouses

        self.dv_strategy = _dv_strategy()
        self._onelake = dict(onelake) if onelake else None
        self._lakehouse_schemas: dict[str, list[str]] = {}
        self._default_schemas: dict[str, str] = {}
        self._runtime_confs: dict[str, str] = {}  # confs set after start, re-applied to every context's session
        self.contexts: dict[str, Context] = {}
        self._cell_context: str | None = None
        catalog = None
        if onelake:
            catalog = {
                "dv_strategy": self.dv_strategy,
                "workspace_id": workspace_id or "",  # set by the first register_lakehouse when empty
                "lakehouses": {lh["name"]: lh["id"] for lh in lakehouses},
                "write_mode": write_mode,
                # a file: URI, so shadows resolve whatever a session's default filesystem is (files_mode = lazy)
                "shadow_root": _hadoop_file_uri(self.shadow_root),
            }
        self.spark = build_spark(
            extra_jars=self.extra_jars,
            extra_packages=self.extra_packages,
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
        self._tame_sigint()
        self._install_interrupt_log_filter()
        self._register_lakehouses(lakehouses, default_lakehouse)
        self._bootstrap_namespace()
        # The "default" context is the root session and IPython's own namespace, so a
        # host that never asks for contexts sees exactly the single-REPL behaviour.
        default_ctx = Context("default", self.spark, self.shell.user_module, self.default_lakehouse, None, seeded=True)
        self.contexts["default"] = default_ctx
        self._active: Context = default_ctx
        if self.default_lakehouse:
            self._set_default_fs(self.spark, self.default_lakehouse)
        self._lazy_hooks = None
        if self.files_mode == "lazy" and self.files is not None:
            from .lazy_files import LazyFilesHooks

            self._lazy_hooks = LazyFilesHooks(self)
            self._lazy_hooks.install()
        if self.files is not None and self.default_lakehouse:
            self._activate_files(self.default_lakehouse)
        if preload and getattr(self, "lakehouses", None):
            self.start_preload(preload)

    def _activate_files(self, lakehouse: str) -> None:
        """Point /lakehouse/default at this lakehouse's mirror and pull the
        configured Files/ subtrees (cached: unchanged files are skipped)."""
        self.files_link = self.files.link_default(lakehouse)
        if self.files_mode == "mirror":
            self._set_spark_working_dir(Path(self.files_link["files_root"]).parent)
        for rel in self.files_sync:
            try:
                self.files_sync_report.append(self.files.pull(lakehouse, [rel]).to_dict())
            except Exception as exc:  # keep the session usable; report instead
                self.files_sync_report.append({"direction": "pull", "lakehouse": lakehouse, "paths": [rel],
                                               "errors": [f"{type(exc).__name__}: {exc}"]})

    def _files_fs_uri(self, lakehouse: str) -> str | None:
        """`lakehouse://<ws>@<lh>.onelake...`: the filesystem a session's relative
        `Files/` resolves against under files_mode = lazy (ch.fs.LakehouseFileSystem)."""
        from .discovery import ONELAKE_HOST

        info = self._resolve_lakehouse(lakehouse)
        if info is None:
            return None
        return f"lakehouse://{info.workspace_id}@{info.id}.{ONELAKE_HOST}"

    def _set_default_fs(self, spark, lakehouse: str | None) -> str | None:
        """files_mode = lazy: a session's `fs.defaultFS` (its Hadoop configuration is
        derived from its SQL conf, so this is per session, hence per context) is the
        lakehouse's OneLake root: `spark.read.csv("Files/x")` reads OneLake directly,
        and a write under sandbox/readonly is refused by the filesystem. Returns the
        URI set, or None (mirror mode, or no lakehouse: back to file:///)."""
        if self.files_mode != "lazy" or self._onelake is None:
            return None
        uri = self._files_fs_uri(lakehouse) if lakehouse else None
        spark.conf.set("fs.defaultFS", uri or "file:///")
        return uri

    def _set_spark_working_dir(self, lakehouse_dir: Path) -> None:
        """On Fabric a relative `Files/x` resolves against the default lakehouse.
        Locally the default filesystem is file:/// with the JVM's cwd as its
        working directory; point that working directory at the lakehouse mirror
        dir (<...>/<lakehouse-id>, whose Files/ is the mirror) so
        spark.read.csv("Files/x") and df.write...("Files/out") land there too."""
        try:
            jvm = self.spark._jvm
            fs = jvm.org.apache.hadoop.fs.FileSystem.getLocal(self.spark._jsc.hadoopConfiguration())
            fs.setWorkingDirectory(jvm.org.apache.hadoop.fs.Path(str(lakehouse_dir)))
            self.spark_working_dir = str(lakehouse_dir)
        except Exception as exc:  # pragma: no cover - best effort
            self.spark_working_dir = None
            print(f"local-spark: could not set the Spark working directory: {exc}", file=sys.stderr)

    def mirror_status(self) -> dict:
        """The Files mirror per lakehouse: pulled subtrees, lazily fetched files, local size."""
        if self.files is None:
            raise RuntimeError("no Fabric workspace configured (set [workspace] in local-spark.toml)")
        out = self.files.status()
        out["files_mode"] = self.files_mode
        return out

    def clear_mirror(self, lakehouse: str | None = None, paths: list[str] | None = None) -> dict:
        """Delete mirror contents (subtrees of one lakehouse, one lakehouse, or all).
        Unpushed local writes go with them; the next open fetches again."""
        if self.files is None:
            raise RuntimeError("no Fabric workspace configured (set [workspace] in local-spark.toml)")
        return self.files.clear(lakehouse, paths)

    def sync_files(self, paths: list[str] | None = None, direction: str = "pull", lakehouse: str | None = None) -> dict:
        if self.files is None:
            raise RuntimeError("no Fabric workspace configured (set [workspace] in local-spark.toml)")
        name = lakehouse or self.default_lakehouse
        if not name:
            raise RuntimeError("no lakehouse given and no default lakehouse configured")
        if direction == "pull":
            return self.files.pull(name, paths or self.files_sync or None).to_dict()
        if direction == "push":
            return self.files.push(name, paths).to_dict()
        raise ValueError("direction must be 'pull' or 'push'")

    def _make_shell(self):
        from IPython.core.interactiveshell import InteractiveShell

        shell = InteractiveShell.instance()
        # Plain (non-ANSI) tracebacks — the transport is text, not a terminal.
        shell.run_line_magic("colors", "nocolor")
        return shell

    def _bootstrap_namespace(self):
        """Seed the namespace with the things a Fabric notebook would have."""
        self._install_delta_forname_bridge()
        self._install_notebookutils()
        self._seed_namespace(self.shell.user_ns, self.spark)

    def _seed_namespace(self, ns: dict, spark) -> None:
        import pyspark.sql.functions as F
        import pyspark.sql.types as T
        from pyspark.sql import Window

        ns.update({"spark": spark, "sc": spark.sparkContext, "F": F, "T": T, "Window": Window, "display": self.display,
                   "notebookutils": self._shim, "mssparkutils": self._shim})

    # ---- contexts ----

    def _resolve_context(self, context: str | None) -> Context:
        ctx = self.contexts.get(context or "default")
        if ctx is None:
            raise ValueError(f"unknown context {context!r}; known: {sorted(self.contexts)}")
        return ctx

    def _activate(self, ctx: Context) -> None:
        """Run the next cell in this context: swap its namespace into the (singleton)
        IPython shell. Execution is sequential, so one shell serves every context."""
        if self._active is not ctx:
            self.shell.user_module = ctx.module
            self.shell.user_ns = ctx.ns
            self._active = ctx
        if not ctx.seeded:
            self.shell.init_user_ns()  # In, Out, _, get_ipython, exit: hidden names IPython expects in user_ns
            ctx.seeded = True

    def create_context(self, id: str, default_lakehouse: str | None = None, default_schema: str | None = None,
                       name: str | None = None) -> dict:
        """A new isolated REPL: fresh namespace and `spark.newSession()` with the
        engine's runtime confs re-applied (a new session does not inherit them), its
        current database set from the default lakehouse (and schema) like a Fabric
        notebook's. Shares the SparkContext, catalog, clones, and cache with the rest."""
        import types

        if not isinstance(id, str) or not id.strip():
            raise ValueError("create_context: id must be a non-empty string")
        if id in self.contexts:
            raise ValueError(f"context {id!r} already exists")
        sess = self.spark.newSession()
        for k, v in self._runtime_confs.items():
            sess.conf.set(k, v)
        module = types.ModuleType("__main__")
        module, _ns = self.shell.prepare_user_module(module)
        self._seed_namespace(module.__dict__, sess)
        ctx = Context(id, sess, module, None, None, name=(name or None))
        if default_lakehouse:
            info = self._resolve_lakehouse(default_lakehouse)
            if info is None:
                raise ValueError(f"unknown lakehouse {default_lakehouse!r}; known: {sorted(self.lakehouses)}")
            if default_schema:
                if default_schema not in self._lakehouse_schemas.get(info.name, []):
                    raise ValueError(f"lakehouse {info.name!r} has no schema {default_schema!r}; "
                                     f"known: {self._lakehouse_schemas.get(info.name, [])}")
                db = self._default_db(info.name, default_schema)
            else:
                db = self._default_db(info.name)
            sess.sql(f"USE {db}")
            ctx.default_lakehouse, ctx.default_schema = info.name, default_schema or (
                self._default_schemas.get(info.name) if self._lakehouse_schemas.get(info.name) else None)
            self._set_default_fs(sess, info.name)
        self.contexts[id] = ctx
        return self._context_info(ctx)

    def drop_context(self, id: str, force: bool = False) -> dict:
        """Release a context. On the data socket a request waits behind the running
        cell, so the context is idle by the time this runs; on the control socket
        (where a cell may be in flight) ``force`` interrupts the cell and the drop
        happens as soon as it ends (`scheduled: true`)."""
        if id == "default":
            raise ValueError("the default context cannot be dropped")
        ctx = self._resolve_context(id)
        if self._cell_running and self._cell_context == id:
            if not force:
                raise RuntimeError(f"context {id!r} is running a cell; interrupt it first (or drop with force)")
            ctx.pending_drop = True
            self.interrupt(context=id)
            return {"id": id, "dropped": False, "scheduled": True, "contexts": sorted(self.contexts)}
        try:  # temp views belong to the session; drop them so the JVM can release the plans
            it = ctx.spark._jsparkSession.sessionState().catalog().listLocalTempViews("*").iterator()
            names = []
            while it.hasNext():
                names.append(str(it.next().table()))
            for n in names:
                ctx.spark.catalog.dropTempView(n)
        except Exception:
            pass
        del self.contexts[id]
        if self._active is ctx:
            self._activate(self.contexts["default"])
        ctx.module.__dict__.clear()
        return {"id": id, "dropped": True, "scheduled": False, "contexts": sorted(self.contexts)}

    def _context_info(self, ctx: Context) -> dict:
        return {"id": ctx.id, "name": ctx.name, "default_lakehouse": ctx.default_lakehouse, "default_schema": ctx.default_schema,
                "dropping": ctx.pending_drop,
                "files_fs": _safe(lambda: ctx.spark.conf.get("fs.defaultFS")) if self.files_mode == "lazy" else None,
                "current_database": _safe(ctx.spark.catalog.currentDatabase), "current_catalog": _safe(ctx.spark.catalog.currentCatalog),
                "created_at": ctx.created_at, "cells": ctx.cells, "last_activity": ctx.last_activity,
                "idle_s": None if (self._cell_running and self._cell_context == ctx.id) else
                (round(time.time() - ctx.last_activity, 1) if ctx.last_activity else None)}

    def _set_conf(self, key: str, value: str) -> None:
        """A runtime conf that every context's session must share (lakehouse ids,
        schema catalogs): set on the root session, remembered for contexts created
        later, pushed to the ones that exist."""
        self.spark.conf.set(key, value)
        self._runtime_confs[key] = value
        for ctx in self.contexts.values():
            if ctx.spark is not self.spark:
                _safe(lambda: ctx.spark.conf.set(key, value))

    def _unset_conf(self, key: str) -> None:
        self._runtime_confs.pop(key, None)
        for sess in [self.spark] + [c.spark for c in self.contexts.values() if c.spark is not self.spark]:
            _safe(lambda: sess.conf.unset(key))

    def _install_notebookutils(self) -> None:
        """Make ``import notebookutils`` / ``import mssparkutils`` resolve to the
        local shim inside cells."""
        import sys

        from .notebookutils_shim import NotebookUtils

        shim = NotebookUtils(_ShimEngine(self))
        # Assign, don't setdefault: IPython's shell is a process-wide singleton,
        # so a later engine in the same process must replace an earlier shim.
        sys.modules["notebookutils"] = shim
        sys.modules["mssparkutils"] = shim
        self._shim = shim

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
        # Wrap Delta's own forName, never a previous engine's bridge: the class is
        # process-wide, and tests build several engines in one process.
        original = getattr(DeltaTable, "_localspark_original_forName", None) or DeltaTable.forName
        DeltaTable._localspark_original_forName = original

        def forName(cls, sparkSession, tableOrViewName):
            engine._refuse_if_readonly(tableOrViewName)
            sparkSession.table(tableOrViewName)
            if engine.write_mode != "writethrough" and (dv := engine.is_dv_table(tableOrViewName)):
                raise PermissionError(engine.dv_refusal(dv))
            return original(sparkSession, tableOrViewName)

        DeltaTable.forName = classmethod(forName)

        # DeltaTable.create*(spark).tableName("lh.t").execute() consults the V1
        # catalog too (dwlib's ChangeMgr does this): touch the table first so an
        # untouched OneLake table is materialized under the write policy.
        try:
            from delta.tables import DeltaTableBuilder
        except ImportError:  # pragma: no cover
            return
        orig_table_name = getattr(DeltaTableBuilder, "_localspark_original_tableName", None) or DeltaTableBuilder.tableName
        orig_execute = getattr(DeltaTableBuilder, "_localspark_original_execute", None) or DeltaTableBuilder.execute
        DeltaTableBuilder._localspark_original_tableName = orig_table_name
        DeltaTableBuilder._localspark_original_execute = orig_execute

        def tableName(self_, identifier):
            self_._localspark_table = identifier
            return orig_table_name(self_, identifier)

        def execute(self_):
            name = getattr(self_, "_localspark_table", None)
            if name:
                engine._touch_table(name)
            return orig_execute(self_)

        DeltaTableBuilder.tableName = tableName
        DeltaTableBuilder.execute = execute

    def _touch_table(self, name: str) -> None:
        """Resolve a lakehouse table through OneLakeCatalog (materializing it on
        first touch); a table that does not exist yet is not an error here."""
        try:
            self.spark.table(name)
        except Exception:
            pass

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

    @classmethod
    def _fq(cls, db: str, table: str | None = None) -> str:
        """A session-catalog name qualified with `spark_catalog`, so engine
        internals resolve the same whatever catalog the user made current
        (`USE <lakehouse>` on a schema-enabled lakehouse switches to its V2 catalog)."""
        name = f"spark_catalog.{cls._q(db)}"
        return name if table is None else f"{name}.{cls._q(table)}"

    def _current_db_name(self, spark=None) -> str:
        """`<catalog>.<database>` of the session's current namespace, quoted, for a
        later `USE` that restores it exactly (the user may have `USE`d a V2 catalog)."""
        cat, db = (spark if spark is not None else self.spark).sql("SELECT current_catalog(), current_schema()").first()
        return f"{self._q(cat)}.{self._q(db)}"

    def _register_lakehouses(self, lakehouses: list[dict], default_lakehouse: str | None = None) -> None:
        """Register each (non-excluded) lakehouse as a Spark database and select
        the default. Tables are NOT mounted here: OneLakeCatalog resolves them on
        first touch (and mount_table/mount_tables force it explicitly)."""
        from .discovery import LakehouseInfo

        self.lakehouses: dict[str, LakehouseInfo] = {}
        self._mounted: dict[str, set] = {}  # lakehouse name -> mounted table names
        for lh in lakehouses:
            self._register_one(lh)
        if default_lakehouse and self.lakehouses:
            info = self._resolve_lakehouse(default_lakehouse)
            if info is None:
                raise ValueError(
                    f"default lakehouse {default_lakehouse!r} is not in the workspace "
                    f"(or is excluded); known: {sorted(self.lakehouses)}"
                )
            # Unqualified names now resolve here, like a Fabric notebook's default lakehouse.
            self.spark.sql(f"USE {self._default_db(info.name)}")
            self.default_lakehouse = info.name

    def _register_one(self, lh: dict, runtime: bool = False):
        """Register one lakehouse: a session database, schema databases and the
        schema catalog when it has schemas, and (after init) the catalog confs the
        session was not started with. The OneLake catalog jar reads
        `spark.localspark.lakehouse.<name>` from the session conf on every
        resolution, so a lakehouse added here resolves on first touch like the rest."""
        from .discovery import LakehouseInfo

        info = LakehouseInfo(name=lh["name"], id=lh["id"], workspace_id=lh["workspace_id"])
        self.lakehouses[info.name] = info
        if self.files is not None:
            self.files.lakehouses[info.name] = info  # the Files mirror resolves /lakehouse/<name> by this registry
        if runtime and self._onelake:
            self._set_conf(f"spark.localspark.lakehouse.{info.name}", info.id)
            session_ws = self.spark.conf.get("spark.localspark.workspace_id", "")
            if not session_ws:  # a session started with no lakehouses has no workspace yet; the resolver needs one
                self._set_conf("spark.localspark.workspace_id", info.workspace_id)
            elif info.workspace_id != session_ws:
                self._set_conf(f"spark.localspark.lakehouse_ws.{info.name}", info.workspace_id)
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self._fq(info.name)}")
        # Schema-enabled lakehouse (Tables/<schema>/<table>): the host may say so
        # ("schemas": [...]); otherwise detected from the OneLake listing. Each
        # schema becomes a session database `<lakehouse>__<schema>`, and the
        # lakehouse gets a V2 catalog so `lakehouse.schema.table` works as on Fabric.
        schemas = lh.get("schemas")
        if schemas is None and lh.get("detect_schemas", True) and self._onelake:
            schemas = self._detect_schemas(info)
        if schemas:
            self._register_schemas(info, list(schemas), lh.get("default_schema") or "dbo")
        return info

    def register_lakehouse(self, entry: dict) -> dict:
        """Attach a lakehouse after init (same entry shape as `init`'s
        `lakehouses`): its tables resolve by name from now on, its shadows live
        under the shared shadow root keyed by lakehouse id."""
        for key in ("name", "id", "workspace_id"):
            if not entry.get(key):
                raise ValueError(f"register_lakehouse: {key!r} is required")
        existing = self._resolve_lakehouse(entry["name"])
        if existing is not None and existing.id != entry["id"]:
            raise ValueError(f"lakehouse {entry['name']!r} is already registered with id {existing.id}; unregister it first")
        info = self._register_one(entry, runtime=True)
        return {"name": info.name, "id": info.id, "workspace_id": info.workspace_id,
                "schemas": self._lakehouse_schemas.get(info.name, []), "lakehouses": sorted(self.lakehouses)}

    def unregister_lakehouse(self, name: str) -> dict:
        """Detach a lakehouse: names stop resolving to OneLake, and its database is
        dropped from the session catalog when it holds nothing but shadows (the
        shadow files stay; re-registering re-links them). A schema catalog plugin
        already loaded by Spark stays loaded until the runtime restarts, but
        resolves nothing once the lakehouse conf is gone."""
        info = self._resolve_lakehouse(name)
        if info is None:
            raise ValueError(f"unknown lakehouse {name!r}; known: {sorted(self.lakehouses)}")
        dropped = []
        for db in [info.name] + [f"{info.name}__{sc}" for sc in self._lakehouse_schemas.get(info.name, [])]:
            try:
                self.spark.sql(f"DROP DATABASE IF EXISTS {self._fq(db)} CASCADE")
                dropped.append(db)
            except Exception as exc:
                print(f"local-spark: could not drop database {db}: {exc}", file=sys.stderr)
        for key in (f"spark.localspark.lakehouse.{info.name}", f"spark.localspark.lakehouse_ws.{info.name}",
                    f"spark.sql.catalog.{info.name}", f"spark.sql.catalog.{info.name}.lakehouse",
                    f"spark.sql.catalog.{info.name}.default_schema"):
            self._unset_conf(key)
        self.lakehouses.pop(info.name, None)
        if self.files is not None:
            self.files.lakehouses.pop(info.name, None)
        self._lakehouse_schemas.pop(info.name, None)
        self._default_schemas.pop(info.name, None)
        self._mounted.pop(info.name, None)
        if self.default_lakehouse == info.name:
            self.default_lakehouse = None
        return {"name": info.name, "dropped_databases": dropped, "lakehouses": sorted(self.lakehouses)}

    def _detect_schemas(self, info) -> list[str]:
        """Schema folders under Tables/ (directories that are not Delta tables but
        contain Delta tables), via the JVM's authenticated OneLake filesystem."""
        try:
            entries = [str(e) for e in self.spark._jvm.ch.fs.OneLakeCatalog.listOneLakeTables(info.workspace_id, info.id)]
        except Exception as exc:
            print(f"local-spark: could not list Tables/ of {info.name}: {exc}", file=sys.stderr)
            return []
        return sorted({e.split("/", 1)[0] for e in entries if "/" in e})

    def _default_db(self, lakehouse: str, schema: str | None = None) -> str:
        """What to `USE` for this default lakehouse. A schema-enabled lakehouse: its
        catalog (`USE <lakehouse>`, or `<lakehouse>.<schema>`), so the session's
        current catalog is the lakehouse as on Fabric and `t`, `dbo.t`, and
        `<lh>.dbo.t` all resolve; the catalog passes `delta.\`path\`` and other
        lakehouses' two-part names through to the session catalog, and engine
        internals are `spark_catalog.`-qualified, so nothing else moves. A plain
        lakehouse: its session database, `spark_catalog.`-qualified."""
        schemas = self._lakehouse_schemas.get(lakehouse)
        if schemas:
            if schema:
                return f"{self._q(lakehouse)}.{self._q(schema)}"
            return self._q(lakehouse)
        return self._fq(lakehouse)

    def _register_schemas(self, info, schemas: list[str], default_schema: str) -> None:
        self._lakehouse_schemas[info.name] = schemas
        self._default_schemas[info.name] = default_schema
        for schema in schemas:
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self._fq(f'{info.name}__{schema}')}")
        # `test.dbo.publicholidays` / `USE test` (-> dbo), as on Fabric: a thin V2
        # catalog named after the lakehouse that delegates to the session catalog.
        self._set_conf(f"spark.sql.catalog.{info.name}", "ch.fs.OneLakeSchemaCatalog")
        self._set_conf(f"spark.sql.catalog.{info.name}.lakehouse", info.name)
        self._set_conf(f"spark.sql.catalog.{info.name}.default_schema", default_schema)

    def list_tables(self, lakehouse: str) -> list[str]:
        """Table names from OneLake storage (not the Fabric REST endpoint, which
        refuses schema-enabled lakehouses): `table` for Tables/<table>,
        `schema/table` for Tables/<schema>/<table>."""
        info = self._resolve_lakehouse(lakehouse)
        if info is None:
            raise ValueError(f"unknown lakehouse {lakehouse!r}; known: {sorted(self.lakehouses)}")
        return [str(e) for e in self.spark._jvm.ch.fs.OneLakeCatalog.listOneLakeTables(info.workspace_id, info.id)]

    @staticmethod
    def _qualified(lakehouse: str, entry: str) -> tuple[str, str]:
        """OneLake entry -> (session database, table): 'dbo/t' -> ('lh__dbo', 't')."""
        if "/" in entry:
            schema, table = entry.split("/", 1)
            return f"{lakehouse}__{schema}", table
        return lakehouse, entry

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
        # `table` or `schema/table` (the entry form list_tables and preload use), so a
        # schema-enabled lakehouse's table has a spelling mount_table accepts too.
        db, tb = self._qualified(info.name, table)
        self.spark.table(self._fq(db, tb))
        with self._notice_lock:
            self._mounted.setdefault(info.name, set()).add(table)
        return {
            "lakehouse": info.name,
            "table": table,
            "database": db,
            "path": info.table_path(table),
            "write_mode": self.write_mode,
        }

    def mount_tables(self, lakehouse: str, tables: list[str], workers: int | None = None) -> dict:
        """Materialize several tables in parallel; each is an independent Delta
        log read from OneLake. Per-table errors are captured, not fatal. The
        mount notices these produce are consumed here (the result lists them),
        not left for the next cell."""
        workers = workers or self.preload_workers
        timings: dict[str, float] = {}
        lh_name = self._resolve_lakehouse(lakehouse).name if self._resolve_lakehouse(lakehouse) else lakehouse
        claimed = {f"{lh_name}.{t}".lower() for t in tables}
        with self._notice_lock:
            self._claimed_mounts |= claimed

        def one(table: str):
            t0 = time.time()
            try:
                self.mount_table(lakehouse, table)
                return table, None, time.time() - t0
            except Exception as exc:  # keep going; report per-table
                return table, f"{type(exc).__name__}: {exc}", time.time() - t0

        mounted, failed = [], []
        try:
            with ThreadPoolExecutor(max_workers=max(1, min(workers, len(tables) or 1))) as pool:
                for table, error, secs in pool.map(one, tables):
                    timings[table] = round(secs, 2)
                    if error is None:
                        mounted.append(table)
                    else:
                        failed.append({"table": table, "error": error})
        finally:
            self._consume_mount_notices(claimed)
        return {"lakehouse": lakehouse, "mounted": mounted, "failed": failed, "seconds": timings}

    # ---- eager catalog population (preload) ----

    def start_preload(self, lakehouses: list[str] | None = None, workers: int | None = None) -> dict:
        """Materialize every table of the given lakehouses (names, or ["all"]) in
        a background thread, `workers` tables at a time. Returns the status
        immediately; the next result after completion carries a notice. A second
        call while one runs returns the running status unchanged."""
        if self._preload_thread is not None and self._preload_thread.is_alive():
            return self.preload_status()
        explicit: dict[str, list[str] | None] = {}
        if isinstance(lakehouses, dict):  # {"lakehouse": ["t1", "dbo/t2"]} — no listing needed
            for n, tables in lakehouses.items():
                info = self._resolve_lakehouse(n)
                if info is None:
                    raise ValueError(f"unknown lakehouse {n!r}; known: {sorted(self.lakehouses)}")
                explicit[info.name] = list(tables) if tables else None
            targets = list(explicit)
        else:
            names = list(lakehouses or []) or ["all"]
            if any(n.lower() == "all" for n in names):
                targets = sorted(self.lakehouses)
            else:
                targets = []
                for n in names:
                    info = self._resolve_lakehouse(n)
                    if info is None:
                        raise ValueError(f"unknown lakehouse {n!r}; known: {sorted(self.lakehouses)}")
                    targets.append(info.name)
        workers = workers or self.preload_workers
        self.preload_state = {"state": "running", "lakehouses": {n: {"state": "pending"} for n in targets},
                              "started_at": time.time(), "finished_at": None, "workers": workers,
                              "tables_total": 0, "tables_done": 0, "tables_failed": 0}
        self._preload_thread = threading.Thread(target=self._run_preload, args=(targets, workers, explicit), name="lsm-preload", daemon=True)
        self._preload_thread.start()
        return self.preload_status()

    def _run_preload(self, targets: list[str], workers: int, explicit: dict | None = None) -> None:
        st = self.preload_state
        explicit = explicit or {}
        try:
            for name in targets:
                info = self._resolve_lakehouse(name)
                entry = st["lakehouses"][name]
                entry.update({"state": "listing"})
                t0 = time.time()
                try:
                    # Listing comes from OneLake storage through the JVM (already
                    # authenticated), not the Fabric REST endpoint: no extra credential,
                    # and it works for schema-enabled lakehouses (Tables/<schema>/<table>).
                    tables = explicit.get(name) or self.list_tables(name)
                except Exception as exc:
                    entry.update({"state": "failed", "error": f"{type(exc).__name__}: {exc}", "seconds": round(time.time() - t0, 1)})
                    print(f"local-spark: preload of {name} could not list tables: {exc}", file=sys.stderr)
                    continue
                already = self._mounted.get(info.name, set())
                todo = [t for t in tables if t not in already]
                qualified = {t: self._qualified(info.name, t) for t in todo}
                with self._notice_lock:
                    self._claimed_mounts |= {f"{db}.{tb}".lower() for db, tb in qualified.values()}
                entry.update({"state": "mounting", "total": len(tables), "done": len(tables) - len(todo), "failed": 0})
                st["tables_total"] += len(tables)
                st["tables_done"] += len(tables) - len(todo)

                def one(table: str, _info=info, _entry=entry, _q=qualified):
                    db, tb = _q[table]
                    try:
                        self.spark.table(self._fq(db, tb))
                        with self._notice_lock:
                            self._mounted.setdefault(_info.name, set()).add(table)
                        return None
                    except Exception as exc:
                        return f"{type(exc).__name__}: {str(exc).splitlines()[0][:200]}"
                    finally:
                        self._consume_mount_notices({f"{db}.{tb}".lower()})

                errors = {}
                with ThreadPoolExecutor(max_workers=max(1, min(workers, len(todo) or 1)), thread_name_prefix="lsm-preload") as pool:
                    for table, err in zip(todo, pool.map(one, todo)):
                        entry["done"] += 1
                        st["tables_done"] += 1
                        if err:
                            errors[table] = err
                            entry["failed"] += 1
                            st["tables_failed"] += 1
                entry.update({"state": "done", "seconds": round(time.time() - t0, 1), "errors": errors})
            st["state"] = "done"
        except Exception as exc:  # pragma: no cover - defensive
            st["state"] = "failed"
            st["error"] = f"{type(exc).__name__}: {exc}"
        finally:
            st["finished_at"] = time.time()
            self._last_activity = st["finished_at"]
            secs = st["finished_at"] - (st["started_at"] or st["finished_at"])
            done = st["tables_done"] - st["tables_failed"]
            if done:  # failures are reported by preload_status and stderr, never as a notice on an unrelated cell
                with self._notice_lock:
                    self._pending_notices.append(
                        f"preloaded {done} tables across {len(targets)} lakehouse(s) in {secs:.0f} s "
                        f"({st['workers']} workers); every table now resolves without a first-touch mount"
                    )
            if st["tables_failed"] or any(e.get("state") == "failed" for e in st["lakehouses"].values()):
                print(f"local-spark: preload finished with failures: {st}", file=sys.stderr)

    def preload_status(self) -> dict:
        st = dict(self.preload_state)
        if st.get("started_at"):
            st["elapsed_s"] = round((st.get("finished_at") or time.time()) - st["started_at"], 1)
        return st

    def wait_preload(self, timeout: float | None = None) -> dict:
        if self._preload_thread is not None:
            self._preload_thread.join(timeout)
        return self.preload_status()

    def _consume_mount_notices(self, own: set[str]) -> None:
        """A background/explicit mount of `own` tables finished: drain the JVM's
        mount queue, drop our own entries, park the others (a user cell may have
        materialized them meanwhile) for that cell's result, release the claims."""
        with self._notice_lock:
            for n in self._drain_jvm_mounts():
                if _notice_table(n) not in self._claimed_mounts:
                    self._stray_notices.append(n)
            self._claimed_mounts -= own

    def table_features(self, lakehouse: str, tables: list[str], workers: int = 8) -> dict:
        """Delta protocol features per table, read from OneLake without
        materializing anything: {table: {"features": [...], "deletion_vectors": bool,
        "error": str|None}}. Opt-in (one Delta log read per table)."""
        from delta.tables import DeltaTable

        info = self._resolve_lakehouse(lakehouse)
        if info is None:
            raise LookupError(f"unknown lakehouse {lakehouse!r}")

        def one(table: str):
            try:
                row = DeltaTable.forPath(self.spark, info.table_path(table)).detail().select("tableFeatures").first()
                feats = sorted(row[0] or [])
                return table, {"features": feats, "deletion_vectors": "deletionVectors" in feats, "error": None}
            except Exception as exc:
                return table, {"features": [], "deletion_vectors": None, "error": f"{type(exc).__name__}: {str(exc).splitlines()[0][:160]}"}

        with ThreadPoolExecutor(max_workers=max(1, min(workers, len(tables) or 1))) as pool:
            return {"tables": dict(pool.map(one, tables)), "dv_strategy": self.dv_strategy}

    def _running(self, method: str, job_description: str | None = None):
        """Context for one interruptible unit of work (a cell, a query): marks the
        engine busy for `interrupt` / `status`, bumps the cell generation the
        interrupt watchdog is bound to, names the Spark jobs it starts
        (`job_description`, shown in `status.cell.jobs` and the Spark UI), and
        consumes a leftover interrupt at the end."""
        engine = self

        class _Running:
            def __enter__(self_):
                engine._interrupt_requested = False
                engine._cell_gen += 1
                engine._cell_method = method
                engine._cell_context = engine._active.id
                engine._active.cells += 1
                engine._cell_started = time.time()
                engine._cell_running = True
                try:
                    sc = engine.spark.sparkContext
                    ctx = engine._active
                    sc.setLocalProperty("spark.jobGroup.id", ctx.id)
                    sc.setLocalProperty("spark.job.description", (job_description or ctx.name or "")[:200] or None)
                except Exception:
                    pass

            def __exit__(self_, *exc):
                engine._cell_running = False
                engine._cell_started = None
                engine._cell_method = None
                engine._cell_context = None
                engine._last_activity = time.time()
                engine._active.last_activity = engine._last_activity
                if engine._interrupt_requested:
                    engine._drain_pending_interrupt()
                    engine.jvm_alive(retries=12, delay=0.25)  # re-establish this thread's JVM connection (see jvm_alive)
                try:
                    sc = engine.spark.sparkContext
                    sc.setLocalProperty("spark.jobGroup.id", None)
                    sc.setLocalProperty("spark.job.description", None)
                except Exception:
                    pass
                if engine._active.pending_drop:  # requested over the control socket while this cell ran
                    engine.drop_context(engine._active.id)
                return False

        return _Running()

    def _exec(self, code: str, on_output=None, capture_result: bool = False,
              job_description: str | None = None, context: str | None = None) -> tuple[ExecResult, BaseException | None]:
        """Run a cell; also return the raised exception (the runner needs to
        recognize NotebookExit, which IPython otherwise reports as an error).
        ``capture_result``: a Spark or pandas DataFrame that is the cell's last
        expression is attached as a display (Fabric shows a bare `df` as a grid)."""
        from IPython.utils.capture import capture_output

        self._displays, self._blobs = [], []
        self._activate(self._resolve_context(context))
        with self._running("run_code", job_description):
            if on_output is None:
                with capture_output() as cap:
                    result = self.shell.run_cell(code, store_history=True)
                cap_stdout, cap_stderr = cap.stdout, cap.stderr
            else:
                # Stream stdout/stderr as they are written (protocol v2 events) and
                # still collect them for the final result.
                out_tee, err_tee = _Tee("stdout", on_output), _Tee("stderr", on_output)
                with capture_output(stdout=False, stderr=False, display=True) as cap:
                    saved = sys.stdout, sys.stderr
                    sys.stdout, sys.stderr = out_tee, err_tee
                    try:
                        result = self.shell.run_cell(code, store_history=True)
                    finally:
                        sys.stdout, sys.stderr = saved
                        out_tee.close(); err_tee.close()
                cap_stdout, cap_stderr = out_tee.getvalue(), err_tee.getvalue()
        if capture_result and result.success and result.result is not None:
            try:
                self._capture_result(result.result)
            except Exception as exc:  # the cell itself succeeded; say why the grid is missing
                cap_stderr += f"\nlocal-spark: could not capture the result as Arrow: {type(exc).__name__}: {exc}\n"

        error = None
        tb = None
        exc = result.error_before_exec or result.error_in_exec
        if exc is not None and not str(exc):
            error = repr(exc)
        if exc is not None:
            error = f"{type(exc).__name__}: {exc}"
            # IPython's captured stderr is unreliable for tracebacks; format from
            # the exception object directly so the agent always sees the detail.
            if result.error_in_exec is not None:
                tb = "".join(
                    _tb.format_exception(type(exc), exc, exc.__traceback__)
                )

        stdout = cap_stdout
        # Rich display outputs (e.g. displayhook) land in cap.outputs; fold their
        # text/plain representation into stdout so nothing is silently dropped.
        for out in cap.outputs:
            text = out.data.get("text/plain") if hasattr(out, "data") else None
            if text:
                stdout += text + "\n"

        if error and exc is not None and self._interrupt_requested and not isinstance(exc, KeyboardInterrupt):
            error = f"KeyboardInterrupt: interrupted (Spark jobs cancelled); underlying {error}"
        if error:

            stdout = self.annotate_error(stdout) or stdout

            error = self.annotate_error(error) or error

        outcome = ExecResult(
            ok=bool(result.success),
            stdout=_truncate(stdout),
            stderr=_truncate(cap_stderr),
            error=error,
            # By intent, not by exception type: a cancelled Spark job surfaces as a
            # Py4JError / SparkException, not as KeyboardInterrupt.
            interrupted=exc is not None and (isinstance(exc, KeyboardInterrupt) or self._interrupt_requested),
            displays=list(self._displays),
            traceback=_truncate(tb) if tb else None,
            execution_count=self.shell.execution_count,
        )
        return outcome, exc

    _VIEW_WRITE_MARKERS = ("EXPECT_TABLE_NOT_VIEW", "view", "not a Delta table", "DELTA_TABLE_NOT_FOUND",
                           "DELTA_MISSING_DELTA_TABLE", "DELTA_UNSUPPORTED_SOURCE", "UNSUPPORTED_INSERT",
                           "DELTA_MERGE_UNRESOLVED_EXPRESSION", "only supports Delta sources")

    def annotate_error(self, text: str | None) -> str | None:
        """Append the deletion-vector explanation when an error is a write against
        one of the live views standing in for a deletion-vector table."""
        if not text or not any(m in text for m in self._VIEW_WRITE_MARKERS):
            return text
        try:
            dv = self._dv_tables()
        except Exception:
            return text
        for t in dv:
            if re.search(rf"\b{re.escape(t['table'])}\b", text, re.IGNORECASE):
                return text.rstrip("\n") + "\n\nlocal-spark: " + self.dv_refusal(t)
        return text

    def run_code(self, code: str, on_output=None, capture_result: bool = False,
                 job_description: str | None = None, context: str | None = None) -> ExecResult:
        """Run a cell of Python against a context's persistent namespace (the
        default context unless ``context`` names another). ``on_output`` (stream,
        text) receives stdout/stderr as the cell writes them; ``capture_result``
        attaches a bare trailing DataFrame as a display; ``job_description`` names
        the cell's Spark jobs."""
        res = self._exec(code, on_output=on_output, capture_result=capture_result, job_description=job_description,
                         context=context)[0]
        res.notices.extend(self.drain_mount_notices())
        self.blobs_out = list(self._blobs)
        return res

    def interrupt(self, context: str | None = None) -> dict:
        """Stop the running cell: cancel every Spark job, then raise
        KeyboardInterrupt in the cell's thread. Called from the control thread.
        A cell inside a long JVM call returns once its job is cancelled; a tight
        C-extension loop cannot be interrupted. With ``context``, only a cell of
        that context is stopped."""
        import _thread
        import signal

        if not self._cell_running:
            return {"interrupted": False, "state": "idle", "reason": "idle: no cell is running"}
        if context is not None and context != self._cell_context:
            return {"interrupted": False, "state": "idle",
                    "reason": f"idle: context {context!r} is not running (running: {self._cell_context!r})"}
        self._interrupt_requested = True
        # From this (control) thread, py4j's pinned-thread mode gives us our own
        # JVM connection, so the cancel never touches the connection the cell is
        # blocked on. (The cell's own thread must not call into the JVM from its
        # SIGINT handler, which is why _tame_sigint drops pyspark's handler.)
        t0 = time.time()
        try:
            self.spark.sparkContext.cancelAllJobs()
        except Exception as exc:  # the JVM may be gone; still interrupt Python
            cancel = f"cancelAllJobs failed: {type(exc).__name__}: {exc}"
        else:
            cancel = f"spark jobs cancelled in {time.time() - t0:.2f}s"
        # A real SIGINT to the cell's thread: it wakes a C-level time.sleep and a
        # blocking py4j socket read (EINTR), and pyspark's own SIGINT handler then
        # cancels jobs again and raises KeyboardInterrupt. interrupt_main() only
        # sets a pending flag that those blocking calls never check.
        main = threading.main_thread().ident
        if hasattr(signal, "pthread_kill") and main is not None:
            signal.pthread_kill(main, signal.SIGINT)
        else:  # Windows: trips the SIGINT event, which wakes time.sleep and raises on the next bytecode
            _thread.interrupt_main()
        # A job that starts after cancelAllJobs() (still planning when we cancelled)
        # would run to completion: keep cancelling until the cell has ended.
        threading.Thread(target=self._cancel_until_idle, args=(self._cell_gen,), name="lsm-interrupt-watchdog", daemon=True).start()
        return {"interrupted": True, "state": "interrupting", "detail": cancel, "method": self._cell_method,
                "context": self._cell_context,
                "elapsed_s": round(time.time() - self._cell_started, 1) if self._cell_started else None}

    def status(self, context: str | None = None) -> dict:
        """For the control socket: what is running and for how long. Spark's active
        job count comes from this thread's own JVM connection, so it works while
        the cell's thread is blocked in a Spark call. With ``context``, `cell_running`
        and `cell` describe that context only."""
        preload_running = self.preload_state.get("state") == "running"
        running = self._cell_running and (context is None or context == self._cell_context)
        out: dict = {"initialized": True, "cell_running": running, "cell": None,
                     "preload": self.preload_state.get("state", "idle"),
                     "idle_s": None if (self._cell_running or preload_running) else round(time.time() - self._last_activity, 1),
                     "last_activity": self._last_activity, "contexts": sorted(self.contexts),
                     "dropping": sorted(c.id for c in self.contexts.values() if c.pending_drop)}
        if running:
            cell: dict = {"method": self._cell_method, "context": self._cell_context,
                          "context_name": _safe(lambda: self.contexts[self._cell_context].name),
                          "elapsed_s": round(time.time() - (self._cell_started or time.time()), 1),
                          "interrupt_requested": self._interrupt_requested, "active_jobs": None}
            try:
                sc = self.spark.sparkContext
                ids = sorted(sc.statusTracker().getActiveJobsIds())
                cell["active_jobs"] = len(ids)
                store = sc._jsc.sc().statusStore()
                jobs = []
                for jid in ids[:5]:
                    try:
                        j = store.job(jid)
                        desc, grp = j.description(), j.jobGroup()
                        jobs.append({"id": jid, "name": j.name(), "description": desc.get() if desc.isDefined() else None,
                                     "group": grp.get() if grp.isDefined() else None})
                    except Exception:
                        jobs.append({"id": jid})
                cell["jobs"] = jobs
            except Exception:
                pass
            out["cell"] = cell
        return out

    def jvm_alive(self, retries: int = 1, delay: float = 0.25) -> bool:
        """One trivial JVM call, retried. After an interrupt the cell's thread has
        lost its pinned py4j connection (py4j closes it on KeyboardInterrupt) and
        the first reconnect is sometimes answered with an empty line by the
        gateway ("Answer from Java side is empty") although the JVM is healthy;
        the next attempt goes through. Callers that decide "dead JVM" must use
        this with retries rather than trust that text."""
        for attempt in range(max(1, retries)):
            try:
                self.spark._jvm.java.lang.System.currentTimeMillis()
                if attempt:
                    print(f"local-spark: JVM connection re-established after {attempt + 1} attempts", file=sys.stderr)
                return True
            except Exception:
                if attempt + 1 < retries:
                    time.sleep(delay)
        return False

    def _tame_sigint(self) -> None:
        """pyspark installs a SIGINT handler that calls cancelAllJobs() from the
        interrupted thread itself. When the interrupt lands while that thread is
        inside a py4j read (always, for a cell blocked in Spark), the handler's
        call re-enters the same pinned connection: `RuntimeError: reentrant call
        inside <_io.BufferedReader>`, a Py4JNetworkError, and py4j's own error
        logging on the cell's stderr. Our interrupt() cancels jobs from the control
        thread, so the plain KeyboardInterrupt handler is all the cell needs."""
        import signal

        if threading.current_thread() is not threading.main_thread():
            return
        current = signal.getsignal(signal.SIGINT)
        if callable(current) and getattr(current, "__name__", "") == "signal_handler":
            signal.signal(signal.SIGINT, signal.default_int_handler)

    def _install_interrupt_log_filter(self) -> None:
        """While an interrupt is in progress, py4j's `logging.exception(...)` lines
        about the connection it is tearing down ("KeyboardInterrupt while sending
        command", "Exception while sending command") are the interrupt's own
        noise, not the cell's output: drop them from the loggers they come from."""
        import logging

        engine = self

        class _Quiet(logging.Filter):
            def filter(self, record: logging.LogRecord) -> bool:
                if not engine._interrupt_requested:
                    return True
                msg = record.getMessage()
                return not ("while sending command" in msg or record.name.startswith("py4j"))

        for name in (None, "py4j", "py4j.java_gateway", "py4j.clientserver"):
            logging.getLogger(name).addFilter(_Quiet())

    def _drain_pending_interrupt(self) -> None:
        """An interrupt that cancelled the Spark job before Python consumed the
        queued SIGINT would otherwise fire into the NEXT cell (seen on Windows,
        where interrupt_main only queues). Consume it under a no-op handler."""
        import signal

        for _ in range(2):
            try:
                prev = signal.getsignal(signal.SIGINT)
                signal.signal(signal.SIGINT, lambda *_a: None)
                try:
                    time.sleep(0.05)  # a pending interrupt fires here, into the no-op
                finally:
                    signal.signal(signal.SIGINT, prev)
                return
            except KeyboardInterrupt:
                continue  # it fired before the swap; now it is consumed

    def _cancel_until_idle(self, gen: int, timeout: float = 120.0) -> None:
        """Keep cancelling the interrupted cell's jobs until that cell ends. Never
        touches a later cell: the generation is re-checked right before each cancel."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            time.sleep(0.5)
            if not (self._cell_running and self._cell_gen == gen):
                return
            try:
                self.spark.sparkContext.cancelAllJobs()
            except Exception:
                return

    # ---- Arrow results ----

    def _arrow_from_df(self, df, limit: int) -> tuple[bytes, dict]:
        """Arrow IPC stream of up to `limit` rows (fetching one extra to flag
        truncation). Spark 4 has DataFrame.toArrow; Spark 3.5 has the private
        _collect_as_arrow (stable across 3.5.x)."""
        import pyarrow as pa

        head = df.limit(limit + 1)
        if hasattr(head, "toArrow"):
            table = head.toArrow()
        else:
            from pyspark.sql.pandas.types import to_arrow_schema

            batches = head._collect_as_arrow()
            schema = to_arrow_schema(head.schema)
            table = pa.Table.from_batches(batches, schema=schema) if batches else schema.empty_table()
        truncated = table.num_rows > limit
        if truncated:
            table = table.slice(0, limit)
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        data = sink.getvalue().to_pybytes()
        return data, {"arrow_bytes": len(data), "row_count": table.num_rows, "truncated": truncated,
                      "limit": limit, "columns": list(table.schema.names)}

    def _capture_result(self, value) -> None:
        """A bare Spark or pandas DataFrame as the cell's last expression → one
        `displays` entry tagged `source: "result"` (opt-in via capture_result)."""
        import pyarrow as pa

        try:
            from pyspark.sql import DataFrame
        except ImportError:  # pragma: no cover
            DataFrame = ()
        limit = self.default_sql_limit
        if isinstance(value, DataFrame):
            data, meta = self._arrow_from_df(value, limit)
        else:
            try:
                import pandas as pd
            except ImportError:  # pragma: no cover
                return
            if not isinstance(value, pd.DataFrame):
                return
            table = pa.Table.from_pandas(value.head(limit + 1), preserve_index=False)
            truncated = table.num_rows > limit
            if truncated:
                table = table.slice(0, limit)
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, table.schema) as writer:
                writer.write_table(table)
            data = sink.getvalue().to_pybytes()
            meta = {"arrow_bytes": len(data), "row_count": table.num_rows, "truncated": truncated,
                    "limit": limit, "columns": list(table.schema.names)}
        meta.update(kind="arrow", source="result")
        self._displays.append(meta)
        self._blobs.append(data)

    def display(self, obj, limit: int | None = None) -> None:
        """`display(df)` as on Fabric: a DataFrame becomes an Arrow result attached
        to the cell (hosts render it in a grid; the MCP server prints a table);
        anything else is printed."""
        try:
            from pyspark.sql import DataFrame
        except ImportError:  # pragma: no cover
            DataFrame = ()
        if isinstance(obj, DataFrame):
            data, meta = self._arrow_from_df(obj, limit or self.default_sql_limit)
            meta.update(kind="arrow", source="display")
            self._displays.append(meta)
            self._blobs.append(data)
        else:
            print(repr(obj))

    def drain_mount_notices(self) -> list[str]:
        """Notices for the result being built: tables OneLakeCatalog materialized
        since the last drain ("mounted <lakehouse>.<table> in N s (<how>)", so
        first-touch cost is visible next to the cell's output instead of hidden
        in its timing), plus anything a background thread parked for us."""
        with self._notice_lock:
            # entries claimed by a preload / explicit mount in flight are not this cell's
            fresh = [n for n in self._drain_jvm_mounts() if _notice_table(n) not in self._claimed_mounts]
            out = self._stray_notices + fresh + self._pending_notices
            self._stray_notices, self._pending_notices = [], []
        return out

    def _drain_jvm_mounts(self) -> list[str]:
        try:
            items = list(self.spark._jvm.ch.fs.OneLakeCatalog.drainMaterialized())
        except Exception:
            return []
        out = []
        for item in items:
            try:
                name, millis, how = str(item).split("\t")
                out.append(f"mounted {name} in {float(millis) / 1000:.1f} s ({how})")
            except ValueError:
                out.append(f"mounted {item}")
        return out

    # ---- notebook runner ----

    def _resolve_notebook_path(self, path: str) -> Path:
        p = Path(path).expanduser()
        candidates = [p]
        if self.notebooks_root and not p.is_absolute():
            candidates.append(Path(self.notebooks_root).expanduser() / p)
        for c in candidates:
            if c.is_file():
                return c
            if c.is_dir() and (c / "notebook-content.py").is_file():
                return c / "notebook-content.py"
        # a Fabric display name under the notebooks root
        if self.notebooks_root:
            from .notebook import index_notebooks

            if self._notebook_index is None or path not in self._notebook_index:
                self._notebook_index = index_notebooks(self.notebooks_root)
            if path in self._notebook_index:
                return self._notebook_index[path]
        raise FileNotFoundError(
            f"notebook {path!r} not found (tried the path as given"
            + (f", under notebooks root {self.notebooks_root!r}, and as a .platform displayName there" if self.notebooks_root else "; no notebooks root configured")
            + ")"
        )

    def run_notebook(
        self,
        path: str,
        cells=None,
        stop_on_error: bool = True,
        default_lakehouse: str | None = None,
        parameters: dict | None = None,
        context: str | None = None,
        isolated: bool = False,
    ) -> dict:
        """Run a Fabric notebook (Git .py format) cell by cell in a context's namespace.
        ``isolated`` runs it in a throwaway context (own namespace and Spark session,
        dropped afterwards), so the run cannot disturb the caller's variables, temp
        views, SQL conf, or current database; its variables are not visible after."""
        from .notebook import load_notebook, select_cells, strip_line_magics
        from .notebookutils_shim import NotebookExit

        if isolated:
            import uuid

            if context is not None:
                raise ValueError("run_notebook: an isolated run creates its own context; do not pass `context`")
            cid = f"nb-isolated-{uuid.uuid4().hex[:8]}"
            self.create_context(cid, name=f"isolated run: {path}")
            try:
                res = self.run_notebook(path, cells, stop_on_error, default_lakehouse, parameters, context=cid)
            finally:
                _safe(lambda: self.drop_context(cid))
            res["isolated"] = True
            res["context"] = cid
            return res

        nb_path = self._resolve_notebook_path(path)
        nb = load_notebook(nb_path)
        selected = select_cells(cells, len(nb.cells))
        warnings = list(nb.warnings)
        ctx = self._resolve_context(context)
        self._activate(ctx)
        context = ctx.id

        # Default lakehouse for this run: explicit arg, else the notebook's META.
        lh_name = default_lakehouse or nb.default_lakehouse_name
        prev_db = self._current_db_name(ctx.spark)
        effective_db = prev_db
        switched = False
        if lh_name and getattr(self, "lakehouses", None):
            info = self._resolve_lakehouse(lh_name)
            if info is None:
                warnings.append(
                    f"default lakehouse {lh_name!r} is not registered (excluded or not in the "
                    f"workspace); running against {prev_db!r}"
                )
            else:
                ctx.spark.sql(f"USE {self._default_db(info.name)}")
                prev_fs = _safe(lambda: ctx.spark.conf.get("fs.defaultFS"))
                self._set_default_fs(ctx.spark, info.name)
                effective_db, switched = info.name, True
                if self.files is not None and info.name != self.default_lakehouse:
                    self._activate_files(info.name)
                    warnings.append(f"/lakehouse/default repointed to {info.name!r} for this run"
                                    + ("" if self.files_link.get("linked") else f" (link unavailable: {self.files_link.get('reason')})"))
        elif lh_name:
            warnings.append(f"notebook names default lakehouse {lh_name!r} but no Fabric workspace is configured")

        params = dict(parameters or {})
        injected = not params
        results: list[dict] = []
        first_tb = None
        first_error = None
        exit_value = None
        status = "ok"
        try:
            for cell in nb.cells:
                if cell.index not in selected:
                    continue
                entry = {"index": cell.index, "kind": cell.kind, "language": cell.language, "line": cell.line}
                if cell.kind == "markdown":
                    entry["status"] = "skipped"
                    entry["notices"] = self.drain_mount_notices()
                    results.append(entry)
                    continue
                # Parameters override the parameters cell (as a pipeline run does);
                # with no parameters cell, inject before the first code cell.
                if not injected and cell.kind != "parameters" and not nb.has_parameters_cell:
                    self.shell.user_ns.update(params)
                    injected = True
                code, unsupported = strip_line_magics(cell)
                if unsupported:
                    entry["unsupported"] = unsupported
                if cell.language == "sparksql":
                    try:
                        res = self.run_sql(code, context=context)
                        entry["status"] = "ok"
                        entry["stdout"] = _sql_preview(res)
                    except Exception as exc:
                        entry["status"] = "error"
                        entry["error"] = f"{type(exc).__name__}: {exc}".splitlines()[0]
                        first_error = first_error or entry["error"]
                elif cell.language == "python":
                    res, exc = self._exec(code, context=context)
                    entry["stdout"] = res.stdout
                    if isinstance(exc, NotebookExit):
                        entry["status"] = "exited"
                        exit_value = exc.value
                        results.append(entry)
                        break
                    if res.ok:
                        entry["status"] = "ok"
                    else:
                        entry["status"] = "error"
                        entry["error"] = res.error
                        first_error = first_error or res.error
                        first_tb = first_tb or res.traceback
                else:
                    entry["status"] = "unsupported"
                    entry["error"] = f"cell magic %%{cell.cell_magic} is not supported locally"
                    first_error = first_error or entry["error"]
                if cell.kind == "parameters" and not injected:
                    self.shell.user_ns.update(params)
                    injected = True
                results.append(entry)
                if entry["status"] in ("error", "unsupported") and stop_on_error:
                    break
        finally:
            if switched:
                ctx.spark.sql(f"USE {prev_db}")
                if self.files_mode == "lazy" and prev_fs:
                    _safe(lambda: ctx.spark.conf.set("fs.defaultFS", prev_fs))
                if self.files is not None and effective_db != self.default_lakehouse and self.default_lakehouse:
                    self._activate_files(self.default_lakehouse)
        if any(r.get("status") in ("error", "unsupported") for r in results):
            status = "error"
        return {
            "path": str(nb_path),
            "status": status,
            "default_lakehouse": effective_db if switched else None,
            "cells_total": len(nb.cells),
            "cells": results,
            "first_error": first_error,
            "first_traceback": first_tb,
            "exit_value": exit_value,
            "warnings": warnings,
        }

    # ---- helpers the notebookutils shim calls ----

    def credential(self):
        """Tokens come from the host's token endpoint when one is configured (the
        MCP server's TokenServer, or an embedding host): the worker then needs no
        Azure credential of its own. DefaultAzureCredential is the fallback for a
        worker started with no endpoint."""
        if self._cred is None:
            if self._onelake and self._onelake.get("endpoint"):
                from .host_credential import HostTokenCredential

                self._cred = HostTokenCredential(self._onelake["endpoint"], self._onelake.get("secret", ""))
            else:
                from azure.identity import DefaultAzureCredential

                self._cred = DefaultAzureCredential()
        return self._cred

    def workspace_id(self) -> str:
        infos = list(getattr(self, "lakehouses", {}).values())
        if not infos:
            raise RuntimeError("no Fabric workspace configured (set [workspace] in local-spark.toml)")
        return infos[0].workspace_id

    def fabric_client(self):
        if self._fabric_client is None:
            from .discovery import FabricAPIClient

            self._fabric_client = FabricAPIClient(credential=self.credential())
        return self._fabric_client

    def runtime_context(self) -> dict:
        lh = self._resolve_lakehouse(self.default_lakehouse) if self.default_lakehouse else None
        return {
            "currentWorkspaceId": lh.workspace_id if lh else None,
            "defaultLakehouseId": lh.id if lh else None,
            "defaultLakehouseName": lh.name if lh else None,
            "currentNotebookName": None,
        }

    def _onelake_fs(self, path: str):
        """(file_system_client, relative path) for an abfss:// OneLake URL."""
        from urllib.parse import urlparse

        from azure.storage.filedatalake import DataLakeServiceClient

        u = urlparse(path)
        workspace = u.username or u.netloc.split("@")[0]
        host = u.hostname
        service = DataLakeServiceClient(f"https://{host}", credential=self.credential())
        return service.get_file_system_client(workspace), u.path.lstrip("/")

    def files_resolve(self, path: str):
        if self.files is None:
            return None
        return self.files.resolve(path, self.default_lakehouse)

    def files_mount(self, source: str, mount_point: str) -> dict:
        if self.files is None:
            raise RuntimeError("no Fabric workspace configured (set [workspace] in local-spark.toml)")
        return self.files.mount(source, mount_point, self.default_lakehouse)

    def onelake_ls(self, path: str) -> list:
        from .notebookutils_shim import FileInfo

        fs, rel = self._onelake_fs(path)
        base = path.rstrip("/")
        return [
            FileInfo(name=p.name.rsplit("/", 1)[-1], path=f"{base}/{p.name.rsplit('/', 1)[-1]}",
                     size=p.content_length or 0, isDir=bool(p.is_directory))
            for p in fs.get_paths(path=rel, recursive=False)
        ]

    def _onelake_guard_write(self, path: str, op: str) -> None:
        if self.write_mode != "writethrough":
            raise PermissionError(
                f"notebookutils.fs.{op}: write_mode is '{self.write_mode}', so {path} on OneLake is not modified. "
                "Use a /lakehouse/... path (the local mirror) or run with write_mode = writethrough.")

    def onelake_is_dir(self, path: str) -> bool | None:
        """True / False for a directory / file on OneLake, None when absent."""
        fs, rel = self._onelake_fs(path)
        try:
            props = fs.get_file_client(rel).get_file_properties()
        except Exception:
            return None
        return str((props.metadata or {}).get("hdi_isfolder", "")).lower() == "true"

    def onelake_read(self, path: str, max_bytes: int | None = None) -> bytes:
        fs, rel = self._onelake_fs(path)
        dl = fs.get_file_client(rel).download_file(offset=0, length=max_bytes) if max_bytes else fs.get_file_client(rel).download_file()
        return dl.readall()

    def onelake_write(self, path: str, data: bytes, overwrite: bool = False) -> None:
        self._onelake_guard_write(path, "put")
        fs, rel = self._onelake_fs(path)
        if not overwrite and self.onelake_is_dir(path) is not None:
            raise FileExistsError(f"{path} exists; pass overwrite=True")
        fs.get_file_client(rel).upload_data(data, overwrite=True)

    def onelake_append(self, path: str, data: bytes, create: bool = False) -> None:
        self._onelake_guard_write(path, "append")
        fs, rel = self._onelake_fs(path)
        client = fs.get_file_client(rel)
        try:
            size = int(client.get_file_properties().size or 0)
        except Exception:
            if not create:
                raise FileNotFoundError(f"{path} does not exist (pass createFileIfNotExists=True)")
            client.create_file()
            size = 0
        client.append_data(data, offset=size)
        client.flush_data(size + len(data))

    def onelake_mkdirs(self, path: str) -> None:
        self._onelake_guard_write(path, "mkdirs")
        fs, rel = self._onelake_fs(path)
        fs.get_directory_client(rel).create_directory()

    def onelake_rm(self, path: str, recurse: bool = False) -> None:
        self._onelake_guard_write(path, "rm")
        fs, rel = self._onelake_fs(path)
        is_dir = self.onelake_is_dir(path)
        if is_dir is None:
            raise FileNotFoundError(path)
        if is_dir:
            if not recurse and any(True for _ in fs.get_paths(path=rel, recursive=False)):
                raise IsADirectoryError(f"{path} is a non-empty directory; pass recurse=True")
            fs.get_directory_client(rel).delete_directory()
        else:
            fs.get_file_client(rel).delete_file()

    def onelake_rename(self, src: str, dst: str) -> None:
        """Rename within one OneLake filesystem (same workspace)."""
        self._onelake_guard_write(dst, "mv")
        fs, rel = self._onelake_fs(src)
        fs2, rel2 = self._onelake_fs(dst)
        if fs.file_system_name != fs2.file_system_name:
            raise ValueError("mv across workspaces: copy then remove")
        if self.onelake_is_dir(src):
            fs.get_directory_client(rel).rename_directory(f"{fs.file_system_name}/{rel2}")
        else:
            fs.get_file_client(rel).rename_file(f"{fs.file_system_name}/{rel2}")

    def files_mounts(self) -> list[dict]:
        if self.files is None:
            return []
        out = []
        for mp, lh in sorted(self.files.mounts.items()):
            info = self._resolve_lakehouse(lh)
            source = f"abfss://{info.workspace_id}@onelake.dfs.fabric.microsoft.com/{info.id}" if info else lh
            out.append({"mountPoint": mp, "source": source, "lakehouse": lh,
                        "localPath": self.files.mirror_dir(lh).parent.as_posix() if info else None})
        return out

    def onelake_exists(self, path: str) -> bool:
        fs, rel = self._onelake_fs(path)
        try:
            fs.get_file_client(rel).get_file_properties()
            return True
        except Exception:
            try:
                fs.get_directory_client(rel).get_directory_properties()
                return True
            except Exception:
                return False

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

    def _sql_with_automount(self, sql: str, spark=None):
        """spark.sql, transparently mounting referenced Fabric tables on first
        use. Each iteration mounts one newly-referenced table; the per-table
        guard prevents loops if a mount doesn't resolve the reference."""
        from pyspark.errors import AnalysisException

        spark = spark if spark is not None else self.spark
        while True:
            try:
                return spark.sql(sql)
            except AnalysisException as exc:
                if not self._automount_missing(exc):
                    raise

    def _dml_metrics(self, sql: str, df, spark) -> dict | None:
        """Affected-row counts for a DML statement. Delta's UPDATE / DELETE / MERGE
        return them as the result frame (`num_affected_rows`, …); INSERT and CTAS
        return an empty frame, so those come from the table's latest commit
        (`DESCRIBE HISTORY … LIMIT 1`, `operationMetrics.numOutputRows`)."""
        target = _sql_write_target(sql)
        if not target:
            return None
        try:
            cols = set(df.columns)
            if "num_affected_rows" in cols:
                row = df.first().asDict()
                out = {"affected_rows": int(row.get("num_affected_rows") or 0), "source": "result"}
                for k, name in (("num_inserted_rows", "inserted"), ("num_updated_rows", "updated"), ("num_deleted_rows", "deleted")):
                    if k in row:
                        out[name] = int(row[k] or 0)
                return out
            if cols:
                return None  # a SELECT-like result with a write keyword? leave it alone
            hist = spark.sql(f"DESCRIBE HISTORY {target} LIMIT 1").select("operation", "operationMetrics").first()
            if hist is None:
                return None
            m = dict(hist["operationMetrics"] or {})
            op = hist["operation"]
            out = {"operation": op, "source": "history", "table": target}
            if op in ("WRITE", "CREATE TABLE AS SELECT", "REPLACE TABLE AS SELECT", "CREATE OR REPLACE TABLE AS SELECT"):
                out["affected_rows"] = int(m.get("numOutputRows", 0) or 0)
                out["inserted"] = out["affected_rows"]
            elif op == "MERGE":
                ins, upd, dele = (int(m.get(k, 0) or 0) for k in ("numTargetRowsInserted", "numTargetRowsUpdated", "numTargetRowsDeleted"))
                out.update(inserted=ins, updated=upd, deleted=dele, affected_rows=ins + upd + dele)
            elif op == "UPDATE":
                out["updated"] = out["affected_rows"] = int(m.get("numUpdatedRows", 0) or 0)
            elif op == "DELETE":
                out["deleted"] = out["affected_rows"] = int(m.get("numDeletedRows", 0) or 0)
            else:
                out["affected_rows"] = int(m.get("numOutputRows", 0) or 0)
            return out
        except Exception as exc:  # metrics are a courtesy; never fail the statement over them
            return {"error": f"{type(exc).__name__}: {str(exc).splitlines()[0][:200]}"}

    def _stream_arrow_batches(self, df, spark, batch_rows: int | None, on_batch,
                              limit: int | None = None) -> tuple[int, int, bool]:
        """Hand the result to ``on_batch(meta, ipc_bytes)`` one Arrow batch at a time,
        partition by partition in order, so the driver holds one partition at
        most: `Dataset.toArrowBatchRdd` wrapped as a JavaRDD and collected per
        partition. Each blob is a self-contained Arrow IPC stream (schema + one
        batch). With ``limit``, the plan is cut at limit + 1 rows and the extra
        row, if it arrives, is never sent but flags ``truncated``. Returns
        (rows, batches, truncated)."""
        import pyarrow as pa
        from pyspark.sql.pandas.types import to_arrow_schema

        if limit:
            df = df.limit(limit + 1)
        schema = to_arrow_schema(df.schema)
        gw = spark.sparkContext._gateway
        key = "spark.sql.execution.arrow.maxRecordsPerBatch"
        prev = _safe(lambda: spark.conf.get(key))
        if batch_rows:
            spark.conf.set(key, str(int(batch_rows)))
        total = count = 0
        truncated = False
        try:
            rdd = df._jdf.toArrowBatchRdd()
            tag = gw.jvm.scala.reflect.ClassTag.apply(gw.jvm.Class.forName("[B"))
            jrdd = gw.jvm.org.apache.spark.api.java.JavaRDD.fromRDD(rdd, tag)
            for part in range(jrdd.getNumPartitions()):
                ids = gw.new_array(gw.jvm.int, 1)
                ids[0] = part
                for raw in jrdd.collectPartitions(ids)[0]:
                    batch = pa.ipc.read_record_batch(pa.py_buffer(bytes(raw)), schema)
                    if limit and total + batch.num_rows > limit:
                        truncated = True
                        batch = batch.slice(0, limit - total)
                        if batch.num_rows == 0:
                            break
                    sink = pa.BufferOutputStream()
                    with pa.ipc.new_stream(sink, schema) as w:
                        w.write_batch(batch)
                    data = sink.getvalue().to_pybytes()
                    on_batch({"rows": batch.num_rows, "batch": count, "partition": part, "arrow_bytes": len(data)}, data)
                    total += batch.num_rows
                    count += 1
                if truncated:
                    break
        finally:
            if batch_rows and prev is not None:
                _safe(lambda: spark.conf.set(key, prev))
        return total, count, truncated

    def run_sql(self, sql: str, limit: int | None = None, arrow: bool = False,
                job_description: str | None = None, context: str | None = None,
                batch_rows: int | None = None, on_batch=None) -> SqlResult:
        """Run a SQL statement in a context's session and return up to ``limit``
        rows (as JSON rows, or as an Arrow IPC stream in ``blobs_out`` when
        ``arrow`` is set). With ``on_batch`` the whole result streams out as Arrow
        batches of about ``batch_rows`` rows (no limit unless one is given) and the
        reply carries only the counts. DML statements carry ``metrics``."""
        streaming = on_batch is not None
        if limit is None and not streaming:
            limit = self.default_sql_limit
        self.blobs_out = []
        self._activate(self._resolve_context(context))
        t0 = time.time()
        if self.write_mode != "writethrough" and (target := _sql_write_target(sql)):
            if dv := self.is_dv_table(target):
                raise RuntimeError(self.dv_refusal(dv))
        with self._running("run_sql", job_description):
            try:
                try:
                    df = self._sql_with_automount(sql, self._active.spark)
                except Exception as exc:
                    annotated = self.annotate_error(str(exc))
                    if annotated != str(exc):
                        raise RuntimeError(annotated) from exc
                    raise
                columns = list(df.columns)
                metrics = self._dml_metrics(sql, df, self._active.spark)
                if streaming:
                    total, nb, truncated = (self._stream_arrow_batches(df, self._active.spark, batch_rows, on_batch, limit)
                                            if columns else (0, 0, False))
                    return SqlResult(columns=columns, rows=[], row_count=total, truncated=truncated, limit=limit or 0,
                                     notices=self.drain_mount_notices(), metrics=metrics, batches=nb,
                                     elapsed_s=round(time.time() - t0, 3),
                                     arrow={"columns": columns, "row_count": total, "streamed": True, "batches": nb,
                                            "truncated": truncated})
                if arrow and columns:
                    data, meta = self._arrow_from_df(df, limit)
                    self.blobs_out = [data]
                    return SqlResult(columns=meta["columns"], rows=[], row_count=meta["row_count"], truncated=meta["truncated"],
                                     limit=limit, notices=self.drain_mount_notices(), arrow=meta, metrics=metrics,
                                     elapsed_s=round(time.time() - t0, 3))
                # Pull one extra row to detect truncation without a full count.
                collected = df.limit(limit + 1).collect()
            except KeyboardInterrupt:
                raise InterruptedQuery("KeyboardInterrupt: interrupted (Spark jobs cancelled)") from None
            except Exception as exc:
                if self._interrupt_requested:  # a cancelled job surfaces as Py4JError / SparkException
                    raise InterruptedQuery(f"KeyboardInterrupt: interrupted (Spark jobs cancelled); underlying {type(exc).__name__}: {exc}") from exc
                raise
        truncated = len(collected) > limit
        collected = collected[:limit]
        rows = [[_jsonify(v) for v in row] for row in collected]
        return SqlResult(
            columns=columns,
            rows=rows,
            row_count=len(rows),
            truncated=truncated,
            limit=limit,
            notices=self.drain_mount_notices(),
            metrics=metrics,
            elapsed_s=round(time.time() - t0, 3),
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
            "current_catalog": _safe(catalog.currentCatalog),
            "databases": databases,
            "lakehouses": sorted(getattr(self, "lakehouses", {})),
            "default_lakehouse": self.default_lakehouse,
            "write_mode": self.write_mode,
            "shadow_root": self.shadow_root.as_posix(),
            "shadows": [f"{t['lakehouse']}.{t['table']}" for t in self._shadow_tables()],
            "deletion_vector_tables": [f"{t['lakehouse']}.{t['table']}" for t in self._dv_tables()],
            "dv_strategy": self.dv_strategy,
            "profile": _profile_label(),
            "started_at": self.started_at,
            "uptime_s": int(time.time() - self.started_at),
            "java_home": os.environ.get("JAVA_HOME"),
            "python": sys.executable,
            "hadoop_home": os.environ.get("HADOOP_HOME") if os.name == "nt" else None,
            "ivy_dir": _ivy_dir(self.spark),
            "extra_jars": self.extra_jars,
            "extra_packages": self.extra_packages,
            "preload": self.preload_status(),
            "lakehouse_schemas": self._lakehouse_schemas,
            "protocol_version": PROTOCOL_VERSION,
            "files_root": (self.files_link or {}).get("files_root"),
            "spark_working_dir": self.spark_working_dir,
            "files_link": self.files_link,
            "files_sync": self.files_sync_report,
            "execution_count": self.shell.execution_count,
            "default_sql_limit": self.default_sql_limit,
            "features": list(FEATURES),
            "files_mode": self.files_mode,
            "files_hooks": bool(getattr(self, "_lazy_hooks", None) and self._lazy_hooks.installed),
            "contexts": [self._context_info(c) for c in self.contexts.values()],
            "active_context": self._active.id,
        }

    # ---- write-policy shadow ----

    def _shadow_tables(self) -> list[dict]:
        """Shadowed tables on disk: <shadow_root>/<lakehouse-id>/<table>/_delta_log."""
        id_to_name = {info.id: name for name, info in getattr(self, "lakehouses", {}).items()}
        found: list[dict] = []
        if not self.shadow_root.is_dir():
            return found
        listed: dict[str, set[str]] = {}

        def registered(db: str, table: str) -> bool:
            # tables this session's catalog holds now; OneLakeCatalog.tableExists would
            # say yes to anything resolvable in OneLake, which is not the question
            if db not in listed:
                listed[db] = {t["name"].lower() for t in self._registered_tables(db) if not t["temporary"]}
            return table.lower() in listed[db]
        for lh_dir in sorted(self.shadow_root.iterdir()):
            if not lh_dir.is_dir():
                continue
            for table_dir in sorted(lh_dir.iterdir()):
                if (table_dir / "_delta_log").is_dir():
                    state, version, cloned_at = _shadow_state(table_dir)
                    lh_name = id_to_name.get(lh_dir.name, lh_dir.name)
                    # a schema table's shadow dir is "<schema>.<table>" -> lakehouse "<lh>__<schema>"
                    if "." in table_dir.name:
                        schema, tbl = table_dir.name.split(".", 1)
                        lh_name, table_name = f"{lh_name}__{schema}", tbl
                    else:
                        table_name = table_dir.name
                    found.append({
                        "lakehouse": lh_name,
                        "table": table_name,
                        "path": table_dir.as_posix(),
                        "state": state,
                        "version": version,
                        "cloned_at": cloned_at,
                        # a persisted clone from an earlier session is listed before it is touched;
                        # `registered` says whether this session's catalog knows it yet
                        "registered": registered(lh_name, table_name),
                    })
        return found

    DV_VIEW_TAG = "localspark:deletion-vectors"

    def _registered_tables(self, db: str) -> list[dict]:
        """The tables and views the session catalog holds for `db`, with type and
        comment, read from the V1 SessionCatalog so nothing is loaded. Never use
        `spark.catalog.listTables` for this: Spark's CatalogImpl calls loadTable
        for every name it lists, and since 0.7.0 the listing names every table the
        lakehouse has on OneLake, so that call would clone them all (it did: a
        shadow_status cloned 136 tables)."""
        out: list[dict] = []
        try:
            cat = self.spark._jsparkSession.sessionState().catalog()
            it = cat.listTables(db).iterator()
        except Exception:
            return out
        while it.hasNext():
            ident = it.next()
            ttype, comment = "", ""
            try:
                meta = cat.getTempViewOrPermanentTableMetadata(ident)
                ttype = str(meta.tableType().name())
                c = meta.comment()
                comment = str(c.get()) if c.isDefined() else ""
            except Exception:
                pass
            out.append({"name": str(ident.table()), "type": ttype, "comment": comment,
                        "temporary": ident.database().isEmpty()})
        return out

    def _dv_tables(self) -> list[dict]:
        """Lakehouse tables materialized as live views because their Delta
        protocol declares deletionVectors (Delta 3.2 cannot shallow-clone them).
        Read-only in sandbox/readonly; OneLakeCatalog tags the view's comment."""
        found: list[dict] = []
        for name in sorted(getattr(self, "lakehouses", {}) or {}):
            for t in self._registered_tables(name):
                if t["type"] == "VIEW" and t["comment"].startswith(self.DV_VIEW_TAG):
                    desc = t["comment"]
                    src = desc.split("source=", 1)[1] if "source=" in desc else ""
                    found.append({"lakehouse": name, "table": t["name"], "source": src})
        return found

    def is_dv_table(self, table_name: str) -> dict | None:
        parts = [p.strip("`") for p in table_name.split(".")]
        if len(parts) == 1 and self.default_lakehouse:
            parts = [self.default_lakehouse, parts[0]]
        if len(parts) != 2:
            return None
        for t in self._dv_tables():
            if t["lakehouse"].lower() == parts[0].lower() and t["table"].lower() == parts[1].lower():
                return t
        return None

    def dv_refusal(self, table: dict) -> str:
        return (
            f"{table['lakehouse']}.{table['table']} carries Delta deletion vectors (Link to Fabric "
            "mirrors do), which Delta 3.2 cannot shallow-clone; it is registered as a live read-only "
            f"view in write_mode '{self.write_mode}'. Reads work; writes are not supported locally. "
            "Set LOCAL_SPARK_WRITE_MODE=writethrough to write to OneLake, or copy it: "
            f"spark.table('{table['lakehouse']}.{table['table']}').write.saveAsTable('<lakehouse>.<new_name>')."
        )

    def shadow_status(self) -> dict:
        return {
            "write_mode": self.write_mode,
            "shadow_root": self.shadow_root.as_posix(),
            "persistent": self.persist_shadow,
            "tables": self._shadow_tables(),
            "deletion_vector_tables": self._dv_tables(),
        }

    def restore_shadow(self, table: str, version: int = 0) -> dict:
        """Rewind one shadow to a Delta version by truncating its local
        `_delta_log` (default: version 0, the clone commit = the OneLake snapshot
        as first touched). Metadata only, no OneLake reads; local data files added
        by the discarded commits are deleted. RESTORE TABLE cannot do this on a
        shallow clone (its files live on another filesystem: "Wrong FS")."""
        parts = [p.strip("`") for p in table.split(".")]
        if len(parts) == 1 and self.default_lakehouse:
            parts = [self.default_lakehouse, parts[0]]
        if len(parts) != 2:
            raise ValueError(f"table must be <lakehouse>.<table> (got {table!r})")
        match = [t for t in self._shadow_tables()
                 if t["lakehouse"].lower() == parts[0].lower() and t["table"].lower() == parts[1].lower()]
        if not match:
            raise LookupError(f"{parts[0]}.{parts[1]} has no shadow this session (nothing to restore)")
        shadow = match[0]
        removed = _truncate_delta_log(Path(shadow["path"]), version)
        try:
            self.spark._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()
        except Exception:
            pass
        try:
            self.spark.catalog.refreshTable(self._fq(shadow["lakehouse"], shadow["table"]))
        except Exception:
            pass
        state, latest, _cloned = _shadow_state(Path(shadow["path"]))
        return {"lakehouse": shadow["lakehouse"], "table": shadow["table"], "restored_to": version,
                "removed_commits": removed["commits"], "removed_files": removed["files"], "state": state, "version": latest}

    def discard_shadow(self, only: str | None = None, table: str | None = None) -> dict:
        """Drop shadowed tables from the catalog and delete their files, so the
        next touch re-clones from OneLake. OneLake is not affected. `only` limits
        it to the "read" (materialized by a read, unchanged) or "written" ones;
        `table` ("lakehouse.table", or unqualified under the default) to one."""
        want = None
        if table:
            parts = [p.strip("`") for p in table.split(".")]
            if len(parts) == 1 and self.default_lakehouse:
                parts = [self.default_lakehouse, parts[0]]
            if len(parts) != 2:
                raise ValueError(f"table must be <lakehouse>.<table> (got {table!r})")
            want = (parts[0].lower(), parts[1].lower())
        tables = [t for t in self._shadow_tables()
                  if (only is None or t["state"] == only)
                  and (want is None or (t["lakehouse"].lower(), t["table"].lower()) == want)]
        if want and not tables:
            raise LookupError(f"{want[0]}.{want[1]} has no shadow this session")
        for t in tables:
            try:
                self.spark.sql(f"DROP TABLE IF EXISTS {self._fq(t['lakehouse'], t['table'])}")
            except Exception:  # external table; best effort — files go next
                pass
            shutil.rmtree(t["path"], ignore_errors=True)
            self._mounted.get(t["lakehouse"], set()).discard(t["table"])
        if only is None and want is None:
            shutil.rmtree(self.shadow_root, ignore_errors=True)
            self.shadow_root.mkdir(parents=True, exist_ok=True)
            self._mounted = {}
        return {"discarded": len(tables), "tables": tables}

    def stop(self):
        if getattr(self, "_lazy_hooks", None) is not None:
            self._lazy_hooks.uninstall()
        try:
            self.spark.stop()
        except Exception:  # pragma: no cover - best effort on shutdown
            pass
        if self.files is not None:
            self.files.release_link()
        # Session-scoped state (warehouse + non-persistent shadow) goes with the session.
        shutil.rmtree(self._session_dir, ignore_errors=True)


class _ShimEngine:
    """The narrow surface the notebookutils shim needs from the engine."""

    def __init__(self, engine: "SparkEngine"):
        self._e = engine

    def run_notebook(self, path, **kw):
        return self._e.run_notebook(path, **kw)

    def credential(self):
        return self._e.credential()

    def workspace_id(self):
        return self._e.workspace_id()

    def fabric_client(self):
        return self._e.fabric_client()

    def runtime_context(self):
        return self._e.runtime_context()

    def onelake_ls(self, path):
        return self._e.onelake_ls(path)

    def onelake_exists(self, path):
        return self._e.onelake_exists(path)

    def files_resolve(self, path):
        return self._e.files_resolve(path)

    def files_mount(self, source, mount_point):
        return self._e.files_mount(source, mount_point)

    def files_mounts(self):
        return self._e.files_mounts()

    @property
    def write_mode(self):
        return self._e.write_mode

    @property
    def files_hooks(self) -> bool:
        hooks = getattr(self._e, "_lazy_hooks", None)
        return bool(hooks and hooks.installed)

    def onelake_is_dir(self, path):
        return self._e.onelake_is_dir(path)

    def onelake_read(self, path, max_bytes=None):
        return self._e.onelake_read(path, max_bytes)

    def onelake_write(self, path, data, overwrite=False):
        return self._e.onelake_write(path, data, overwrite)

    def onelake_append(self, path, data, create=False):
        return self._e.onelake_append(path, data, create)

    def onelake_mkdirs(self, path):
        return self._e.onelake_mkdirs(path)

    def onelake_rm(self, path, recurse=False):
        return self._e.onelake_rm(path, recurse)

    def onelake_rename(self, src, dst):
        return self._e.onelake_rename(src, dst)


_WRITE_TARGET = re.compile(
    r"^\s*(?:INSERT\s+(?:INTO|OVERWRITE)(?:\s+TABLE)?|MERGE\s+INTO|UPDATE|DELETE\s+FROM"
    r"|CREATE\s+(?:OR\s+REPLACE\s+)?TABLE(?:\s+IF\s+NOT\s+EXISTS)?)\s+([`\w.]+)",
    re.IGNORECASE,
)


def _sql_write_target(sql: str) -> str | None:
    m = _WRITE_TARGET.match(sql)
    return m.group(1) if m else None


def _notice_table(notice: str) -> str:
    """'mounted lh.t in N s (how)' -> 'lh.t' (lowercased)."""
    return notice.split(" ", 2)[1].lower() if notice.startswith("mounted ") else ""




class _Tee(io.TextIOBase):
    """A text stream that keeps everything written and forwards complete lines
    (or 4 KB chunks) to a callback as they arrive."""

    def __init__(self, name: str, on_output):
        self._name, self._cb = name, on_output
        self._all: list[str] = []
        self._pending = ""

    def writable(self) -> bool:
        return True

    def write(self, text: str) -> int:
        if not text:
            return 0
        self._all.append(text)
        self._pending += text
        if "\n" in self._pending or len(self._pending) >= 4096:
            self._emit()
        return len(text)

    def _emit(self) -> None:
        if self._pending:
            chunk, self._pending = self._pending, ""
            try:
                self._cb(self._name, chunk)
            except Exception:
                pass

    def flush(self) -> None:
        self._emit()

    def close(self) -> None:
        self._emit()

    def getvalue(self) -> str:
        return "".join(self._all)


def _ivy_dir(spark) -> str | None:
    try:
        v = spark.conf.get("spark.jars.ivy", None)
    except Exception:
        v = None
    if v:
        return v
    for cand in ("~/.ivy2.5.2", "~/.ivy2"):
        if Path(cand).expanduser().is_dir():
            return str(Path(cand).expanduser())
    return None


def _profile_label() -> str:
    from .profiles import current_profile, installed_versions

    p = current_profile()
    v = installed_versions()
    return f"{p.name} (Fabric Runtime {p.fabric_runtime}; pyspark {v.get('pyspark')}, delta-spark {v.get('delta-spark')}, python {sys.version.split()[0]})"


def _dv_strategy() -> str:
    """How deletion-vector tables materialize in sandbox/readonly: Delta >= 3.3
    can SHALLOW CLONE them (full sandbox, writes land locally); Delta 3.2 cannot,
    so they become live read-only views. Override with LOCAL_SPARK_DV_STRATEGY."""
    forced = os.environ.get("LOCAL_SPARK_DV_STRATEGY", "").strip().lower()
    if forced in ("view", "clone"):
        return forced
    try:
        from importlib.metadata import version

        major, minor = (int(x) for x in version("delta-spark").split(".")[:2])
        return "clone" if (major, minor) >= (3, 3) else "view"
    except Exception:
        return "view"


def _truncate_delta_log(table_dir: Path, version: int) -> dict:
    """Drop every commit after `version` from a local Delta table: later commit
    JSONs, later checkpoints, `_last_checkpoint`, and the data files those
    commits added. Returns counts. Raises if `version` is beyond the log."""
    log = table_dir / "_delta_log"
    versions = sorted(int(p.stem) for p in log.glob("*.json") if p.stem.isdigit())
    if not versions or version < versions[0] or version > versions[-1]:
        raise ValueError(f"version {version} is not in this shadow's log ({versions[0] if versions else '?'}..{versions[-1] if versions else '?'})")
    removed_commits, removed_files = 0, 0
    for v in versions:
        if v <= version:
            continue
        commit = log / f"{v:020d}.json"
        for line in commit.read_text(encoding="utf-8", errors="replace").splitlines():
            try:
                action = json.loads(line)
            except ValueError:
                continue
            add = action.get("add")
            if add and add.get("path") and "://" not in add["path"]:
                f = table_dir / add["path"]
                if f.is_file():
                    f.unlink()
                    removed_files += 1
        commit.unlink()
        removed_commits += 1
    for cp in log.glob("*.checkpoint*.parquet"):
        try:
            if int(cp.name.split(".")[0]) > version:
                cp.unlink()
        except ValueError:
            pass
    for extra in ("_last_checkpoint",):
        if (log / extra).exists():
            (log / extra).unlink()
    for crc in log.glob("*.crc"):
        stem = crc.name.lstrip(".").split(".")[0]
        if stem.isdigit() and int(stem) > version:
            crc.unlink()
    return {"commits": removed_commits, "files": removed_files}


def _shadow_state(table_dir: Path) -> tuple[str, int, str | None]:
    """("read" | "written", latest version, first commit time as ISO-8601 UTC). A
    shadow that is still the initial shallow-clone commit (operation CLONE at its
    first version) was only read; any later commit, or a first commit that is not
    a clone, means local writes. The time is the clone's commitInfo timestamp
    (the file's mtime when absent), so a host can show "cloned at <time>"."""
    log = table_dir / "_delta_log"
    versions = sorted(int(p.stem) for p in log.glob("*.json") if p.stem.isdigit())
    if not versions:
        return "unknown", -1, None
    op, ts = None, None
    first = log / f"{versions[0]:020d}.json"
    try:
        # explicit UTF-8: Windows' default codec (cp1252) fails on Delta's non-ASCII commit metadata
        for line in first.read_text(encoding="utf-8", errors="replace").splitlines():
            if '"commitInfo"' in line:
                ci = json.loads(line).get("commitInfo", {})
                op, ts = ci.get("operation"), ci.get("timestamp")
                break
    except (OSError, ValueError):
        pass
    try:
        seconds = ts / 1000 if isinstance(ts, (int, float)) else first.stat().st_mtime
        cloned_at = datetime.datetime.fromtimestamp(seconds, datetime.timezone.utc).isoformat(timespec="seconds")
    except (OSError, ValueError, OverflowError):
        cloned_at = None
    if op == "CLONE" and len(versions) == 1:
        return "read", versions[0], cloned_at
    return "written", versions[-1], cloned_at


def _sql_preview(res: "SqlResult", max_rows: int = 20) -> str:
    """Compact text for a %%sql cell's result."""
    if not res.columns:
        return "(statement executed)"
    head = " | ".join(res.columns)
    rows = [" | ".join("NULL" if v is None else str(v) for v in r) for r in res.rows[:max_rows]]
    tail = f"[{res.row_count} row(s){'; truncated' if res.truncated else ''}]"
    return "\n".join([head, *rows, tail])


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
