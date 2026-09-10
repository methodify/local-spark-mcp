"""The Spark engine: an IPython InteractiveShell with a persistent namespace and
a live SparkSession injected in. This is the in-process core that the worker
process wraps with IPC; it has no knowledge of MCP or process boundaries, so it
can be unit-tested directly.
"""

from __future__ import annotations

import datetime
import decimal
import json
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
        notebooks_root: str | None = None,
        files_sync: list[str] | None = None,
        mirror_root: str | None = None,
    ):
        self.default_sql_limit = default_sql_limit
        self.notebooks_root = notebooks_root
        self.files_sync = list(files_sync or [])
        self.files: "FilesMirror | None" = None
        self.files_link: dict | None = None
        self.files_sync_report: list[dict] = []
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
        if onelake and lakehouses:
            from .discovery import LakehouseInfo as _LI
            from .files import FilesMirror

            registry = {lh["name"]: _LI(name=lh["name"], id=lh["id"], workspace_id=lh["workspace_id"]) for lh in lakehouses}
            self.files = FilesMirror(
                root=Path(mirror_root).expanduser() if mirror_root else self._state_root / "lakehouses",
                workspace_id=workspace_id, lakehouses=registry, write_mode=write_mode,
                credential_factory=self.credential,
            )
            if default_lakehouse:
                try:
                    env["LOCAL_SPARK_FILES_ROOT"] = str(self.files.mirror_dir(default_lakehouse))
                except LookupError:
                    pass  # reported by _register_lakehouses

        self.dv_strategy = _dv_strategy()
        catalog = None
        if onelake and lakehouses:
            catalog = {
                "dv_strategy": self.dv_strategy,
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
        if self.files is not None and self.default_lakehouse:
            self._activate_files(self.default_lakehouse)

    def _activate_files(self, lakehouse: str) -> None:
        """Point /lakehouse/default at this lakehouse's mirror and pull the
        configured Files/ subtrees (cached: unchanged files are skipped)."""
        self.files_link = self.files.link_default(lakehouse)
        for rel in self.files_sync:
            try:
                self.files_sync_report.append(self.files.pull(lakehouse, [rel]).to_dict())
            except Exception as exc:  # keep the session usable; report instead
                self.files_sync_report.append({"direction": "pull", "lakehouse": lakehouse, "paths": [rel],
                                               "errors": [f"{type(exc).__name__}: {exc}"]})

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
        self._install_notebookutils()

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
        self.shell.user_ns["notebookutils"] = shim
        self.shell.user_ns["mssparkutils"] = shim

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
            return dict(pool.map(one, tables))

    def _exec(self, code: str) -> tuple[ExecResult, BaseException | None]:
        """Run a cell; also return the raised exception (the runner needs to
        recognize NotebookExit, which IPython otherwise reports as an error)."""
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

        if error:

            stdout = self.annotate_error(stdout) or stdout

            error = self.annotate_error(error) or error

        outcome = ExecResult(
            ok=bool(result.success),
            stdout=_truncate(stdout),
            stderr=_truncate(cap.stderr),
            error=error,
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

    def run_code(self, code: str) -> ExecResult:
        """Run a cell of Python against the persistent namespace."""
        return self._exec(code)[0]

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
    ) -> dict:
        """Run a Fabric notebook (Git .py format) cell by cell in this namespace."""
        from .notebook import load_notebook, select_cells, strip_line_magics
        from .notebookutils_shim import NotebookExit

        nb_path = self._resolve_notebook_path(path)
        nb = load_notebook(nb_path)
        selected = select_cells(cells, len(nb.cells))
        warnings = list(nb.warnings)

        # Default lakehouse for this run: explicit arg, else the notebook's META.
        lh_name = default_lakehouse or nb.default_lakehouse_name
        prev_db = self.spark.catalog.currentDatabase()
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
                self.spark.sql(f"USE {self._q(info.name)}")
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
                        res = self.run_sql(code)
                        entry["status"] = "ok"
                        entry["stdout"] = _sql_preview(res)
                    except Exception as exc:
                        entry["status"] = "error"
                        entry["error"] = f"{type(exc).__name__}: {exc}".splitlines()[0]
                        first_error = first_error or entry["error"]
                elif cell.language == "python":
                    res, exc = self._exec(code)
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
                self.spark.sql(f"USE {self._q(prev_db)}")
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
        if self._cred is None:
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
        if self.write_mode != "writethrough" and (target := _sql_write_target(sql)):
            if dv := self.is_dv_table(target):
                raise RuntimeError(self.dv_refusal(dv))
        try:
            df = self._sql_with_automount(sql)
        except Exception as exc:
            annotated = self.annotate_error(str(exc))
            if annotated != str(exc):
                raise RuntimeError(annotated) from exc
            raise
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
            "deletion_vector_tables": [f"{t['lakehouse']}.{t['table']}" for t in self._dv_tables()],
            "dv_strategy": self.dv_strategy,
            "files_root": (self.files_link or {}).get("files_root"),
            "files_link": self.files_link,
            "files_sync": self.files_sync_report,
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
                    state, version = _shadow_state(table_dir)
                    found.append({
                        "lakehouse": id_to_name.get(lh_dir.name, lh_dir.name),
                        "table": table_dir.name,
                        "path": table_dir.as_posix(),
                        "state": state,
                        "version": version,
                    })
        return found

    DV_VIEW_TAG = "localspark:deletion-vectors"

    def _dv_tables(self) -> list[dict]:
        """Lakehouse tables materialized as live views because their Delta
        protocol declares deletionVectors (Delta 3.2 cannot shallow-clone them).
        Read-only in sandbox/readonly; OneLakeCatalog tags the view's comment."""
        found: list[dict] = []
        for name in sorted(getattr(self, "lakehouses", {}) or {}):
            try:
                tables = self.spark.catalog.listTables(name)
            except Exception:
                continue
            for t in tables:
                desc = t.description or ""
                if t.tableType == "VIEW" and desc.startswith(self.DV_VIEW_TAG):
                    src = desc.split("source=", 1)[1] if "source=" in desc else ""
                    found.append({"lakehouse": name, "table": t.name, "source": src})
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

    def discard_shadow(self, only: str | None = None) -> dict:
        """Drop shadowed tables from the catalog and delete their files, so the
        next touch re-clones from OneLake. OneLake is not affected. `only` limits
        it to the "read" (materialized by a read, unchanged) or "written" ones."""
        tables = [t for t in self._shadow_tables() if only is None or t["state"] == only]
        for t in tables:
            try:
                self.spark.sql(f"DROP TABLE IF EXISTS {self._q(t['lakehouse'])}.{self._q(t['table'])}")
            except Exception:  # external table; best effort — files go next
                pass
            shutil.rmtree(t["path"], ignore_errors=True)
            self._mounted.get(t["lakehouse"], set()).discard(t["table"])
        if only is None:
            shutil.rmtree(self.shadow_root, ignore_errors=True)
            self.shadow_root.mkdir(parents=True, exist_ok=True)
            self._mounted = {}
        return {"discarded": len(tables), "tables": tables}

    def stop(self):
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


_WRITE_TARGET = re.compile(
    r"^\s*(?:INSERT\s+(?:INTO|OVERWRITE)(?:\s+TABLE)?|MERGE\s+INTO|UPDATE|DELETE\s+FROM)\s+([`\w.]+)",
    re.IGNORECASE,
)


def _sql_write_target(sql: str) -> str | None:
    m = _WRITE_TARGET.match(sql)
    return m.group(1) if m else None


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


def _shadow_state(table_dir: Path) -> tuple[str, int]:
    """("read" | "written", latest version). A shadow that is still the initial
    shallow-clone commit (operation CLONE at its first version) was only read;
    any later commit, or a first commit that is not a clone, means local writes."""
    log = table_dir / "_delta_log"
    versions = sorted(int(p.stem) for p in log.glob("*.json") if p.stem.isdigit())
    if not versions:
        return "unknown", -1
    op = None
    try:
        # explicit UTF-8: Windows' default codec (cp1252) fails on Delta's non-ASCII commit metadata
        for line in (log / f"{versions[0]:020d}.json").read_text(encoding="utf-8", errors="replace").splitlines():
            if '"commitInfo"' in line:
                op = json.loads(line).get("commitInfo", {}).get("operation")
                break
    except (OSError, ValueError):
        pass
    if op == "CLONE" and len(versions) == 1:
        return "read", versions[0]
    return "written", versions[-1]


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
