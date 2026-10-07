"""Parent-side handle to a worker process.

Owns the worker subprocess lifecycle and the synchronous request/response
channel. ``restart()`` is the "reset runtime" primitive: kill the worker and
spawn a fresh one for a guaranteed-clean slate.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

from .protocol import recv_msg, recv_reply, send_msg

# Spark startup (JVM + Delta/hadoop-azure jar resolution) is slow on a cold
# worker — markedly so on Windows first runs. Tool calls emit MCP progress
# throughout, and a worker that dies is caught at once by the accept loop, so
# a generous ceiling costs nothing but avoids spurious startup failures.
DEFAULT_STARTUP_TIMEOUT = 600.0
DEFAULT_CALL_TIMEOUT = 600.0  # cheap calls; cells/SQL/notebooks run unbounded (MCP pings cover the wait)
LONG_METHODS = {"run_code", "run_sql", "run_notebook", "mount_tables", "sync_files", "table_features", "wait_preload"}


def _worker_spawn() -> tuple[str, dict]:
    """Interpreter + env for the worker, robust to trampoline interpreters.

    Under uvx on Windows the console-script launcher re-execs the BASE
    uv-managed interpreter, so ``sys.executable`` in the server process cannot
    import this package — the worker then dies on ModuleNotFoundError before
    ever connecting back. Prefer the environment's own interpreter when one
    exists next to ``sys.prefix``, and pin the package root onto PYTHONPATH so
    any interpreter we do spawn can import us.
    """
    exe = sys.executable
    candidate = (
        Path(sys.prefix)
        / ("Scripts" if os.name == "nt" else "bin")
        / ("python.exe" if os.name == "nt" else "python")
    )
    if candidate.exists():
        exe = str(candidate)
    pkg_root = str(Path(__file__).resolve().parent.parent)
    env = dict(os.environ)
    existing = env.get("PYTHONPATH")
    env["PYTHONPATH"] = pkg_root if not existing else pkg_root + os.pathsep + existing
    return exe, env


class WorkerError(Exception):
    """A request failed in the worker (carries the remote error/traceback).
    ``fatal`` means the worker/JVM is unusable and must be respawned."""

    def __init__(self, message: str, traceback_str: str | None = None, *, fatal: bool = False, interrupted: bool = False):
        super().__init__(message)
        self.traceback_str = traceback_str
        self.fatal = fatal
        self.interrupted = interrupted  # the call was stopped by `interrupt` (run_sql; run_code reports it in its result)


class WorkerProcess:
    """Spawns and proxies to a single Spark worker process."""

    def __init__(
        self,
        engine_kwargs: dict | None = None,
        *,
        startup_timeout: float = DEFAULT_STARTUP_TIMEOUT,
        call_timeout: float = DEFAULT_CALL_TIMEOUT,
    ):
        self.engine_kwargs = dict(engine_kwargs or {})
        self.startup_timeout = startup_timeout
        self.call_timeout = call_timeout
        self._proc: subprocess.Popen | None = None
        self._conn: socket.socket | None = None
        self._ctl: socket.socket | None = None
        self._ctl_lock = threading.Lock()
        self._ctl_id = 0
        self._id = 0
        self.info: dict | None = None

    @property
    def running(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    @property
    def ready(self) -> bool:
        """Fully started: process up AND the IPC connection + init handshake are
        complete. ``running`` becomes true the instant the subprocess spawns,
        well before the socket/init is usable — callers must gate on ``ready``."""
        return self.running and self._conn is not None and self.info is not None

    def start(self) -> dict:
        """Spawn the worker, wait for connect, run the init handshake.

        Returns the engine info dict from a successful init.
        """
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        port = listener.getsockname()[1]
        ctl_listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ctl_listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        ctl_listener.bind(("127.0.0.1", 0))
        ctl_listener.listen(1)
        ctl_port = ctl_listener.getsockname()[1]
        try:
            # Worker stdout/stderr go to OUR stderr — never the parent's stdout,
            # which the MCP stdio transport owns.
            exe, env = _worker_spawn()
            self._proc = subprocess.Popen(
                [exe, "-m", "local_spark_mcp.worker", "--port", str(port), "--control-port", str(ctl_port)],
                # The worker must NOT inherit our stdin: under an MCP stdio server
                # that handle is the client's pipe. On Windows inheriting it
                # deadlocks the child during interpreter startup (it never reaches
                # __main__), so the worker never connects back. Holding the
                # client's pipe would also mask EOF. DEVNULL is correct on POSIX too.
                stdin=subprocess.DEVNULL,
                stdout=sys.stderr.fileno(),
                stderr=sys.stderr.fileno(),
                env=env,
            )
            # Accept in short slices so a worker that dies at startup surfaces
            # as its exit code immediately, not as a silent full-length timeout.
            deadline = time.monotonic() + self.startup_timeout
            listener.settimeout(1.0)
            while True:
                try:
                    self._conn, _ = listener.accept()
                    break
                except socket.timeout:
                    if self._proc.poll() is not None:
                        raise WorkerError(
                            f"worker exited with code {self._proc.returncode} before"
                            " connecting — its traceback is on the server's stderr"
                        )
                    if time.monotonic() >= deadline:
                        self._kill_proc()
                        raise WorkerError(
                            f"worker did not connect within {self.startup_timeout}s"
                        )
            ctl_listener.settimeout(30.0)
            try:
                self._ctl, _ = ctl_listener.accept()
            except socket.timeout:
                self._ctl = None  # no control channel; interrupt() will say so
        finally:
            listener.close()
            ctl_listener.close()

        self.info = self._call(
            "init", self.engine_kwargs, timeout=self.startup_timeout
        )
        return self.info

    # --- control channel (served by its own thread in the worker) ---
    def control(self, method: str, params: dict | None = None, timeout: float = 30.0) -> dict:
        if self._ctl is None:
            raise WorkerError("no control channel to the worker")
        with self._ctl_lock:
            self._ctl_id += 1
            send_msg(self._ctl, {"id": self._ctl_id, "method": method, "params": params or {}})
            self._ctl.settimeout(timeout)
            resp = recv_msg(self._ctl)
        if resp is None:
            raise WorkerError("control channel closed", fatal=True)
        if not resp.get("ok"):
            raise WorkerError(resp.get("error", "control call failed"))
        return resp["result"]

    def interrupt(self, context: str | None = None) -> dict:
        """Stop the running cell (cancel Spark jobs + KeyboardInterrupt); safe while a
        call is in flight. With ``context``, only a cell of that context."""
        return self.control("interrupt", {"context": context} if context else None)

    def status(self, context: str | None = None) -> dict:
        return self.control("status", {"context": context} if context else None, timeout=10.0)

    def create_context(self, id: str, default_lakehouse: str | None = None, default_schema: str | None = None,
                       name: str | None = None) -> dict:
        """A new isolated REPL (namespace + SparkSession) inside the same JVM; see docs/PROTOCOL.md."""
        return self._call("create_context", {"id": id, "default_lakehouse": default_lakehouse, "default_schema": default_schema,
                                             "name": name})

    def drop_context(self, id: str, force: bool = False, via_control: bool = False) -> dict:
        """Release a context. ``via_control=True`` sends it on the control socket, where
        ``force`` can interrupt a running cell and drop the context when it ends."""
        if via_control:
            return self.control("drop_context", {"id": id, "force": force})
        return self._call("drop_context", {"id": id, "force": force})

    def _call(self, method: str, params: dict | None = None, *, timeout: float | None = None, on_event=None) -> dict:
        """Send one request and return its result. Binary blobs announced by the
        reply are attached as ``result["blobs"]``; event frames (streaming
        output) go to ``on_event(frame)`` as they arrive."""
        if self._conn is None:
            raise WorkerError("worker not started")
        self._id += 1
        req_id = self._id
        send_msg(self._conn, {"id": req_id, "method": method, "params": params or {}})
        if timeout is None:
            timeout = None if method in LONG_METHODS else self.call_timeout
        self._conn.settimeout(timeout)
        try:
            while True:
                resp, blobs = recv_reply(self._conn)
                if resp is None or "event" not in resp:
                    break
                if on_event is not None:
                    on_event(resp)
        except socket.timeout as exc:
            # the reply will arrive later on this socket, out of step with the next
            # request: the connection is unusable from here on
            raise WorkerError(f"worker call '{method}' timed out after {timeout}s; the runtime must be reset", fatal=True) from exc
        except OSError as exc:
            raise WorkerError(f"worker connection lost during '{method}': {exc}", fatal=True) from exc
        if resp is None:
            raise WorkerError(
                f"worker closed connection during '{method}'"
                + (f" (exit code {self._proc.poll()})" if self._proc else "")
            )
        if resp.get("id") != req_id:
            # out-of-step reply: the connection can't be trusted any more
            raise WorkerError(f"worker reply id {resp.get('id')} does not match request {req_id} ('{method}'); the runtime must be reset", fatal=True)
        if not resp.get("ok"):
            raise WorkerError(resp.get("error", "unknown worker error"), resp.get("traceback"), fatal=bool(resp.get("fatal")),
                              interrupted=bool(resp.get("interrupted")))
        if resp.get("fatal"):  # the call completed, but its result says the JVM is gone
            result = resp.get("result") or {}
            raise WorkerError(result.get("error") or "the Spark driver is no longer reachable",
                              result.get("traceback") or result.get("stdout"), fatal=True)
        result = resp["result"]
        if blobs and isinstance(result, dict):
            result["blobs"] = blobs
        return result

    # --- proxied engine operations ---
    def run_code(self, code: str, stream: bool = False, on_event=None, capture_result: bool = False,
                 job_description: str | None = None, context: str | None = None) -> dict:
        """``stream=True`` delivers stdout/stderr frames to ``on_event`` as the cell
        writes them; ``capture_result=True`` attaches a bare trailing DataFrame as a
        display; ``job_description`` names the cell's Spark jobs (status, Spark UI);
        ``context`` selects the REPL (default: the "default" context)."""
        return self._call("run_code", {"code": code, "stream": stream, "capture_result": capture_result,
                                       "job_description": job_description, "context": context}, on_event=on_event)

    def run_sql(self, sql: str, limit: int | None = None, arrow: bool = False, job_description: str | None = None,
                context: str | None = None) -> dict:
        """``arrow=True``: rows come back as one Arrow IPC stream in ``result["blobs"][0]``."""
        return self._call("run_sql", {"sql": sql, "limit": limit, "arrow": arrow, "job_description": job_description,
                                      "context": context})

    def register_lakehouse(self, lakehouse: dict) -> dict:
        """Attach a lakehouse after start: ``{name, id, workspace_id, schemas?, default_schema?, detect_schemas?}``."""
        return self._call("register_lakehouse", {"lakehouse": lakehouse})

    def unregister_lakehouse(self, name: str) -> dict:
        return self._call("unregister_lakehouse", {"name": name})

    def mount_table(self, lakehouse: str, table: str) -> dict:
        return self._call("mount_table", {"lakehouse": lakehouse, "table": table})

    def table_features(self, lakehouse: str, tables: list[str]) -> dict:
        return self._call("table_features", {"lakehouse": lakehouse, "tables": tables})

    def mount_tables(self, lakehouse: str, tables: list[str]) -> dict:
        return self._call("mount_tables", {"lakehouse": lakehouse, "tables": tables})

    def get_info(self) -> dict:
        return self._call("info")

    def run_notebook(self, path: str, cells=None, stop_on_error: bool = True,
                     default_lakehouse: str | None = None, parameters: dict | None = None,
                     context: str | None = None) -> dict:
        return self._call("run_notebook", {
            "path": path, "cells": cells, "stop_on_error": stop_on_error,
            "default_lakehouse": default_lakehouse, "parameters": parameters, "context": context,
        })

    def sync_files(self, paths=None, direction: str = "pull", lakehouse: str | None = None) -> dict:
        return self._call("sync_files", {"paths": paths, "direction": direction, "lakehouse": lakehouse})

    def list_tables(self, lakehouse: str) -> list:
        return self._call("list_tables", {"lakehouse": lakehouse})

    def preload(self, lakehouses=None, workers: int | None = None) -> dict:
        return self._call("preload", {"lakehouses": lakehouses, "workers": workers})

    def preload_status(self) -> dict:
        return self._call("preload_status", timeout=30.0)

    def wait_preload(self, timeout: float | None = None) -> dict:
        return self._call("wait_preload", {"timeout": timeout}, timeout=None)

    def discard_shadow(self, only: str | None = None, table: str | None = None) -> dict:
        return self._call("discard_shadow", {"only": only, "table": table})

    def restore_shadow(self, table: str, version: int = 0) -> dict:
        return self._call("restore_shadow", {"table": table, "version": version})

    def shadow_status(self) -> dict:
        return self._call("shadow_status")


    def ping(self) -> dict:
        return self._call("ping", timeout=10.0)

    # --- lifecycle ---
    def restart(self) -> dict:
        """Reset the runtime: tear down the worker and spawn a fresh one."""
        self.stop()
        return self.start()

    def stop(self) -> None:
        if self._conn is not None:
            try:
                self._id += 1
                send_msg(self._conn, {"id": self._id, "method": "shutdown", "params": {}})
                self._conn.settimeout(10.0)
                recv_msg(self._conn)
            except OSError:
                pass
            finally:
                try:
                    self._conn.close()
                except OSError:
                    pass
                self._conn = None
        if self._ctl is not None:
            try:
                self._ctl.close()
            except OSError:
                pass
            self._ctl = None
        self._kill_proc()
        self.info = None

    def _kill_proc(self) -> None:
        if self._proc is None:
            return
        if self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self._proc.kill()
                self._proc.wait()
        self._proc = None
