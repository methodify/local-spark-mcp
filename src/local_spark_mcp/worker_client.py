"""Parent-side handle to a worker process.

Owns the worker subprocess lifecycle and the synchronous request/response
channel. ``restart()`` is the "reset runtime" primitive: kill the worker and
spawn a fresh one for a guaranteed-clean slate.
"""

from __future__ import annotations

import socket
import subprocess
import sys

from .protocol import recv_msg, send_msg

# Spark startup (JVM + Delta jar resolution) is slow on a cold worker.
DEFAULT_STARTUP_TIMEOUT = 180.0
DEFAULT_CALL_TIMEOUT = 600.0


class WorkerError(Exception):
    """A request failed in the worker (carries the remote error/traceback)."""

    def __init__(self, message: str, traceback_str: str | None = None):
        super().__init__(message)
        self.traceback_str = traceback_str


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
        self._id = 0
        self.info: dict | None = None

    @property
    def running(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    def start(self) -> dict:
        """Spawn the worker, wait for connect, run the init handshake.

        Returns the engine info dict from a successful init.
        """
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        port = listener.getsockname()[1]
        try:
            # Worker stdout/stderr go to OUR stderr — never the parent's stdout,
            # which the MCP stdio transport owns.
            self._proc = subprocess.Popen(
                [sys.executable, "-m", "local_spark_mcp.worker", "--port", str(port)],
                stdout=sys.stderr.fileno(),
                stderr=sys.stderr.fileno(),
            )
            listener.settimeout(self.startup_timeout)
            try:
                self._conn, _ = listener.accept()
            except socket.timeout as exc:
                self._kill_proc()
                raise WorkerError(
                    f"worker did not connect within {self.startup_timeout}s"
                ) from exc
        finally:
            listener.close()

        self.info = self._call(
            "init", self.engine_kwargs, timeout=self.startup_timeout
        )
        return self.info

    def _call(self, method: str, params: dict | None = None, *, timeout: float | None = None) -> dict:
        if self._conn is None:
            raise WorkerError("worker not started")
        self._id += 1
        req_id = self._id
        send_msg(self._conn, {"id": req_id, "method": method, "params": params or {}})
        self._conn.settimeout(timeout if timeout is not None else self.call_timeout)
        try:
            resp = recv_msg(self._conn)
        except socket.timeout as exc:
            raise WorkerError(f"worker call '{method}' timed out") from exc
        if resp is None:
            raise WorkerError(
                f"worker closed connection during '{method}'"
                + (f" (exit code {self._proc.poll()})" if self._proc else "")
            )
        if not resp.get("ok"):
            raise WorkerError(resp.get("error", "unknown worker error"), resp.get("traceback"))
        return resp["result"]

    # --- proxied engine operations ---
    def run_code(self, code: str) -> dict:
        return self._call("run_code", {"code": code})

    def run_sql(self, sql: str, limit: int | None = None) -> dict:
        return self._call("run_sql", {"sql": sql, "limit": limit})

    def get_info(self) -> dict:
        return self._call("info")

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
