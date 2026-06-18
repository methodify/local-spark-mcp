"""The worker process: holds the SparkEngine and serves requests over a socket.

Launched by the MCP server as ``python -m local_spark_mcp.worker --port N``. It
connects back to the parent's listening socket, then serves a synchronous
request/response loop. The Spark session is built lazily on the ``init`` request
so the parent gets an explicit ready/error signal (and the config) over the
protocol rather than via the environment.
"""

from __future__ import annotations

import argparse
import socket
import sys
import traceback

from .protocol import recv_msg, send_msg


def _handle(engine, method: str, params: dict):
    """Dispatch one request. Returns (result, engine) — engine may be created."""
    from .engine import SparkEngine

    if method == "init":
        if engine is not None:
            engine.stop()
        engine = SparkEngine(**params)
        return engine.info(), engine
    if method == "ping":
        return {"pong": True}, engine
    if engine is None:
        raise RuntimeError("engine not initialized; send 'init' first")
    if method == "run_code":
        return engine.run_code(params["code"]).to_dict(), engine
    if method == "run_sql":
        return engine.run_sql(params["sql"], params.get("limit")).to_dict(), engine
    if method == "mount_table":
        return engine.mount_table(params["lakehouse"], params["table"]), engine
    if method == "mount_tables":
        return engine.mount_tables(params["lakehouse"], params["tables"]), engine
    if method == "info":
        return engine.info(), engine
    raise ValueError(f"unknown method: {method!r}")


def run_worker(port: int) -> int:
    sock = socket.create_connection(("127.0.0.1", port))
    engine = None
    try:
        while True:
            req = recv_msg(sock)
            if req is None:
                break  # parent closed
            rid = req.get("id")
            method = req.get("method")
            params = req.get("params") or {}

            if method == "shutdown":
                send_msg(sock, {"id": rid, "ok": True, "result": {}})
                break

            try:
                result, engine = _handle(engine, method, params)
                send_msg(sock, {"id": rid, "ok": True, "result": result})
            except Exception as exc:  # report, keep serving
                send_msg(
                    sock,
                    {
                        "id": rid,
                        "ok": False,
                        "error": f"{type(exc).__name__}: {exc}",
                        "traceback": traceback.format_exc(),
                    },
                )
    finally:
        if engine is not None:
            engine.stop()
        sock.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="local_spark_mcp.worker")
    parser.add_argument("--port", type=int, required=True, help="parent listener port")
    args = parser.parse_args(argv)
    return run_worker(args.port)


if __name__ == "__main__":
    sys.exit(main())
