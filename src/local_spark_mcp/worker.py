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
import threading
import traceback

from .protocol import recv_msg, send_msg, send_reply


_FATAL_MARKERS = ("Py4JNetworkError", "Java gateway process exited", "Answer from Java side is empty",
                  "ConnectionRefusedError", "Connection refused", "ConnectionResetError")


def _result_is_fatal(result, engine=None) -> bool:
    """A cell/SQL result whose error is the dead JVM talking (the cell itself
    ran, so no exception reached the dispatcher). Text markers first; then,
    for any failed result, one trivial JVM call as a liveness probe — a dead
    gateway raises a bare Py4JError that no text marker can tell apart from a
    protocol hiccup."""
    if not isinstance(result, dict) or result.get("ok", True) and "error" not in result:
        return False
    if result.get("interrupted"):
        # An interrupted cell's error often carries py4j connection text
        # (KeyboardInterrupt closed the thread's connection mid-call), which says
        # nothing about the JVM: ask it, with retries (engine.jvm_alive).
        return engine is not None and not engine.jvm_alive(retries=12, delay=0.25)
    text = f"{result.get('error') or ''}\n{result.get('traceback') or ''}\n{result.get('stdout') or ''}"
    if any(m in text for m in _FATAL_MARKERS):
        return True
    if engine is None or getattr(engine, "spark", None) is None:
        return False
    return not engine.jvm_alive()


def _is_fatal(exc: BaseException) -> bool:
    """The JVM is gone (driver OOM, killed): every later call would fail the
    same way, so tell the server to respawn instead of returning errors."""
    names = {type(e).__name__ for e in _chain(exc)}
    msgs = " ".join(str(e) for e in _chain(exc))
    return bool(names & {"Py4JNetworkError", "ConnectionRefusedError", "ConnectionResetError", "BrokenPipeError"}) \
        or "Java gateway process exited" in msgs or "Answer from Java side is empty" in msgs


def _chain(exc):
    seen = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        yield exc
        exc = exc.__cause__ or exc.__context__


def _handle(engine, method: str, params: dict):
    """Dispatch one request. Returns (result, engine) — engine may be created."""
    from .engine import SparkEngine

    if method == "healthcheck":  # works before init: no Spark needed
        from .healthcheck import healthcheck

        return healthcheck(params.get("profile")), engine
    if method == "init":
        if engine is not None:
            engine.stop()
        from .profiles import check_profile

        # Hosts that spawn the worker directly skip the server's checks: refuse
        # a stack that cannot serve the declared profile (or will crash its
        # Python workers on this platform) here, with the same message.
        prof, warnings, errors = check_profile(params.pop("profile", None))
        if errors:
            raise RuntimeError("; ".join(errors))
        engine = SparkEngine(**params)
        info = engine.info()
        info["profile_warnings"] = warnings
        info["control"] = True  # a control socket is served when --control-port was given
        return info, engine
    if method == "create_context":
        return engine.create_context(params["id"], params.get("default_lakehouse"), params.get("default_schema"),
                                     name=params.get("name")), engine
    if method == "drop_context":
        return engine.drop_context(params["id"], force=bool(params.get("force"))), engine
    if method == "register_lakehouse":
        return engine.register_lakehouse(params["lakehouse"]), engine
    if method == "unregister_lakehouse":
        return engine.unregister_lakehouse(params["name"]), engine
    if method == "list_tables":
        return engine.list_tables(params["lakehouse"]), engine
    if method == "preload":
        return engine.start_preload(params.get("lakehouses"), params.get("workers")), engine
    if method == "preload_status":
        return engine.preload_status(), engine
    if method == "wait_preload":
        return engine.wait_preload(params.get("timeout")), engine
    if method == "ping":
        return {"pong": True}, engine
    if engine is None:
        raise RuntimeError("engine not initialized; send 'init' first")
    if method == "run_code":
        return engine.run_code(params["code"], on_output=params.get("_on_output"), capture_result=bool(params.get("capture_result")),
                               job_description=params.get("job_description"), context=params.get("context")).to_dict(), engine
    if method == "run_sql":
        return engine.run_sql(params["sql"], params.get("limit"), bool(params.get("arrow")),
                              job_description=params.get("job_description"), context=params.get("context"),
                              batch_rows=params.get("batch_rows"), on_batch=params.get("_on_batch")).to_dict(), engine
    if method == "mount_table":
        return engine.mount_table(params["lakehouse"], params["table"]), engine
    if method == "table_features":
        return engine.table_features(params["lakehouse"], params["tables"]), engine
    if method == "mount_tables":
        return engine.mount_tables(params["lakehouse"], params["tables"]), engine
    if method == "run_notebook":
        return engine.run_notebook(
            params["path"],
            cells=params.get("cells"),
            stop_on_error=params.get("stop_on_error", True),
            default_lakehouse=params.get("default_lakehouse"),
            parameters=params.get("parameters"),
            context=params.get("context"),
            isolated=bool(params.get("isolated")),
        ), engine
    if method == "mirror_status":
        return engine.mirror_status(), engine
    if method == "clear_mirror":
        return engine.clear_mirror(params.get("lakehouse"), params.get("paths")), engine
    if method == "sync_files":
        return engine.sync_files(params.get("paths"), params.get("direction", "pull"), params.get("lakehouse")), engine
    if method == "restore_shadow":
        return engine.restore_shadow(params["table"], int(params.get("version", 0))), engine
    if method == "shadow_status":
        return engine.shadow_status(), engine
    if method == "discard_shadow":
        return engine.discard_shadow(params.get("only"), params.get("table")), engine
    if method == "info":
        return engine.info(), engine
    raise ValueError(f"unknown method: {method!r}")


class _Shared:
    """Engine handle shared between the request thread and the control thread."""

    engine = None


def _control_loop(port: int, shared: _Shared) -> None:
    """Serve the control socket: interrupt / ping / status / preload_status,
    independent of the main loop (which a running cell blocks)."""
    try:
        sock = socket.create_connection(("127.0.0.1", port))
    except OSError:
        return
    try:
        while True:
            req = recv_msg(sock)
            if req is None:
                return
            rid, method = req.get("id"), req.get("method")
            cparams = req.get("params") or {}
            eng = shared.engine
            try:
                if method == "ping":
                    result = {}
                elif method == "status":
                    result = eng.status(cparams.get("context")) if eng is not None else {"initialized": False, "cell_running": False, "cell": None}
                elif method == "interrupt":
                    result = eng.interrupt(cparams.get("context")) if eng is not None else {"interrupted": False, "reason": "not initialized"}
                elif method == "preload_status":
                    result = eng.preload_status() if eng is not None else {"state": "idle"}
                elif method == "drop_context":  # here a cell may be in flight: force interrupts it and drops at its end
                    if eng is None:
                        raise ValueError("not initialized")
                    result = eng.drop_context(cparams["id"], force=bool(cparams.get("force")))
                else:
                    raise ValueError(f"unknown control method {method!r}")
                send_msg(sock, {"id": rid, "ok": True, "result": result})
            except Exception as exc:
                send_msg(sock, {"id": rid, "ok": False, "error": f"{type(exc).__name__}: {exc}"})
    except OSError:
        return


def run_worker(port: int, control_port: int | None = None) -> int:
    sock = socket.create_connection(("127.0.0.1", port))
    shared = _Shared()
    if control_port:
        threading.Thread(target=_control_loop, args=(control_port, shared), name="lsm-control", daemon=True).start()
    engine = None
    try:
        while True:
            try:
                req = recv_msg(sock)
            except KeyboardInterrupt:
                continue  # an interrupt that landed between cells; nothing to stop
            if req is None:
                break  # parent closed
            rid = req.get("id")
            method = req.get("method")
            params = req.get("params") or {}

            if method == "shutdown":
                send_msg(sock, {"id": rid, "ok": True, "result": {}})
                break

            stream = params.pop("stream", False)
            if stream and method == "run_code":
                def _on_output(stream, text, _rid=rid):
                    send_msg(sock, {"id": _rid, "event": stream, "text": text})
                params["_on_output"] = _on_output
            if method == "run_sql" and (stream or params.get("batch_rows")):
                def _on_batch(meta, blob, _rid=rid):  # one Arrow IPC stream per event frame
                    send_reply(sock, {"id": _rid, "event": "batch", **meta}, [blob])
                params["_on_batch"] = _on_batch
            try:
                result, engine = _handle(engine, method, params)
                shared.engine = engine
                blobs = list(getattr(engine, "blobs_out", []) or []) if engine is not None else []
                if engine is not None:
                    engine.blobs_out = []
                send_reply(sock, {"id": rid, "ok": True, "result": result, "fatal": _result_is_fatal(result, engine)}, blobs)
            except KeyboardInterrupt:  # interrupt landed outside a cell's run_cell
                send_msg(sock, {"id": rid, "ok": False, "error": "KeyboardInterrupt: interrupted", "traceback": None, "fatal": False})
            except Exception as exc:  # report, keep serving
                interrupted = type(exc).__name__ == "InterruptedQuery"
                send_msg(
                    sock,
                    {
                        "id": rid,
                        "ok": False,
                        "error": str(exc) if interrupted else f"{type(exc).__name__}: {exc}",
                        "traceback": None if interrupted else traceback.format_exc(),
                        "fatal": False if interrupted else _is_fatal(exc),
                        "interrupted": interrupted,
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
    parser.add_argument("--control-port", type=int, default=None, help="parent listener for the control socket (interrupt, status)")
    args = parser.parse_args(argv)
    return run_worker(args.port, args.control_port)


if __name__ == "__main__":
    sys.exit(main())
