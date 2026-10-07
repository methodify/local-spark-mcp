"""Length-prefixed JSON framing for the parent<->worker socket.

A dedicated socket (not stdio) carries the protocol so the worker's stdout/stderr
— and Spark/py4j chatter — never collide with framing, and so the MCP server's
own stdout stays clean for the stdio transport.

Wire format: 4-byte big-endian unsigned length, then that many bytes of UTF-8
JSON. Requests: {"id", "method", "params"}. Responses: {"id", "ok", "result"} or
{"id", "ok": false, "error", "traceback"}.

Protocol version 2 adds, without changing the framing above:
- binary payloads: a reply whose JSON carries ``"binary": [n1, n2, ...]`` is
  followed immediately by that many raw byte blobs of those sizes (Arrow IPC
  streams for ``run_sql(arrow=True)`` and ``display(df)``);
- event frames: while a request made with ``"stream": true`` runs, the worker
  may send ``{"id", "event": "stdout"|"stderr", "text"}`` frames before the reply;
- a control socket: a second connection on which ``interrupt``, ``ping``,
  ``status`` and ``preload_status`` are served by their own thread while a cell
  runs on the main one.
"""

from __future__ import annotations

import json
import socket
import struct

PROTOCOL_VERSION = 2  # worker socket protocol (docs/PROTOCOL.md); bumped on incompatible change
# Additive capabilities a version-2 host can test for (init / info `features`).
FEATURES = ["arrow", "streaming", "interrupt", "capture_result", "job_description", "register_lakehouse", "contexts", "files_lazy", "files_lazy_python"]

_HEADER = struct.Struct(">I")


def send_msg(sock: socket.socket, obj: dict) -> None:
    data = json.dumps(obj).encode("utf-8")
    sock.sendall(_HEADER.pack(len(data)) + data)


def _recv_exactly(sock: socket.socket, n: int) -> bytes | None:
    chunks = []
    remaining = n
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            return None  # peer closed
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def recv_msg(sock: socket.socket) -> dict | None:
    """Read one framed message, or None if the peer closed the connection."""
    header = _recv_exactly(sock, _HEADER.size)
    if header is None:
        return None
    (length,) = _HEADER.unpack(header)
    body = _recv_exactly(sock, length)
    if body is None:
        return None
    return json.loads(body)


def send_reply(sock: socket.socket, obj: dict, blobs: list[bytes] | None = None) -> None:
    """Send a JSON frame and, if ``blobs`` is non-empty, announce and append them."""
    if blobs:
        obj = dict(obj, binary=[len(b) for b in blobs])
    send_msg(sock, obj)
    for b in blobs or []:
        sock.sendall(b)


def recv_reply(sock: socket.socket) -> tuple[dict | None, list[bytes]]:
    """Read one JSON frame plus any announced binary blobs."""
    obj = recv_msg(sock)
    if obj is None:
        return None, []
    blobs: list[bytes] = []
    for n in obj.get("binary") or []:
        data = _recv_exactly(sock, int(n))
        if data is None:
            return None, []
        blobs.append(data)
    return obj, blobs
