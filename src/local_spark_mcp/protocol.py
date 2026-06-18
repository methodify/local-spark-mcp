"""Length-prefixed JSON framing for the parent<->worker socket.

A dedicated socket (not stdio) carries the protocol so the worker's stdout/stderr
— and Spark/py4j chatter — never collide with framing, and so the MCP server's
own stdout stays clean for the stdio transport.

Wire format: 4-byte big-endian unsigned length, then that many bytes of UTF-8
JSON. Requests: {"id", "method", "params"}. Responses: {"id", "ok", "result"} or
{"id", "ok": false, "error", "traceback"}.
"""

from __future__ import annotations

import json
import socket
import struct

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
