"""Protocol v2 units: blob framing round trip, the streaming tee, manifest version."""

import json
import socket
import threading

from local_spark_mcp.engine import PROTOCOL_VERSION, _Tee
from local_spark_mcp.profiles import manifest
from local_spark_mcp.protocol import recv_msg, recv_reply, send_msg, send_reply


def _pair():
    a, b = socket.socketpair()
    return a, b


def test_reply_with_blobs_round_trip():
    a, b = _pair()
    blobs = [b"\x00\x01" * 1000, b"second"]
    threading.Thread(target=send_reply, args=(a, {"id": 1, "ok": True, "result": {"x": 1}}, blobs)).start()
    obj, got = recv_reply(b)
    assert obj["binary"] == [2000, 6] and got == blobs and obj["result"] == {"x": 1}
    send_reply(a, {"id": 2, "ok": True, "result": {}})  # no blobs: plain v1 frame
    obj, got = recv_reply(b)
    assert "binary" not in obj and got == []
    # a v1 reader still understands a frame without blobs
    send_msg(a, {"id": 3, "event": "stdout", "text": "hi\n"})
    assert recv_msg(b)["event"] == "stdout"
    a.close(); b.close()


def test_tee_streams_lines_and_keeps_everything():
    seen = []
    t = _Tee("stdout", lambda s, txt: seen.append((s, txt)))
    t.write("partial"); assert seen == []
    t.write(" line\nnext"); assert seen == [("stdout", "partial line\nnext")]  # flushed on newline with what was pending
    t.write("x" * 5000); assert len(seen) == 2 and seen[1][1].endswith("x" * 5000)
    t.close(); assert t.getvalue() == "partial line\nnext" + "x" * 5000


def test_manifest_carries_protocol_version():
    assert PROTOCOL_VERSION == 2 and manifest()["protocol_version"] == 2
