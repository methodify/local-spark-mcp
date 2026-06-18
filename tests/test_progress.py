"""Tests for the keepalive heartbeat: tools must emit MCP progress throughout a
long operation (not just startup), so the client doesn't time out mid-call (e.g.
a run_sql that lazily mounts a table)."""

import asyncio

import pytest

from local_spark_mcp.server import _await_with_progress


def test_pings_during_slow_op():
    pings = []

    async def slow():
        await asyncio.sleep(0.25)
        return "done"

    async def on_wait():
        pings.append(1)

    async def run():
        return await _await_with_progress(slow(), on_wait, interval=0.05)

    assert asyncio.run(run()) == "done"
    assert len(pings) >= 2  # ~4-5 heartbeats over 0.25s at a 0.05s interval


def test_no_ping_when_op_is_fast():
    pings = []

    async def fast():
        return 7

    async def on_wait():
        pings.append(1)

    async def run():
        return await _await_with_progress(fast(), on_wait, interval=1.0)

    assert asyncio.run(run()) == 7
    assert pings == []  # completes before the first interval — no spurious progress


def test_exception_propagates():
    async def boom():
        raise ValueError("kaboom")

    async def run():
        return await _await_with_progress(boom(), None, interval=0.05)

    with pytest.raises(ValueError, match="kaboom"):
        asyncio.run(run())


def test_none_on_wait_is_tolerated():
    async def slow():
        await asyncio.sleep(0.12)
        return "ok"

    async def run():
        return await _await_with_progress(slow(), None, interval=0.05)

    assert asyncio.run(run()) == "ok"
