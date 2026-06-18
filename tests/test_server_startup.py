"""Regression test for the startup race: a concurrent caller (e.g. the
background warmup vs. the first tool call) must not see a half-started worker.
WorkerProcess.running is true the instant the subprocess spawns, but the worker
isn't usable until the IPC connection + init finish — ensure_ready must gate on
`ready`, not `running`."""

import asyncio
import time

import local_spark_mcp.server as server_mod
from local_spark_mcp.config import Config


class FakeWorker:
    """Mimics the real start timing: `running` flips true immediately, but
    `ready` (info set) only after a delay."""

    def __init__(self, engine_kwargs=None):
        self._running = False
        self.info = None

    @property
    def running(self):
        return self._running

    @property
    def ready(self):
        return self._running and self.info is not None

    def start(self):
        self._running = True          # like Popen returning — but not usable yet
        time.sleep(0.3)               # connect + Spark init
        self.info = {"started": True}
        return self.info

    def stop(self):
        self._running = False
        self.info = None

    def run_code(self, code):
        if not self.ready:
            raise AssertionError("called run_code on a half-started worker")
        return {"ok": True, "stdout": "", "code": code}


def test_concurrent_caller_waits_for_ready(monkeypatch):
    monkeypatch.setattr(server_mod, "WorkerProcess", FakeWorker)
    state = server_mod.ServerState(config=Config())  # local-only (no workspace)

    async def scenario():
        warm = asyncio.create_task(state.warmup())  # begins start(): running, not ready
        await asyncio.sleep(0.05)                    # let the worker spawn (running=True)
        res = await state.call("run_code", "x=1")    # must await full readiness
        await warm
        return res

    res = asyncio.run(scenario())
    assert res == {"ok": True, "stdout": "", "code": "x=1"}
    state.shutdown()


def test_lazy_by_default_no_warmup_at_launch(monkeypatch):
    """With warm_on_start unset (default), the lifespan must NOT start the worker;
    it stays lazy until the first tool call."""
    started = {"n": 0}

    class CountingWorker(FakeWorker):
        def start(self):
            started["n"] += 1
            return super().start()

    monkeypatch.setattr(server_mod, "WorkerProcess", CountingWorker)
    mcp = server_mod.build_server(server_mod.ServerState(config=Config()))

    async def scenario():
        async with mcp._mcp_server.lifespan(mcp._mcp_server):  # enter/exit lifespan
            await asyncio.sleep(0.1)  # give any (unwanted) warmup task a chance to run

    asyncio.run(scenario())
    assert started["n"] == 0  # nothing warmed at launch


def test_warm_on_start_triggers_launch_warmup(monkeypatch):
    from local_spark_mcp.config import Config as Cfg, RuntimeConfig

    started = {"n": 0}

    class CountingWorker(FakeWorker):
        def start(self):
            started["n"] += 1
            return super().start()

    monkeypatch.setattr(server_mod, "WorkerProcess", CountingWorker)
    cfg = Cfg(runtime=RuntimeConfig(warm_on_start=True))
    mcp = server_mod.build_server(server_mod.ServerState(config=cfg))

    async def scenario():
        async with mcp._mcp_server.lifespan(mcp._mcp_server):
            await asyncio.sleep(0.5)  # warmup runs FakeWorker.start (~0.3s)

    asyncio.run(scenario())
    assert started["n"] == 1  # eagerly warmed at launch


def test_single_flight_starts_worker_once(monkeypatch):
    starts = {"n": 0}

    class CountingWorker(FakeWorker):
        def start(self):
            starts["n"] += 1
            return super().start()

    monkeypatch.setattr(server_mod, "WorkerProcess", CountingWorker)
    state = server_mod.ServerState(config=Config())

    async def scenario():
        # three concurrent first-callers should trigger exactly one start
        await asyncio.gather(
            state.ensure_ready(), state.ensure_ready(), state.ensure_ready()
        )

    asyncio.run(scenario())
    assert starts["n"] == 1
    state.shutdown()
