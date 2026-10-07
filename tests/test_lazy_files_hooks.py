"""Python-level lazy Files (files_mode = lazy) without Spark or OneLake: the hooks
on open / os.stat / os.listdir / os.scandir / pathlib resolve /lakehouse paths
against an in-memory stand-in for OneLake, fetch on first open, synthesize stat
from remote metadata, merge local-only files into listings, keep writes local
(pushed only in writethrough), leave other paths alone, and uninstall cleanly.
"""

import builtins
import io
import os
import time
from pathlib import Path

import pytest

from local_spark_mcp.discovery import LakehouseInfo
from local_spark_mcp.files import FilesMirror
from local_spark_mcp.lazy_files import LazyFilesHooks

WS = "11111111-1111-1111-1111-111111111111"
LH_A, LH_B = "aaaaaaaa-0000-0000-0000-000000000001", "bbbbbbbb-0000-0000-0000-000000000002"


class FakeMirror(FilesMirror):
    """FilesMirror whose OneLake calls read a dict {lakehouse name: {rel: bytes}}."""

    def __init__(self, remote: dict, **kw):
        super().__init__(**kw)
        self.remote = remote
        self.pushed: list[tuple[str, str]] = []

    def remote_stat(self, lakehouse, rel):
        tree = self.remote[lakehouse]
        if rel in tree:
            return {"is_dir": False, "size": len(tree[rel]), "mtime": 1_700_000_000.0}
        if rel == "" or any(k.startswith(rel + "/") for k in tree):
            return {"is_dir": True, "size": 0, "mtime": 1_700_000_000.0}
        return None

    def remote_list(self, lakehouse, rel):
        tree = self.remote[lakehouse]
        prefix = rel + "/" if rel else ""
        if rel and not any(k.startswith(prefix) for k in tree):
            return None
        names = {}
        for k, v in tree.items():
            if k.startswith(prefix):
                head = k[len(prefix):].split("/", 1)
                names.setdefault(head[0], {"name": head[0], "is_dir": len(head) > 1, "size": 0 if len(head) > 1 else len(v), "mtime": 1_700_000_000.0})
        return list(names.values())

    def fetch_file(self, lakehouse, rel):
        local = self.mirror_dir(lakehouse) / rel
        st = self.remote_stat(lakehouse, rel)
        if st is None:
            raise FileNotFoundError(rel)
        if local.is_file() and local.stat().st_size == st["size"] and local.stat().st_mtime >= st["mtime"]:
            return local
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_bytes(self.remote[lakehouse][rel])
        os.utime(local, (st["mtime"], st["mtime"]))
        self.fetched.setdefault(lakehouse, {})[rel] = st["size"]
        return local

    def push_file(self, lakehouse, rel):
        if self.write_mode != "writethrough":
            raise PermissionError("no")
        self.pushed.append((lakehouse, rel))
        self.remote[lakehouse][rel] = (self.mirror_dir(lakehouse) / rel).read_bytes()
        return len(self.remote[lakehouse][rel])


class FakeEngine:
    def __init__(self, files, default):
        self.files = files
        self.default_lakehouse = default

        class _Ctx:
            default_lakehouse = default
        self._active = _Ctx()


@pytest.fixture
def world(tmp_path):
    remote = {"alpha": {"lib/a.txt": b"alpha-a", "lib/sub/deep.csv": b"x,y\n1,2\n", "top.json": b"{}"},
              "beta": {"lib/b.txt": b"beta-b"}}
    lakehouses = {"alpha": LakehouseInfo("alpha", LH_A, WS), "beta": LakehouseInfo("beta", LH_B, WS)}
    mirror = FakeMirror(remote, root=tmp_path / "mirror", workspace_id=WS, lakehouses=lakehouses, write_mode="sandbox",
                        credential_factory=lambda: None, link_base=str(tmp_path / "lakehouse"), lock_path=tmp_path / "lock.json")
    eng = FakeEngine(mirror, "alpha")
    hooks = LazyFilesHooks(eng)
    orig_open = builtins.open
    hooks.install()
    try:
        yield eng, mirror, hooks, remote
    finally:
        hooks.uninstall()
        assert builtins.open is orig_open and io.open is orig_open


def test_open_fetches_on_first_use_and_refetches_when_newer(world):
    eng, mirror, hooks, remote = world
    with open("/lakehouse/default/Files/lib/a.txt", "rb") as f:
        assert f.read() == b"alpha-a"
    local = mirror.mirror_dir("alpha") / "lib" / "a.txt"
    assert local.is_file() and mirror.fetched["alpha"] == {"lib/a.txt": 7}
    # text mode, by name, through pathlib
    assert Path("/lakehouse/alpha/Files/top.json").read_text() == "{}"
    # a newer remote copy replaces the local one
    remote["alpha"]["lib/a.txt"] = b"alpha-a-v2!!"
    assert open("/lakehouse/default/Files/lib/a.txt").read() == "alpha-a-v2!!"
    # a path that exists nowhere: a FileNotFoundError that names the sync_files call
    with pytest.raises(FileNotFoundError) as ei:
        open("/lakehouse/default/Files/lib/missing.txt")
    assert "sync_files(paths=['lib'], lakehouse='alpha')" in str(ei.value)
    # Windows spelling and the /Files-less form
    assert open("C:\\lakehouse\\default\\Files\\lib\\a.txt").read() == "alpha-a-v2!!"
    assert open("/lakehouse/alpha/lib/a.txt").read() == "alpha-a-v2!!"


def test_stat_listdir_scandir_pathlib(world):
    eng, mirror, hooks, remote = world
    # nothing fetched yet: answers come from OneLake metadata
    assert os.path.isdir("/lakehouse/default/Files/lib") and os.path.isdir("/lakehouse/default/Files/lib/sub")
    assert os.path.isfile("/lakehouse/default/Files/lib/a.txt") and os.path.getsize("/lakehouse/default/Files/lib/a.txt") == 7
    assert not os.path.exists("/lakehouse/default/Files/nope")
    assert Path("/lakehouse/default/Files/lib/sub/deep.csv").exists() and Path("/lakehouse/default/Files/lib").is_dir()
    assert not (mirror.mirror_dir("alpha") / "lib").exists()  # still nothing on disk
    assert sorted(os.listdir("/lakehouse/default/Files")) == ["lib", "top.json"]
    assert sorted(os.listdir("/lakehouse/default/Files/lib")) == ["a.txt", "sub"]
    with os.scandir("/lakehouse/default/Files/lib") as it:
        entries = {e.name: e for e in it}
    assert entries["sub"].is_dir() and entries["a.txt"].is_file() and entries["a.txt"].stat().st_size == 7
    assert entries["a.txt"].path == "/lakehouse/default/Files/lib/a.txt"
    assert sorted(p.name for p in Path("/lakehouse/default/Files/lib").iterdir()) == ["a.txt", "sub"]
    assert sorted(p.as_posix() for p in Path("/lakehouse/default/Files").glob("lib/*.txt")) == ["/lakehouse/default/Files/lib/a.txt"]
    with pytest.raises(FileNotFoundError):
        os.listdir("/lakehouse/default/Files/nowhere")
    # local-only files merge into listings
    (mirror.mirror_dir("alpha") / "lib" / "local_only.txt").parent.mkdir(parents=True, exist_ok=True)
    (mirror.mirror_dir("alpha") / "lib" / "local_only.txt").write_text("l")
    assert sorted(os.listdir("/lakehouse/default/Files/lib")) == ["a.txt", "local_only.txt", "sub"]
    # Tables/ and unrelated paths are untouched
    assert not os.path.exists("/lakehouse/default/Tables/t")
    assert os.path.isdir(str(mirror.root))


def test_default_follows_the_active_context(world):
    eng, mirror, hooks, remote = world
    assert open("/lakehouse/default/Files/lib/a.txt").read() == "alpha-a"
    eng._active.default_lakehouse = "beta"
    assert sorted(os.listdir("/lakehouse/default/Files/lib")) == ["b.txt"]
    assert open("/lakehouse/default/Files/lib/b.txt").read() == "beta-b"
    assert open("/lakehouse/alpha/Files/lib/a.txt").read() == "alpha-a"  # by name still works


def test_writes_stay_local_in_sandbox_and_push_in_writethrough(world):
    eng, mirror, hooks, remote = world
    with open("/lakehouse/default/Files/out/new.txt", "w") as f:
        f.write("hello")
    local = mirror.mirror_dir("alpha") / "out" / "new.txt"
    assert local.read_text() == "hello" and mirror.pushed == [] and "out/new.txt" not in remote["alpha"]
    assert os.path.exists("/lakehouse/default/Files/out/new.txt") and "out" in os.listdir("/lakehouse/default/Files")
    os.makedirs("/lakehouse/default/Files/made/dir", exist_ok=True)
    assert (mirror.mirror_dir("alpha") / "made" / "dir").is_dir()
    os.rename("/lakehouse/default/Files/out/new.txt", "/lakehouse/default/Files/out/renamed.txt")
    assert (mirror.mirror_dir("alpha") / "out" / "renamed.txt").exists()
    os.remove("/lakehouse/default/Files/out/renamed.txt")
    assert not (mirror.mirror_dir("alpha") / "out" / "renamed.txt").exists()
    # writethrough: pushed on close, and appends start from the OneLake copy
    mirror.write_mode = "writethrough"
    with open("/lakehouse/default/Files/lib/a.txt", "a") as f:
        f.write("+more")
    assert mirror.pushed == [("alpha", "lib/a.txt")] and remote["alpha"]["lib/a.txt"] == b"alpha-a+more"
    with Path("/lakehouse/default/Files/w.txt").open("w") as f:
        f.write("pathlib")
    assert remote["alpha"]["w.txt"] == b"pathlib"


def test_status_and_clear(world):
    eng, mirror, hooks, remote = world
    open("/lakehouse/default/Files/lib/a.txt").read()
    st = mirror.status()
    assert st["lakehouses"]["alpha"]["fetched_files"] == 1 and st["lakehouses"]["alpha"]["fetched_bytes"] == 7
    assert st["lakehouses"]["alpha"]["local_files"] == 1 and st["lakehouses"]["beta"]["local_files"] == 0
    assert st["total_files"] == 1 and st["total_bytes"] == 7
    r = mirror.clear("alpha", ["lib"])
    assert r["removed"] and not (mirror.mirror_dir("alpha") / "lib").exists() and mirror.fetched["alpha"] == {}
    assert os.path.isfile("/lakehouse/default/Files/lib/a.txt")  # still answers from OneLake
    assert open("/lakehouse/default/Files/lib/a.txt").read() == "alpha-a"  # and fetches again
    assert mirror.clear()["lakehouses"] == ["alpha", "beta"]


def test_ipython_open_is_hooked(world):
    eng, mirror, hooks, remote = world
    import IPython.core.interactiveshell as ish

    assert getattr(ish.io_open, "__self__", None) is hooks  # a bound method: compare the instance
    # the function a cell's `open` resolves to
    f = ish._modified_open("/lakehouse/default/Files/lib/a.txt", "rb")
    assert f.read() == b"alpha-a"
    f.close()


def test_uninstall_restores_everything(tmp_path):
    lakehouses = {"alpha": LakehouseInfo("alpha", LH_A, WS)}
    mirror = FakeMirror({"alpha": {}}, root=tmp_path, workspace_id=WS, lakehouses=lakehouses, write_mode="sandbox",
                        credential_factory=lambda: None, link_base=str(tmp_path / "lh"), lock_path=tmp_path / "lock.json")
    before = (builtins.open, os.stat, os.listdir, os.scandir, os.mkdir, os.remove, os.rename)
    hooks = LazyFilesHooks(FakeEngine(mirror, "alpha"))
    hooks.install()
    assert builtins.open is not before[0] and os.stat is not before[1]
    hooks.uninstall()
    assert (builtins.open, os.stat, os.listdir, os.scandir, os.mkdir, os.remove, os.rename) == before
    import IPython.core.interactiveshell as ish

    assert ish.io_open is before[0]
