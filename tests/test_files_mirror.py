"""FilesMirror over a fake OneLake data-plane client (no network, no Spark)."""

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from local_spark_mcp import files as files_mod
from local_spark_mcp.discovery import LakehouseInfo
from local_spark_mcp.files import FilesMirror

T1 = datetime(2026, 1, 1, tzinfo=timezone.utc)
T2 = T1 + timedelta(days=1)


class _Props:
    def __init__(self, size, mtime):
        self.size, self.last_modified = size, mtime


class _Entry:
    def __init__(self, name, data=None, mtime=None, is_dir=False):
        self.name, self.is_directory = name, is_dir
        self.content_length = 0 if is_dir else len(data)
        self.last_modified = mtime


class _FileClient:
    def __init__(self, store, path):
        self.store, self.path = store, path

    def get_file_properties(self):
        if self.path not in self.store:
            raise FileNotFoundError(self.path)
        data, mtime = self.store[self.path]
        return _Props(len(data), mtime)

    def download_file(self):
        data = self.store[self.path][0]
        return type("D", (), {"readall": staticmethod(lambda: data)})()

    def upload_data(self, data, overwrite=False):
        self.store[self.path] = (bytes(data), datetime.now(timezone.utc))


class FakeFS:
    def __init__(self, store):
        self.store = store  # remote path -> (bytes, mtime)

    def get_paths(self, path, recursive=False):
        prefix = path.rstrip("/") + "/"
        hits = [(k, v) for k, v in self.store.items() if k.startswith(prefix)]
        if not hits:
            raise FileNotFoundError(path)
        dirs = {os.path.dirname(k) for k, _ in hits if os.path.dirname(k) != path.rstrip("/")}
        for d in sorted(dirs):
            yield _Entry(d, is_dir=True)
        for k, (data, mtime) in hits:
            yield _Entry(k, data, mtime)

    def get_file_client(self, path):
        return _FileClient(self.store, path)


def make(tmp_path, write_mode="sandbox", store=None, link_base=None):
    store = store if store is not None else {
        "lh1/Files/lib/a.whl": (b"AAA", T1),
        "lh1/Files/lib/sub/b.txt": (b"BB", T1),
        "lh1/Files/other.txt": (b"O", T1),
    }
    m = FilesMirror(
        root=tmp_path / "mirror", workspace_id="ws",
        lakehouses={"customer": LakehouseInfo("customer", "lh1", "ws")},
        write_mode=write_mode, credential_factory=lambda: None,
        link_base=link_base or tmp_path / "lakehouse", lock_path=tmp_path / "lock.json",
        filesystem=FakeFS(store),
    )
    return m, store


def test_pull_writes_then_skips_then_repulls_on_change(tmp_path):
    m, store = make(tmp_path)
    r = m.pull("customer", ["lib/"])
    assert (r.transferred, r.skipped, r.errors) == (2, 0, [])
    a = m.mirror_dir("customer") / "lib" / "a.whl"
    assert a.read_bytes() == b"AAA" and abs(a.stat().st_mtime - T1.timestamp()) < 1
    assert (m.mirror_dir("customer") / "lib" / "sub" / "b.txt").read_bytes() == b"BB"
    r = m.pull("customer", ["lib/"])
    assert (r.transferred, r.skipped) == (0, 2)
    store["lh1/Files/lib/sub/b.txt"] = (b"BBB", T2)
    r = m.pull("customer", ["lib/"])
    assert (r.transferred, r.skipped) == (1, 1)
    assert (m.mirror_dir("customer") / "lib" / "sub" / "b.txt").read_bytes() == b"BBB"


def test_pull_single_file_and_missing_path(tmp_path):
    m, _ = make(tmp_path)
    assert m.pull("customer", ["other.txt"]).transferred == 1  # falls back to file properties
    r = m.pull("customer", ["nope/"])
    assert r.transferred == 0 and r.errors and "nope" in r.errors[0]


@pytest.mark.parametrize("mode", ["sandbox", "readonly"])
def test_push_refused_unless_writethrough(tmp_path, mode):
    m, _ = make(tmp_path, write_mode=mode)
    with pytest.raises(PermissionError, match="LOCAL_SPARK_WRITE_MODE"):
        m.push("customer")


def test_push_uploads_only_changes(tmp_path):
    m, store = make(tmp_path, write_mode="writethrough")
    m.pull("customer", ["lib/"])
    (m.mirror_dir("customer") / "new.txt").write_text("hello")
    r = m.push("customer")
    assert r.transferred == 1 and r.skipped == 2 and not r.errors
    assert store["lh1/Files/new.txt"][0] == b"hello"


def test_resolve_paths_and_mounts(tmp_path):
    m, _ = make(tmp_path)
    base = m.mirror_dir("customer")
    assert m.resolve("/lakehouse/default/Files/lib/x", "customer") == base / "lib" / "x"
    assert m.resolve("/lakehouse/customer/Files/y", None) == base / "y"
    assert m.resolve("/lakehouse/default", "customer") == base
    assert m.resolve("/lakehouse/default/Tables/t", "customer") is None  # never mirrored
    assert m.resolve("/tmp/x", "customer") is None
    assert m.resolve("/lakehouse/default/Files/x", None) is None  # no default lakehouse
    m.mounts["/mnt/cust"] = "customer"
    assert m.resolve("/mnt/cust/Files/z", None) == base / "z"
    assert m.resolve("/mnt/cust/z", None) == base / "z"


@pytest.mark.skipif(os.name == "nt", reason="symlink semantics; Windows uses junctions (validated live)")
def test_link_default_lock_and_conflicts(tmp_path, monkeypatch):
    m, _ = make(tmp_path)
    rep = m.link_default("customer")
    assert rep["linked"], rep
    link = tmp_path / "lakehouse" / "default"
    assert link.is_symlink() and Path(os.path.realpath(link)) == m.mirror_dir("customer").parent.resolve()
    assert (link / "Files").is_dir()  # /lakehouse/default/Files is the mirror
    assert json.loads((tmp_path / "lock.json").read_text())["pid"] == os.getpid()
    assert m.link_default("customer")["linked"]  # idempotent

    # another LIVE session holds it for a different lakehouse -> refuse, point at the mirror instead
    (tmp_path / "lock.json").write_text(json.dumps({"lakehouse": "silver", "lakehouse_id": "lh9", "pid": 99999}))
    monkeypatch.setattr(files_mod, "_pid_alive", lambda pid: True)
    rep = m.link_default("customer")
    assert not rep["linked"] and "held by another live session" in rep["reason"] and "LOCAL_SPARK_FILES_ROOT" in rep["reason"]
    monkeypatch.setattr(files_mod, "_pid_alive", lambda pid: False)  # dead session -> take over
    assert m.link_default("customer")["linked"]

    # a real directory at the link path is never replaced
    link.unlink()
    link.mkdir()
    rep = m.link_default("customer")
    assert not rep["linked"] and "real directory" in rep["reason"]

    m.release_link()
    assert not (tmp_path / "lock.json").exists()


@pytest.mark.skipif(os.name == "nt", reason="symlink semantics")
def test_link_base_dangling_symlink_is_repaired(tmp_path):
    # /lakehouse -> <missing dir under a writable location>: create the target, then link
    target = tmp_path / "target_missing"
    (tmp_path / "lh_link").symlink_to(target)
    m, _ = make(tmp_path, link_base=tmp_path / "lh_link")
    rep = m.link_default("customer")
    assert rep["linked"], rep
    assert target.is_dir() and (target / "default").is_symlink()


@pytest.mark.skipif(os.name == "nt", reason="symlink semantics")
def test_mount_registers_and_links(tmp_path):
    m, _ = make(tmp_path)
    mp = tmp_path / "mnt" / "cust"
    rep = m.mount("abfss://ws@onelake.dfs.fabric.microsoft.com/lh1/Files", str(mp), None)
    assert rep["linked"] and m.mounts[str(mp)] == "customer" and mp.is_symlink()
    assert Path(os.path.realpath(mp)) == m.mirror_dir("customer").resolve()  # source ended in /Files
    assert m.resolve(str(mp / "Files" / "q"), None) == m.mirror_dir("customer") / "q"
    with pytest.raises(LookupError):
        m.mount("abfss://ws@onelake.dfs.fabric.microsoft.com/unknown-id/Files", str(tmp_path / "mnt2"), None)
