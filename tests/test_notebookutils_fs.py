"""notebookutils.fs over both path kinds, without Spark or OneLake: the local side
is a temp mirror directory, the abfss:// side an in-memory stand-in for the
OneLake data plane behind the engine's onelake_* helpers."""

import types
from pathlib import Path

import pytest

from local_spark_mcp.notebookutils_shim import _FS


class FakeEngine:
    """The _ShimEngine surface: local paths resolve under `mirror`, abfss paths hit `remote`."""

    def __init__(self, mirror: Path, write_mode="sandbox"):
        self.mirror, self.write_mode, self.files_hooks = mirror, write_mode, False
        self.remote: dict[str, bytes] = {}  # "abfss://ws@host/lh/Files/x" -> bytes; dirs are implied

    def files_resolve(self, path):
        p = str(path).replace("\\", "/")
        if p.startswith("abfss://") or not p.startswith("/lakehouse/"):
            return None
        parts = [x for x in p.split("/") if x][2:]
        if parts and parts[0] == "Files":
            parts = parts[1:]
        return self.mirror.joinpath(*parts)

    def files_mounts(self):
        return [{"mountPoint": "/mnt/x", "source": "abfss://ws@host/lh", "lakehouse": "lh", "localPath": str(self.mirror)}]

    def files_mount(self, source, mount_point):
        return {"linked": True}

    def _guard(self, op, path):
        if self.write_mode != "writethrough":
            raise PermissionError(f"notebookutils.fs.{op}: write_mode is '{self.write_mode}', so {path} on OneLake is not modified.")

    def onelake_is_dir(self, path):
        path = path.rstrip("/")
        if path in self.remote:
            return False
        return True if any(k.startswith(path + "/") for k in self.remote) else None

    def onelake_exists(self, path):
        return self.onelake_is_dir(path) is not None

    def onelake_ls(self, path):
        from local_spark_mcp.notebookutils_shim import FileInfo
        base = path.rstrip("/")
        names = {}
        for k, v in self.remote.items():
            if k.startswith(base + "/"):
                head = k[len(base) + 1:].split("/", 1)
                names.setdefault(head[0], FileInfo(head[0], f"{base}/{head[0]}", 0 if len(head) > 1 else len(v), len(head) > 1))
        return sorted(names.values(), key=lambda f: f.name)

    def onelake_read(self, path, max_bytes=None):
        data = self.remote[path]
        return data[:max_bytes] if max_bytes else data

    def onelake_write(self, path, data, overwrite=False):
        self._guard("put", path)
        if path in self.remote and not overwrite:
            raise FileExistsError(path)
        self.remote[path] = bytes(data)

    def onelake_append(self, path, data, create=False):
        self._guard("append", path)
        if path not in self.remote and not create:
            raise FileNotFoundError(path)
        self.remote[path] = self.remote.get(path, b"") + bytes(data)

    def onelake_mkdirs(self, path):
        self._guard("mkdirs", path)

    def onelake_rm(self, path, recurse=False):
        self._guard("rm", path)
        kind = self.onelake_is_dir(path)
        if kind is None:
            raise FileNotFoundError(path)
        if kind and not recurse:
            raise IsADirectoryError(path)
        for k in [k for k in self.remote if k == path or k.startswith(path.rstrip("/") + "/")]:
            del self.remote[k]

    def onelake_rename(self, src, dst):
        self._guard("mv", dst)
        for k in [k for k in self.remote if k == src or k.startswith(src + "/")]:
            self.remote[dst + k[len(src):]] = self.remote.pop(k)


@pytest.fixture
def fs(tmp_path):
    eng = FakeEngine(tmp_path / "mirror")
    eng.mirror.mkdir()
    return _FS(eng), eng


def test_local_side_round_trip(fs):
    f, eng = fs
    assert f.mkdirs("/lakehouse/default/Files/a/b") and (eng.mirror / "a" / "b").is_dir()
    assert f.put("/lakehouse/default/Files/a/t.txt", "hello")
    with pytest.raises(FileExistsError):
        f.put("/lakehouse/default/Files/a/t.txt", "again")
    assert f.put("/lakehouse/default/Files/a/t.txt", "hello", overwrite=True)
    assert f.head("/lakehouse/default/Files/a/t.txt") == "hello" and f.head("/lakehouse/default/Files/a/t.txt", 2) == "he"
    with pytest.raises(FileNotFoundError):
        f.append("/lakehouse/default/Files/a/new.txt", "x")
    assert f.append("/lakehouse/default/Files/a/new.txt", "x", createFileIfNotExists=True)
    assert f.append("/lakehouse/default/Files/a/new.txt", "y")
    assert f.head("/lakehouse/default/Files/a/new.txt") == "xy"
    assert [e.name for e in f.ls("/lakehouse/default/Files/a")] == ["b", "new.txt", "t.txt"]
    assert f.cp("/lakehouse/default/Files/a/t.txt", "/lakehouse/default/Files/c/t2.txt") and f.head("/lakehouse/default/Files/c/t2.txt") == "hello"
    with pytest.raises(IsADirectoryError):
        f.cp("/lakehouse/default/Files/a", "/lakehouse/default/Files/a2")
    assert f.cp("/lakehouse/default/Files/a", "/lakehouse/default/Files/a2", recurse=True)
    assert sorted(e.name for e in f.ls("/lakehouse/default/Files/a2")) == ["b", "new.txt", "t.txt"]
    assert f.mv("/lakehouse/default/Files/a2/t.txt", "/lakehouse/default/Files/d/moved.txt", create_path=True)
    assert f.exists("/lakehouse/default/Files/d/moved.txt") and not f.exists("/lakehouse/default/Files/a2/t.txt")
    with pytest.raises(FileExistsError):
        f.mv("/lakehouse/default/Files/a/t.txt", "/lakehouse/default/Files/d/moved.txt")
    assert f.mv("/lakehouse/default/Files/a/t.txt", "/lakehouse/default/Files/d/moved.txt", overwrite=True)
    with pytest.raises(IsADirectoryError):
        f.rm("/lakehouse/default/Files/a2")
    assert f.rm("/lakehouse/default/Files/a2", recurse=True) and not (eng.mirror / "a2").exists()
    assert f.rm("/lakehouse/default/Files/d/moved.txt") and not f.exists("/lakehouse/default/Files/d/moved.txt")
    with pytest.raises(FileNotFoundError):
        f.rm("/lakehouse/default/Files/nope")
    m = f.mounts()
    assert m[0].mountPoint == "/mnt/x" and f.refreshMounts() is True


def test_remote_side_respects_write_mode(fs):
    f, eng = fs
    eng.remote["abfss://ws@host/lh/Files/r.txt"] = b"remote"
    assert f.head("abfss://ws@host/lh/Files/r.txt") == "remote" and f.exists("abfss://ws@host/lh/Files/r.txt")
    for call in (lambda: f.put("abfss://ws@host/lh/Files/w.txt", "x"), lambda: f.rm("abfss://ws@host/lh/Files/r.txt"),
                 lambda: f.mkdirs("abfss://ws@host/lh/Files/d"), lambda: f.append("abfss://ws@host/lh/Files/r.txt", "y"),
                 lambda: f.mv("abfss://ws@host/lh/Files/r.txt", "abfss://ws@host/lh/Files/r2.txt")):
        with pytest.raises(PermissionError, match="write_mode is 'sandbox'"):
            call()
    assert eng.remote == {"abfss://ws@host/lh/Files/r.txt": b"remote"}
    # reading remote into the mirror is a write on the local side only: allowed
    assert f.cp("abfss://ws@host/lh/Files/r.txt", "/lakehouse/default/Files/pulled.txt") and f.head("/lakehouse/default/Files/pulled.txt") == "remote"
    # writethrough: everything passes through, including a cross-side mv (copy then remove)
    eng.write_mode = "writethrough"
    assert f.put("abfss://ws@host/lh/Files/w.txt", "x") and f.append("abfss://ws@host/lh/Files/w.txt", "y")
    assert eng.remote["abfss://ws@host/lh/Files/w.txt"] == b"xy"
    assert f.mv("abfss://ws@host/lh/Files/w.txt", "abfss://ws@host/lh/Files/sub/w2.txt", create_path=True)
    assert "abfss://ws@host/lh/Files/sub/w2.txt" in eng.remote
    assert f.mv("/lakehouse/default/Files/pulled.txt", "abfss://ws@host/lh/Files/up.txt")
    assert eng.remote["abfss://ws@host/lh/Files/up.txt"] == b"remote" and not f.exists("/lakehouse/default/Files/pulled.txt")
    assert f.rm("abfss://ws@host/lh/Files/sub", recurse=True) and "abfss://ws@host/lh/Files/sub/w2.txt" not in eng.remote
