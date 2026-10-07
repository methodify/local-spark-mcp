"""Python-level lazy Files: ``/lakehouse/<default|name>/Files/...`` without a
synced mirror.

Under ``files_mode = "lazy"`` the engine installs these hooks so that the Fabric
paths work for plain Python IO the way they do on Fabric, fetching only what is
touched: ``open()`` of a file fetches it into the mirror on first use (and
re-fetches when OneLake has a newer copy), ``os.listdir`` / ``os.scandir`` list
the OneLake directory merged with local-only files, ``os.stat`` answers from
OneLake metadata for files not yet fetched, and writes land in the mirror
(pushed on close only in writethrough). Everything else, and every path outside
the ``/lakehouse/`` prefixes and registered mount points, goes straight to the
original functions, so a library opening its own files never pays.

``/lakehouse/default`` means the active context's default lakehouse (so two
notebooks with different default lakehouses each see their own Files/), not the
process-wide link.

Native readers (DuckDB, Arrow ``OSFile``, …) open files from C and bypass
these hooks: for them the file must be in the mirror first (``sync_files``).
"""

from __future__ import annotations

import builtins
import errno
import io
import os
import stat as _stat
import sys
import time
from pathlib import Path

PREFIXES = ("/lakehouse/",)


def _norm(path) -> str | None:
    if isinstance(path, int):
        return None
    try:
        p = os.fspath(path)
    except TypeError:
        return None
    if isinstance(p, bytes):
        try:
            p = p.decode()
        except UnicodeDecodeError:
            return None
    p = p.replace("\\", "/")
    if len(p) > 2 and p[1] == ":" and p[2] == "/":
        p = p[2:]
    return p


class _DirEntry:
    """A minimal os.DirEntry stand-in for lakehouse listings."""

    def __init__(self, name: str, path: str, is_dir: bool, size: int, mtime: float):
        self.name, self.path, self._is_dir, self._size, self._mtime = name, path, is_dir, size, mtime

    def is_dir(self, *, follow_symlinks=True) -> bool:
        return self._is_dir

    def is_file(self, *, follow_symlinks=True) -> bool:
        return not self._is_dir

    def is_symlink(self) -> bool:
        return False

    def inode(self) -> int:
        return 0

    def stat(self, *, follow_symlinks=True):
        return _fake_stat(self._is_dir, self._size, self._mtime)

    def __fspath__(self) -> str:
        return self.path

    def __repr__(self) -> str:
        return f"<DirEntry {self.name!r}>"


class _ScanDir:
    def __init__(self, entries):
        self._it = iter(entries)

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._it)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def close(self):
        pass


def _fake_stat(is_dir: bool, size: int, mtime: float) -> os.stat_result:
    mode = (_stat.S_IFDIR | 0o755) if is_dir else (_stat.S_IFREG | 0o644)
    t = int(mtime or time.time())
    return os.stat_result((mode, 0, 0, 1, 0, 0, size, t, t, t))


class _PushOnClose:
    """Wraps a mirror file opened for writing; pushes to OneLake when closed (writethrough)."""

    def __init__(self, inner, push):
        self._inner, self._push, self._done = inner, push, False

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def __iter__(self):
        return iter(self._inner)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    def close(self):
        if not self._done:
            self._done = True
            self._inner.close()
            self._push()


class LazyFilesHooks:
    def __init__(self, engine):
        self.engine = engine
        self.files = engine.files
        self._orig: dict = {}
        self.installed = False

    # ---- resolution ----

    def _default_lakehouse(self) -> str | None:
        active = getattr(self.engine, "_active", None)
        return (getattr(active, "default_lakehouse", None) if active is not None else None) or self.engine.default_lakehouse

    def _locate(self, path):
        p = _norm(path)
        if p is None:
            return None
        if not (p.startswith(PREFIXES) or p.rstrip("/") == "/lakehouse" or any(
                p == m.replace("\\", "/").rstrip("/") or p.startswith(m.replace("\\", "/").rstrip("/") + "/") for m in self.files.mounts)):
            return None
        return self.files.locate(p, self._default_lakehouse())

    def _local(self, lakehouse: str, rel: str) -> Path:
        base = self.files.mirror_dir(lakehouse)
        return base / rel if rel else base

    def _not_found(self, path, lakehouse: str, rel: str) -> FileNotFoundError:
        parent = rel.rsplit("/", 1)[0] if "/" in rel else ""
        return FileNotFoundError(
            errno.ENOENT,
            f"No such file or directory in OneLake (lakehouse {lakehouse!r}, Files/{rel}). If a native reader "
            f"needs it on disk, pull it first: sync_files(paths=[{(parent or rel)!r}], lakehouse={lakehouse!r})",
            str(path))

    def _remote_stat(self, lakehouse, rel):
        try:
            return self.files.remote_stat(lakehouse, rel)
        except Exception as exc:  # the data plane is unreachable: behave like a plain mirror
            print(f"local-spark: lazy Files: OneLake metadata for {lakehouse}/Files/{rel} unavailable: {exc}", file=sys.stderr)
            return None

    # ---- hooks ----

    def _open(self, file, mode="r", *args, **kwargs):
        hit = self._locate(file)
        if hit is None:
            return self._orig["open"](file, mode, *args, **kwargs)
        lakehouse, rel = hit
        local = self._local(lakehouse, rel)
        writing = any(c in mode for c in "wax+")
        if not writing:
            if not local.is_file() or self._stale(lakehouse, rel, local):
                st = self._remote_stat(lakehouse, rel)
                if st is None:
                    if local.exists():
                        return self._orig["open"](local, mode, *args, **kwargs)
                    raise self._not_found(file, lakehouse, rel)
                if st["is_dir"]:
                    raise IsADirectoryError(errno.EISDIR, "Is a directory", str(file))
                local = self.files.fetch_file(lakehouse, rel)
            return self._orig["open"](local, mode, *args, **kwargs)
        local.parent.mkdir(parents=True, exist_ok=True)
        if "a" in mode or "+" in mode:  # start from the OneLake copy when there is one and no local one
            if not local.exists() and self._remote_stat(lakehouse, rel) not in (None,) and not self._remote_stat(lakehouse, rel)["is_dir"]:
                self.files.fetch_file(lakehouse, rel)
        f = self._orig["open"](local, mode, *args, **kwargs)
        if self.files.write_mode == "writethrough":
            return _PushOnClose(f, lambda: self._push(lakehouse, rel))
        return f

    def _stale(self, lakehouse: str, rel: str, local: Path) -> bool:
        st = self._remote_stat(lakehouse, rel)
        if st is None or st["is_dir"]:
            return False
        ls = local.stat()
        return not (ls.st_size == st["size"] and ls.st_mtime >= st["mtime"])

    def _push(self, lakehouse: str, rel: str) -> None:
        try:
            self.files.push_file(lakehouse, rel)
        except Exception as exc:
            print(f"local-spark: lazy Files: push of {lakehouse}/Files/{rel} failed: {exc}", file=sys.stderr)

    def _stat(self, path, *args, **kwargs):
        hit = self._locate(path)
        if hit is None:
            return self._orig["stat"](path, *args, **kwargs)
        lakehouse, rel = hit
        local = self._local(lakehouse, rel)
        if local.exists():
            return self._orig["stat"](local, *args, **kwargs)
        if rel == "":
            return _fake_stat(True, 0, 0)
        st = self._remote_stat(lakehouse, rel)
        if st is None:
            raise self._not_found(path, lakehouse, rel)
        return _fake_stat(st["is_dir"], st["size"], st["mtime"])

    def _entries(self, path, lakehouse: str, rel: str) -> list[_DirEntry]:
        base = _norm(path).rstrip("/")
        local = self._local(lakehouse, rel)
        seen: dict[str, _DirEntry] = {}
        if local.is_dir():
            for e in self._orig["scandir"](local):
                st = e.stat()
                seen[e.name] = _DirEntry(e.name, f"{base}/{e.name}", e.is_dir(), st.st_size, st.st_mtime)
        try:
            remote = self.files.remote_list(lakehouse, rel)
        except Exception as exc:
            print(f"local-spark: lazy Files: OneLake listing of {lakehouse}/Files/{rel} unavailable: {exc}", file=sys.stderr)
            remote = None
        if remote is None and not local.is_dir():
            raise self._not_found(path, lakehouse, rel)
        for r in remote or []:
            seen.setdefault(r["name"], _DirEntry(r["name"], f"{base}/{r['name']}", r["is_dir"], r["size"], r["mtime"]))
        return [seen[k] for k in sorted(seen)]

    def _listdir(self, path="."):
        hit = self._locate(path)
        if hit is None:
            return self._orig["listdir"](path)
        return [e.name for e in self._entries(path, *hit)]

    def _scandir(self, path="."):
        hit = self._locate(path)
        if hit is None:
            return self._orig["scandir"](path)
        return _ScanDir(self._entries(path, *hit))

    def _passthrough_local(self, name):
        """mkdir / remove / rename & co.: act on the mirror copy of a lakehouse path."""
        orig = self._orig[name]

        def hooked(path, *args, **kwargs):
            hit = self._locate(path)
            if hit is not None:
                path = self._local(*hit)
                if name in ("rename", "replace") and args:
                    dst = self._locate(args[0])
                    if dst is not None:
                        args = (self._local(*dst),) + tuple(args[1:])
            return orig(path, *args, **kwargs)

        return hooked

    def install(self) -> None:
        if self.installed:
            return
        # Import IPython first: its `open` (_modified_open) captured `io.open` at import,
        # and we want that captured value to be the original, saved below.
        try:
            import IPython.core.interactiveshell as _ish
        except Exception:
            _ish = None
        self._orig = {"open": builtins.open, "stat": os.stat, "lstat": os.lstat, "listdir": os.listdir, "scandir": os.scandir,
                      "mkdir": os.mkdir, "remove": os.remove, "unlink": os.unlink, "rmdir": os.rmdir,
                      "rename": os.rename, "replace": os.replace}
        self._captured = []
        if _ish is not None and callable(getattr(_ish, "io_open", None)):
            self._captured.append((_ish, "io_open", _ish.io_open))
            _ish.io_open = self._open
        builtins.open = self._open
        io.open = self._open
        os.stat = self._stat
        os.lstat = self._stat
        os.listdir = self._listdir
        os.scandir = self._scandir
        for name in ("mkdir", "remove", "unlink", "rmdir", "rename", "replace"):
            setattr(os, name, self._passthrough_local(name))
        # Python 3.13's pathlib glob goes through glob._StringGlobber, which captured
        # os.scandir / os.lstat as staticmethods at import time; point those at the hooks too.
        import glob as _glob

        self._globbers = []
        for cls_name in ("_StringGlobber", "_Globber", "_PathGlobber"):
            cls = getattr(_glob, cls_name, None)
            if cls is None:
                continue
            saved = {}
            for attr, hook in (("scandir", self._scandir), ("lstat", self._stat)):
                if attr in cls.__dict__ and isinstance(cls.__dict__[attr], staticmethod):
                    saved[attr] = cls.__dict__[attr]
                    setattr(cls, attr, staticmethod(hook))
            if saved:
                self._globbers.append((cls, saved))
        self.installed = True

    def uninstall(self) -> None:
        if not self.installed:
            return
        builtins.open = self._orig["open"]
        io.open = self._orig["open"]
        os.stat, os.lstat, os.listdir, os.scandir = self._orig["stat"], self._orig["lstat"], self._orig["listdir"], self._orig["scandir"]
        for name in ("mkdir", "remove", "unlink", "rmdir", "rename", "replace"):
            setattr(os, name, self._orig[name])
        for cls, saved in getattr(self, "_globbers", []):
            for attr, value in saved.items():
                setattr(cls, attr, value)
        self._globbers = []
        for mod, attr, value in getattr(self, "_captured", []):
            setattr(mod, attr, value)
        self._captured = []
        self.installed = False
