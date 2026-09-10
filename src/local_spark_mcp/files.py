"""Local mirror of lakehouse ``Files/`` so ``/lakehouse/default/Files`` works for
plain Python IO, subprocesses, and native libraries reading a data directory.

- Mirror dir: ``<mirror_root>/<workspace-id>/<lakehouse-id>/Files`` — keyed by
  ids, so every project that touches the same lakehouse shares one copy.
- Selective sync: only the configured ``Files/`` subtrees are pulled (through
  the OneLake data plane with the ambient credential); unchanged files are
  skipped by size + modification time. Push is allowed only in ``writethrough``.
- The literal path: ``/lakehouse/default`` (``C:\\lakehouse\\default`` on Windows)
  is a symlink / junction to the mirror of the current default lakehouse. It is
  one global path shared by every session on the machine, so a lockfile records
  the owner and we refuse to repoint it while another live session disagrees.
  ``LOCAL_SPARK_FILES_ROOT`` always names the mirror directly for code that can
  avoid the global path.
- ``Tables/`` is never mirrored; Spark reaches those through the catalog.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

ONELAKE_URL = "https://onelake.dfs.fabric.microsoft.com"


@dataclass
class SyncResult:
    direction: str
    lakehouse: str
    transferred: int = 0
    skipped: int = 0
    bytes: int = 0
    errors: list[str] = field(default_factory=list)
    paths: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "direction": self.direction, "lakehouse": self.lakehouse, "transferred": self.transferred,
            "skipped": self.skipped, "bytes": self.bytes, "errors": self.errors, "paths": self.paths,
        }


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        import ctypes

        handle = ctypes.windll.kernel32.OpenProcess(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
        if not handle:
            return False
        ctypes.windll.kernel32.CloseHandle(handle)
        return True
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _norm_rel(path: str) -> str:
    return path.replace("\\", "/").strip("/")


class FilesMirror:
    def __init__(
        self,
        *,
        root: Path,
        workspace_id: str,
        lakehouses: dict,  # name -> LakehouseInfo
        write_mode: str,
        credential_factory,
        link_base: str | None = None,
        lock_path: Path | None = None,
        filesystem=None,  # injectable DataLakeFileSystemClient (tests)
    ):
        self.root = Path(root)
        self.workspace_id = workspace_id
        self.lakehouses = lakehouses
        self.write_mode = write_mode
        self._cred = credential_factory
        self.link_base = Path(link_base) if link_base else (Path(r"C:\lakehouse") if os.name == "nt" else Path("/lakehouse"))
        self.lock_path = Path(lock_path) if lock_path else self.root / ".lakehouse-link.json"
        self._fs = filesystem
        self.mounts: dict[str, str] = {}  # mount point -> lakehouse name

    # ---------- names / paths ----------

    def _lh(self, name: str):
        info = self.lakehouses.get(name)
        if info is None:
            for known, lh in self.lakehouses.items():
                if known.lower() == name.lower():
                    return lh
            raise LookupError(f"unknown lakehouse {name!r}; known: {sorted(self.lakehouses)}")
        return info

    def mirror_dir(self, lakehouse: str) -> Path:
        d = self.root / self.workspace_id / self._lh(lakehouse).id / "Files"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def resolve(self, path: str, default_lakehouse: str | None) -> Path | None:
        """Map a Fabric-style path to the mirror. Handles ``/lakehouse/default/...``,
        ``/lakehouse/<name>/...`` (with or without the ``Files/`` segment), and
        registered mount points. Returns None for anything else."""
        p = path.replace("\\", "/")
        for mount, lakehouse in sorted(self.mounts.items(), key=lambda kv: -len(kv[0])):
            m = mount.replace("\\", "/").rstrip("/")
            if p == m or p.startswith(m + "/"):
                rel = _norm_rel(p[len(m):])
                rel = rel[len("Files/"):] if rel.startswith("Files/") else ("" if rel == "Files" else rel)
                return self.mirror_dir(lakehouse) / rel if rel else self.mirror_dir(lakehouse)
        parts = [s for s in p.split("/") if s]
        if len(parts) < 2 or parts[0].lower() != "lakehouse":
            return None
        name = default_lakehouse if parts[1] == "default" else parts[1]
        if not name:
            return None
        rest = parts[2:]
        if rest and rest[0] == "Files":
            rest = rest[1:]
        elif rest and rest[0] == "Tables":
            return None  # never mirrored
        base = self.mirror_dir(name)
        return base.joinpath(*rest) if rest else base

    # ---------- OneLake data plane ----------

    def _filesystem(self):
        if self._fs is None:
            from azure.storage.filedatalake import DataLakeServiceClient

            self._fs = DataLakeServiceClient(ONELAKE_URL, credential=self._cred()).get_file_system_client(self.workspace_id)
        return self._fs

    def _remote(self, lakehouse: str, rel: str = "") -> str:
        base = f"{self._lh(lakehouse).id}/Files"
        rel = _norm_rel(rel)
        return f"{base}/{rel}" if rel else base

    @staticmethod
    def _epoch(dt) -> float:
        try:
            return dt.timestamp()
        except Exception:
            return 0.0

    def pull(self, lakehouse: str, paths: list[str] | None = None, progress=None) -> SyncResult:
        """Download the given ``Files/`` subtrees (or files) into the mirror,
        skipping files whose size and modification time already match."""
        fs = self._filesystem()
        local_root = self.mirror_dir(lakehouse)
        result = SyncResult("pull", lakehouse, paths=[_norm_rel(p) for p in (paths or [""])])
        for rel in result.paths:
            remote = self._remote(lakehouse, rel)
            try:
                entries = list(fs.get_paths(path=remote, recursive=True))
            except Exception as exc:
                # maybe a single file rather than a directory
                try:
                    props = fs.get_file_client(remote).get_file_properties()
                    entries = [type("E", (), {"name": remote, "is_directory": False, "content_length": props.size, "last_modified": props.last_modified})()]
                except Exception:
                    result.errors.append(f"{rel or '/'}: {type(exc).__name__}: {str(exc).splitlines()[0][:160]}")
                    continue
            prefix = self._remote(lakehouse) + "/"
            for entry in entries:
                if getattr(entry, "is_directory", False):
                    continue
                item_rel = entry.name[len(prefix):] if entry.name.startswith(prefix) else entry.name
                local = local_root / item_rel
                size = int(getattr(entry, "content_length", 0) or 0)
                mtime = self._epoch(getattr(entry, "last_modified", None))
                if local.is_file() and local.stat().st_size == size and local.stat().st_mtime >= mtime:
                    result.skipped += 1
                    continue
                try:
                    local.parent.mkdir(parents=True, exist_ok=True)
                    data = fs.get_file_client(entry.name).download_file().readall()
                    local.write_bytes(data)
                    if mtime:
                        os.utime(local, (mtime, mtime))
                    result.transferred += 1
                    result.bytes += len(data)
                    if progress:
                        progress(item_rel, len(data))
                except Exception as exc:
                    result.errors.append(f"{item_rel}: {type(exc).__name__}: {str(exc).splitlines()[0][:160]}")
        return result

    def push(self, lakehouse: str, paths: list[str] | None = None, progress=None) -> SyncResult:
        """Upload local changes to OneLake. Allowed only in ``writethrough``."""
        if self.write_mode != "writethrough":
            raise PermissionError(
                f"write_mode is '{self.write_mode}': refusing to push Files to OneLake. Set "
                "LOCAL_SPARK_WRITE_MODE (or [runtime] write_mode in local-spark.toml) to "
                "'writethrough' to upload."
            )
        fs = self._filesystem()
        local_root = self.mirror_dir(lakehouse)
        result = SyncResult("push", lakehouse, paths=[_norm_rel(p) for p in (paths or [""])])
        for rel in result.paths:
            start = local_root / rel if rel else local_root
            files = [start] if start.is_file() else [p for p in start.rglob("*") if p.is_file()] if start.is_dir() else []
            if not files and not start.exists():
                result.errors.append(f"{rel or '/'}: not present in the local mirror")
                continue
            for local in files:
                item_rel = local.relative_to(local_root).as_posix()
                remote = self._remote(lakehouse, item_rel)
                client = fs.get_file_client(remote)
                st = local.stat()
                try:
                    props = client.get_file_properties()
                    if props.size == st.st_size and self._epoch(props.last_modified) >= st.st_mtime:
                        result.skipped += 1
                        continue
                except Exception:
                    pass  # missing remotely -> upload
                try:
                    data = local.read_bytes()
                    client.upload_data(data, overwrite=True)
                    result.transferred += 1
                    result.bytes += len(data)
                    if progress:
                        progress(item_rel, len(data))
                except Exception as exc:
                    result.errors.append(f"{item_rel}: {type(exc).__name__}: {str(exc).splitlines()[0][:160]}")
        return result

    # ---------- the literal path ----------

    def _read_lock(self) -> dict | None:
        try:
            return json.loads(self.lock_path.read_text())
        except (OSError, json.JSONDecodeError):
            return None

    def _link_base_dir(self) -> tuple[Path | None, str]:
        base = self.link_base
        if base.is_symlink() or base.exists():
            target = Path(os.path.realpath(base))
            if not target.exists():
                try:
                    target.mkdir(parents=True)
                except OSError as exc:
                    return None, (f"{base} points at {target}, which does not exist and cannot be created "
                                  f"({exc.strerror}). One-time fix: sudo mkdir -p {target} && sudo chown $USER {target}")
            if os.access(target, os.W_OK):
                return target, "ok"
            return None, f"{base} resolves to {target}, which you cannot write. One-time fix: sudo chown $USER {target}"
        try:
            base.mkdir(parents=True)  # works unelevated on Windows (C:\lakehouse); needs root on POSIX
            return base, "ok"
        except OSError:
            return None, f"{base} does not exist. One-time fix: sudo mkdir {base} && sudo chown $USER {base}"

    def link_default(self, lakehouse: str) -> dict:
        """Point ``<link_base>/default`` at this lakehouse's mirror, unless another
        live session holds it for a different lakehouse. Never deletes a real
        directory found at the link path."""
        info = self._lh(lakehouse)
        mirror = self.mirror_dir(info.name)
        target = mirror.parent  # the lakehouse dir: <link>/Files is the mirror, like /lakehouse/default/Files
        report = {"lakehouse": info.name, "files_root": str(mirror), "linked": False, "path": str(self.link_base / "default")}
        lock = self._read_lock()
        if lock and lock.get("lakehouse_id") != info.id and lock.get("pid") != os.getpid() and _pid_alive(int(lock.get("pid", 0))):
            report["reason"] = (f"{self.link_base / 'default'} is held by another live session (pid {lock['pid']}) for "
                                f"lakehouse {lock.get('lakehouse')!r}; not repointing. Use LOCAL_SPARK_FILES_ROOT={mirror} instead.")
            return report
        base_dir, msg = self._link_base_dir()
        if base_dir is None:
            report["reason"] = msg + f" Until then, use LOCAL_SPARK_FILES_ROOT={mirror}."
            return report
        link = base_dir / "default"
        report["path"] = str(link)
        try:
            # A junction whose target was deleted (pytest temp cleanup, a purged
            # session dir) still occupies the name: detect it by attributes, not
            # by exists(), or mklink fails with "already exists".
            if link.is_symlink() or (os.name == "nt" and _is_junction(link)):
                if target.exists() and Path(os.path.realpath(link)) == target.resolve():
                    report["linked"] = True
                else:
                    if os.name == "nt":
                        os.rmdir(link)  # junctions (dangling or not) are removed like directories
                    else:
                        link.unlink()
            elif link.exists():
                report["reason"] = f"{link} is a real directory, not a link; refusing to replace it. Use LOCAL_SPARK_FILES_ROOT={mirror}."
                return report
            if not report["linked"]:
                if os.name == "nt":
                    subprocess.run(["cmd", "/c", "mklink", "/J", str(link), str(target)], check=True, capture_output=True)
                else:
                    os.symlink(target, link)
                report["linked"] = True
        except (OSError, subprocess.CalledProcessError) as exc:
            report["reason"] = f"could not create {link}: {exc}. Use LOCAL_SPARK_FILES_ROOT={mirror}."
            return report
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self.lock_path.write_text(json.dumps({
            "lakehouse": info.name, "lakehouse_id": info.id, "workspace_id": self.workspace_id,
            "pid": os.getpid(), "link": str(link), "target": str(target), "mirror": str(mirror), "time": time.time(),
        }))
        return report

    def release_link(self) -> None:
        lock = self._read_lock()
        if lock and lock.get("pid") == os.getpid():
            try:
                self.lock_path.unlink()
            except OSError:
                pass

    def mount(self, source: str, mount_point: str, default_lakehouse: str | None) -> dict:
        """Register a mount point for a lakehouse (``abfss://…/<lakehouse-id>`` or
        ``/lakehouse/<name>``) and try to link it; the registry works even when the
        link can't be created."""
        name = None
        if source.startswith("abfss://"):
            seg = source.split("onelake.dfs.fabric.microsoft.com/", 1)[-1].split("/")[0]
            for lh_name, info in self.lakehouses.items():
                if info.id == seg or lh_name.lower() == seg.lower():
                    name = lh_name
        else:
            parts = [s for s in source.replace("\\", "/").split("/") if s]
            if len(parts) >= 2 and parts[0].lower() == "lakehouse":
                name = default_lakehouse if parts[1] == "default" else parts[1]
        if not name:
            raise LookupError(f"cannot map mount source {source!r} to a known lakehouse")
        info = self._lh(name)
        self.mounts[mount_point] = info.name
        mirror = self.mirror_dir(info.name)
        target = mirror if source.rstrip("/").endswith("/Files") else mirror.parent
        report = {"mountPoint": mount_point, "lakehouse": info.name, "files_root": str(mirror), "linked": False}
        try:
            mp = Path(mount_point)
            if mp.is_symlink():
                mp.unlink()
            elif os.name == "nt" and _is_junction(mp):
                os.rmdir(mp)
            if not mp.exists():
                mp.parent.mkdir(parents=True, exist_ok=True)
                if os.name == "nt":
                    subprocess.run(["cmd", "/c", "mklink", "/J", str(mp), str(target)], check=True, capture_output=True)
                else:
                    os.symlink(target, mp)
                report["linked"] = True
        except (OSError, subprocess.CalledProcessError) as exc:
            report["reason"] = f"could not link {mount_point}: {exc}; fs.ls/exists still resolve it via the mirror."
        return report


def _is_junction(path: Path) -> bool:
    try:
        return bool(os.stat(path, follow_symlinks=False).st_file_attributes & 0x400)  # REPARSE_POINT
    except (OSError, AttributeError):
        return False
