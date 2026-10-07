"""A ``notebookutils`` / ``mssparkutils`` shim for running Fabric notebooks
locally. Covers the members the workspace corpus uses; anything else raises
NotImplementedError naming the member. Installed into ``sys.modules`` at engine
bootstrap so ``import notebookutils`` / ``import mssparkutils`` work in cells.
"""

from __future__ import annotations

import os
import types
from dataclasses import dataclass
from pathlib import Path


class NotebookExit(Exception):
    """Raised by notebookutils.notebook.exit(value); the runner catches it."""

    def __init__(self, value=None):
        super().__init__("" if value is None else str(value))
        self.value = value


class NotebookRunError(Exception):
    """runMultiple failure; ``.result`` carries the per-activity results, as on
    Fabric (the corpus reads ``e.result``)."""

    def __init__(self, message: str, result: dict):
        super().__init__(message)
        self.result = result


@dataclass
class FileInfo:
    name: str
    path: str
    size: int
    isDir: bool  # noqa: N815 — Fabric's field name

    @property
    def isFile(self) -> bool:  # noqa: N802
        return not self.isDir


_TYPE_CAST = {
    "String": str, "Guid": str, "DateTime": str,
    "Integer": int, "Number": float, "Boolean": lambda v: v if isinstance(v, bool) else str(v).lower() == "true",
}


class _Credentials:
    def __init__(self, engine):
        self._engine = engine

    def getSecret(self, akvName: str, secret: str, linkedService=None):  # noqa: N802,N803
        """Real Key Vault read with the ambient (az login) credential.
        ``akvName`` may be a vault name or a full https:// URL, as on Fabric."""
        try:
            from azure.keyvault.secrets import SecretClient
        except ImportError as exc:  # pragma: no cover
            raise ImportError("azure-keyvault-secrets is required for notebookutils.credentials.getSecret") from exc
        vault_url = akvName if akvName.startswith("https://") else f"https://{akvName}.vault.azure.net"
        return SecretClient(vault_url=vault_url, credential=self._engine.credential()).get_secret(secret).value


class _VariableLibrary:
    def __init__(self, engine):
        self._engine = engine

    def getLibrary(self, name: str):  # noqa: N802
        """Resolve a Variable Library item by display name in the configured
        workspace and return an object whose attributes are its variables (the
        active value set's overrides applied, if any)."""
        ws = self._engine.workspace_id()
        client = self._engine.fabric_client()
        items = [i for i in client.list_items(ws, "VariableLibrary") if i.get("displayName") == name]
        if not items:
            raise LookupError(f"Variable library {name!r} not found in workspace {ws}")
        parts = client.get_item_definition_parts(ws, items[0]["id"])
        variables = parts.get("variables.json", {}).get("variables", [])
        settings = parts.get("settings.json", {})
        values = {v["name"]: _TYPE_CAST.get(v.get("type"), str)(v.get("value")) for v in variables}
        active = settings.get("activeValueSetName")
        if active:
            overrides = parts.get(f"valueSets/{active}.json", {}).get("variableOverrides", [])
            for o in overrides:
                if o.get("name") in values:
                    values[o["name"]] = _TYPE_CAST.get(next((v.get("type") for v in variables if v["name"] == o["name"]), "String"), str)(o.get("value"))
        return types.SimpleNamespace(**values)


class _FS:
    def __init__(self, engine):
        self._engine = engine

    def _local(self, path: str) -> Path:
        local = self._engine.files_resolve(path)
        if local is None:
            raise FileNotFoundError(
                f"notebookutils.fs: {path!r} is not an abfss:// path, a /lakehouse/... path, or a "
                "registered mount point (Tables/ is never mirrored — use the catalog)."
            )
        return local

    def ls(self, path: str) -> list[FileInfo]:
        if path.startswith("abfss://"):
            return self._engine.onelake_ls(path)
        local = self._local(path)
        base = path.rstrip("/")
        if getattr(self._engine, "files_hooks", False):  # lazy: the hooks answer from OneLake + local
            with os.scandir(str(path)) as it:
                entries = sorted(it, key=lambda e: e.name)
            return [FileInfo(name=e.name, path=f"{base}/{e.name}", size=0 if e.is_dir() else e.stat().st_size, isDir=e.is_dir())
                    for e in entries]
        if not local.exists():
            raise FileNotFoundError(f"{path} (mirror: {local}) does not exist locally; sync_files may pull it")
        return [
            FileInfo(name=child.name, path=f"{base}/{child.name}",
                     size=child.stat().st_size if child.is_file() else 0, isDir=child.is_dir())
            for child in sorted(local.iterdir())
        ]

    def exists(self, path: str) -> bool:
        if path.startswith("abfss://"):
            return self._engine.onelake_exists(path)
        if getattr(self._engine, "files_hooks", False):
            return self._engine.files_resolve(path) is not None and os.path.exists(str(path))
        local = self._engine.files_resolve(path)
        return bool(local and local.exists())

    def mount(self, source: str, mountPoint: str, extraConfigs=None):  # noqa: N803
        return self._engine.files_mount(source, mountPoint).get("linked", False) or True

    def unmount(self, mountPoint: str):  # noqa: N803
        return True

    def mounts(self) -> list:
        return [types.SimpleNamespace(**m) for m in self._engine.files_mounts()]

    def getMountPath(self, mountPoint: str, scope: str = "") -> str:  # noqa: N802, N803
        return str(self._local(mountPoint))

    def refreshMounts(self) -> bool:  # noqa: N802
        return True

    # ---- the rest of Fabric's fs surface: both path kinds ----
    # abfss:// goes to the OneLake data plane (writes only in writethrough);
    # /lakehouse/... and mount points go to the local mirror: under files_mode =
    # lazy through the hooked path (fetch on read, context-aware "default"),
    # otherwise through the mirror directory. Mirror writes are never pushed here;
    # that is sync_files (push) or, under lazy + writethrough, the hooked open().

    @staticmethod
    def _remote(path: str) -> bool:
        return str(path).startswith("abfss://")

    def _os_path(self, path: str) -> str:
        """The path to hand os/open for a local-side path."""
        return str(path) if getattr(self._engine, "files_hooks", False) else str(self._local(path))

    def _is_dir(self, path: str) -> bool | None:
        if self._remote(path):
            return self._engine.onelake_is_dir(path)
        p = self._os_path(path)
        if not os.path.exists(p):
            return None
        return os.path.isdir(p)

    def _read(self, path: str, max_bytes: int | None = None) -> bytes:
        if self._remote(path):
            return self._engine.onelake_read(path, max_bytes)
        with open(self._os_path(path), "rb") as f:
            return f.read(max_bytes) if max_bytes else f.read()

    def _write(self, path: str, data: bytes, overwrite: bool) -> None:
        if self._remote(path):
            self._engine.onelake_write(path, data, overwrite)
            return
        p = self._os_path(path)
        if os.path.exists(p) and not overwrite:
            raise FileExistsError(f"{path} exists; pass overwrite=True")
        os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
        with open(p, "wb") as f:
            f.write(data)

    def _children(self, path: str) -> list[FileInfo]:
        return self.ls(path)

    def mkdirs(self, dir: str) -> bool:  # noqa: A002 — Fabric's parameter name
        if self._remote(dir):
            self._engine.onelake_mkdirs(dir)
        else:
            os.makedirs(self._os_path(dir), exist_ok=True)
        return True

    def rm(self, dir: str, recurse: bool = False) -> bool:  # noqa: A002
        kind = self._is_dir(dir)
        if kind is None:
            raise FileNotFoundError(dir)
        if self._remote(dir):
            self._engine.onelake_rm(dir, recurse)
            return True
        p = self._os_path(dir)
        if kind:
            if not recurse and os.listdir(p):
                raise IsADirectoryError(f"{dir} is a non-empty directory; pass recurse=True")
            for child in self.ls(dir):
                self.rm(child.path, True)
            os.rmdir(p)
        else:
            os.remove(p)
        return True

    def put(self, file: str, content: str, overwrite: bool = False) -> bool:
        self._write(file, content.encode("utf-8") if isinstance(content, str) else bytes(content), overwrite)
        return True

    def head(self, file: str, maxBytes: int = 1024 * 100) -> str:  # noqa: N803
        return self._read(file, maxBytes).decode("utf-8", errors="replace")

    def append(self, file: str, content: str, createFileIfNotExists: bool = False) -> bool:  # noqa: N803
        data = content.encode("utf-8") if isinstance(content, str) else bytes(content)
        if self._remote(file):
            self._engine.onelake_append(file, data, createFileIfNotExists)
            return True
        p = self._os_path(file)
        if not os.path.exists(p) and not createFileIfNotExists:
            raise FileNotFoundError(f"{file} does not exist (pass createFileIfNotExists=True)")
        os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
        with open(p, "ab") as f:
            f.write(data)
        return True

    def cp(self, from_: str, to: str, recurse: bool = False) -> bool:
        kind = self._is_dir(from_)
        if kind is None:
            raise FileNotFoundError(from_)
        if kind:
            if not recurse:
                raise IsADirectoryError(f"{from_} is a directory; pass recurse=True")
            self.mkdirs(to)
            for child in self.ls(from_):
                self.cp(child.path, f"{to.rstrip('/')}/{child.name}", True)
            return True
        self._write(to, self._read(from_), overwrite=True)
        return True

    def mv(self, from_: str, to: str, create_path: bool = False, overwrite: bool = False) -> bool:
        if self._is_dir(to) is not None and not overwrite:
            raise FileExistsError(f"{to} exists; pass overwrite=True")
        if self._remote(from_) and self._remote(to):
            if overwrite and self._is_dir(to) is not None:
                self._engine.onelake_rm(to, True)
            if create_path:
                parent = to.rsplit("/", 1)[0]
                if self._engine.onelake_is_dir(parent) is None:
                    self._engine.onelake_mkdirs(parent)
            self._engine.onelake_rename(from_, to)
            return True
        if not self._remote(from_) and not self._remote(to):
            src, dst = self._os_path(from_), self._os_path(to)
            if os.path.exists(dst) and overwrite:
                self.rm(to, True)
            if create_path:
                os.makedirs(os.path.dirname(dst) or ".", exist_ok=True)
            os.rename(src, dst)
            return True
        self.cp(from_, to, recurse=True)  # across the two sides: copy, then remove
        self.rm(from_, recurse=True)
        return True


class _Notebook:
    def __init__(self, engine):
        self._engine = engine

    def run(self, path: str, timeoutSeconds: int | None = None, arguments: dict | None = None, workspace=None):  # noqa: N803
        res = self._engine.run_notebook(path, parameters=arguments or {}, stop_on_error=True)
        if res.get("status") == "error":
            raise NotebookRunError(f"notebook {path!r} failed: {res.get('first_error')}", res)
        return res.get("exit_value")

    def runMultiple(self, dag, timeoutInSeconds=None, concurrency=None):  # noqa: N802,N803
        """Run a Fabric DAG sequentially in dependency order (Fabric runs
        activities concurrently; ordering-dependent bugs won't reproduce here).
        Returns {activity: {"exitVal", "exception"}}; raises NotebookRunError
        (with .result) if any activity failed, matching Fabric."""
        activities = list(dag.get("activities", []))
        by_name = {a["name"]: a for a in activities}
        done: dict[str, dict] = {}
        order: list[str] = []
        visiting: set[str] = set()

        def visit(name):
            if name in order:
                return
            if name in visiting:
                raise ValueError(f"runMultiple DAG has a cycle at {name!r}")
            visiting.add(name)
            for dep in by_name[name].get("dependencies", []):
                if dep not in by_name:
                    raise ValueError(f"activity {name!r} depends on unknown activity {dep!r}")
                visit(dep)
            visiting.discard(name)
            order.append(name)

        for a in activities:
            visit(a["name"])
        failed = False
        for name in order:
            act = by_name[name]
            if any(done[d]["exception"] is not None for d in act.get("dependencies", [])):
                done[name] = {"exitVal": None, "exception": "skipped: upstream activity failed"}
                failed = True
                continue
            res = self._engine.run_notebook(act.get("path", name), parameters=act.get("args") or {}, stop_on_error=True)
            exc = res.get("first_error") if res.get("status") == "error" else None
            done[name] = {"exitVal": res.get("exit_value"), "exception": exc}
            failed = failed or exc is not None
        if failed:
            raise NotebookRunError("one or more activities failed", done)
        return done

    def exit(self, value=None):
        raise NotebookExit(value)


class _Session:
    def stop(self):  # no-op locally; the session belongs to the MCP server
        return None


class _Runtime:
    def __init__(self, engine):
        self._engine = engine

    @property
    def context(self) -> dict:
        return self._engine.runtime_context()


class NotebookUtils(types.ModuleType):
    """Module-shaped object registered as both ``notebookutils`` and ``mssparkutils``."""

    _KNOWN = ("credentials", "variableLibrary", "fs", "notebook", "session", "runtime")

    def __init__(self, engine):
        super().__init__("notebookutils")
        self.credentials = _Credentials(engine)
        self.variableLibrary = _VariableLibrary(engine)
        self.fs = _FS(engine)
        self.notebook = _Notebook(engine)
        self.session = _Session()
        self.runtime = _Runtime(engine)
        self.__all__ = list(self._KNOWN)

    def __getattr__(self, name):
        if name.startswith("__"):
            raise AttributeError(name)
        raise NotImplementedError(
            f"notebookutils.{name} is not available locally (supported: {', '.join(self._KNOWN)})."
        )
