"""A ``notebookutils`` / ``mssparkutils`` shim for running Fabric notebooks
locally. Covers the members the workspace corpus uses; anything else raises
NotImplementedError naming the member. Installed into ``sys.modules`` at engine
bootstrap so ``import notebookutils`` / ``import mssparkutils`` work in cells.
"""

from __future__ import annotations

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
        if not local.exists():
            raise FileNotFoundError(f"{path} (mirror: {local}) does not exist locally; sync_files may pull it")
        base = path.rstrip("/")
        return [
            FileInfo(name=child.name, path=f"{base}/{child.name}",
                     size=child.stat().st_size if child.is_file() else 0, isDir=child.is_dir())
            for child in sorted(local.iterdir())
        ]

    def exists(self, path: str) -> bool:
        if path.startswith("abfss://"):
            return self._engine.onelake_exists(path)
        local = self._engine.files_resolve(path)
        return bool(local and local.exists())

    def mount(self, source: str, mountPoint: str, extraConfigs=None):  # noqa: N803
        return self._engine.files_mount(source, mountPoint).get("linked", False) or True

    def unmount(self, mountPoint: str):  # noqa: N803
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
