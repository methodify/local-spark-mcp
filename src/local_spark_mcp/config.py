"""Configuration schema and loader for local-spark-mcp.

The source of truth is a ``local-spark.toml`` file in the project working
directory. Environment variables (``LOCAL_SPARK_*``) override individual
settings on top of the file. Nothing here is secret — auth is ambient via the
Azure CLI (``az login``) / ``DefaultAzureCredential``.
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

CONFIG_FILENAME = "local-spark.toml"
ENV_PREFIX = "LOCAL_SPARK_"
WRITE_MODES = ("sandbox", "readonly", "writethrough")


class ConfigError(Exception):
    """Raised when configuration is missing or invalid."""


@dataclass
class WorkspaceConfig:
    """Which Fabric workspace to target. Exactly one of name/id is required."""

    name: str | None = None
    id: str | None = None


@dataclass
class LakehouseConfig:
    """Lakehouse selection. All lakehouses are included by default; ``exclude``
    trims the noise (cheap, since table hydration is lazy)."""

    exclude: list[str] = field(default_factory=list)
    # Unqualified table names resolve here (USE <default> at session init), the
    # way a Fabric notebook's default lakehouse works.
    default: str | None = None


@dataclass
class SparkConfig:
    """Spark session tuning."""

    driver_memory: str = "8g"
    extra_configs: dict[str, str] = field(default_factory=dict)
    # Environment variables applied to BOTH the driver process and Spark Python
    # workers (via spark.executorEnv.*). Use for native-lib data dirs
    # (JAGEOCODER_DB2_DIR, LIBPOSTAL_DATA_DIR, …) and PYTHONPATH so distributed
    # (mapPartitions/UDF) code can import + init the same libs as the driver.
    env: dict[str, str] = field(default_factory=dict)


@dataclass
class RuntimeConfig:
    """Server/worker runtime behavior."""

    default_sql_limit: int = 100
    java_home: str | None = None
    token_jar_path: str | None = None  # override for the HttpTokenProvider jar
    hadoop_home: str | None = None  # Windows winutils dir (default: bundled)
    warm_on_start: bool = False  # eagerly start Spark at launch (default: lazy)
    # Write policy for lakehouse tables. sandbox: writes land in a local shadow
    # (shallow clones + new tables), OneLake untouched. readonly: like sandbox
    # but creating tables and DeltaTable.forName writes are refused.
    # writethrough: writes go to OneLake. See OneLakeCatalog.
    write_mode: str = "sandbox"
    # Session-scoped shadows are deleted when the session ends; set true to keep
    # them under <state_root>/lakehouses/<workspace-id>/shadow across sessions.
    persist_shadow: bool = False
    state_root: str = "~/.local-spark"  # per-session warehouse, shadows, mirrors


@dataclass
class NotebooksConfig:
    """Where the Fabric Git export lives; notebook.run() resolves names against
    the .platform displayName under this root."""

    root: str | None = None


@dataclass
class FilesConfig:
    """Files mirror: which ``Files/`` subtrees to pull for the default lakehouse
    (e.g. ["lib/", "metadata/"]) and where mirrors live (default
    <state_root>/lakehouses/<workspace-id>/<lakehouse-id>/Files)."""

    sync: list[str] = field(default_factory=list)
    mirror_root: str | None = None


@dataclass
class Config:
    workspace: WorkspaceConfig = field(default_factory=WorkspaceConfig)
    notebooks: NotebooksConfig = field(default_factory=NotebooksConfig)
    files: FilesConfig = field(default_factory=FilesConfig)
    lakehouses: LakehouseConfig = field(default_factory=LakehouseConfig)
    spark: SparkConfig = field(default_factory=SparkConfig)
    runtime: RuntimeConfig = field(default_factory=RuntimeConfig)
    source_path: Path | None = None

    def validate(self) -> None:
        ws = self.workspace
        # name and id are mutually exclusive. A workspace is *optional* here —
        # local-only operation needs none. The Fabric/discovery layer calls
        # require_workspace() when it actually needs to connect.
        if ws.name and ws.id:
            raise ConfigError(
                "Set only one of workspace.name or workspace.id, not both."
            )
        if self.runtime.default_sql_limit <= 0:
            raise ConfigError("runtime.default_sql_limit must be a positive integer.")
        if self.runtime.write_mode not in WRITE_MODES:
            raise ConfigError(
                f"runtime.write_mode must be one of {', '.join(WRITE_MODES)} "
                f"(got {self.runtime.write_mode!r})."
            )

    def require_workspace(self) -> WorkspaceConfig:
        """Return the workspace, raising if none is configured (used by the
        Fabric discovery layer)."""
        if not (self.workspace.name or self.workspace.id):
            raise ConfigError(
                "A target Fabric workspace is required for this operation: set "
                "workspace.name or workspace.id in local-spark.toml "
                "(or LOCAL_SPARK_WORKSPACE_NAME / LOCAL_SPARK_WORKSPACE_ID)."
            )
        return self.workspace


def find_config_file(start: Path | None = None) -> Path | None:
    """Walk up from ``start`` (default cwd) looking for ``local-spark.toml``."""
    start = (start or Path.cwd()).resolve()
    for directory in (start, *start.parents):
        candidate = directory / CONFIG_FILENAME
        if candidate.is_file():
            return candidate
    return None


_TRUE = {"1", "true", "yes", "on"}
_FALSE = {"0", "false", "no", "off", ""}


def _parse_bool(value: str, context: str) -> bool:
    v = value.strip().lower()
    if v in _TRUE:
        return True
    if v in _FALSE:
        return False
    raise ConfigError(f"{context} must be a boolean (got {value!r}).")


def _require_str(table: dict, key: str, context: str) -> str | None:
    value = table.get(key)
    if value is not None and not isinstance(value, str):
        raise ConfigError(f"{context}.{key} must be a string.")
    return value


def _parse_file(path: Path) -> Config:
    try:
        with path.open("rb") as fh:
            data = tomllib.load(fh)
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"Failed to parse {path}: {exc}") from exc

    ws = data.get("workspace", {})
    nbs = data.get("notebooks", {})
    files = data.get("files", {})
    sync = files.get("sync", [])
    if not isinstance(sync, list) or not all(isinstance(x, str) for x in sync):
        raise ConfigError("files.sync must be a list of strings.")
    lh = data.get("lakehouses", {})
    spark = data.get("spark", {})
    runtime = data.get("runtime", {})

    exclude = lh.get("exclude", [])
    if not isinstance(exclude, list) or not all(isinstance(x, str) for x in exclude):
        raise ConfigError("lakehouses.exclude must be a list of strings.")

    extra = spark.get("extra_configs", {})
    if not isinstance(extra, dict) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in extra.items()
    ):
        raise ConfigError("spark.extra_configs must be a table of string->string.")

    spark_env = spark.get("env", {})
    if not isinstance(spark_env, dict) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in spark_env.items()
    ):
        raise ConfigError("spark.env must be a table of string->string.")

    warm_on_start = runtime.get("warm_on_start", False)
    if not isinstance(warm_on_start, bool):
        raise ConfigError("runtime.warm_on_start must be a boolean (true/false).")
    persist_shadow = runtime.get("persist_shadow", False)
    if not isinstance(persist_shadow, bool):
        raise ConfigError("runtime.persist_shadow must be a boolean (true/false).")

    config = Config(
        workspace=WorkspaceConfig(
            name=_require_str(ws, "name", "workspace"),
            id=_require_str(ws, "id", "workspace"),
        ),
        notebooks=NotebooksConfig(root=_require_str(nbs, "root", "notebooks")),
        files=FilesConfig(sync=list(sync), mirror_root=_require_str(files, "mirror_root", "files")),
        lakehouses=LakehouseConfig(
            exclude=list(exclude),
            default=_require_str(lh, "default", "lakehouses"),
        ),
        spark=SparkConfig(
            driver_memory=_require_str(spark, "driver_memory", "spark") or "8g",
            extra_configs=dict(extra),
            env=dict(spark_env),
        ),
        runtime=RuntimeConfig(
            default_sql_limit=int(runtime.get("default_sql_limit", 100)),
            java_home=_require_str(runtime, "java_home", "runtime"),
            token_jar_path=_require_str(runtime, "token_jar_path", "runtime"),
            hadoop_home=_require_str(runtime, "hadoop_home", "runtime"),
            warm_on_start=warm_on_start,
            write_mode=(_require_str(runtime, "write_mode", "runtime") or "sandbox").lower(),
            persist_shadow=persist_shadow,
            state_root=_require_str(runtime, "state_root", "runtime") or "~/.local-spark",
        ),
        source_path=path,
    )
    return config


def _apply_env_overrides(config: Config) -> None:
    """Apply LOCAL_SPARK_* environment overrides in place."""
    env = os.environ

    if (name := env.get(f"{ENV_PREFIX}WORKSPACE_NAME")) is not None:
        config.workspace.name = name
        config.workspace.id = None  # name and id are mutually exclusive
    if (wsid := env.get(f"{ENV_PREFIX}WORKSPACE_ID")) is not None:
        config.workspace.id = wsid
        config.workspace.name = None

    if (exclude := env.get(f"{ENV_PREFIX}LAKEHOUSE_EXCLUDE")) is not None:
        config.lakehouses.exclude = [s.strip() for s in exclude.split(",") if s.strip()]

    if (mem := env.get(f"{ENV_PREFIX}DRIVER_MEMORY")) is not None:
        config.spark.driver_memory = mem

    if (limit := env.get(f"{ENV_PREFIX}SQL_LIMIT")) is not None:
        try:
            config.runtime.default_sql_limit = int(limit)
        except ValueError as exc:
            raise ConfigError(f"{ENV_PREFIX}SQL_LIMIT must be an integer.") from exc

    if (java_home := env.get(f"{ENV_PREFIX}JAVA_HOME")) is not None:
        config.runtime.java_home = java_home

    if (jar := env.get(f"{ENV_PREFIX}TOKEN_JAR_PATH")) is not None:
        config.runtime.token_jar_path = jar

    if (hadoop := env.get(f"{ENV_PREFIX}HADOOP_HOME")) is not None:
        config.runtime.hadoop_home = hadoop

    if (warm := env.get(f"{ENV_PREFIX}WARM_ON_START")) is not None:
        config.runtime.warm_on_start = _parse_bool(warm, f"{ENV_PREFIX}WARM_ON_START")

    if (default_lh := env.get(f"{ENV_PREFIX}DEFAULT_LAKEHOUSE")) is not None:
        config.lakehouses.default = default_lh or None

    if (mode := env.get(f"{ENV_PREFIX}WRITE_MODE")) is not None:
        config.runtime.write_mode = mode.strip().lower()

    if (persist := env.get(f"{ENV_PREFIX}PERSIST_SHADOW")) is not None:
        config.runtime.persist_shadow = _parse_bool(persist, f"{ENV_PREFIX}PERSIST_SHADOW")

    if (root := env.get(f"{ENV_PREFIX}STATE_ROOT")) is not None:
        config.runtime.state_root = root

    if (nb_root := env.get(f"{ENV_PREFIX}NOTEBOOKS_ROOT")) is not None:
        config.notebooks.root = nb_root or None

    if (sync := env.get(f"{ENV_PREFIX}FILES_SYNC")) is not None:
        config.files.sync = [s.strip() for s in sync.split(",") if s.strip()]

    if (mirror := env.get(f"{ENV_PREFIX}MIRROR_ROOT")) is not None:
        config.files.mirror_root = mirror or None


def load_config(path: Path | None = None, *, search_from: Path | None = None) -> Config:
    """Load configuration.

    Resolution order:
      1. If ``path`` is given, parse that file (must exist).
      2. Else search up from ``search_from`` (or cwd) for ``local-spark.toml``.
      3. Else start from defaults (env vars must then supply the workspace).
    Environment overrides are applied last, then the result is validated.
    """
    if path is not None:
        path = Path(path)
        if not path.is_file():
            raise ConfigError(f"Config file not found: {path}")
        config = _parse_file(path)
    else:
        found = find_config_file(search_from)
        config = _parse_file(found) if found else Config()

    _apply_env_overrides(config)
    config.validate()
    return config
