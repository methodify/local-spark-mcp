"""Locate a Hadoop home on Windows (``bin/winutils.exe`` + ``hadoop.dll``).

Spark 3.5 cannot start on Windows without winutils: Hadoop's ``Shell`` class
initializes during ``SparkSubmit.prepareSubmitEnvironment`` and throws
``HADOOP_HOME and hadoop.home.dir are unset``. We bundle a matching build (see
``winutils/PROVENANCE.md``) so Windows works out of the box, while still
preferring an explicit config value or the user's own Hadoop install.

On POSIX this is a no-op — winutils is a Windows-only shim.
"""

from __future__ import annotations

import os
from pathlib import Path


class HadoopNotFoundError(Exception):
    """Raised when Windows needs winutils and none can be located."""


def is_windows() -> bool:
    return os.name == "nt"


def _is_hadoop_home(path: str | Path) -> bool:
    return (Path(path) / "bin" / "winutils.exe").is_file()


def bundled_hadoop_home() -> Path:
    """The winutils copy shipped inside this package."""
    return Path(__file__).resolve().parent / "winutils"


def resolve_hadoop_home(explicit: str | None = None) -> str | None:
    """Return a HADOOP_HOME for Windows, or None where it isn't needed.

    Order: explicit config → ambient ``HADOOP_HOME`` → the bundled winutils.
    """
    if not is_windows():
        return None

    if explicit:
        if not _is_hadoop_home(explicit):
            raise HadoopNotFoundError(
                f"Configured runtime.hadoop_home has no bin\\winutils.exe: {explicit}"
            )
        return str(explicit)

    env = os.environ.get("HADOOP_HOME")
    if env and _is_hadoop_home(env):
        return env

    bundled = bundled_hadoop_home()
    if _is_hadoop_home(bundled):
        return str(bundled)

    raise HadoopNotFoundError(
        "Spark on Windows requires Hadoop's winutils.exe, and the copy bundled "
        "with local-spark-mcp is missing. Install winutils (e.g. from "
        "https://github.com/cdarlint/winutils, hadoop-3.3.x/bin) into a folder "
        "containing bin\\winutils.exe, then set runtime.hadoop_home in "
        "local-spark.toml (or LOCAL_SPARK_HADOOP_HOME / HADOOP_HOME)."
    )
