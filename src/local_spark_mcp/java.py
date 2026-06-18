"""Resolve a Spark-compatible ``JAVA_HOME``.

Spark 3.5 supports Java 8/11/17 but *not* 21+. The system ``java`` on this class
of host is often 21, so we prefer a known-good JDK: an explicit config value, a
vfox-managed Java 17, then the ambient ``JAVA_HOME``.
"""

from __future__ import annotations

import glob
import os
from pathlib import Path

VFOX_JAVA_GLOB = "~/.version-fox/cache/java/v-1[178].*/*/"


class JavaNotFoundError(Exception):
    """Raised when no Spark-compatible JDK can be located."""


def _is_jdk(path: str | Path) -> bool:
    return (Path(path) / "bin" / "java").exists()


def _vfox_candidates() -> list[str]:
    # Newest version first (v-17.* sorts after v-11.*, reverse puts 17 ahead).
    matches = sorted(glob.glob(os.path.expanduser(VFOX_JAVA_GLOB)), reverse=True)
    return [m.rstrip("/") for m in matches if _is_jdk(m)]


def resolve_java_home(explicit: str | None = None) -> str:
    """Return a JAVA_HOME suitable for Spark 3.5.

    Resolution order: explicit (from config) → vfox-managed Java 17/11 →
    ambient ``JAVA_HOME``. Raises if none is usable.
    """
    if explicit:
        if not _is_jdk(explicit):
            raise JavaNotFoundError(f"Configured java_home is not a JDK: {explicit}")
        return str(explicit)

    candidates = _vfox_candidates()
    if candidates:
        return candidates[0]

    env = os.environ.get("JAVA_HOME")
    if env and _is_jdk(env):
        return env

    raise JavaNotFoundError(
        "No Spark-compatible JDK found. Install Java 17 (e.g. `vfox install "
        "java@17.0.16-bsg`) or set runtime.java_home in local-spark.toml "
        "(LOCAL_SPARK_JAVA_HOME)."
    )
