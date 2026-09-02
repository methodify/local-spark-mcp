"""Resolve a Spark-compatible ``JAVA_HOME``.

Spark 3.5 supports Java 8/11/17 but *not* 21+. The system ``java`` on this class
of host is often 21, so we prefer a known-good JDK: an explicit config value, a
vfox-managed Java 17/11, then the ambient ``JAVA_HOME``.

Cross-platform notes: the launcher is ``bin/java`` on POSIX and ``bin\\java.exe``
on Windows, and vfox lays its cache out with either a nested
``v-<ver>/<dist>/`` directory or the JDK directly under ``v-<ver>/``.
"""

from __future__ import annotations

import glob
import os
from pathlib import Path

# vfox cache layouts: nested (linux/windows: v-17.0.2+8/java-17.0.2+8/) and flat.
VFOX_JAVA_GLOBS = (
    "~/.version-fox/cache/java/v-1[178].*/*/",
    "~/.version-fox/cache/java/v-1[178].*/",
)


class JavaNotFoundError(Exception):
    """Raised when no Spark-compatible JDK can be located."""


def _is_jdk(path: str | Path) -> bool:
    """True if ``path`` looks like a JDK home (``bin/java`` or ``bin/java.exe``)."""
    bin_dir = Path(path) / "bin"
    return (bin_dir / "java").is_file() or (bin_dir / "java.exe").is_file()


def _vfox_candidates() -> list[str]:
    # Newest version first (v-17.* sorts after v-11.*, reverse puts 17 ahead).
    matches: list[str] = []
    for pattern in VFOX_JAVA_GLOBS:
        matches.extend(glob.glob(os.path.expanduser(pattern)))
    ordered = sorted(set(matches), reverse=True)
    return [m.rstrip("\\/") for m in ordered if _is_jdk(m)]


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
