"""Resolve a Spark-compatible ``JAVA_HOME``.

Spark 3.5 supports Java 8/11/17 but *not* 21+. The system ``java`` on this class
of host is often 21, so we prefer a known-good JDK: an explicit config value, a
vfox-managed Java 17/11, the ambient ``JAVA_HOME``, then ``java`` on PATH.

Every candidate is resolved with ``realpath`` before the check, so junctions and
symlinks (vfox ``current``, ``/usr/lib/jvm/default``) are accepted when their
target is a JDK, and a path to the launcher itself (``.../bin/java.exe``) is
normalized to its home. The launcher is ``bin/java`` on POSIX and ``bin\\java.exe``
on Windows; vfox lays its cache out with either a nested ``v-<ver>/<dist>/``
directory or the JDK directly under ``v-<ver>/``.
"""

from __future__ import annotations

import glob
import os
import re
import shutil
from pathlib import Path

# vfox cache layouts: nested (linux/windows: v-17.0.2+8/java-17.0.2+8/) and flat.
VFOX_JAVA_GLOBS = (
    "~/.version-fox/cache/java/v-1[178].*/*/",
    "~/.version-fox/cache/java/v-1[178].*/",
    "~/.version-fox/cache/java/v-21.*/*/",
    "~/.version-fox/cache/java/v-21.*/",
)
SUPPORTED_MAJORS = (8, 11, 17)  # fabric-1.3 (Spark 3.5); fabric-2.0 passes (17, 21)


class JavaNotFoundError(Exception):
    """Raised when no Spark-compatible JDK can be located."""


def _launcher(home: Path) -> Path | None:
    for name in ("java", "java.exe"):
        if (home / "bin" / name).is_file():
            return home / "bin" / name
    return None


def normalize_home(path: str | Path) -> Path:
    """realpath, and ``.../bin/java[.exe]`` -> the home directory two levels up."""
    p = Path(os.path.realpath(os.path.expanduser(str(path))))
    if p.name.lower() in ("java", "java.exe") and p.parent.name == "bin":
        return p.parent.parent
    return p


def java_major(home: Path) -> int | None:
    """Major version from the JDK's ``release`` file (``JAVA_VERSION="17.0.16"``);
    None if there is no such file."""
    try:
        text = (home / "release").read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    m = re.search(r'JAVA_VERSION="?(\d+)(?:\.(\d+))?', text)
    if not m:
        return None
    major = int(m.group(1))
    return int(m.group(2)) if major == 1 and m.group(2) else major  # "1.8.0" -> 8


def check_jdk(path: str | Path, majors: tuple[int, ...] = SUPPORTED_MAJORS, spark: str = "Spark 3.5") -> tuple[Path | None, str]:
    """(home, verdict). home is None when the candidate is rejected; verdict
    explains either way."""
    home = normalize_home(path)
    if not home.is_dir():
        return None, "does not exist"
    if _launcher(home) is None:
        return None, "not a JDK (no bin/java)"
    major = java_major(home)
    if major is not None and major not in majors:
        return None, f"Java {major}; {spark} needs {'/'.join(map(str, majors))}"
    return home, f"ok (Java {major})" if major else "ok"


def _is_jdk(path: str | Path) -> bool:
    return check_jdk(path)[0] is not None


def _vfox_candidates() -> list[str]:
    # Newest version first (v-17.* sorts after v-11.*, reverse puts 17 ahead).
    matches: list[str] = []
    for pattern in VFOX_JAVA_GLOBS:
        matches.extend(glob.glob(os.path.expanduser(pattern)))
    return [m.rstrip("\\/") for m in sorted(set(matches), reverse=True)]


def _path_candidate() -> str | None:
    exe = shutil.which("java")
    return os.path.realpath(exe) if exe else None


def resolve_java_home(
    explicit: str | None = None,
    *,
    origin: str | None = None,
    majors: tuple[int, ...] | None = None,
    prefer: tuple[int, ...] | None = None,
    spark: str | None = None,
) -> str:
    """Return a JAVA_HOME suitable for the active profile's Spark.

    Resolution order: explicit (from config) → vfox-managed JDKs (in the
    profile's preferred major order) → ambient ``JAVA_HOME`` → ``java`` on PATH.
    Raises with every candidate tried and where it came from if none is usable.
    ``origin`` names where the explicit value came from, for the error message.
    ``majors``/``prefer``/``spark`` default to the installed profile's.
    """
    if majors is None or prefer is None or spark is None:
        from .profiles import current_profile

        prof = current_profile()
        majors = majors or prof.java_majors
        prefer = prefer or prof.java_preferred
        spark = spark or f"Spark {prof.pyspark.rsplit('.', 1)[0]}"
    if explicit:
        home, verdict = check_jdk(explicit, majors, spark)
        if home is None:
            src = f" (from {origin})" if origin else ""
            raise JavaNotFoundError(f"Configured java_home{src} is not a usable JDK: {explicit} -> {verdict}")
        return str(home)

    tried: list[str] = []
    for source, candidate in _candidates(prefer):
        if candidate is None:
            tried.append(f"  {source}: no match")
            continue
        home, verdict = check_jdk(candidate, majors, spark)
        if home is not None:
            return str(home)
        tried.append(f"  {source}: {candidate} -> {verdict}")

    want = prefer[0] if prefer else majors[-1]
    raise JavaNotFoundError(
        f"No {spark}-compatible JDK found (needs Java {'/'.join(map(str, majors))}). Install one "
        f"(e.g. `vfox install java@{want}`) or set runtime.java_home in local-spark.toml "
        "(LOCAL_SPARK_JAVA_HOME). Tried:\n" + "\n".join(tried)
    )


def _candidates(prefer: tuple[int, ...] = (17, 11, 8)) -> list[tuple[str, str | None]]:
    out: list[tuple[str, str | None]] = []
    vfox = _vfox_candidates()
    if vfox:
        rank = {m: i for i, m in enumerate(prefer)}
        vfox = sorted(vfox, key=lambda c: rank.get(java_major(normalize_home(c)) or -1, len(rank)))
        out += [("vfox", c) for c in vfox]
    else:
        out.append(("vfox (~/.version-fox/cache/java)", None))
    out.append(("JAVA_HOME", os.environ.get("JAVA_HOME") or None))
    out.append(("java on PATH", _path_candidate()))
    return out
