"""Runtime profiles: version sets that match a Microsoft Fabric Spark runtime.

A profile is chosen at install time through an extra
(``local-spark-mcp[fabric-1.3]`` or ``local-spark-mcp[fabric-2.0]``): that is
what pins pyspark and delta-spark. At startup the server detects which stack is
installed, checks it against the declared profile (``[runtime] profile`` /
``LOCAL_SPARK_PROFILE``, optional), and derives everything version-specific
from it: which bundled jar (one Scala line per Spark major), which hadoop-azure,
which Java majors, which Python the runtime uses.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass


@dataclass(frozen=True)
class Profile:
    name: str
    fabric_runtime: str
    pyspark: str
    delta: str
    python: tuple[int, int]
    java_majors: tuple[int, ...]  # accepted
    java_preferred: tuple[int, ...]  # search order among candidates
    scala: str  # binary version of the bundled jar
    hadoop_azure: str
    spark_major: int

    @property
    def extra(self) -> str:
        return f"local-spark-mcp[{self.name}]"

    def describe(self) -> str:
        return (f"{self.name} = Fabric Runtime {self.fabric_runtime}: pyspark {self.pyspark}, "
                f"delta-spark {self.delta}, Python {self.python[0]}.{self.python[1]}, "
                f"Java {'/'.join(map(str, self.java_majors))}, Scala {self.scala} jar")


PROFILES: dict[str, Profile] = {
    "fabric-1.3": Profile(
        name="fabric-1.3", fabric_runtime="1.3", pyspark="3.5.9", delta="3.2.0", python=(3, 11),
        java_majors=(8, 11, 17), java_preferred=(17, 11, 8), scala="2.12", hadoop_azure="3.3.4", spark_major=3,
    ),
    "fabric-2.0": Profile(
        name="fabric-2.0", fabric_runtime="2.0", pyspark="4.1.1", delta="4.2.0", python=(3, 13),
        java_majors=(17, 21), java_preferred=(21, 17), scala="2.13", hadoop_azure="3.4.1", spark_major=4,
    ),
}
DEFAULT_PROFILE = "fabric-1.3"
INSTALL_HINT = "install with `local-spark-mcp[fabric-1.3]` (Spark 3.5 / Delta 3.2) or `local-spark-mcp[fabric-2.0]` (Spark 4.1 / Delta 4.2)"


def installed_versions() -> dict[str, str | None]:
    from importlib.metadata import PackageNotFoundError, version

    out: dict[str, str | None] = {}
    for dist in ("pyspark", "delta-spark"):
        try:
            out[dist] = version(dist)
        except PackageNotFoundError:
            out[dist] = None
    return out


def detect_profile(versions: dict | None = None) -> Profile | None:
    """The profile matching the installed pyspark major, or None when no
    Spark stack is installed."""
    versions = versions or installed_versions()
    spark = versions.get("pyspark")
    if not spark:
        return None
    major = int(spark.split(".")[0])
    for p in PROFILES.values():
        if p.spark_major == major:
            return p
    return None


def _version_tuple(v: str | None) -> tuple[int, ...]:
    try:
        return tuple(int(x) for x in (v or "0").split(".")[:3])
    except ValueError:
        return (0,)


# SPARK-53759: PySpark's Python workers crash on Windows under Python 3.12+
# (WinError 10038 / "Python worker exited unexpectedly"). Fixed in pyspark
# 3.5.9, 4.0.3, 4.1.2. delta-spark 4.2.0 pins pyspark <= 4.1.1, so on Windows
# the fabric-2.0 profile needs Python 3.11 until Delta allows a newer pyspark.
_WINDOWS_WORKER_FIX = {3: (3, 5, 9), 4: (4, 1, 2)}


def check_profile(declared: str | None, *, versions: dict | None = None, python=None, windows: bool | None = None) -> tuple[Profile, list[str], list[str]]:
    """(profile, warnings, errors). Errors mean the installed stack cannot serve
    the declared profile; warnings are parity drift (patch versions, Python)."""
    versions = versions or installed_versions()
    python = tuple(python or sys.version_info[:2])
    windows = os.name == "nt" if windows is None else windows
    warnings: list[str] = []
    errors: list[str] = []
    if declared is not None and declared not in PROFILES:
        errors.append(f"unknown runtime profile {declared!r}; known: {', '.join(PROFILES)}")
        declared = None
    detected = detect_profile(versions)
    if detected is None:
        if not versions.get("pyspark"):
            errors.append(f"no Spark stack is installed (pyspark missing): {INSTALL_HINT}")
        else:
            errors.append(f"pyspark {versions['pyspark']} matches no runtime profile: {INSTALL_HINT}")
        return PROFILES[declared or DEFAULT_PROFILE], warnings, errors
    if declared is not None and declared != detected.name:
        errors.append(
            f"runtime profile {declared!r} is declared but the installed stack is pyspark "
            f"{versions['pyspark']} / delta-spark {versions.get('delta-spark')} ({detected.name}); "
            f"install {PROFILES[declared].extra} or declare {detected.name!r}"
        )
        return detected, warnings, errors
    p = detected
    spark_v = _version_tuple(versions.get("pyspark"))
    fix = _WINDOWS_WORKER_FIX.get(p.spark_major)
    if windows and python >= (3, 12) and fix and spark_v < fix:
        errors.append(
            f"pyspark {versions['pyspark']} Python workers crash on Windows under Python "
            f"{python[0]}.{python[1]} (SPARK-53759; fixed in {'.'.join(map(str, fix))}, which the "
            f"{p.name} Delta pin does not allow yet). Run this profile with Python 3.11 on Windows: "
            "`uvx --python 3.11 ...`"
        )
        return p, warnings, errors
    if versions.get("pyspark") != p.pyspark:
        warnings.append(f"pyspark {versions['pyspark']} installed; {p.name} pins {p.pyspark}")
    if versions.get("delta-spark") != p.delta:
        warnings.append(f"delta-spark {versions.get('delta-spark')} installed; {p.name} pins {p.delta}")
    if tuple(python) != p.python:
        warnings.append(
            f"Python {python[0]}.{python[1]} here; Fabric Runtime {p.fabric_runtime} uses "
            f"{p.python[0]}.{p.python[1]} (works, but parity prefers `uvx --python {p.python[0]}.{p.python[1]}`)"
        )
    return p, warnings, errors


def current_profile() -> Profile:
    """Best-effort profile for code paths that just need a version set."""
    return detect_profile() or PROFILES[DEFAULT_PROFILE]
