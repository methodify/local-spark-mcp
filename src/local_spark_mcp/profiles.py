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
    # Session confs that make the local session behave like the Fabric runtime's
    # production session where Spark's own default differs. [spark.extra_configs]
    # still wins over these.
    session_confs: dict = None  # type: ignore[assignment]

    @property
    def extra(self) -> str:
        return f"local-spark-mcp[{self.name}]"

    def describe(self) -> str:
        return (f"{self.name} = Fabric Runtime {self.fabric_runtime}: pyspark {self.pyspark}, "
                f"delta-spark {self.delta}, Python {self.python[0]}.{self.python[1]}, "
                f"Java {'/'.join(map(str, self.java_majors))}, Scala {self.scala} jar")


# Session confs where a Fabric production session differs from Spark's own
# default. Taken from the SparkListenerEnvironmentUpdate events of real Runtime
# 1.3 and 2.0 sessions (2026-09-14); Fabric-only classes (Gluten cost
# evaluator, cloud committers, RocksDB state store, native Parquet writer,
# V-Order) are deliberately not copied. [spark.extra_configs] overrides these.
_FABRIC_COMMON_CONFS = {
    "spark.sql.session.timeZone": "UTC",  # Fabric JVMs run with user.timezone=UTC
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.autoBroadcastJoinThreshold": "26214400",  # 25 MB (Spark: 10 MB)
    "spark.sql.cbo.enabled": "true",
    "spark.sql.cbo.joinReorder.enabled": "true",
    "spark.sql.execution.arrow.pyspark.enabled": "true",
    "spark.sql.execution.arrow.pyspark.fallback.enabled": "true",
    "spark.sql.legacy.createHiveTableByDefault": "false",
    "spark.sql.legacy.replaceDatabricksSparkAvro.enabled": "false",
    "spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly": "false",
    "spark.sql.parquet.outputTimestampType": "TIMESTAMP_MICROS",
    "spark.sql.statistics.fallBackToHdfs": "true",
    "spark.databricks.delta.vacuum.parallelDelete.enabled": "true",
}

PROFILES: dict[str, Profile] = {
    "fabric-1.3": Profile(
        name="fabric-1.3", fabric_runtime="1.3", pyspark="3.5.5", delta="3.2.0", python=(3, 11),
        java_majors=(8, 11, 17), java_preferred=(17, 11, 8), scala="2.12", hadoop_azure="3.3.4", spark_major=3,
        # Not spark.sql.sources.default=delta here: on Spark 3.5 / Delta 3.2 it
        # breaks df.write.mode("overwrite").saveAsTable(<new table>) ("does not
        # support truncate in batch mode"), even with format("delta"). Fabric's
        # 3.5 build evidently carries a fix; upstream 3.5.9 does not. Residual:
        # untyped CREATE TABLE / df.write.save() are parquet locally under 1.3.
        session_confs={**_FABRIC_COMMON_CONFS, "spark.databricks.delta.optimizeWrite.enabled": "true"},
    ),
    "fabric-2.0": Profile(
        name="fabric-2.0", fabric_runtime="2.0", pyspark="4.1.1", delta="4.2.0", python=(3, 13),
        java_majors=(17, 21), java_preferred=(21, 17), scala="2.13", hadoop_azure="3.4.1", spark_major=4,
        session_confs={
            **_FABRIC_COMMON_CONFS,
            # Spark 4 flipped ANSI on; a Runtime 2.0 production session runs it off
            # (ADO #286): notebooks that cast '' to bigint fail locally otherwise.
            "spark.sql.ansi.enabled": "false",
            "spark.sql.unionOutputPartitioning": "false",
            "spark.sql.sources.default": "delta",  # untyped CREATE TABLE / df.write.save mean Delta
            # Runtime 2.0 creates tables with deletion vectors by default.
            "spark.databricks.delta.properties.defaults.enableDeletionVectors": "true",
        },
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
# 3.5.9, 4.0.3, 4.1.2. Neither profile can use those: Delta 3.2 breaks on Spark
# 3.5.6+ (V2 overwrite needs TRUNCATE, delta-io/delta#4671) and delta-spark
# 4.2.0 pins pyspark <= 4.1.1. On Windows both profiles run on Python 3.11.
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
            f"{python[0]}.{python[1]} (SPARK-53759; fixed in {'.'.join(map(str, fix))}, which this "
            f"profile's Delta cannot run on). Run {p.name} with Python 3.11 on Windows: "
            "`uvx --python 3.11 ...`"
        )
        return p, warnings, errors
    if versions.get("pyspark") != p.pyspark:
        warnings.append(f"pyspark {versions['pyspark']} installed; {p.name} pins {p.pyspark}")
    if versions.get("delta-spark") != p.delta:
        warnings.append(f"delta-spark {versions.get('delta-spark')} installed; {p.name} pins {p.delta}")
    if tuple(python) != p.python:
        if windows and fix and spark_v < fix:
            warnings.append(
                f"Python {python[0]}.{python[1]} here; Fabric Runtime {p.fabric_runtime} uses "
                f"{p.python[0]}.{p.python[1]}, but on Windows this profile needs 3.11 until pyspark "
                f"{'.'.join(map(str, fix))} is allowed (SPARK-53759)"
            )
        else:
            warnings.append(
                f"Python {python[0]}.{python[1]} here; Fabric Runtime {p.fabric_runtime} uses "
                f"{p.python[0]}.{p.python[1]} (works, but parity prefers `uvx --python {p.python[0]}.{p.python[1]}`)"
            )
    return p, warnings, errors


def current_profile() -> Profile:
    """Best-effort profile for code paths that just need a version set."""
    return detect_profile() or PROFILES[DEFAULT_PROFILE]
