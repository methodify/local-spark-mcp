"""Build the local, Delta-enabled Spark session.

This is the local-only session used for Milestone A (no Fabric/OneLake). Fabric
auth + OneLake configs are layered on in a later milestone. Configs that matter
for Fabric parity (Delta extension + Delta catalog) are set here so code proven
locally transfers.
"""

from __future__ import annotations

import os
import sys

from pathlib import Path

from .hadoop import resolve_hadoop_home
from .java import resolve_java_home


def preferred_python() -> str:
    """The environment's own interpreter, when one exists next to ``sys.prefix``.

    Under uv/uvx the *running* interpreter can be the BASE managed python (a
    venv ``python.exe`` trampoline re-execs it). That base interpreter has none
    of the venv's site-packages, so Spark's Python workers would rely on Spark's
    own PYTHONPATH to find pyspark — and die the moment a user-supplied
    PYTHONPATH (via [spark.env]) replaces it. The venv interpreter has pyspark on
    its own path, so it stays correct either way.
    """
    candidate = (
        Path(sys.prefix)
        / ("Scripts" if os.name == "nt" else "bin")
        / ("python.exe" if os.name == "nt" else "python")
    )
    return str(candidate) if candidate.exists() else sys.executable


def build_spark(
    *,
    driver_memory: str = "8g",
    app_name: str = "local-spark-mcp",
    extra_configs: dict[str, str] | None = None,
    java_home: str | None = None,
    log_level: str = "WARN",
    onelake: dict | None = None,
    env: dict[str, str] | None = None,
    hadoop_home: str | None = None,
):
    """Create a Delta-enabled SparkSession.

    JAVA_HOME is resolved and set in the process environment *before* the JVM is
    launched. Delta jars are pulled via ``configure_spark_with_delta_pip`` to
    match the installed ``delta-spark`` version (needs network on first run).

    If ``onelake`` (dict of endpoint/secret/jar_path) is given, OneLake ABFS auth
    is wired through HttpTokenProvider and the hadoop-azure package is added so
    the session can read ``abfss://...@onelake.dfs.fabric.microsoft.com`` paths.
    """
    os.environ["JAVA_HOME"] = resolve_java_home(java_home)

    # Make Spark hermetic to this interpreter: use the venv's bundled pyspark
    # (drop any ambient SPARK_HOME pointing at an external distro) and force
    # driver and workers onto the same Python to avoid PYTHON_VERSION_MISMATCH.
    os.environ.pop("SPARK_HOME", None)
    _py = preferred_python()
    os.environ["PYSPARK_PYTHON"] = _py
    os.environ["PYSPARK_DRIVER_PYTHON"] = _py

    # Windows: Spark won't even start without Hadoop's winutils (Shell.<clinit>
    # throws "HADOOP_HOME and hadoop.home.dir are unset"). Resolve one (bundled
    # by default) and put its bin/ on PATH so hadoop.dll is loadable too.
    resolved_hadoop = resolve_hadoop_home(hadoop_home)
    if resolved_hadoop:
        os.environ["HADOOP_HOME"] = resolved_hadoop
        hadoop_bin = str(Path(resolved_hadoop) / "bin")
        if hadoop_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

    # User env vars: apply to this (driver) process — so run_code imports/inits
    # see them, and the JVM inherits them — and to Spark Python workers via
    # spark.executorEnv.*, so distributed code can import + init the same libs.
    user_env = dict(env or {})
    for key, value in user_env.items():
        os.environ[key] = value

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    configs = dict(extra_configs or {})
    for key, value in user_env.items():
        configs.setdefault(f"spark.executorEnv.{key}", value)
    extra_packages: list[str] = []
    if onelake:
        from .fabric import HADOOP_AZURE_PACKAGE, onelake_spark_configs

        configs.update(onelake_spark_configs(**onelake))
        extra_packages.append(HADOOP_AZURE_PACKAGE)

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.sources.default", "delta")
    )
    for key, value in configs.items():
        builder = builder.config(key, value)

    spark = configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()
    spark.sparkContext.setLogLevel(log_level)
    return spark
