"""Build the local, Delta-enabled Spark session.

This is the local-only session used for Milestone A (no Fabric/OneLake). Fabric
auth + OneLake configs are layered on in a later milestone. Configs that matter
for Fabric parity (Delta extension + Delta catalog) are set here so code proven
locally transfers.
"""

from __future__ import annotations

import os
import sys

from .java import resolve_java_home


def build_local_spark(
    *,
    driver_memory: str = "8g",
    app_name: str = "local-spark-mcp",
    extra_configs: dict[str, str] | None = None,
    java_home: str | None = None,
    log_level: str = "WARN",
):
    """Create a local Delta-enabled SparkSession.

    JAVA_HOME is resolved and set in the process environment *before* the JVM is
    launched. Delta jars are pulled via ``configure_spark_with_delta_pip`` to
    match the installed ``delta-spark`` version (needs network on first run).
    """
    os.environ["JAVA_HOME"] = resolve_java_home(java_home)

    # Make Spark hermetic to this interpreter: use the venv's bundled pyspark
    # (drop any ambient SPARK_HOME pointing at an external distro) and force
    # driver and workers onto the same Python to avoid PYTHON_VERSION_MISMATCH.
    os.environ.pop("SPARK_HOME", None)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

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
    for key, value in (extra_configs or {}).items():
        builder = builder.config(key, value)

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel(log_level)
    return spark
