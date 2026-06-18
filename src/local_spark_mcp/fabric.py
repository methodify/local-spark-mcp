"""Fabric/OneLake glue: locate the token-provider JAR and build the Spark
configs that wire ABFS auth to the localhost token endpoint.

(Discovery + lazy table hydration land in Milestone B.2; this module currently
covers the OneLake auth wiring only.)
"""

from __future__ import annotations

import glob
from pathlib import Path

# Spark 3.5.0 bundles Hadoop 3.3.4; match hadoop-azure to it (3.3.6 collides with
# the bundled hadoop-common). Validated against live OneLake — works with
# GUID-based abfss paths. NOTE: name-based paths ("<lakehouse>.Lakehouse/...")
# make OneLake return HTTP 400, so always address by workspace/lakehouse GUID:
#   abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/{table}
HADOOP_AZURE_PACKAGE = "org.apache.hadoop:hadoop-azure:3.3.4"
PROVIDER_CLASS = "ch.fs.HttpTokenProvider"
_JAR_GLOB = "token-provider/target/scala-2.12/*.jar"


def repo_root() -> Path:
    # src/local_spark_mcp/fabric.py -> repo root
    return Path(__file__).resolve().parents[2]


def _packaged_jar_dir() -> Path:
    # jar shipped inside the installed package (wheel / uvx-from-GitHub)
    return Path(__file__).resolve().parent / "jars"


def default_jar_path() -> str | None:
    """Locate the HttpTokenProvider jar.

    Prefer a fresh in-repo `sbt package` build (dev/editable installs), then fall
    back to the jar bundled inside the installed package (so `uvx`/pip installs
    from GitHub work without sbt). Returns None if neither is present.
    """
    dev = sorted(glob.glob(str(repo_root() / _JAR_GLOB)))
    if dev:
        return dev[-1]
    packaged = sorted(glob.glob(str(_packaged_jar_dir() / "*.jar")))
    return packaged[-1] if packaged else None


def onelake_spark_configs(*, endpoint: str, secret: str, jar_path: str) -> dict[str, str]:
    """Spark configs that route OneLake ABFS auth through HttpTokenProvider.

    Hadoop keys are set via the ``spark.hadoop.`` prefix so Spark propagates them
    into the filesystem's Hadoop Configuration (where the provider reads them).
    """
    return {
        # Our provider must share a classloader with hadoop-azure (also added via
        # the package mechanism onto Spark's jar classloader), so it links its
        # CustomTokenProviderAdaptee superclass. spark.jars puts it there; ABFS
        # resolves the provider via the Hadoop Configuration's (Spark) classloader.
        "spark.jars": jar_path,
        "spark.hadoop.fs.azure.account.auth.type": "Custom",
        "spark.hadoop.fs.azure.account.oauth.provider.type": PROVIDER_CLASS,
        "spark.hadoop.fs.azure.tokenprovider.endpoint": endpoint,
        "spark.hadoop.fs.azure.tokenprovider.secret": secret,
    }
