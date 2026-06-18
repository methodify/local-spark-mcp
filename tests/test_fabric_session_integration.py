"""Integration test for the Fabric-enabled Spark session WITHOUT Azure: build a
real session with OneLake configs, then load + invoke the HttpTokenProvider the
exact way Hadoop's ABFS driver does (via the Hadoop Configuration's classloader),
fetching a token from the local endpoint. Validates the whole auth-wiring chain
up to (but not including) a live abfss:// network read.

    LOCAL_SPARK_RUN_INTEGRATION=1 .venv/bin/python -m pytest tests/test_fabric_session_integration.py -v
"""

import base64
import json
import os
import time
from collections import namedtuple

import pytest

from local_spark_mcp.fabric import default_jar_path
from local_spark_mcp.token_server import TokenServer

pytestmark = pytest.mark.skipif(
    os.environ.get("LOCAL_SPARK_RUN_INTEGRATION") != "1",
    reason="set LOCAL_SPARK_RUN_INTEGRATION=1 to run (starts a real Spark session)",
)

AccessToken = namedtuple("AccessToken", "token expires_on")


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


def test_onelake_provider_loads_and_fetches_through_hadoop():
    if not default_jar_path():
        pytest.skip("token provider jar not built (cd token-provider && sbt package)")

    from local_spark_mcp.engine import SparkEngine

    exp = int(time.time()) + 3600
    jwt = ".".join(
        [_b64url(b'{"alg":"none"}'), _b64url(json.dumps({"exp": exp}).encode()), "sig"]
    )

    class Cred:
        def get_token(self, *a, **k):
            return AccessToken(jwt, exp)

    srv = TokenServer(credential=Cred())
    srv.start()
    engine = None
    try:
        engine = SparkEngine(
            driver_memory="2g",
            onelake={"endpoint": srv.url, "secret": srv.secret, "jar_path": default_jar_path()},
        )
        spark = engine.spark
        # the provider jar is registered on the session
        jars = spark._jsc.sc().listJars().mkString("\n")
        assert "httptokenprovider" in jars.lower()

        # Resolve + instantiate the provider exactly as Hadoop ABFS would.
        jvm = spark._jvm
        loader = jvm.org.apache.spark.util.Utils.getContextOrSparkClassLoader()
        conf = spark._jsc.hadoopConfiguration()
        conf.setClassLoader(loader)
        base = jvm.java.lang.Class.forName(
            "org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee",
            True,
            loader,
        )
        klass = conf.getClass("fs.azure.account.oauth.provider.type", None, base)
        assert klass.getName() == "ch.fs.HttpTokenProvider"

        inst = klass.newInstance()
        inst.initialize(conf, "onelake.dfs.fabric.microsoft.com")
        assert inst.getAccessToken() == jwt  # fetched from our local endpoint
        # expiry parsed from the JWT exp claim (epoch seconds -> Date)
        assert inst.getExpiryTime().getTime() == exp * 1000
    finally:
        if engine is not None:
            engine.stop()
        srv.stop()
