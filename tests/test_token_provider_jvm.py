"""JVM-side validation of the HttpTokenProvider JAR against the Python token
server — no Azure needed. Requires the Scala jar to be built and Java 17.

    cd token-provider && JAVA_HOME=<jdk17> sbt package
    LOCAL_SPARK_RUN_JVM=1 .venv/bin/python -m pytest tests/test_token_provider_jvm.py -v
"""

import base64
import glob
import json
import os
import subprocess
import time
from collections import namedtuple
from pathlib import Path

import pytest

from local_spark_mcp.token_server import TokenServer

REPO = Path(__file__).resolve().parent.parent

pytestmark = pytest.mark.skipif(
    os.environ.get("LOCAL_SPARK_RUN_JVM") != "1",
    reason="set LOCAL_SPARK_RUN_JVM=1 to run (needs the built Scala jar + Java)",
)

AccessToken = namedtuple("AccessToken", "token expires_on")


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


def _classpath() -> str:
    import pyspark

    jar = glob.glob(str(REPO / "token-provider/target/scala-2.12/*.jar"))
    if not jar:
        pytest.skip("token provider jar not built (cd token-provider && sbt package)")

    azure = sorted(
        glob.glob(os.path.expanduser("~/.cache/coursier/**/hadoop-azure-3.3.*.jar"), recursive=True)
        + glob.glob(os.path.expanduser("~/.ivy2/**/hadoop-azure-3.3.*.jar"), recursive=True)
    )
    if not azure:
        pytest.skip("hadoop-azure jar not found in build caches")

    pyspark_jars = glob.glob(str(Path(pyspark.__file__).parent / "jars" / "*.jar"))
    return ":".join([jar[0], azure[-1], *pyspark_jars])


def _java() -> str:
    from local_spark_mcp.java import resolve_java_home

    return str(Path(resolve_java_home()) / "bin" / "java")


def test_provider_fetches_parses_and_caches():
    exp = int(time.time()) + 3600
    jwt = ".".join(
        [_b64url(b'{"alg":"none"}'), _b64url(json.dumps({"exp": exp}).encode()), "sig"]
    )

    class Cred:
        def get_token(self, *a, **k):
            return AccessToken(jwt, exp)

    srv = TokenServer(credential=Cred())
    srv.start()
    try:
        out = subprocess.run(
            [_java(), "-cp", _classpath(), "ch.fs.TokenProbe", srv.url, srv.secret],
            capture_output=True,
            text=True,
            timeout=90,
        )
    finally:
        srv.stop()

    assert out.returncode == 0, out.stderr[-2000:]
    assert f"TOKEN={jwt}" in out.stdout
    assert "CACHED_MATCH=true" in out.stdout
    # Expiry parsed from the JWT exp claim (rendered in the JVM's local tz).
    assert str(time.localtime(exp).tm_year) in out.stdout


def test_provider_rejected_without_secret():
    """Wrong/absent secret -> endpoint 403 -> provider raises (no token)."""

    class Cred:
        def get_token(self, *a, **k):
            return AccessToken("x.y.z", 0)

    srv = TokenServer(credential=Cred())
    srv.start()
    try:
        out = subprocess.run(
            [_java(), "-cp", _classpath(), "ch.fs.TokenProbe", srv.url, "wrong-secret"],
            capture_output=True,
            text=True,
            timeout=90,
        )
    finally:
        srv.stop()
    assert out.returncode != 0
    assert "403" in out.stderr
