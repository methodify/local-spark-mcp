"""The worker's credential comes from the host token endpoint: scope parameter on
the endpoint, caching, expiry from the JWT, error surfacing."""

import base64
import json
import time
from collections import namedtuple

import httpx
import pytest

from local_spark_mcp.host_credential import HostTokenCredential
from local_spark_mcp.token_server import TokenServer

AccessToken = namedtuple("AccessToken", "token expires_on")


def _jwt(exp: int, aud: str) -> str:
    def b64(d):
        return base64.urlsafe_b64encode(json.dumps(d).encode()).decode().rstrip("=")
    return f"{b64({'alg': 'none'})}.{b64({'exp': exp, 'aud': aud})}.sig"


class Cred:
    def __init__(self):
        self.calls = []

    def get_token(self, *scopes, **_):
        self.calls.append(scopes[0])
        return AccessToken(_jwt(int(time.time()) + 3600, scopes[0]), int(time.time()) + 3600)


def test_endpoint_serves_any_scope_and_worker_credential_caches():
    cred = Cred()
    srv = TokenServer(credential=cred); srv.start()
    try:
        host = HostTokenCredential(srv.url, srv.secret)
        storage = host.get_token("https://storage.azure.com/.default")
        fabric = host.get_token("https://api.fabric.microsoft.com/.default")
        assert json.loads(base64.urlsafe_b64decode(fabric.token.split(".")[1] + "=="))["aud"] == "https://api.fabric.microsoft.com/.default"
        assert storage.token != fabric.token and fabric.expires_on > time.time() + 3000
        host.get_token("https://api.fabric.microsoft.com/.default")  # cached: no second mint
        assert cred.calls == ["https://storage.azure.com/.default", "https://api.fabric.microsoft.com/.default"]
        # the JVM's plain GET (no scope) still gets a storage token
        r = httpx.get(srv.url, headers={"X-Token-Secret": srv.secret})
        assert r.status_code == 200 and json.loads(base64.urlsafe_b64decode(r.text.split(".")[1] + "=="))["aud"] == "https://storage.azure.com/.default"
        with pytest.raises(RuntimeError, match="HTTP 403"):
            HostTokenCredential(srv.url, "wrong").get_token("x")
    finally:
        srv.stop()
