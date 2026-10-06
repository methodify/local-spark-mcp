"""A credential that fetches tokens from the host's token endpoint.

The worker never holds Azure credentials of its own: whoever started it (the MCP
server's TokenServer, or a host such as Cobalt SQL Works) serves tokens over the
loopback endpoint the JVM already uses, and this adapter asks that endpoint for
any scope the Python side needs (Fabric REST, OneLake data plane, Key Vault).
``GET <endpoint>?scope=<scope>`` with the shared secret in ``X-Token-Secret``;
the body is the bearer token.
"""

from __future__ import annotations

import base64
import json
import threading
import time
from collections import namedtuple
from urllib.parse import quote

import httpx

AccessToken = namedtuple("AccessToken", "token expires_on")
SECRET_HEADER = "X-Token-Secret"


def _jwt_exp(token: str) -> int | None:
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        return int(json.loads(base64.urlsafe_b64decode(payload)).get("exp"))
    except Exception:
        return None


class HostTokenCredential:
    """azure-core style credential (``get_token(*scopes)``) backed by the host endpoint."""

    def __init__(self, endpoint: str, secret: str, *, refresh_margin: int = 300):
        self.endpoint = endpoint
        self.secret = secret
        self._cache: dict[str, AccessToken] = {}
        self._lock = threading.Lock()
        self._margin = refresh_margin

    def get_token(self, *scopes: str, **_kw) -> AccessToken:
        scope = scopes[0] if scopes else ""
        with self._lock:
            cached = self._cache.get(scope)
            if cached and cached.expires_on - time.time() > self._margin:
                return cached
            url = f"{self.endpoint}?scope={quote(scope, safe='')}" if scope else self.endpoint
            resp = httpx.get(url, headers={SECRET_HEADER: self.secret}, timeout=30.0)
            if resp.status_code != 200:
                raise RuntimeError(f"host token endpoint {self.endpoint} returned HTTP {resp.status_code} for scope {scope!r}: {resp.text[:200]}")
            token = resp.text.strip()
            exp = _jwt_exp(token) or int(time.time()) + 3000
            tok = AccessToken(token, exp)
            self._cache[scope] = tok
            return tok

    def close(self) -> None:  # azure-core protocol compatibility
        pass
