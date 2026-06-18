"""Localhost OneLake token endpoint.

The MCP server (parent process) owns one ``DefaultAzureCredential`` and serves
freshly-minted OneLake *storage* tokens over a 127.0.0.1-only HTTP endpoint. The
JVM-side ``ch.fs.HttpTokenProvider`` (a Hadoop ``CustomTokenProviderAdaptee``)
GETs this endpoint whenever ABFS needs a token. Minting in Python means
``azure-identity`` transparently refreshes near expiry, so the JVM always gets a
live token — fixing the stale-token failure mode of the old file-based provider.

Security: bound to loopback only, and every request must present a shared secret
(generated per server start) in the ``X-Token-Secret`` header, so other local
processes can't harvest Azure tokens off the port.
"""

from __future__ import annotations

import secrets
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# Scope for OneLake / ADLS Gen2 (ABFS) data-plane access.
DEFAULT_SCOPE = "https://storage.azure.com/.default"
SECRET_HEADER = "X-Token-Secret"


class TokenServer:
    """A loopback HTTP endpoint that returns OneLake bearer tokens as plain text."""

    def __init__(self, *, scope: str = DEFAULT_SCOPE, credential=None, host: str = "127.0.0.1"):
        self.scope = scope
        self.host = host
        self.secret = secrets.token_urlsafe(32)
        self._credential = credential
        self._lock = threading.Lock()
        self._httpd: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def credential(self):
        if self._credential is None:
            from azure.identity import DefaultAzureCredential

            self._credential = DefaultAzureCredential()
        return self._credential

    def get_token(self) -> str:
        """Mint (or return cached) a storage token. azure-identity handles the
        caching/refresh; the lock just serializes concurrent JVM fetches."""
        with self._lock:
            return self.credential.get_token(self.scope).token

    @property
    def port(self) -> int | None:
        return self._httpd.server_address[1] if self._httpd else None

    @property
    def url(self) -> str | None:
        return f"http://{self.host}:{self.port}/token" if self._httpd else None

    def start(self) -> str:
        self._httpd = ThreadingHTTPServer((self.host, 0), _make_handler(self))
        self._thread = threading.Thread(
            target=self._httpd.serve_forever,
            name="onelake-token-server",
            daemon=True,
        )
        self._thread.start()
        return self.url  # type: ignore[return-value]

    def stop(self) -> None:
        if self._httpd is not None:
            self._httpd.shutdown()
            self._httpd.server_close()
            self._httpd = None
        self._thread = None


def _make_handler(server: TokenServer):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):  # silence default stderr access logging
            pass

        def do_GET(self):  # noqa: N802 (BaseHTTPRequestHandler API)
            if self.path.split("?", 1)[0] != "/token":
                self.send_error(404, "not found")
                return
            if not secrets.compare_digest(
                self.headers.get(SECRET_HEADER, ""), server.secret
            ):
                self.send_error(403, "forbidden")
                return
            try:
                token = server.get_token()
            except Exception as exc:  # surface auth failures to the JVM
                self.send_error(500, f"token error: {type(exc).__name__}")
                return
            body = token.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return Handler
