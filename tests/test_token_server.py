import urllib.error
import urllib.request
from collections import namedtuple

import pytest

from local_spark_mcp.token_server import SECRET_HEADER, TokenServer

AccessToken = namedtuple("AccessToken", "token expires_on")


class FakeCredential:
    def __init__(self, token="fake.jwt.token"):
        self.token = token
        self.scopes = []

    def get_token(self, *scopes, **kwargs):
        self.scopes.append(scopes)
        return AccessToken(self.token, 0)


@pytest.fixture
def server():
    cred = FakeCredential()
    s = TokenServer(credential=cred)
    s.start()
    s._fake = cred  # for assertions
    yield s
    s.stop()


def _get(url, secret=None):
    req = urllib.request.Request(url)
    if secret is not None:
        req.add_header(SECRET_HEADER, secret)
    return urllib.request.urlopen(req, timeout=5)


def test_returns_token_with_correct_secret(server):
    resp = _get(server.url, server.secret)
    assert resp.status == 200
    assert resp.read().decode() == "fake.jwt.token"


def test_uses_storage_scope(server):
    _get(server.url, server.secret)
    assert server._fake.scopes[0] == ("https://storage.azure.com/.default",)


def test_rejects_missing_secret(server):
    with pytest.raises(urllib.error.HTTPError) as exc:
        _get(server.url)
    assert exc.value.code == 403


def test_rejects_wrong_secret(server):
    with pytest.raises(urllib.error.HTTPError) as exc:
        _get(server.url, "not-the-secret")
    assert exc.value.code == 403


def test_rejects_unknown_path(server):
    base = server.url.rsplit("/", 1)[0]
    with pytest.raises(urllib.error.HTTPError) as exc:
        _get(base + "/nope", server.secret)
    assert exc.value.code == 404


def test_500_on_credential_failure():
    class BadCred:
        def get_token(self, *a, **k):
            raise RuntimeError("no creds")

    s = TokenServer(credential=BadCred())
    s.start()
    try:
        with pytest.raises(urllib.error.HTTPError) as exc:
            _get(s.url, s.secret)
        assert exc.value.code == 500
    finally:
        s.stop()


def test_bound_to_loopback(server):
    assert server.host == "127.0.0.1"
    assert server.url.startswith("http://127.0.0.1:")
