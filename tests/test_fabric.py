from pathlib import Path

import pytest

from local_spark_mcp import fabric
from local_spark_mcp.config import (
    Config,
    ConfigError,
    LakehouseConfig,
    RuntimeConfig,
    WorkspaceConfig,
)
from local_spark_mcp.discovery import LakehouseInfo
from local_spark_mcp.server import ServerState, format_mount


def test_onelake_configs_shape():
    cfg = fabric.onelake_spark_configs(
        endpoint="http://127.0.0.1:5/token", secret="s3cr3t", jar_path="/x/y.jar"
    )
    assert cfg["spark.jars"] == "/x/y.jar"
    assert cfg["spark.hadoop.fs.azure.account.auth.type"] == "Custom"
    assert cfg["spark.hadoop.fs.azure.account.oauth.provider.type"] == "ch.fs.HttpTokenProvider"
    assert cfg["spark.hadoop.fs.azure.tokenprovider.endpoint"] == "http://127.0.0.1:5/token"
    assert cfg["spark.hadoop.fs.azure.tokenprovider.secret"] == "s3cr3t"


def test_default_jar_path_finds_built_jar():
    # The jar is built during B.1; if missing this signals a broken build.
    jar = fabric.default_jar_path()
    assert jar is not None and Path(jar).is_file()
    assert jar.endswith(".jar")


def test_fabric_disabled_without_workspace():
    state = ServerState(config=Config())
    assert state._fabric_enabled() is False
    # local-only engine kwargs carry no onelake block
    assert "onelake" not in state._engine_kwargs()


def test_fabric_enabled_with_workspace():
    state = ServerState(config=Config(workspace=WorkspaceConfig(name="W")))
    assert state._fabric_enabled() is True


def test_resolve_jar_uses_override_and_validates(tmp_path):
    missing = Config(
        workspace=WorkspaceConfig(name="W"),
        runtime=RuntimeConfig(token_jar_path=str(tmp_path / "nope.jar")),
    )
    state = ServerState(config=missing)
    with pytest.raises(ConfigError):
        state._resolve_jar()

    jar = tmp_path / "present.jar"
    jar.write_text("")
    ok = ServerState(
        config=Config(
            workspace=WorkspaceConfig(name="W"),
            runtime=RuntimeConfig(token_jar_path=str(jar)),
        )
    )
    assert ok._resolve_jar() == str(jar)


# ---- discovery wiring (no Spark, mocked Fabric client) ----

class _FakeClient:
    def __init__(self, lakehouses, tables=None):
        self._lhs = lakehouses
        self._tables = tables or {}
    def resolve_workspace(self, name=None, id=None):
        return id or f"resolved-{name}"
    def list_lakehouses(self, ws):
        return [LakehouseInfo(n, f"id-{n}", ws) for n in self._lhs]
    def list_tables(self, ws, lh):
        return self._tables.get(lh, [])


def _fabric_state(exclude=None, client=None):
    cfg = Config(
        workspace=WorkspaceConfig(name="Data Warehouse"),
        lakehouses=LakehouseConfig(exclude=exclude or []),
    )
    state = ServerState(config=cfg)
    state._cred = object()
    state._fabric_client = client
    return state


def test_discover_applies_exclude():
    state = _fabric_state(exclude=["silver"], client=_FakeClient(["customer", "silver", "gold"]))
    state._discover()
    assert state._workspace_id == "resolved-Data Warehouse"
    assert [lh.name for lh in state._lakehouses] == ["customer", "gold"]


def test_engine_kwargs_include_lakehouses_in_fabric_mode(tmp_path):
    jar = tmp_path / "p.jar"
    jar.write_text("")
    state = _fabric_state(client=_FakeClient(["customer"]))
    state.config.runtime.token_jar_path = str(jar)
    state._discover()

    class _TS:
        url = "http://127.0.0.1:9/token"
        secret = "s"

    state._token_server = _TS()
    kw = state._engine_kwargs()
    assert kw["onelake"]["jar_path"] == str(jar)
    assert kw["lakehouses"] == [{"name": "customer", "id": "id-customer", "workspace_id": "resolved-Data Warehouse"}]


def test_find_lakehouse_unknown_raises():
    state = _fabric_state(client=_FakeClient(["customer"]))
    state._discover()
    state._find_lakehouse("customer")  # ok
    with pytest.raises(ConfigError, match="Unknown lakehouse"):
        state._find_lakehouse("nope")


def test_format_mount_with_failures():
    res = {"lakehouse": "customer", "mounted": ["a", "b"], "failed": [{"table": "c", "error": "boom"}]}
    out = format_mount(res)
    assert "Mounted 2 table(s) in customer: a, b" in out
    assert "1 failed" in out and "c: boom" in out


def test_format_mount_caps_long_list():
    res = {"lakehouse": "lh", "mounted": [f"t{i}" for i in range(60)], "failed": []}
    out = format_mount(res, max_listed=50)
    assert "+10 more" in out
