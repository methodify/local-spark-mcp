from pathlib import Path

import pytest

from local_spark_mcp import fabric
from local_spark_mcp.config import Config, ConfigError, RuntimeConfig, WorkspaceConfig
from local_spark_mcp.server import ServerState


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
