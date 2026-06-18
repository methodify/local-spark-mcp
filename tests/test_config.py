import textwrap
from pathlib import Path

import pytest

from local_spark_mcp.config import (
    Config,
    ConfigError,
    find_config_file,
    load_config,
)


def write_config(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "local-spark.toml"
    path.write_text(textwrap.dedent(body))
    return path


def test_loads_full_file(tmp_path):
    path = write_config(
        tmp_path,
        """
        [workspace]
        name = "My Workspace"

        [lakehouses]
        exclude = ["Staging", "Sandbox"]

        [spark]
        driver_memory = "16g"
        [spark.extra_configs]
        "spark.sql.shuffle.partitions" = "8"

        [runtime]
        default_sql_limit = 50
        """,
    )
    cfg = load_config(path)
    assert cfg.workspace.name == "My Workspace"
    assert cfg.workspace.id is None
    assert cfg.lakehouses.exclude == ["Staging", "Sandbox"]
    assert cfg.spark.driver_memory == "16g"
    assert cfg.spark.extra_configs == {"spark.sql.shuffle.partitions": "8"}
    assert cfg.runtime.default_sql_limit == 50
    assert cfg.source_path == path


def test_defaults_applied(tmp_path):
    path = write_config(
        tmp_path,
        """
        [workspace]
        id = "abc-123"
        """,
    )
    cfg = load_config(path)
    assert cfg.workspace.id == "abc-123"
    assert cfg.lakehouses.exclude == []
    assert cfg.spark.driver_memory == "8g"
    assert cfg.runtime.default_sql_limit == 100


def test_workspace_name_and_id_mutually_exclusive(tmp_path):
    both = write_config(
        tmp_path,
        """
        [workspace]
        name = "W"
        id = "x"
        """,
    )
    with pytest.raises(ConfigError):
        load_config(both)


def test_workspace_optional_locally(tmp_path):
    # No workspace is fine for local-only operation; loads without error.
    neither = write_config(tmp_path, "[spark]\ndriver_memory = '4g'\n")
    cfg = load_config(neither)
    assert cfg.workspace.name is None and cfg.workspace.id is None
    # ...but require_workspace() raises when the Fabric layer needs it.
    with pytest.raises(ConfigError):
        cfg.require_workspace()


def test_env_override_workspace(tmp_path, monkeypatch):
    path = write_config(
        tmp_path,
        """
        [workspace]
        name = "FileWorkspace"
        """,
    )
    monkeypatch.setenv("LOCAL_SPARK_WORKSPACE_ID", "env-guid")
    cfg = load_config(path)
    # id from env wins and clears the file-provided name
    assert cfg.workspace.id == "env-guid"
    assert cfg.workspace.name is None


def test_env_override_scalars(tmp_path, monkeypatch):
    path = write_config(tmp_path, '[workspace]\nid = "x"\n')
    monkeypatch.setenv("LOCAL_SPARK_DRIVER_MEMORY", "32g")
    monkeypatch.setenv("LOCAL_SPARK_SQL_LIMIT", "250")
    monkeypatch.setenv("LOCAL_SPARK_LAKEHOUSE_EXCLUDE", "A, B ,C")
    cfg = load_config(path)
    assert cfg.spark.driver_memory == "32g"
    assert cfg.runtime.default_sql_limit == 250
    assert cfg.lakehouses.exclude == ["A", "B", "C"]


def test_warm_on_start_defaults_false(tmp_path):
    cfg = load_config(write_config(tmp_path, '[workspace]\nid = "x"\n'))
    assert cfg.runtime.warm_on_start is False


def test_warm_on_start_from_file(tmp_path):
    cfg = load_config(
        write_config(tmp_path, '[workspace]\nid = "x"\n[runtime]\nwarm_on_start = true\n')
    )
    assert cfg.runtime.warm_on_start is True


def test_warm_on_start_wrong_type(tmp_path):
    path = write_config(tmp_path, '[workspace]\nid = "x"\n[runtime]\nwarm_on_start = "yes"\n')
    with pytest.raises(ConfigError):
        load_config(path)


def test_warm_on_start_env_override(tmp_path, monkeypatch):
    path = write_config(tmp_path, '[workspace]\nid = "x"\n')
    monkeypatch.setenv("LOCAL_SPARK_WARM_ON_START", "1")
    assert load_config(path).runtime.warm_on_start is True
    monkeypatch.setenv("LOCAL_SPARK_WARM_ON_START", "off")
    assert load_config(path).runtime.warm_on_start is False


def test_warm_on_start_env_invalid(tmp_path, monkeypatch):
    path = write_config(tmp_path, '[workspace]\nid = "x"\n')
    monkeypatch.setenv("LOCAL_SPARK_WARM_ON_START", "maybe")
    with pytest.raises(ConfigError):
        load_config(path)


def test_bad_sql_limit_env(tmp_path, monkeypatch):
    path = write_config(tmp_path, '[workspace]\nid = "x"\n')
    monkeypatch.setenv("LOCAL_SPARK_SQL_LIMIT", "not-a-number")
    with pytest.raises(ConfigError):
        load_config(path)


def test_invalid_exclude_type(tmp_path):
    path = write_config(
        tmp_path,
        """
        [workspace]
        id = "x"
        [lakehouses]
        exclude = "Staging"
        """,
    )
    with pytest.raises(ConfigError):
        load_config(path)


def test_missing_explicit_path(tmp_path):
    with pytest.raises(ConfigError):
        load_config(tmp_path / "nope.toml")


def test_env_only_no_file(tmp_path, monkeypatch):
    # No config file anywhere under an isolated dir; env supplies the workspace.
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("LOCAL_SPARK_WORKSPACE_NAME", "EnvOnly")
    cfg = load_config(search_from=tmp_path)
    assert cfg.workspace.name == "EnvOnly"


def test_find_config_walks_up(tmp_path):
    write_config(tmp_path, '[workspace]\nid = "x"\n')
    nested = tmp_path / "a" / "b"
    nested.mkdir(parents=True)
    found = find_config_file(nested)
    assert found == tmp_path / "local-spark.toml"
