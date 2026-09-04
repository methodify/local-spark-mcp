"""LOCAL_SPARK_CONFIG / --no-config, value origins in errors, the startup
source report, and jar validation (REQUEST-003)."""

import zipfile

import pytest

from local_spark_mcp.config import ConfigError, load_config
from local_spark_mcp.fabric import validate_jar
from local_spark_mcp.server import _parse_args


def write(tmp_path, name, body):
    p = tmp_path / name
    p.write_text(body, encoding="utf-8")
    return p


def test_config_env_none_skips_search(tmp_path, monkeypatch):
    write(tmp_path, "local-spark.toml", '[runtime]\njava_home = "C:/stale/bin/java.exe"\n')
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("LOCAL_SPARK_CONFIG", "none")
    cfg = load_config(search_from=tmp_path)
    assert cfg.source_path is None and cfg.runtime.java_home is None
    assert "LOCAL_SPARK_CONFIG='none'" in cfg.describe_sources()[0]
    monkeypatch.setenv("LOCAL_SPARK_CONFIG", "")
    assert load_config(search_from=tmp_path).source_path is None


def test_config_env_path_is_explicit(tmp_path, monkeypatch):
    other = write(tmp_path, "elsewhere.toml", '[spark]\ndriver_memory = "2g"\n')
    write(tmp_path, "local-spark.toml", '[spark]\ndriver_memory = "9g"\n')
    monkeypatch.setenv("LOCAL_SPARK_CONFIG", str(other))
    cfg = load_config(search_from=tmp_path)
    assert cfg.source_path == other and cfg.spark.driver_memory == "2g"
    monkeypatch.setenv("LOCAL_SPARK_CONFIG", str(tmp_path / "missing.toml"))
    with pytest.raises(ConfigError, match="not found.*LOCAL_SPARK_CONFIG"):
        load_config(search_from=tmp_path)


def test_no_config_kwarg(tmp_path, monkeypatch):
    write(tmp_path, "local-spark.toml", '[spark]\ndriver_memory = "9g"\n')
    monkeypatch.delenv("LOCAL_SPARK_CONFIG", raising=False)
    cfg = load_config(search_from=tmp_path, no_config=True)
    assert cfg.source_path is None and cfg.spark.driver_memory == "8g"
    assert cfg.describe_sources()[0] == "config file: none (--no-config)"


def test_cli_flags():
    assert _parse_args(["--no-config"]).no_config is True
    assert _parse_args(["--config", "x.toml"]).config == "x.toml"
    with pytest.raises(SystemExit):
        _parse_args(["--config", "x.toml", "--no-config"])


def test_origins_name_file_and_env(tmp_path, monkeypatch):
    path = write(tmp_path, "local-spark.toml", '[runtime]\njava_home = "/from/file"\nwrite_mode = "sandbox"\n')
    monkeypatch.setenv("LOCAL_SPARK_WRITE_MODE", "readonly")
    cfg = load_config(path)
    assert cfg.origin("runtime.java_home") == f"local-spark.toml at {path}"
    assert cfg.origin("runtime.write_mode") == "LOCAL_SPARK_WRITE_MODE"
    assert cfg.origin("runtime.state_root").startswith("not from")
    lines = cfg.describe_sources()
    assert lines[0] == f"config file: {path}"
    assert "env override: LOCAL_SPARK_WRITE_MODE -> runtime.write_mode" in lines


def test_validation_error_names_origin(tmp_path, monkeypatch):
    path = write(tmp_path, "local-spark.toml", '[runtime]\nwrite_mode = "yolo"\n')
    with pytest.raises(ConfigError, match=r"write_mode.*\[from local-spark.toml at .*\]"):
        load_config(path)
    good = write(tmp_path, "ok.toml", "[spark]\n")
    monkeypatch.setenv("LOCAL_SPARK_WRITE_MODE", "nope")
    with pytest.raises(ConfigError, match=r"\[from LOCAL_SPARK_WRITE_MODE\]"):
        load_config(good)


def test_no_file_found_note(tmp_path, monkeypatch):
    monkeypatch.delenv("LOCAL_SPARK_CONFIG", raising=False)
    cfg = load_config(search_from=tmp_path)
    assert cfg.describe_sources()[0].startswith("config file: none (none found in or above")


def _jar(path, classes):
    with zipfile.ZipFile(path, "w") as zf:
        for c in classes:
            zf.writestr(c.replace(".", "/") + ".class", b"\xca\xfe\xba\xbe")


def test_validate_jar_accepts_bundled_and_rejects_old(tmp_path):
    from local_spark_mcp.fabric import default_jar_path

    validate_jar(default_jar_path())  # the shipped jar has both classes
    old = tmp_path / "httptokenprovider_2.12-0.1.jar"
    _jar(old, ["ch.fs.HttpTokenProvider"])
    with pytest.raises(ValueError, match=r"\(from LOCAL_SPARK_TOKEN_JAR_PATH\).*missing ch.fs.OneLakeCatalog"):
        validate_jar(str(old), origin="LOCAL_SPARK_TOKEN_JAR_PATH")
    (tmp_path / "junk.jar").write_bytes(b"nope")
    with pytest.raises(ValueError, match="not a valid jar"):
        validate_jar(str(tmp_path / "junk.jar"))
    with pytest.raises(FileNotFoundError):
        validate_jar(str(tmp_path / "absent.jar"))


def test_stale_project_file_reproduction(tmp_path, monkeypatch):
    """REQUEST-003's case: a project's forgotten local-spark.toml points java_home
    at a launcher and token_jar_path at a pre-catalog jar. Startup must fail
    fast naming the file, or succeed with --no-config."""
    from tests.test_java import make_jdk
    from local_spark_mcp.server import ServerState

    jdk = make_jdk(tmp_path)
    launcher = jdk / "bin" / ("java.exe" if __import__("os").name == "nt" else "java")
    old_jar = tmp_path / "tmp" / "httptokenprovider_2.12-0.1.jar"
    old_jar.parent.mkdir()
    _jar(old_jar, ["ch.fs.HttpTokenProvider"])
    write(tmp_path, "local-spark.toml",
          f'[runtime]\njava_home = {str(launcher)!r}\ntoken_jar_path = "tmp/httptokenprovider_2.12-0.1.jar"\n'.replace("\\", "/"))
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("LOCAL_SPARK_WORKSPACE_NAME", "W")
    monkeypatch.delenv("LOCAL_SPARK_CONFIG", raising=False)
    monkeypatch.delenv("LOCAL_SPARK_JAVA_HOME", raising=False)
    monkeypatch.delenv("LOCAL_SPARK_TOKEN_JAR_PATH", raising=False)

    cfg = load_config(search_from=tmp_path)
    state = ServerState(cfg)
    with pytest.raises(ConfigError) as exc:  # the launcher path is now normalized; the old jar is the failure
        state.validate_runtime()
    assert "missing ch.fs.OneLakeCatalog" in str(exc.value) and f"local-spark.toml at {cfg.source_path}" in str(exc.value)

    cfg2 = load_config(search_from=tmp_path, no_config=True)
    monkeypatch.setenv("LOCAL_SPARK_JAVA_HOME", str(launcher))
    cfg2 = load_config(search_from=tmp_path, no_config=True)
    resolved = ServerState(cfg2).validate_runtime()
    assert resolved["java_home"] == str(jdk.resolve()) and resolved["jar"].endswith(".jar")
