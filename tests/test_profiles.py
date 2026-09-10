"""Runtime profiles: detection, declared-vs-installed checks, jar selection, config."""

import pytest

from local_spark_mcp import fabric
from local_spark_mcp.config import load_config
from local_spark_mcp.profiles import PROFILES, check_profile, detect_profile


def test_profiles_are_consistent_with_extras():
    import tomllib
    from pathlib import Path

    extras = tomllib.loads((Path(__file__).resolve().parents[1] / "pyproject.toml").read_text())["project"]["optional-dependencies"]
    for name, p in PROFILES.items():
        assert f"pyspark=={p.pyspark}" in extras[name][0] and f"delta-spark=={p.delta}" in extras[name][1]


def test_detect_by_spark_major():
    assert detect_profile({"pyspark": "3.5.9", "delta-spark": "3.2.0"}).name == "fabric-1.3"
    assert detect_profile({"pyspark": "4.1.1", "delta-spark": "4.2.0"}).name == "fabric-2.0"
    assert detect_profile({"pyspark": None}) is None


def test_check_profile_exact_match_is_silent():
    p, warnings, errors = check_profile("fabric-2.0", versions={"pyspark": "4.1.1", "delta-spark": "4.2.0"}, python=(3, 13))
    assert p.name == "fabric-2.0" and warnings == [] and errors == []


def test_check_profile_drift_warns_and_mismatch_errors():
    _, warnings, errors = check_profile(None, versions={"pyspark": "3.5.7", "delta-spark": "3.2.0"}, python=(3, 12))
    assert errors == [] and any("pins 3.5.9" in w for w in warnings) and any("Python 3.12" in w for w in warnings)
    _, _, errors = check_profile("fabric-2.0", versions={"pyspark": "3.5.9", "delta-spark": "3.2.0"}, python=(3, 11))
    assert errors and "install local-spark-mcp[fabric-2.0]" in errors[0]
    _, _, errors = check_profile(None, versions={"pyspark": None, "delta-spark": None})
    assert errors and "local-spark-mcp[fabric-1.3]" in errors[0]
    _, _, errors = check_profile("fabric-9", versions={"pyspark": "3.5.9", "delta-spark": "3.2.0"})
    assert errors and "unknown runtime profile" in errors[0]


def test_jar_follows_scala_line():
    a = fabric.default_jar_path("2.12")
    b = fabric.default_jar_path("2.13")
    assert a and a.endswith(".jar") and "2.12" in a
    assert b and b.endswith(".jar") and "2.13" in b
    fabric.validate_jar(a)
    fabric.validate_jar(b)


def test_profile_config_and_env(tmp_path, monkeypatch):
    path = tmp_path / "local-spark.toml"
    path.write_text('[runtime]\nprofile = "fabric-2.0"\n', encoding="utf-8")
    cfg = load_config(path)
    assert cfg.runtime.profile == "fabric-2.0" and cfg.origin("runtime.profile").startswith("local-spark.toml")
    monkeypatch.setenv("LOCAL_SPARK_PROFILE", "Fabric-1.3")
    cfg = load_config(path)
    assert cfg.runtime.profile == "fabric-1.3" and cfg.origin("runtime.profile") == "LOCAL_SPARK_PROFILE"


def test_windows_python_gate_for_spark_4():
    v20 = {"pyspark": "4.1.1", "delta-spark": "4.2.0"}
    _, _, errors = check_profile(None, versions=v20, python=(3, 13), windows=True)
    assert errors and "SPARK-53759" in errors[0] and "--python 3.11" in errors[0]
    _, _, errors = check_profile(None, versions=v20, python=(3, 12), windows=True)
    assert errors
    _, warnings, errors = check_profile(None, versions=v20, python=(3, 11), windows=True)
    assert errors == [] and any("Python 3.11" in w for w in warnings)  # parity drift only
    _, _, errors = check_profile(None, versions=v20, python=(3, 13), windows=False)
    assert errors == []
    _, _, errors = check_profile(None, versions={"pyspark": "3.5.9", "delta-spark": "3.2.0"}, python=(3, 13), windows=True)
    assert errors == []  # 3.5.9 carries the fix
