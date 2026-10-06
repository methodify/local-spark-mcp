"""0.3.5 units: manifest matches profiles.json, config keys for preload / jars /
packages, discard_shadow(table=), preload formatter, healthcheck shape."""

import json
from pathlib import Path

import pytest

from local_spark_mcp.config import ConfigError, load_config
from local_spark_mcp.healthcheck import healthcheck
from local_spark_mcp.profiles import PROFILES, manifest
from local_spark_mcp.server import format_info, format_preload

ROOT = Path(__file__).resolve().parents[1]


def test_manifest_matches_committed_json():
    committed = json.loads((ROOT / "profiles.json").read_text(encoding="utf-8"))
    assert committed == manifest(), "run scripts/write_manifest.py after editing profiles.py"
    assert committed["schema"] == 1 and set(committed["profiles"]) == set(PROFILES)
    p20 = committed["profiles"]["fabric-2.0"]
    assert p20["python"] == "3.13" and p20["python_windows"] == "3.11"  # SPARK-53759 rule, explicit
    assert committed["profiles"]["fabric-1.3"]["python_windows"] == "3.11"
    assert p20["session_confs"]["spark.sql.ansi.enabled"] == "false"


def test_config_preload_jars_packages(tmp_path, monkeypatch):
    path = tmp_path / "local-spark.toml"
    path.write_text('''
[workspace]
id = "x"
[lakehouses]
preload = ["dataverse", "silver"]
preload_workers = 24
[spark]
jars = ["/opt/x.jar"]
packages = ["org.example:thing:1.0"]
''', encoding="utf-8")
    cfg = load_config(path)
    assert cfg.lakehouses.preload == ["dataverse", "silver"] and cfg.lakehouses.preload_workers == 24
    assert cfg.spark.jars == ["/opt/x.jar"] and cfg.spark.packages == ["org.example:thing:1.0"]
    monkeypatch.setenv("LOCAL_SPARK_PRELOAD", "all")
    monkeypatch.setenv("LOCAL_SPARK_PRELOAD_WORKERS", "8")
    monkeypatch.setenv("LOCAL_SPARK_PACKAGES", "a:b:1, c:d:2")
    cfg = load_config(path)
    assert cfg.lakehouses.preload == ["all"] and cfg.lakehouses.preload_workers == 8
    assert cfg.spark.packages == ["a:b:1", "c:d:2"] and cfg.origin("spark.packages") == "LOCAL_SPARK_PACKAGES"
    bad = tmp_path / "bad.toml"
    bad.write_text('[lakehouses]\npreload_workers = 0\n', encoding="utf-8")
    with pytest.raises(ConfigError, match="preload_workers"):
        load_config(bad)


def test_preload_formatter_and_info_line():
    st = {"state": "running", "tables_total": 400, "tables_done": 120, "tables_failed": 1, "workers": 16, "elapsed_s": 42.0,
          "lakehouses": {"dataverse": {"state": "mounting", "total": 345, "done": 120, "failed": 1, "errors": {"t": "boom"}},
                         "silver": {"state": "pending"}}}
    out = format_preload(st)
    assert out.startswith("preload: running — 120/400 tables, 1 failed (16 workers, 42.0s)")
    assert "  dataverse: 120/345, 1 failed mounting" in out and "    t: boom" in out and "  silver: pending" in out
    assert format_preload({"state": "idle"}).startswith("preload: idle")
    info = format_info({"spark_version": "4.1.1", "databases": ["a"], "lakehouses": ["a"], "preload": st,
                        "java_home": "/jdk", "python": "/py", "ivy_dir": "/ivy", "profile": "fabric-2.0 (…)"})
    assert "  preload: running" in info and "  java_home: /jdk" in info and "  ivy_dir: /ivy" in info


def test_healthcheck_without_spark():
    hc = healthcheck(None)
    assert hc["profile"] in PROFILES and hc["installed"]["pyspark"] and hc["jar"].endswith(".jar")
    assert isinstance(hc["ok"], bool) and "java_home" in hc
