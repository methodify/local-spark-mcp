"""Fabric package rosters: shape, source, manifest exposure, extras in step, status and plan."""

import json
import re
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

from local_spark_mcp import fabric_packages as fp
from local_spark_mcp.profiles import PROFILES, manifest

VERSION = re.compile(r"^\d+(\.\d+)*((a|b|rc)\d+)?(\.post\d+)?$")


def test_rosters_cover_both_profiles_with_pypi_versions():
    assert set(fp.ROSTERS) == set(PROFILES)
    for profile, pins in fp.ROSTERS.items():
        assert len(pins) >= 40, profile
        for name, ver in pins.items():
            assert VERSION.match(ver), (profile, name, ver)
            assert name == name.lower() and "_" not in name
        for banned in ("pyspark", "delta-spark", "notebookutils", "synapseml", "semantic-link-sempy", "torch", "py4j"):
            assert banned not in pins, (profile, banned)
        src = fp.source(profile)
        assert src["commit"] and src["file"].endswith(".yml") and src["url"].startswith("https://github.com/microsoft/synapse-spark-runtime/blob/")
    # the two profiles share a core, at different versions
    common = set(fp.ROSTERS["fabric-2.0"]) & set(fp.ROSTERS["fabric-1.3"])
    assert {"pandas", "numpy", "pyarrow", "scikit-learn", "matplotlib", "plotly", "sqlalchemy", "pyodbc", "azure-storage-blob", "nltk"} <= common
    assert fp.ROSTERS["fabric-2.0"]["pandas"] != fp.ROSTERS["fabric-1.3"]["pandas"]


def test_manifest_carries_the_rosters():
    m = manifest()
    for profile in PROFILES:
        assert m["profiles"][profile]["python_packages"] == fp.ROSTERS[profile]
        assert m["profiles"][profile]["python_packages_source"]["commit"] == fp.SOURCE["commit"]
    assert "pyspark" in m["python_packages_excluded"]
    committed = json.loads((Path(__file__).resolve().parents[1] / "profiles.json").read_text(encoding="utf-8"))
    assert committed["profiles"]["fabric-2.0"]["python_packages"] == fp.ROSTERS["fabric-2.0"]


def test_pyproject_extras_match_the_rosters():
    data = tomllib.loads((Path(__file__).resolve().parents[1] / "pyproject.toml").read_text(encoding="utf-8"))
    extras = data["project"]["optional-dependencies"]
    for profile, pins in fp.ROSTERS.items():
        got = {}
        for req in extras[f"{profile}-packages"]:
            if req.startswith("scipy>="):
                continue  # the Python < 3.12 fallback for fabric-2.0; checked below
            name, _, rest = req.partition("==")
            got[name.strip()] = rest.split(";")[0].strip()
        assert got == pins, profile
    scipy_lines = [r for r in extras["fabric-2.0-packages"] if r.startswith("scipy")]
    assert len(scipy_lines) == 2 and any("python_version < '3.12'" in r for r in scipy_lines)
    fb = fp.fallbacks("fabric-2.0")["scipy"]
    assert any(r.startswith(fb["requirement"]) and fb["marker"] in r for r in scipy_lines)  # defined once, in FALLBACKS


def test_fallbacks_are_data_and_drive_requirements_and_status(monkeypatch):
    assert fp.marker_applies("python_version < '3.12'", (3, 11)) and not fp.marker_applies("python_version < '3.12'", (3, 13))
    assert fp.marker_applies("python_version >= '3.12'", (3, 12))
    with pytest.raises(ValueError):
        fp.marker_applies("sys_platform == 'win32'")
    reqs_311 = fp.requirements("fabric-2.0", (3, 11))
    reqs_313 = fp.requirements("fabric-2.0", (3, 13))
    assert "scipy>=1.15,<1.18" in reqs_311 and "scipy==1.18.0" not in reqs_311
    assert "scipy==1.18.0" in reqs_313 and len(reqs_311) == len(reqs_313) == len(fp.ROSTERS["fabric-2.0"])
    assert fp._spec_satisfied("1.17.1", "scipy>=1.15,<1.18") and not fp._spec_satisfied("1.18.0", "scipy>=1.15,<1.18")
    assert fp._spec_satisfied("2.3.3", "pandas==2.3.3") and not fp._spec_satisfied("2.3.4", "pandas==2.3.3")
    monkeypatch.setattr(fp, "_installed_version", lambda n: "1.17.1" if n == "scipy" else None)
    monkeypatch.setattr(fp.sys, "version_info", (3, 11, 9, "final", 0))
    st = fp.status("fabric-2.0")
    assert "scipy" not in st["mismatched"] and st["variants"]["scipy"]["have"] == "1.17.1" and st["installed"] == 1
    assert "platform fallback: scipy 1.17.1" in fp.summary_line(st)
    monkeypatch.setattr(fp.sys, "version_info", (3, 13, 2, "final", 0))
    st = fp.status("fabric-2.0")
    assert st["mismatched"]["scipy"] == {"want": "1.18.0", "have": "1.17.1"} and not st["variants"]
    m = manifest()
    assert m["profiles"]["fabric-2.0"]["python_packages_fallbacks"]["scipy"]["marker"] == "python_version < '3.12'"
    assert m["profiles"]["fabric-1.3"]["python_packages_fallbacks"] == {}


def test_status_and_plan_shapes():
    st = fp.status("fabric-2.0")
    assert st["total"] == len(fp.ROSTERS["fabric-2.0"]) and st["installed"] + len(st["mismatched"]) + len(st["missing"]) == st["total"]
    assert isinstance(st["complete"], bool) and "Fabric packages (fabric-2.0):" in fp.summary_line(st)
    with pytest.raises(ValueError, match="unknown profile"):
        fp.roster("fabric-9")
    res = fp.install("fabric-1.3", python=sys.executable, dry_run=True)
    assert res["dry_run"] and res["requested"] == len(fp.ROSTERS["fabric-1.3"]) and "pandas==2.1.4" in res["command"]
    out = subprocess.run([sys.executable, "-m", "local_spark_mcp.fabric_packages", "plan", "--profile", "fabric-2.0", "--json"],
                         capture_output=True, text=True, check=True).stdout
    plan = json.loads(out)
    assert plan["packages"] == fp.ROSTERS["fabric-2.0"] and plan["source"]["commit"] == fp.SOURCE["commit"]
    out = subprocess.run([sys.executable, "-m", "local_spark_mcp.fabric_packages", "status", "--profile", "fabric-2.0", "--json"],
                         capture_output=True, text=True).stdout
    assert json.loads(out)["profile"] == "fabric-2.0"
