"""JDK discovery: realpath'd candidates, launcher-path normalization, version
gate, PATH fallback, and the candidate list in the error (REQUEST-002)."""

import os
import sys

import pytest

from local_spark_mcp import java as java_mod
from local_spark_mcp.java import JavaNotFoundError, check_jdk, normalize_home, resolve_java_home


def make_jdk(root, name="jdk17", version="17.0.16", release=True):
    home = root / name
    (home / "bin").mkdir(parents=True)
    (home / "bin" / ("java.exe" if os.name == "nt" else "java")).write_bytes(b"")
    if release:
        (home / "release").write_text(f'JAVA_VERSION="{version}"\nOS_ARCH="x86_64"\n', encoding="utf-8")
    return home


@pytest.fixture
def isolated(monkeypatch, tmp_path):
    """No vfox, no JAVA_HOME, no java on PATH unless a test adds them."""
    monkeypatch.setattr(java_mod, "_vfox_candidates", lambda: [])
    monkeypatch.delenv("JAVA_HOME", raising=False)
    monkeypatch.setattr(java_mod.shutil, "which", lambda name: None)
    return tmp_path


def test_launcher_path_normalizes_to_home(isolated):
    home = make_jdk(isolated)
    launcher = home / "bin" / ("java.exe" if os.name == "nt" else "java")
    assert normalize_home(launcher) == home.resolve()
    assert resolve_java_home(str(launcher)) == str(home.resolve())


@pytest.mark.skipif(os.name == "nt", reason="symlink creation needs privileges on Windows; junctions validated live")
def test_symlink_candidate_is_followed(isolated):
    home = make_jdk(isolated)
    link = isolated / "current"
    link.symlink_to(home)
    assert resolve_java_home(str(link)) == str(home.resolve())


def test_unsupported_major_is_rejected_with_reason(isolated):
    home = make_jdk(isolated, "jdk21", "21.0.2")
    with pytest.raises(JavaNotFoundError, match=r"Java 21; Spark 3.5 needs 8/11/17"):
        resolve_java_home(str(home))


def test_java8_release_format(isolated):
    home = make_jdk(isolated, "jdk8", "1.8.0_392")
    assert check_jdk(home)[1] == "ok (Java 8)"


def test_explicit_error_names_origin(isolated):
    with pytest.raises(JavaNotFoundError, match=r"\(from LOCAL_SPARK_JAVA_HOME\).*does not exist"):
        resolve_java_home(str(isolated / "nope"), origin="LOCAL_SPARK_JAVA_HOME")


def test_java_home_env_then_path_fallback(isolated, monkeypatch):
    jdk21 = make_jdk(isolated, "jdk21", "21.0.2")
    jdk17 = make_jdk(isolated, "jdk17")
    monkeypatch.setenv("JAVA_HOME", str(jdk21))
    launcher = jdk17 / "bin" / ("java.exe" if os.name == "nt" else "java")
    monkeypatch.setattr(java_mod.shutil, "which", lambda name: str(launcher))
    # JAVA_HOME is Java 21 -> skipped; PATH java's grandparent is the home
    assert resolve_java_home() == str(jdk17.resolve())


def test_error_lists_every_candidate_and_source(isolated, monkeypatch):
    jdk21 = make_jdk(isolated, "jdk21", "21.0.2")
    monkeypatch.setenv("JAVA_HOME", str(jdk21))
    monkeypatch.setattr(java_mod.shutil, "which", lambda name: str(isolated / "bin" / "java"))
    with pytest.raises(JavaNotFoundError) as exc:
        resolve_java_home()
    msg = str(exc.value)
    assert "vfox" in msg and "no match" in msg
    assert "JAVA_HOME:" in msg and "Java 21" in msg
    assert "java on PATH:" in msg and "not a JDK (no bin/java)" in msg


def test_vfox_wins_over_java_home(isolated, monkeypatch):
    vfox = make_jdk(isolated, "vfox17")
    monkeypatch.setattr(java_mod, "_vfox_candidates", lambda: [str(vfox)])
    monkeypatch.setenv("JAVA_HOME", str(make_jdk(isolated, "other17")))
    assert resolve_java_home() == str(vfox.resolve())
