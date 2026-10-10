"""Runtime health without a SparkSession: installed versions, profile verdict,
JDK and winutils resolution, jar validity. Used by the worker's `healthcheck`
method (works before `init`) and as a CLI:

    python -m local_spark_mcp.healthcheck [--json]
"""

from __future__ import annotations

import json
import os
import platform
import sys


def healthcheck(declared_profile: str | None = None) -> dict:
    from . import __version__
    from .fabric import default_jar_path, validate_jar
    from .hadoop import HadoopNotFoundError, is_windows, resolve_hadoop_home
    from .java import JavaNotFoundError, resolve_java_home
    from .profiles import check_profile, installed_versions
    from .protocol import PROTOCOL_VERSION

    out: dict = {
        "package": "local-spark-mcp",
        "version": __version__,
        "protocol_version": PROTOCOL_VERSION,
        "python": sys.version.split()[0],
        "python_executable": sys.executable,
        "platform": platform.platform(),
        "installed": installed_versions(),
        "ok": True,
        "problems": [],
        "warnings": [],
    }
    prof, warnings, errors = check_profile(declared_profile)
    out["profile"] = prof.name
    out["profile_declared"] = declared_profile
    try:
        from .fabric_packages import status as _pkg_status

        out["fabric_packages"] = _pkg_status(prof.name)
    except Exception as exc:  # pragma: no cover
        out["fabric_packages"] = {"error": f"{type(exc).__name__}: {exc}"}
    out["warnings"] += warnings
    out["problems"] += errors
    try:
        out["java_home"] = resolve_java_home(majors=prof.java_majors, prefer=prof.java_preferred,
                                             spark=f"Spark {prof.pyspark.rsplit('.', 1)[0]}")
    except JavaNotFoundError as exc:
        out["java_home"] = None
        out["problems"].append(str(exc))
    if is_windows():
        try:
            out["hadoop_home"] = resolve_hadoop_home(None)
        except HadoopNotFoundError as exc:
            out["hadoop_home"] = None
            out["problems"].append(str(exc))
    else:
        out["hadoop_home"] = None
    jar = default_jar_path(prof.scala)
    out["jar"] = jar
    if jar:
        try:
            validate_jar(jar, origin="bundled")
        except (FileNotFoundError, ValueError) as exc:
            out["problems"].append(str(exc))
    else:
        out["problems"].append(f"no catalog jar for Scala {prof.scala} found in the package")
    out["ok"] = not out["problems"]
    return out


def main(argv: list[str] | None = None) -> int:
    import argparse

    ap = argparse.ArgumentParser(prog="local_spark_mcp.healthcheck")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--profile", help="declared profile to check against (default: detect)")
    args = ap.parse_args(argv)
    hc = healthcheck(args.profile or os.environ.get("LOCAL_SPARK_PROFILE"))
    if args.json:
        print(json.dumps(hc, indent=2))
    else:
        print(f"local-spark-mcp {hc['version']}  python {hc['python']}  profile {hc['profile']}  {'OK' if hc['ok'] else 'PROBLEMS'}")
        print(f"  pyspark {hc['installed'].get('pyspark')}  delta-spark {hc['installed'].get('delta-spark')}")
        print(f"  java_home: {hc['java_home']}")
        if hc["hadoop_home"] is not None:
            print(f"  hadoop_home: {hc['hadoop_home']}")
        print(f"  jar: {hc['jar']}")
        for w in hc["warnings"]:
            print(f"  warning: {w}")
        for p in hc["problems"]:
            print(f"  problem: {p}")
    return 0 if hc["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
