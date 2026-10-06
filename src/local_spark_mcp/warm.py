"""Pre-warm: resolve the profile's Spark packages (Delta, hadoop-azure) into the
Ivy cache so the first real session does not wait on Maven.

    python -m local_spark_mcp.warm [--ivy DIR] [--java-home PATH]

Starts a minimal local session with the same packages the server would use,
then stops it. Ivy's own progress lines go to stderr. Exit 0 on success.
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
import time


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="local_spark_mcp.warm")
    ap.add_argument("--ivy", metavar="DIR", help="Ivy cache directory (spark.jars.ivy); default: Spark's")
    ap.add_argument("--java-home", metavar="PATH", help="JDK to use; default: the profile's resolution")
    ap.add_argument("--quiet", action="store_true", help="suppress the summary line")
    args = ap.parse_args(argv)

    from .fabric import default_jar_path
    from .profiles import check_profile, current_profile
    from .spark_session import build_spark

    prof, warnings, errors = check_profile(None)
    if errors:
        print("local-spark-mcp warm: " + "; ".join(errors), file=sys.stderr)
        return 2
    for w in warnings:
        print(f"local-spark-mcp warm: {w}", file=sys.stderr)
    extra = {}
    if args.ivy:
        extra["spark.jars.ivy"] = os.path.abspath(os.path.expanduser(args.ivy))
    t0 = time.time()
    # A Fabric-shaped session (jar + hadoop-azure) so everything the real session
    # needs is resolved; the token endpoint is never contacted.
    spark = build_spark(
        driver_memory="1g",
        extra_configs=extra,
        java_home=args.java_home,
        onelake={"endpoint": "http://127.0.0.1:1/token", "secret": "warm", "jar_path": default_jar_path()},
        warehouse_dir=tempfile.mkdtemp(prefix="lsm-warm-"),
    )
    try:
        version = spark.version
        ivy = spark.conf.get("spark.jars.ivy", None) or os.path.expanduser("~/.ivy2.5.2")
    finally:
        spark.stop()
    if not args.quiet:
        print(f"warm: profile {prof.name}, Spark {version}, packages resolved into {ivy} in {time.time() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
