"""Manual live OneLake read smoke test. Requires `az login` and access to the
target workspace. Reads a Delta table over abfss:// through the full stack
(token endpoint -> HttpTokenProvider -> Spark).

    .venv/bin/python scripts/smoke_onelake.py <workspace_id> <lakehouse_id> <table>

Addresses by GUID (name-based "<lakehouse>.Lakehouse" paths return HTTP 400).
Look up lakehouse/table GUIDs via the Fabric REST API or `fab`.
"""

import sys

from local_spark_mcp.engine import SparkEngine
from local_spark_mcp.fabric import default_jar_path
from local_spark_mcp.token_server import TokenServer


def main(argv):
    if len(argv) != 3:
        print(__doc__)
        return 2
    workspace_id, lakehouse_id, table = argv
    path = (
        f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/"
        f"{lakehouse_id}/Tables/{table}"
    )

    jar = default_jar_path()
    if not jar:
        print("token provider jar not built: cd token-provider && sbt package")
        return 1

    srv = TokenServer()  # real DefaultAzureCredential
    srv.start()
    engine = None
    try:
        engine = SparkEngine(
            driver_memory="4g",
            onelake={"endpoint": srv.url, "secret": srv.secret, "jar_path": jar},
        )
        print(f"Spark {engine.spark.version} up; reading {path}")
        res = engine.run_code(
            f"""
df = spark.read.format("delta").load({path!r})
print("rows:", df.count(), "cols:", len(df.columns))
df.printSchema()
df.show(5, truncate=40)
"""
        )
        print(res.stdout)
        if not res.ok:
            print("FAILED:", res.error)
            return 1
        print("OK")
        return 0
    finally:
        if engine is not None:
            engine.stop()
        srv.stop()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
