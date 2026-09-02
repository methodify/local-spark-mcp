# local-spark-mcp

An MCP server that gives an agent a **stateful local Spark session to work in** —
a Jupyter-notebook-shaped surface with the UI stripped away. The agent runs
PySpark "cells" against a long-lived session (state persists across calls), runs
SQL and gets rows back, and manages the runtime through tools.

The purpose is **local exploration in service of authoring PySpark notebooks that
will run on Microsoft Fabric**: figure things out locally against the same OneLake
Delta data, then hand the honed code to the user as a notebook to run on Fabric
with a reasonably similar outcome — no cloud compute burned while exploring.

## Status

Milestones A, B.1, B.2 complete and validated live. See `CLAUDE.md` for the
architecture and the locked design decisions.

## Running it (via `uvx`, from GitHub)

No clone or build needed — `uvx` installs and runs it in an ephemeral
environment. Register it as an MCP server in Claude Code (`.mcp.json`):

```json
{
  "mcpServers": {
    "local-spark": {
      "command": "uvx",
      "args": ["--from", "git+https://github.com/methodify/local-spark-mcp", "local-spark-mcp"],
      "env": { "LOCAL_SPARK_WORKSPACE_NAME": "Data Warehouse" }
    }
  }
}
```

Runs on **Linux/WSL and Windows** (both validated end to end against live
OneLake). Prerequisites on the host:

- **Java 17** for Spark 3.5 (the server prefers a vfox-managed JDK 17, else
  `JAVA_HOME`; or set `runtime.java_home` / `LOCAL_SPARK_JAVA_HOME`). System
  Java 21 will not work.
- **`az login`** — OneLake/Fabric auth is ambient via `DefaultAzureCredential`.
- **Windows**: nothing extra. Hadoop's `winutils.exe`/`hadoop.dll` (required for
  Spark to start at all on Windows) ship inside the package; point
  `runtime.hadoop_home` at your own Hadoop if you prefer. Python 3.11 is used on
  every platform — it matches Fabric Runtime 1.3, and pyspark 3.5.0's Python
  workers crash on Windows under 3.12.

The prebuilt OneLake token-provider jar ships inside the package, so Fabric mode
works out of the box (no sbt needed). First run downloads PySpark/Delta jars and
is slow; subsequent runs reuse the cached environment. Use `--refresh` to pick up
a new commit: `uvx --refresh --from git+https://github.com/methodify/local-spark-mcp local-spark-mcp`.

### Native / 3rd-party Python libs in distributed code

Spark Python workers run the **same interpreter** as the driver, so a library
installed into the server's environment is importable in both `run_code` and in
distributed code (`mapPartitions` / UDFs). Install such libs into that env — e.g.
`uvx --with jageocoder --with postal --from git+…/local-spark-mcp local-spark-mcp`
— and set any data-dir env vars (and/or `PYTHONPATH`) under `[spark.env]` in
`local-spark.toml`; those are applied to both the driver and the workers. (A
runtime `sys.path.append` only affects the driver — workers won't see it.)

## Configuration

Configuration lives in a `local-spark.toml` file in the working directory (see
`local-spark.example.toml`), discovered by walking up from where the server is
launched. Environment variables (`LOCAL_SPARK_*`) override individual settings —
convenient in the MCP `env` block above when you don't want a file. With no
workspace configured the server runs local-only (no Fabric). Auth is ambient via
`az login`, so nothing in the config is secret.
