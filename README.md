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

Prerequisites on the host:

- **Java 17** for Spark 3.5 (the server prefers a vfox-managed JDK 17, else
  `JAVA_HOME`; or set `runtime.java_home` / `LOCAL_SPARK_JAVA_HOME`). System
  Java 21 will not work.
- **`az login`** — OneLake/Fabric auth is ambient via `DefaultAzureCredential`.

The prebuilt OneLake token-provider jar ships inside the package, so Fabric mode
works out of the box (no sbt needed). First run downloads PySpark/Delta jars and
is slow; subsequent runs reuse the cached environment. Use `--refresh` to pick up
a new commit: `uvx --refresh --from git+https://github.com/methodify/local-spark-mcp local-spark-mcp`.

## Configuration

Configuration lives in a `local-spark.toml` file in the working directory (see
`local-spark.example.toml`), discovered by walking up from where the server is
launched. Environment variables (`LOCAL_SPARK_*`) override individual settings —
convenient in the MCP `env` block above when you don't want a file. With no
workspace configured the server runs local-only (no Fabric). Auth is ambient via
`az login`, so nothing in the config is secret.
