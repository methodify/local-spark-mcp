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

Greenfield, under active construction. See `CLAUDE.md` for the architecture and
the locked design decisions.

## Configuration

Configuration lives in a `local-spark.toml` file in the project working directory
(see `local-spark.example.toml`). Environment variables (`LOCAL_SPARK_*`) override
individual settings. Auth is ambient via the Azure CLI (`az login`), so nothing in
the config is secret.
