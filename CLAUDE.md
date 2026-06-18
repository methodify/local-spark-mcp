# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`local-spark-mcp` is an MCP server that gives an agent a **stateful local Spark
session to work in** — a Jupyter-notebook-shaped surface with the notebook UI
stripped away. The agent runs arbitrary PySpark against a long-lived Python
process (state persists across calls, like notebook cells sharing a kernel),
runs SQL and gets rows back, and manages/resets the runtime through tools.

The point is **local exploration in service of authoring PySpark notebooks that
will eventually run on Microsoft Fabric.** The agent figures things out locally
(no cloud compute burned), then hands the honed code to the user as a notebook
they can run on Fabric with a reasonably similar outcome. So fidelity to the
Fabric runtime matters: local Spark should read the same OneLake Delta data and
behave close enough that conclusions transfer.

**Status: Milestone A + B.1 complete.** The MCP server runs, holds a stateful
Spark session in a worker subprocess, and exposes `run_code` / `run_sql` /
`session_info` / `reset_runtime`. OneLake auth is wired end to end: when a
`[workspace]` is configured the server starts the loopback token endpoint and a
Fabric-enabled Spark session that can read `abfss://` paths (provider loading +
token fetch validated through Hadoop without Azure; the live `abfss://` read
against a real workspace is the one remaining unverified step). **Not yet built
(Milestone B.2):** Fabric discovery (`FabricAPIClient`), workspace name→GUID
resolution, lakehouse→database registration, and `list_tables` / `mount_table`
tools — described as *intent* below.

## Commands

```bash
# Setup (Python 3.12 via uv; pulls pyspark/delta — slow first time)
uv venv --python 3.12
uv pip install -e ".[dev]"

# Build the OneLake token-provider jar (needs Java 17 + sbt; required for Fabric mode)
cd token-provider && JAVA_HOME=<jdk17> sbt package && cd ..

# Fast tests (config + formatters + token server; no Spark)
.venv/bin/python -m pytest -q

# Integration/e2e tests — start a REAL Spark session (~80–100s each), gated:
LOCAL_SPARK_RUN_INTEGRATION=1 .venv/bin/python -m pytest -m '' tests/test_worker_integration.py tests/test_server_e2e.py tests/test_fabric_session_integration.py -v
# JVM token-provider tests (needs built jar + Java):
LOCAL_SPARK_RUN_JVM=1 .venv/bin/python -m pytest tests/test_token_provider_jvm.py -v
# Single test: append `::test_name`

# Manual engine smoke (no MCP, no Fabric)
.venv/bin/python scripts/smoke_engine.py

# Run the server (stdio MCP); reads ./local-spark.toml
.venv/bin/local-spark-mcp        # or: .venv/bin/python -m local_spark_mcp.server
```

Register with Claude Code as an MCP server with `command` = the venv's
`local-spark-mcp` (or `python -m local_spark_mcp.server`) and `cwd` = the project
holding `local-spark.toml`. Worker stdout/stderr (Spark/JVM logs) go to the
server's **stderr**; stdout is reserved for the MCP transport.

## Current architecture (as built)

- `config.py` — `local-spark.toml` schema + loader (file discovery, `LOCAL_SPARK_*`
  env overrides, validation). Workspace is optional until the Fabric layer needs it.
- `java.py` — resolve a Spark-compatible `JAVA_HOME` (prefers vfox Java 17).
- `spark_session.py` — local Delta session builder; pins `JAVA_HOME`, drops
  ambient `SPARK_HOME`, forces `PYSPARK_PYTHON=sys.executable` (hermetic to venv).
- `engine.py` — `SparkEngine`: IPython `InteractiveShell` + injected `spark`/`sc`/
  `F`/`T`/`Window`. `run_code` (captured stdout + traceback), `run_sql`
  (rows + truncation), `info`.
- `protocol.py` / `worker.py` / `worker_client.py` — length-prefixed JSON over a
  dedicated localhost socket; worker process holds the engine; `WorkerProcess`
  spawns/handshakes/proxies and `restart()` = reset.
- `server.py` — FastMCP stdio server; one serialized worker; lazy startup;
  blocking IPC offloaded to a thread; Fabric mode auto-enabled by `[workspace]`.
- `token_server.py` — loopback OneLake token endpoint (DefaultAzureCredential,
  secret-guarded); owned by the server, outlives worker restarts.
- `fabric.py` — token-provider jar discovery + OneLake Spark config builder.
- `token-provider/` — sbt project for `ch.fs.HttpTokenProvider` (build with
  `sbt package`; output jar referenced via `spark.jars`).

## Intended tool surface (from the design brief)

- **run_code** — arbitrary PySpark executed against the persistent session,
  returning stdout/stderr/result/traceback. The core REPL primitive.
- **run_sql** — run SQL and return rows, with an optional `limit` defaulting to
  ~100.
- **Catalog/hydration** (lazy model): list lakehouses/databases, list a
  lakehouse's tables (enumerated via the storage API), and mount specific tables
  or all-tables-in-a-lakehouse on demand.
- **State management**: inspect session/catalog state; reset the runtime
  (= respawn the worker for a clean slate).

## Locked design decisions

Settled with the user during design (2026-06-17). Rationale lives in the
sections below; this is the summary of record.

1. **Execution model — worker subprocess.** The MCP server proxies cell/SQL
   calls over IPC to a dedicated worker process that holds the SparkSession (JVM)
   and the Python namespace. A Spark/JVM crash doesn't kill the server; "reset
   runtime" = kill & respawn the worker for a guaranteed-clean slate.
2. **REPL engine — IPython `InteractiveShell`.** `run_cell()` against a
   persistent `user_ns`: captured stdout/stderr, rich tracebacks, last-expression
   echo. Notebook semantics without the wire-protocol surface.
3. **Discovery — hand-rolled Fabric REST** (reuse the reference's
   `FabricAPIClient`: httpx + `DefaultAzureCredential`, scope
   `https://api.fabric.microsoft.com/.default`). In-process, proven, no
   shell-out. (`fab`/`sempy` remain available but aren't the path.)
4. **OneLake token — minted in Python, served to the JVM over localhost.**
   See the dedicated section below; this supersedes the file/`refreshtoken`
   approach and fixes the token-expiry bug.
5. **Config — project config file + env override.** A version-controlled file in
   the project working dir (e.g. `local-spark.toml`) is the source of truth for
   workspace, lakehouse selection, and Spark/runtime settings; env vars override
   host-specific bits. Nothing in it is secret (auth is ambient via `az login`).
6. **Workspace reference — name or GUID.** Accept either; resolve a display name
   to its GUID via the Fabric REST API at startup.
7. **Lakehouse selection — all by default, optional exclude-list.** Every
   lakehouse in the workspace is registered; excludes trim noise. Cheap because
   hydration is lazy (see #8).
8. **Table hydration — lazy.** At startup, register selected lakehouses as Spark
   databases only. Tables are enumerated/mounted on demand via tools
   (`CREATE TABLE ... USING DELTA LOCATION` per table). Keeps startup fast even
   for a workspace with many lakehouses/tables.

## The reference implementation — read this first

`~/src/local_spark` is prior art the user wrote for exactly this problem (local
Spark against Fabric OneLake data). It is a *library* (`fabric_spark`), not an
MCP server, but it solves the hard parts we need to carry over. Key files:

- `src/fabric_spark/core.py` — everything important lives here:
  - `FabricSparkSession.create()` (~line 203) — builds a Delta-enabled Spark
    session, pulls Hadoop-Azure packages, discovers + mounts all lakehouses in a
    workspace. This is the "magically connected to OneLake" entry point.
  - `FabricAPIClient` (~line 24) — `DefaultAzureCredential` → Fabric REST API
    (`https://api.fabric.microsoft.com/v1/workspaces/{id}/lakehouses`) to
    discover lakehouses.
  - `Lakehouse` (~line 72) — constructs OneLake ABFS paths and mounts Delta
    tables into the Spark catalog as `CREATE TABLE ... USING DELTA LOCATION`.
  - `LocalSparkSession` / `LocalDatabase` (~line 402+) — the offline path: scan
    a local dir tree (`base_path/<db>/<table>/`) and mount Delta tables found on
    disk. Useful when working against data fetched locally.
  - `create_simple_spark()` — bare Delta-enabled session, no Azure.

Reusable knowledge to lift:

- **OneLake ABFS path format:**
  `abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/{table_name}`
- **Critical Spark configs** for Fabric/Delta parity:
  `spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension`,
  `spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`,
  plus Hadoop-Azure packages `hadoop-azure`, `hadoop-azure-datalake`,
  `hadoop-common` (3.3.6 in the reference).
- **Catalog hydration / whitelisting:** the user wants to target a workspace
  with *many* lakehouses and include only some. The reference mounts everything;
  the brief calls for whitelisting/explicit selection — design that in.

### OneLake auth — minted in Python, served to the JVM over localhost

This is the heavy lift that makes OneLake "just work." **Why the JVM (not
Python):** Hadoop's ABFS filesystem driver runs in the Spark JVM and needs a
bearer token at the moment it hits OneLake storage. There's no clean way to
inject a Python-side token into that flow, so auth must plug into Hadoop's
official extension point, `CustomTokenProviderAdaptee`. **Locked design:**

1. The MCP server (Python) owns one `DefaultAzureCredential` and runs a tiny
   **localhost-only HTTP endpoint** (bind `127.0.0.1`, ephemeral port) that
   returns a freshly-minted OneLake storage token (scope
   `https://storage.azure.com/.default`). `azure-identity` caches and re-mints
   transparently, so every request yields a live token.
2. A new Scala provider — `HttpTokenProvider extends CustomTokenProviderAdaptee`
   — reads the endpoint URL from its Hadoop `Configuration` and does an HTTP GET
   on `getAccessToken()`. `getExpiryTime()` parses the real JWT `exp` (minus a
   ~5-min buffer) so ABFS refreshes proactively.
3. **Security:** the server generates a random secret at startup, passes it to
   the worker, and requires it as a header on `/token` — localhost binding +
   shared secret stops other local processes from harvesting Azure tokens.
4. **Spark wiring:** `fs.azure.account.auth.type=Custom`,
   `fs.azure.account.oauth.provider.type=ch.fs.HttpTokenProvider`,
   `spark.jars=<the built jar>`, plus the endpoint URL + secret in config.

**Why this design:** the user's prior `FileTokenProvider` (in `~/src/token`)
shelled out to a `~/bin/refreshtoken` script that wrote `~/.azure/mytoken.txt`,
and **silently failed to re-mint on expiry** (the `command.!` shell-out swallowed
errors to `-1` and fell back to a stale file, so Spark lost OneLake access until
restart). Minting in Python removes every one of those failure modes and the
disk handoff. A file-based mode can remain as a config-selectable fallback.

**Prior art to adapt:** `~/src/token` — `src/main/scala/FileTokenProvider.scala`
(the `CustomTokenProviderAdaptee` shape to copy), `build.sbt` (`version := "0.2"`,
`scalaVersion := "2.12.18"`, hadoop-azure 3.4.0). Build with `sbt` (1.9.8, via
sdkman): `sbt package` → `target/scala-2.12/<artifact>.jar`. ⚠️ The JAR path in
the reference's `docs/USAGE_EXAMPLES.md` (`.../scala-3.3.1/...0.1.jar`) is
**stale** (old Scala 3 / v0.1 build) — build fresh.

Local-disk fallback also exists at a higher level (`LocalSparkSession` over
Delta tables fetched to disk) when over-the-wire reads aren't wanted at all.

## Environment (verified on this machine)

- **`uv`** is the package manager (`uv 0.10.7`). The reference uses
  `uv pip install -e .` and a `src/` layout with `pyproject.toml`.
- **Python:** repo targets **3.12** (`.python-version` in the reference); note
  the default `python3` on PATH is conda's **3.11.5** — use a uv venv to pin.
- **Java:** system `java` is 21, but Spark 3.5 officially supports only Java
  8/11/17. **`vfox` is installed and has Java 17.0.16-bsg** — use that for the
  Spark process (the server should pin/select Java 17, e.g. via vfox or by
  setting `JAVA_HOME`). Bundling a JVM is also an option if we want full
  "just works" portability.
- **Build toolchain (for the token JAR):** `sbt` 1.9.8 and `scala` are
  installed via sdkman; `jq` is on PATH.
- **Spark/Delta pinning:** match the Fabric runtime — reference uses
  `pyspark==3.5.0`, `delta-spark==3.2.0`. Keep these pinned; mismatches break
  Delta and Fabric parity.
- **Auth:** `DefaultAzureCredential` / `az login` (the `az` CLI is installed).
  See the token-provider chain above for how this reaches Spark's ABFS layer.
- **`fab` CLI (ms-fabric-cli) 1.6.1 is installed.** It's a filesystem-shaped
  interface over Fabric/OneLake: `ls`/`cd`/`find`/`pwd` over workspaces & items,
  `table` to manage Delta tables, `auth`, and `api` for authenticated Fabric API
  requests. Strong candidate for lakehouse/table discovery (could replace the
  reference's hand-rolled `FabricAPIClient` REST calls) — and it's Python, so
  usable as a CLI, a library, or reference. Repo:
  https://github.com/microsoft/fabric-cli
- **`semantic-link-sempy` 0.6.0 is installed** — another Python window into
  Fabric / OneLake; worth considering for discovery or auth.

## Working notes

- Spark sessions are heavy and slow to start; the MCP server holds one alive
  across tool calls — that persistence is the whole product. Design startup,
  reset, and error recovery around a single long-lived session per server.
- Keep the local session's behavior honest about Fabric: same Delta version,
  same catalog shape, so code the agent proves out locally transfers.
</content>
</invoke>
