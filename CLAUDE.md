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

**Status: Milestones A, B.1, B.2 complete — the core vision is functional.** The
MCP server holds a stateful Spark session in a worker subprocess and exposes
`run_code` / `run_sql` / `session_info` / `reset_runtime` plus the Fabric tools
`list_lakehouses` / `list_tables` / `mount_table` / `mount_lakehouse`. When a
`[workspace]` is configured it starts the loopback token endpoint, a
Fabric-enabled Spark session, and REST discovery; lakehouses register as Spark
databases and their tables mount lazily. **Validated live end to end** against a
real workspace: discovery (8 lakehouses, 63 tables), mount, and an `abfss://`
Delta read/query (1.4M rows) through the full stack.

**REQUEST-001 (notebook parity, from `~/src/claude-fabric`) shipped as 0.2.x**,
validated live on Linux and Windows: `OneLakeCatalog` (tables resolve by name on
first touch, no mount step), default lakehouse, the write policy (sandbox /
readonly / writethrough, shadows via Delta shallow clone), `run_notebook` + the
`notebookutils`/`mssparkutils` shim, and the Files mirror behind
`/lakehouse/default/Files` (`files.py`). Ask 5 (PyPI) is deferred by the user;
the project is Apache-2.0 (LICENSE + NOTICE, shipped in 0.2.1). The response lives at
`~/src/claude-fabric/tmp/exchange/local-spark-mcp/RESPONSE-001-notebook-parity.md`.

⚠️ **Address OneLake by GUID, not name.** `abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/{table}`
works; name-based paths (`{lakehouse}.Lakehouse/...`) make OneLake return HTTP
400. `FabricAPIClient` / `LakehouseInfo` build GUID paths.

## Commands

```bash
# Setup (Python 3.11 via uv — see cross-platform notes; pulls pyspark/delta, slow first time)
uv venv --python 3.11
uv pip install -e ".[dev]"

# Build the Scala jar (ch.fs.HttpTokenProvider + ch.fs.OneLakeCatalog) AND bundle
# it into the package (needs Java 17 + sbt). The bundled jar
# (src/local_spark_mcp/jars/localsparkjars_*.jar) ships in the wheel so `uvx`/pip
# installs from GitHub work without sbt — re-run and commit this whenever
# anything under token-provider/src changes.
JAVA_HOME=<jdk17> scripts/build_jar.sh

# Fast tests (config + formatters + token server; no Spark)
.venv/bin/python -m pytest -q

# Integration/e2e tests — start a REAL Spark session (~80–100s each), gated:
LOCAL_SPARK_RUN_INTEGRATION=1 .venv/bin/python -m pytest -m '' tests/test_worker_integration.py tests/test_server_e2e.py tests/test_fabric_session_integration.py -v
# JVM token-provider tests (needs built jar + Java):
LOCAL_SPARK_RUN_JVM=1 .venv/bin/python -m pytest tests/test_token_provider_jvm.py -v
# Live catalog + write-policy, notebookutils, Files-mirror, and deletion-vector tests against a real workspace (az login; writes only to a sandbox shadow / the local mirror):
LOCAL_SPARK_LIVE=1 LOCAL_SPARK_LIVE_WORKSPACE_ID=<guid> .venv/bin/python -m pytest tests/test_onelake_catalog_live.py tests/test_notebookutils_live.py tests/test_files_mirror_live.py tests/test_deletion_vectors_live.py -v
# Notebook runner (local Spark, synthetic notebooks) and the parser over the real Git export (skips if absent):
LOCAL_SPARK_RUN_INTEGRATION=1 .venv/bin/python -m pytest tests/test_notebook_runner_integration.py tests/test_notebook_parser.py -v
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
  `LOCAL_SPARK_CONFIG=<path>|none` (CLI `--config` / `--no-config`) selects or
  skips the file. Every value records its origin (`Config.origins`, file path or
  env var); validation errors and `server.validate_runtime()` (JDK, jar classes,
  winutils — checked in the server before the worker spawns) name it, and
  `describe_sources()` is logged to stderr at launch.
  `[spark.env]` sets env vars on BOTH the driver process and Spark Python workers
  (`spark.executorEnv.*`) — for native-lib data dirs / PYTHONPATH so distributed
  (mapPartitions/UDF) code can import + init the same libs as the driver. Worker/
  driver interpreter parity already holds (`PYSPARK_PYTHON=preferred_python()`), so a
  lib pip-installed into the server's env is importable on both; `[spark.env]`
  covers the data dirs / path. A driver-only runtime `sys.path` change does NOT
  reach workers — install into the env or use PYTHONPATH instead.
- `java.py` — resolve a Spark-compatible `JAVA_HOME`: explicit → vfox Java
  17/11 → `JAVA_HOME` → `java` on PATH. Every candidate is `realpath`'d (vfox
  `current` junction, `/usr/lib/jvm/default`), a `bin/java[.exe]` path is
  normalized to its home, and the `release` file's major must be 8/11/17 (system
  Java 21 is rejected with the reason). The error lists each candidate and its
  source. Accepts `bin/java` **or** `bin/java.exe`.
- `hadoop.py` — Windows only: resolve a HADOOP_HOME with `bin/winutils.exe`.
  Spark 3.5 **cannot start** on Windows without it (`Shell.<clinit>` throws
  "HADOOP_HOME and hadoop.home.dir are unset"). Order: `runtime.hadoop_home`
  → ambient `HADOOP_HOME` → the winutils bundled in the wheel
  (`winutils/`, see its PROVENANCE.md). No-op on POSIX.
- `spark_session.py` — Delta session builder; pins `JAVA_HOME`, drops ambient
  `SPARK_HOME`, points `PYSPARK_PYTHON` at `preferred_python()` (hermetic to the
  env, correct under uv/uvx trampolines). In Fabric mode swaps the session
  catalog to `ch.fs.OneLakeCatalog` and passes it `spark.localspark.*` confs
  (workspace id, one `lakehouse.<name>=<guid>` per lakehouse, write mode, shadow
  root). Always sets `spark.sql.warehouse.dir` to the per-session dir so managed
  tables never land in the project cwd.
- `token-provider/src/main/scala/OneLakeCatalog.scala` — the session catalog:
  `DeltaCatalog` + on-demand resolution of `<lakehouse>.<table>` (checks OneLake
  for `Tables/<table>/_delta_log`, materializes into the session catalog on first
  touch) + the write policy in `createTable` **and the six `stage*` overloads**
  (Delta is a `StagingTableCatalog`, so CTAS/`saveAsTable` bypass `createTable`).
  A `ThreadLocal` reentrancy guard keeps the nested `CREATE TABLE` from
  re-entering the resolver. Only OneLake existence checks are cached.
  **Deletion-vector tables** (Link-to-Fabric mirrors such as `dataverse_l2f`;
  protocol feature `deletionVectors`): Delta 3.2 cannot SHALLOW CLONE them
  (`DELTA_ADDING_DELETION_VECTORS_DISALLOWED`, and forcing the feature trips
  the tightBounds check), so under `spark.localspark.dv_strategy=view` (the
  default; Python picks it for delta-spark < 3.3) they become a live read-only
  `VIEW` over the source path tagged `COMMENT 'localspark:deletion-vectors
  source=…'`; `dv_strategy=clone` (delta-spark >= 3.3, validated live with a
  jar built against 3.3.3) clones with `delta.enableDeletionVectors=true` and
  gives the full sandbox. **The jar is compiled against a specific Delta
  (`provided`) and is not binary-compatible across minors** — a Delta bump
  means rebuilding and re-bundling it.
- `engine.py` — `SparkEngine`: IPython `InteractiveShell` + injected `spark`/`sc`/
  `F`/`T`/`Window`. `run_code` (captured stdout + traceback), `run_sql`
  (rows + truncation), `info`. Owns per-session state under
  `<state_root>/sessions/<pid>-<ts>/` (warehouse + session-scoped shadow;
  deleted on stop, stale ones purged after 7 days) or a persistent shadow under
  `<state_root>/lakehouses/<workspace-id>/shadow`. `USE`s the default lakehouse.
  Installs the `DeltaTable.forName` bridge (`forName` resolves via the V1
  catalog, which bypasses V2 plugins; the bridge resolves through `spark.table`
  first, and refuses in readonly). `mount_table` forces catalog materialization;
  `mount_tables` runs them in a thread pool. `shadow_status` / `discard_shadow`.
  Deletion-vector views: `_dv_tables()` finds them by the comment tag;
  `run_sql` pre-checks SQL write targets (`_sql_write_target`) and the
  `forName` bridge refuses with `dv_refusal()`; cell errors from writes against
  such a view are annotated (`annotate_error`); `table_features()` reads
  protocols per table for `list_tables(features=True)`.
  `run_notebook(path, cells, stop_on_error, default_lakehouse, parameters)`
  runs a parsed notebook cell by cell in the same namespace: markdown skipped,
  `%%sql` via `run_sql`, line magics stripped and reported, `USE` of the
  notebook's own default lakehouse for the run (restored after), parameters
  applied after the PARAMETERS CELL (so they override, as in a pipeline run),
  `NotebookExit` recognized via `_exec` (which returns the raised exception
  alongside the `ExecResult`).
- `protocol.py` / `worker.py` / `worker_client.py` — length-prefixed JSON over a
  dedicated localhost socket; worker process holds the engine; `WorkerProcess`
  spawns/handshakes/proxies and `restart()` = reset.
- `server.py` — FastMCP stdio server; one serialized worker; lazy startup;
  blocking IPC offloaded to a thread; Fabric mode auto-enabled by `[workspace]`.
  Reports the package version (`__version__`, pinned to `pyproject.toml` by a
  test) as `serverInfo.version` instead of the mcp SDK's.
- `token_server.py` — loopback OneLake token endpoint (DefaultAzureCredential,
  secret-guarded); owned by the server, outlives worker restarts.
- `fabric.py` — token-provider jar discovery + OneLake Spark config builder +
  `validate_jar` (opens the zip; both `ch.fs.HttpTokenProvider` and
  `ch.fs.OneLakeCatalog` must be present, else a message naming the jar, its
  origin, and the missing class — a stale 0.1 jar in a project file caused a
  ClassNotFound on the first query otherwise).
- `discovery.py` — `FabricAPIClient` (REST: resolve workspace, list lakehouses /
  tables, paging; `list_items` + `get_item_definition_parts` with LRO polling,
  used for Variable Libraries) + `LakehouseInfo` (GUID abfss path builder).
- `notebook.py` — parser for the Fabric Git `notebook-content.py` format
  (markers with exactly 20 asterisks, `# META` JSON following each code cell,
  `# MAGIC`-prefixed `%%sql` cells, `# `-prefixed markdown, raw `%pip`/`!pip`
  line magics). Warns, never refuses. `select_cells` ("3", "0-4,7"),
  `index_notebooks` (display name → path via each `.platform`, since folder names
  often differ). Validated against all 143 notebooks in the workspace export.
- `notebookutils_shim.py` — `notebookutils` / `mssparkutils` for local runs:
  `credentials.getSecret` (Key Vault, ambient credential), `variableLibrary
  .getLibrary` (Fabric REST definition → attributes, active value set applied),
  `fs.ls`/`exists` for `abfss://` (OneLake data plane) and for `/lakehouse/...`
  and mount points (the Files mirror), `fs.mount` (registers + links a mount
  point to the mirror), `notebook.run`/`runMultiple` (sequential in
  dependency order; raises with `.result` like Fabric)/`exit`, `session.stop`
  (no-op), `runtime.context`. Anything else raises `NotImplementedError` naming
  the member. Registered in `sys.modules` and the namespace at bootstrap.
- `files.py` — `FilesMirror`: the local mirror of a lakehouse's `Files/` under
  `<mirror_root>/<workspace-id>/<lakehouse-id>/Files` (default
  `<state_root>/lakehouses/...`, shared across projects). Selective `pull` of
  configured subtrees through the OneLake data plane (`azure-storage-file-
  datalake`; unchanged files skipped by size + mtime), `push` only in
  writethrough. `link_default` points `/lakehouse/default` (POSIX symlink) or
  `C:\lakehouse\default` (junction, `mklink /J`, unelevated) at the lakehouse
  dir so `<link>/Files` is the mirror; a lockfile
  (`<mirror_root>/.lakehouse-link.json`) records the owner and the link is never
  repointed while another LIVE session holds it for a different lakehouse, never
  replaces a real directory, and a dangling `/lakehouse` symlink whose target is
  under a writable location is repaired (this machine: root-owned
  `/lakehouse -> ~/src/mosaic/lakehouse`). When the link can't be made the
  one-time sudo command is reported and the session continues;
  `LOCAL_SPARK_FILES_ROOT` (driver + `spark.executorEnv`) always names the
  mirror. `resolve()` maps `/lakehouse/<default|name>/Files/...` and mount
  points for the shim. `Tables/` is never mirrored.
- `token-provider/` — sbt project (`LocalSparkJars`) for `ch.fs.HttpTokenProvider`
  and `ch.fs.OneLakeCatalog`; spark-sql and delta-spark are `provided`. Its built
  jar is bundled into `src/local_spark_mcp/jars/` (committed, shipped in the
  wheel) so `uvx`/pip installs work without sbt. `default_jar_path()` prefers a
  fresh in-repo build, else the bundled jar. Rebuild + re-bundle via
  `scripts/build_jar.sh`.

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
- **Write policy**: `shadow_status` (write mode + which lakehouse tables are
  shadowed locally, each `read` — an untouched shallow clone — or `written`,
  derived from the shadow's `_delta_log`: CLONE at version 0 and nothing after
  = read) and `discard_shadow(only=None|"read"|"written")` (drop the shadows;
  next touch re-clones).
- **run_notebook** — run a Fabric notebook from its Git `.py` source in the
  persistent namespace, with cell selection, parameters, and `notebookutils`.
- **sync_files** — pull `Files/` subtrees into the mirror behind
  `/lakehouse/default/Files` (or push, in writethrough only).
- **list_tables(features=True)** — per-table Delta protocol scan flagging
  deletion-vector tables (read-only here on Delta 3.2).

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
8. **Table hydration — resolved on first touch by `OneLakeCatalog`, with a
   write policy.** At startup, register selected lakehouses as Spark databases
   only, and `USE` the default lakehouse (`[lakehouses] default`) if set. Tables
   are NOT pre-mounted: the session catalog is `ch.fs.OneLakeCatalog` (extends
   Delta's `DeltaCatalog`), which on a catalog miss for `<lakehouse>.<table>`
   checks OneLake for `Tables/<table>/_delta_log` and materializes the table — so
   `spark.table`, `spark.read.table`, `spark.sql`, `saveAsTable`, `INSERT`, SQL
   `MERGE`, and (through a small `DeltaTable.forName` bridge, because `forName`
   uses the V1 catalog) `dwlib`'s MERGE all work by name with no mount step,
   exactly like the Fabric runtime; a table that doesn't exist still raises the
   original not-found. Materialization honors `runtime.write_mode`: **sandbox**
   (default) creates a Delta SHALLOW CLONE under the shadow root — metadata
   only, reads stay live from OneLake, writes land locally, OneLake is never
   modified; **readonly** is sandbox plus refusal of `createTable` and
   `forName`; **writethrough** points at OneLake. New tables under a lakehouse
   follow the same policy. Shadows are keyed by lakehouse id and session-scoped
   by default (`persist_shadow` opts in). `mount_table`/`mount_lakehouse` force
   materialization (in parallel for bulk); `run_sql`'s catch-and-mount fallback
   stays as a no-op safety net. Worker startup is **lazy by default**
   (first tool call that needs it), single-flight, and the cold start is covered
   by MCP progress heartbeats emitted throughout every long op — so no client
   timeout, just first-call lag on whichever agent uses it. Lazy keeps a swarm of
   agents from each warming a JVM; opt into eager warm-at-launch with
   `runtime.warm_on_start = true` (env `LOCAL_SPARK_WARM_ON_START`).

9. **Files — a real local mirror, not a shim or FUSE.** `/lakehouse/default/Files`
   is a symlink/junction to `<mirror_root>/<ws>/<lh>/Files`; only configured
   subtrees sync (`[files] sync`), writes stay local under the write policy,
   push is writethrough-only. The global path is guarded by a lockfile and
   never clobbered; `LOCAL_SPARK_FILES_ROOT` is the always-available fallback.
   `run_notebook` repoints the link for a notebook whose default lakehouse
   differs and restores it afterwards.

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

## Cross-platform notes (Windows validated end to end)

- **Python 3.11 is pinned** (`>=3.11,<3.12`). It matches Fabric Runtime 1.3
  (Spark 3.5 / Delta 3.2 / Python 3.11), and pyspark 3.5.0's Python workers
  **crash on Windows under 3.12** ("Python worker exited unexpectedly") — proven
  with vanilla pyspark, so it is not our bug. Linux tolerates 3.12; Windows does
  not. Don't raise this ceiling without re-testing Windows `mapPartitions`.
- **The worker must not inherit the server's stdin** (`stdin=subprocess.DEVNULL`
  in `worker_client`). Under an MCP stdio server that handle is the client's
  pipe; inheriting it deadlocks the child during interpreter startup on Windows
  — it never reaches `__main__`, so the worker never connects back.
- **`spark.jars` must be a `file://` URI** (`fabric._as_file_uri`). A bare
  Windows path (`C:\...`) is parsed by Spark/Hadoop as a URI whose scheme is the
  drive letter.
- Spark's Python workers use `preferred_python()` (the env's own interpreter),
  since under uv/uvx the running interpreter may be the BASE managed python,
  which lacks the venv's site-packages.
- Tests must not interpolate raw filesystem paths into generated code —
  `C:\Users\...` becomes an invalid escape. Use `Path.as_posix()`.
- **Always pass `encoding="utf-8"` to `read_text`/`write_text`/`open`.** Windows
  defaults to cp1252, and Delta commit JSON (among others) contains non-ASCII
  bytes; the resulting `UnicodeDecodeError` silently mis-classified every
  shadow as `written` on Windows.

## Environment (verified on this machine)

- **`uv`** is the package manager (`uv 0.10.7`). The reference uses
  `uv pip install -e .` and a `src/` layout with `pyproject.toml`.
- **Python:** repo targets **3.11** (see the cross-platform notes above for why);
  the default `python3` on PATH is conda's 3.11.5 — use a uv venv to pin.
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

- IPython's `InteractiveShell.instance()` is a **process-wide singleton**. One
  engine per worker process in production, but tests that build several
  `SparkEngine`s in one process share the shell and its `user_ns` — anything
  installed into the namespace must be assigned, not `setdefault`ed, or a later
  engine keeps the earlier engine's objects (this bit the `notebookutils` shim).

- Spark sessions are heavy and slow to start; the MCP server holds one alive
  across tool calls — that persistence is the whole product. Design startup,
  reset, and error recovery around a single long-lived session per server.
- Keep the local session's behavior honest about Fabric: same Delta version,
  same catalog shape, so code the agent proves out locally transfers.
</content>
</invoke>
