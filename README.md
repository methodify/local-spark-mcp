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

Version 0.2.x: the core session plus **notebook parity** — a Fabric notebook
from the Git export runs unmodified against real OneLake data, in a sandbox by
default. Validated live on Linux/WSL and Windows. See `CLAUDE.md` for the
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

## Working like a Fabric notebook

- **Tables by name.** Lakehouses are Spark databases; `spark.table("dataverse.custtable")`,
  `spark.sql`, `saveAsTable`, `INSERT`, `MERGE`, and `DeltaTable.forName` resolve
  `<lakehouse>.<table>` on first touch, with no mount step. Set
  `[lakehouses] default` (env `LOCAL_SPARK_DEFAULT_LAKEHOUSE`) and unqualified
  names resolve against it, as on Fabric.
- **Write policy** (`[runtime] write_mode`, env `LOCAL_SPARK_WRITE_MODE`).
  `sandbox` (default): nothing reaches OneLake — a table you write becomes a
  local Delta shallow clone (metadata only, so reads stay live and the first
  write is quick) and new tables land locally; later reads in the session see
  them. `readonly`: sandbox plus refusal of table creation and `DeltaTable.forName`.
  `writethrough`: writes go to OneLake. `shadow_status` lists the local shadows
  with a state: `read` (materialized by a read, unchanged) or `written` (has
  local writes); `discard_shadow` resets them, or only the `read` or `written`
  ones with `only=`. Shadows are session-scoped unless `persist_shadow = true`.
- **`run_notebook`** runs a notebook from its Git `.py` source cell by cell in
  the persistent namespace, with cell selection (`"0-4,7"`), parameters
  (applied after the PARAMETERS CELL), and the notebook's own default lakehouse.
  `%pip` / `!pip` / `%run` lines are reported, not run — bring libraries in with
  `uvx --with`. `[notebooks] root` (env `LOCAL_SPARK_NOTEBOOKS_ROOT`) lets you
  address notebooks by Fabric display name.
- **`notebookutils` / `mssparkutils`** are importable: `credentials.getSecret`
  (Key Vault), `variableLibrary.getLibrary` (Fabric REST), `fs.ls` / `fs.exists`
  / `fs.mount`, `notebook.run` / `runMultiple` / `exit`, `session.stop`,
  `runtime.context`. Other members raise `NotImplementedError` naming the member.
- **`/lakehouse/default/Files` is a real directory** — a link to a local mirror
  of the default lakehouse's `Files/`, so `open`, `os.listdir`, subprocesses,
  and native libraries reading a data directory all work. List the subtrees to
  pull under `[files] sync` (env `LOCAL_SPARK_FILES_SYNC`) — subtrees like
  `lib/` or single files like `_dwlib_hydrate_options.txt`; unchanged files are
  skipped, so a 2 GB tree downloads once. Writes land in the mirror; `sync_files`
  pulls more on demand and pushes only in `writethrough`. `Tables/` is never
  mirrored. The mirror lives under `~/.local-spark/lakehouses/<workspace-id>/<lakehouse-id>/Files`
  (env `LOCAL_SPARK_MIRROR_ROOT`), shared by every project that uses that
  lakehouse, and `LOCAL_SPARK_FILES_ROOT` names it for code that avoids the
  global path.

### The `/lakehouse` path

The link is one global path per machine, so a lockfile records which session
owns it and the server refuses to repoint it while another live session holds
it for a different lakehouse (session_info reports this, and the mirror path
still works).

- **Linux and WSL:** `/lakehouse` must exist and be writable by you. One-time
  setup: `sudo mkdir /lakehouse && sudo chown $USER /lakehouse`. If `/lakehouse`
  is already a symlink into a directory you own, the server uses it as is. The
  server never creates `/lakehouse` itself; it reports the command and runs
  without the link.
- **Windows:** `C:\lakehouse\default` is a directory junction, created without
  elevation. A path beginning with `/` resolves against the current drive, so
  `/lakehouse/default/Files` works when the session runs from `C:`.

## Configuration

Configuration lives in a `local-spark.toml` file in the working directory (see
`local-spark.example.toml`), discovered by walking up from where the server is
launched. Environment variables (`LOCAL_SPARK_*`) override individual settings —
convenient in the MCP `env` block above when you don't want a file. With no
workspace configured the server runs local-only (no Fabric). Auth is ambient via
`az login`, so nothing in the config is secret.

## License

Apache License 2.0. See `LICENSE`, and `NOTICE` for the bundled third-party
components (Apache Hadoop winutils).
