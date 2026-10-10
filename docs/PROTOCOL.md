A schema-enabled default lakehouse is the session's **current catalog** (`USE
<lakehouse>`, at its default schema), as on Fabric, so `t`, `dbo.t`, and
`<lakehouse>.dbo.t` all resolve. The catalog passes two kinds of name through to
the session catalog untranslated: Delta path identifiers (`delta.\`/abs/path\``)
and a one-part namespace that is not one of its schemas but is a session
database (another lakehouse, so `customer.sources_name` keeps working while
`test` is current). Everything the worker runs on its own behalf
(materialization in the catalog jar, mounts, shadow management, the notebook
runner's `USE`) is `spark_catalog.`-qualified, so it works whatever is current.
A plain default lakehouse is `USE spark_catalog.<lakehouse>` as before.

# Worker protocol

The MCP server runs a worker subprocess that holds the SparkSession and the
Python namespace, and talks to it over a localhost TCP socket. Hosts other than
the MCP server (Cobalt SQL Works embeds the worker this way) may use the same
protocol. It is a supported interface from 0.3.5 on. This document describes
protocol version 2 (0.4.0); `protocol_version` is in the `init` and `info`
replies and in `profiles.json`. Version 2 is a superset of version 1: the
framing is unchanged, and a client that never asks for streaming or Arrow sees
exactly the version-1 behavior.

## Transport

Spawn `python -m local_spark_mcp.worker --port N` from an environment that has
one runtime profile installed (`local-spark-mcp[fabric-2.0]`, say). The worker
connects to `127.0.0.1:N`, so the host listens first. Give it no stdin
(`DEVNULL`); its stdout and stderr carry Spark and JVM logs.

Every frame is a 4-byte big-endian length followed by that many bytes of UTF-8
JSON. The worker handles one request at a time, in order; a long cell holds the
data socket until it finishes. Version 2 adds three things on top:

- **Binary payloads.** A reply whose JSON carries `"binary": [n1, n2, …]` is
  followed immediately by that many raw blobs of those byte sizes. Used for
  Arrow IPC streams (`run_sql` with `arrow: true`, and `display(df)` in a cell).
- **Event frames.** A `run_code` request with `"stream": true` receives
  `{"id", "event": "stdout" | "stderr", "text"}` frames as the cell writes (one
  per line, or per 4 KB), then the normal reply. Frames with an `event` key and
  no `ok` key are events; a client not streaming never sees them.
- **A control socket.** Spawn the worker with `--control-port M` as well; it
  connects to that port second. The control connection carries the same framing
  and is served by its own thread, so it answers while a cell runs:
  `interrupt`, `ping`, `status`, `preload_status`, and `drop_context`. `status` returns
  `{initialized, cell_running, preload, idle_s, last_activity, cell}` where
  `idle_s` is the time since the last cell, query, or preload ended (`null`
  while one runs) and `cell` is `null` when idle,
  else `{method, elapsed_s, active_jobs, jobs, interrupt_requested}` (`active_jobs`
  is Spark's active job count and `jobs` up to five `{id, name, description,
  group}` entries, `name` being the call site such as `collect at <stdin>:1` and
  `description` what `job_description` on the call (or `setJobDescription` in the cell) set; both read over
  the control thread's own JVM connection, absent if the JVM did not answer). Replies never carry blobs or events.

Request:

```json
{"id": 7, "method": "run_code", "params": {"code": "print(1)"}}
```

Reply:

```json
{"id": 7, "ok": true, "result": {...}, "fatal": false}
{"id": 7, "ok": false, "error": "ValueError: ...", "traceback": "...", "fatal": false}
```

`fatal: true` means the JVM is gone (driver out of memory, killed, or a cell
whose failure a liveness probe confirmed): discard the worker and spawn a new
one. The MCP server does exactly that and reports it on the next result.

## Methods

| Method | Params | Result |
|---|---|---|
| `healthcheck` | `profile?` | Works before `init`: versions, `protocol_version`, profile verdict, JDK and winutils resolution, jar validity (`healthcheck.healthcheck()` shape). |
| `init` | `SparkEngine` keyword arguments (below) plus `profile?` | The `info` dict plus `profile_warnings`. Runs `check_profile` first and fails with its message when the installed stack cannot serve the declared profile or would crash its Python workers on this platform. |
| `ping` | | `{}` |
| `run_code` | `code`, `stream?`, `capture_result?`, `job_description?`, `context?` | `ExecResult`: `ok`, `stdout`, `stderr`, `error`, `traceback`, `execution_count`, `notices`, `interrupted`, `displays` (one `{kind: "arrow", source, columns, row_count, truncated, limit, arrow_bytes}` per `display(df)` (`source: "display"`) and, with `capture_result: true`, for a Spark or pandas DataFrame that is the cell's last expression (`source: "result"`, same row cap; stdout still carries the repr); blobs in the same order) |
| `run_sql` | `sql`, `limit?`, `arrow?`, `job_description?`, `context?`, `stream?` / `batch_rows?` | `SqlResult`: `columns`, `rows`, `row_count`, `truncated`, `limit`, `notices`, `elapsed_s`, `metrics` (DML, see **Commit metrics**); with `arrow: true`, `rows` is empty and `arrow` = `{arrow_bytes, row_count, truncated, limit, columns}` with one blob following; with `stream: true` (or `batch_rows`), the result streams as `{"id", "event": "batch", "rows", "batch", "partition", "arrow_bytes", "binary": [n]}` frames each followed by one self-contained Arrow IPC stream (schema + one batch of about `batch_rows` rows), partition by partition in query order, and the reply carries `row_count`, `batches`, `elapsed_s`, `arrow.streamed: true` and no rows; no `limit` applies unless one is passed, and when one does, `truncated: true` says the result had more rows (a `limit + 1` probe, never sent); interruptible on the control socket |
| `run_notebook` | `path`, `cells?`, `stop_on_error?`, `default_lakehouse?`, `parameters?`, `context?`, `isolated?` (a throwaway context for this run, dropped after; result carries `isolated: true` and the context id) | per-cell results (see `engine.run_notebook`) |
| `info` | | session snapshot: versions, databases, lakehouses, write mode, shadows, profile, `java_home`, `python`, `hadoop_home`, `ivy_dir`, `preload`, `started_at`, `protocol_version`, … |
| `mount_table` | `lakehouse`, `table` (`t` or `schema/t`, as `list_tables` and `preload` spell them) | materialize one table now; returns the session `database` it landed in |
| `mount_tables` | `lakehouse`, `tables` | materialize many in parallel; `mounted`, `failed`, `seconds` per table |
| `preload` | `lakehouses?` (names, `["all"]`, or `{"lakehouse": ["t1", "dbo/t2"]}` for explicit tables with no listing), `workers?` | start background eager population; returns status at once. Failures appear in `preload_status` and on the worker's stderr, never as a cell notice |
| `create_context` | `id`, `default_lakehouse?`, `default_schema?`, `name?` | a new isolated REPL in the same JVM (see **Contexts**); `name` is the display name (the notebook's title), used as the Spark job group description; returns `{id, name, default_lakehouse, default_schema, current_database, current_catalog, created_at, cells, last_activity, idle_s}` |
| `drop_context` | `id`, `force?` | release its namespace and session (`default` cannot be dropped). On the data socket the request waits behind a running cell, so the context is idle when it runs; on the **control socket** `force: true` interrupts a running cell and drops the context when it ends (`{dropped: false, scheduled: true}`), otherwise `{dropped: true}` |
| `register_lakehouse` | `lakehouse` (`{name, id, workspace_id, schemas?, default_schema?, detect_schemas?}`, as in `init`) | attach a lakehouse after start (its own workspace is fine): session database, schema catalog, first-touch resolution, shadows under the shared root |
| `unregister_lakehouse` | `name` | detach it: databases dropped from the session catalog (shadow files stay and re-link on re-registration), confs removed |
| `list_tables` | `lakehouse` | table entries from OneLake storage (`t` for `Tables/t`, `schema/t` for `Tables/schema/t`), not the Fabric REST endpoint, so schema-enabled lakehouses work |
| `preload_status` | | `state` (`idle` / `running` / `done` / `failed`), per-lakehouse progress, counts, `elapsed_s` |
| `wait_preload` | `timeout?` | block until done (or timeout); returns status |
| `table_features` | `lakehouse`, `tables` | Delta protocol features per table |
| `sync_files` | `paths?`, `direction?`, `lakehouse?` | Files mirror pull/push |
| `mirror_status` | | per lakehouse: `mirror_dir`, `pulled`, `fetched_files`, `fetched_bytes`, `fetched` (first 200), `local_files`, `local_bytes`; plus `total_files`, `total_bytes` |
| `clear_mirror` | `lakehouse?`, `paths?` | delete mirror contents (one lakehouse's subtrees or files, one lakehouse, or all); returns `removed` |
| `shadow_status` | | write mode and shadowed tables with `state`, `version`, `cloned_at` (first commit, ISO-8601 UTC) and `registered` (known to this session's catalog yet; a persisted clone from an earlier session is listed before any touch) |
| `discard_shadow` | `only?` (`read` / `written`), `table?` | drop shadows |
| `restore_shadow` | `table`, `version?` | rewind one shadow's local log |
| `shutdown` | | reply `{}` then exit |

`init` keyword arguments (all optional): `driver_memory`, `extra_configs`,
`env`, `java_home`, `hadoop_home`, `onelake` (`{endpoint, secret, jar_path}`:
the host must run a token endpoint, see `token_server.py`), `lakehouses`
(`[{name, id, workspace_id}]`), `default_lakehouse`, `write_mode`,
`persist_shadow`, `state_root`, `notebooks_root`, `files_sync`, `mirror_root`,
`default_sql_limit`, `preload` (list of lakehouse names, `["all"]`, or the
`{lakehouse: [tables]}` form), `preload_workers` (default 32), `extra_jars`,
`extra_packages`, `files_mode` (`"mirror"`, the default, or `"lazy"`; see
**Files**). Each `lakehouses` entry may add `schemas` (`["dbo", …]`),
`default_schema` (default `dbo`), and `detect_schemas` (default true: the worker
lists `Tables/` once at start and treats folders of Delta tables as schemas).

## Contexts

One JVM, one isolated REPL per notebook, the way Fabric's high-concurrency
sessions work. A context is its own Python namespace plus its own
`spark.newSession()`: separate variables, imports, temp views, SQL conf,
current database, and UDF registry; shared `SparkContext`, catalog
(databases, tables, clones), cached data, and jars. `init` creates the
`default` context, which is the root session and the namespace a version-2
host already uses, so hosts that never pass `context` see no change.
`create_context(id, default_lakehouse?, default_schema?)` sets the new
session's current database from the lakehouse (`spark_catalog.<lh>`, or
`<lh>__<schema>` for a schema-enabled one) and re-applies the worker's runtime
confs (lakehouse ids, schema catalogs) to it; `register_lakehouse` later
pushes to every context. `run_code`, `run_sql`, and `run_notebook` take
`context`; `interrupt` and `status` on the control socket take an optional
`context` (`status.cell.context` and `context_name` name the running one, `status.contexts`
lists them, `status.dropping` names contexts whose forced drop is scheduled);
`info.contexts` (with per-context `last_activity`, `idle_s`, and `dropping`) and
`info.active_context` describe them. Each context's cells run under the Spark
job group `<context id>` with the context's `name` as description (overridden
per cell by `job_description`), so the Spark UI groups a notebook's jobs. Execution is
sequential across contexts (one request loop); the API does not preclude a
later concurrent mode. `display`, `capture_result`, and `notebookutils` work in
every context; `notebookutils.notebook.run` runs in the calling context.
`/lakehouse/default/Files` is one link per process and follows the session's
default lakehouse; with `files_mode: "lazy"` Spark's `Files/` follows the
context's default lakehouse (see **Files**).

`init` and `info` carry `features`, the additive capabilities of this worker:
`arrow`, `streaming`, `interrupt`, `capture_result`, `job_description`,
`register_lakehouse`, `contexts`, `files_lazy`, `files_lazy_python`, `sql_stream`, `commit_metrics`, `catalog_listing`.

## Files

`files_mode` decides what Spark's relative `Files/…` means.

- `"mirror"` (default): the local mirror behind `/lakehouse/default/Files`, populated
  by `files_sync` and `sync_files`; a relative `Files/x` in Spark resolves there
  through the local filesystem's working directory.
- `"lazy"`: the default lakehouse's OneLake `Files/`, read directly. The session's
  default filesystem is `lakehouse://<workspace-id>@<lakehouse-id>.onelake.dfs.fabric.microsoft.com`
  (`ch.fs.LakehouseFileSystem` in the catalog jar, a wrapper over ABFS with the
  lakehouse as root), so `spark.read.csv("Files/x")`, `binaryFile.\`Files/x\``,
  and Delta paths under `Files/` stream from OneLake with the host token and
  nothing is mirrored. Under `write_mode` sandbox or readonly the filesystem
  refuses anything that would modify OneLake (`create`, `mkdirs`, `delete`,
  `rename`, …) with `write_mode=sandbox: … refused because it would modify
  OneLake. Write to /lakehouse/default/Files (the local mirror) instead, or start
  the runtime with write_mode = writethrough`; writethrough passes writes through.
  Each context's session has its own default filesystem, so a context's `Files/`
  follows *its* default lakehouse (`info.contexts[].files_fs`); `run_notebook`
  switches it with the notebook's default lakehouse and restores it after. Read it
  with `spark.conf.get("fs.defaultFS")` in the session: the SparkContext's
  `hadoopConfiguration().get("fs.defaultFS")` still says `file:///`, because the
  per-session value lives in the session's SQL conf, which Spark copies into the
  Hadoop configuration it actually uses for that session.
  Shadows and the warehouse are `file:` URIs, so they resolve the same under any
  default filesystem.

  Python IO under `"lazy"` (0.6.1): the worker hooks `open` (and `io.open`),
  `os.stat`/`lstat`, `os.listdir`, `os.scandir`, and the local mutators
  (`mkdir`, `remove`, `unlink`, `rmdir`, `rename`, `replace`) for paths under
  `/lakehouse/` (a Windows drive prefix is ignored) and registered mount points
  only; every other path goes straight to the original function. `open` of a
  file fetches it into the mirror on first use and re-fetches when OneLake has a
  newer copy; `os.stat` and the `os.path` / `pathlib` predicates answer from
  OneLake metadata for files not yet fetched; listings merge the OneLake
  directory with local-only files; writes land in the mirror and are pushed on
  close only in writethrough. `/lakehouse/default` means the **active
  context's** default lakehouse. A path that exists nowhere raises a
  `FileNotFoundError` naming the `sync_files(paths=[...], lakehouse=...)` call
  that would pull its folder. Native readers (DuckDB, Arrow `OSFile`, …) open
  files from C and bypass the hooks: they need the file in the mirror first
  (`sync_files`), and their own error is the OS's. `mirror_status` reports, per
  lakehouse, the pulled subtrees, lazily fetched files and bytes, and the local
  size; `clear_mirror(lakehouse?, paths?)` deletes mirror contents (unpushed local
  writes included; the next open fetches again). `info.files_hooks` says whether
  the hooks are installed.

## Tokens

The worker holds no Azure credential of its own when `onelake.endpoint` is set.
The JVM asks that endpoint for OneLake storage tokens; since 0.4.1 the Python
side asks it for every other scope too (`GET <endpoint>?scope=<scope>` with the
same `X-Token-Secret` header; the body is the bearer token). Scopes requested:
`https://api.fabric.microsoft.com/.default` (table discovery fallback, Variable
Libraries, notebook discovery), `https://storage.azure.com/.default` (Files
mirror, `notebookutils.fs` on `abfss://`), and `https://vault.azure.net/.default`
(`credentials.getSecret`). A host that serves only the storage scope breaks
those features with a clear HTTP error, not a credential-chain message in a
cell. Without an endpoint the worker falls back to `DefaultAzureCredential`.

## Schema-enabled lakehouses

A lakehouse with `Tables/<schema>/<table>` gets one session database per
schema, `<lakehouse>__<schema>`, plus a V2 catalog named after the lakehouse
(`ch.fs.OneLakeSchemaCatalog`) so the Fabric spelling works: `test.dbo.holidays`,
`SHOW NAMESPACES IN test`, `USE test` (selects `default_schema`). The catalog
delegates every operation to the session catalog, so first-touch resolution,
shadows (`<schema>.<table>` under the lakehouse's shadow dir), and the write
policy apply unchanged. Top-level `Tables/<table>` entries stay reachable as
`test.table`. `info.lakehouse_schemas` maps lakehouse to schemas.

The session's current catalog stays `spark_catalog`: a default lakehouse with
schemas selects `spark_catalog.<lakehouse>__<default_schema>`, so unqualified
names resolve to its default schema as on Fabric. `USE <lakehouse>` (making the
V2 catalog current) is the user's choice, and everything the worker runs on its
own behalf (materialization in the catalog jar, mounts, shadow management, the
notebook runner's `USE`) is `spark_catalog.`-qualified, so it works either way.
Spark itself behaves differently once a V2 catalog is current: two-part names
resolve inside that catalog, and `delta.\`path\`` fails with
`UNSUPPORTED_DATASOURCE_FOR_DIRECT_QUERY`; write `spark_catalog.delta.\`path\``.

## Interrupt (control socket)

`interrupt` cancels every Spark job (`SparkContext.cancelAllJobs`, issued from
the control thread over its own JVM connection) and raises `KeyboardInterrupt`
in the cell's thread; the reply comes back as soon as the cancel returns:
`{"interrupted", "state": "interrupting" | "idle", "detail", "method",
"elapsed_s"}`. The in-flight `run_code` then returns `ok: false`, `interrupted:
true`, `error` starting with `KeyboardInterrupt`; an in-flight `run_sql` fails
with `ok: false`, `interrupted: true`, `fatal: false`. Session state (variables,
shadows) is kept. When nothing is running the result is `{"interrupted": false,
"reason": "idle: …"}`. After the first cancel a watchdog keeps cancelling until
the cell ends, so a job that was still being planned when you interrupted does
not run to completion.

The worker replaces pyspark's own SIGINT handler with Python's default one.
pyspark's handler calls `cancelAllJobs()` from the interrupted thread, and when
the interrupt lands inside a py4j read (a cell blocked in Spark) that call
re-enters the connection the thread is reading on: `RuntimeError: reentrant call
inside <_io.BufferedReader>`, then `Py4JNetworkError`, with py4j's error logging
on the cell's stderr. The control thread already cancels the jobs, so the cell's
thread only needs the `KeyboardInterrupt`. py4j's log lines about the connection
it tears down during an interrupt are filtered out of the cell's stderr.

py4j closes the interrupted thread's JVM connection when the `KeyboardInterrupt`
lands inside a call, so the cell's exception may be a `Py4JNetworkError` rather
than `KeyboardInterrupt` (the result is still `interrupted: true`). The worker
then re-establishes the connection before replying, retrying because the first
reconnect is sometimes answered with an empty line by a healthy gateway, and an
interrupted result is flagged `fatal` only when that probe fails, never from
connection text in the error.

Limits: a cell inside a long JVM call returns when its job is cancelled
(seconds); a tight loop inside a C extension cannot be interrupted; and on
**Windows** a cell that is sleeping or blocked in pure Python (not in Spark)
returns only when that blocking call ends, since the interrupt there is queued
rather than delivered (on Linux a real `SIGINT` wakes it at once). Spark work,
the case that matters, stops within seconds on both platforms.

## Arrow

`display(df)` is pre-bound in the namespace, as on Fabric: a DataFrame becomes
an Arrow IPC stream attached to the cell result (up to `default_sql_limit` rows,
or `display(df, limit=N)`); anything else is printed. `run_sql(arrow=true)`
returns the result set the same way. Spark 4 uses `DataFrame.toArrow`; Spark 3.5
uses `_collect_as_arrow`. Types survive (decimals, timestamps, nested types)
where the JSON rows flatten them.

## Commit metrics

A `run_sql` statement whose first keyword is `INSERT`, `UPDATE`, `DELETE`,
`MERGE`, or `CREATE [OR REPLACE] TABLE … AS` carries `metrics`:
`{affected_rows, inserted?, updated?, deleted?, source, operation?, table?}`.
Delta returns the counts for UPDATE, DELETE, and MERGE as the statement's result
frame (`num_affected_rows`, …; `source: "result"`); INSERT and CTAS return an
empty frame, so those come from the target's latest commit (`DESCRIBE HISTORY …
LIMIT 1`, `operationMetrics.numOutputRows`; `source: "history"`). A failure to
read them yields `metrics.error` and never fails the statement. The MCP
`run_sql` tool prints `N row(s) affected`.

## Catalog listing

`SHOW TABLES [IN <lakehouse>]` and `SHOW TABLES IN <lakehouse>.<schema>` list
what the lakehouse has on OneLake (every `Tables/<t>`, or `Tables/<schema>/<t>`),
OneLake-cased, merged with what the session catalog already holds (clones, new
local tables, views), without mounting anything; resolution still happens on
first touch. The listing is cached for 60 s per lakehouse (new tables appear
after that). `spark.catalog.listTables(...)` sees the same names, but Spark's
`CatalogImpl` loads every table it lists to fill in its type and description,
and loading is first touch here, so that call materializes every table of the
lakehouse (one clone each). Use `SHOW TABLES`, or the `list_tables` method, to
list without touching.

## Versions and profiles

`profiles.json` at the repo root (and `python -m local_spark_mcp.profiles
--json` for the installed package) describes every runtime profile: pins,
Python per platform, Java majors and preference, Scala line, hadoop-azure,
session confs, and since 0.8.0 `python_packages` (Fabric's notebook-facing
Python packages at the runtime's versions, name → version) with
`python_packages_source` (the `synapse-spark-runtime` file, commit, and URL
they were taken from) and `python_packages_fallbacks` (name → `{marker,
requirement, reason}`: where Fabric's exact pin cannot be installed on some
Python, the requirement to use there instead; markers are
`python_version <op> 'X.Y'` only, so a host can evaluate them without a
resolver, and a version that satisfies the fallback counts as installed, not
mismatched); the top level carries `python_packages_excluded` (name → reason). `healthcheck` reports `fabric_packages` (`installed`, `mismatched`,
`missing`, `complete`) for the detected profile. The installer is
`python -m local_spark_mcp.fabric_packages install [--profile] [--python] [--json]`:
one resolution of the roster, then per package for whatever failed, with
`installed`, `skipped` (requirement + reason), `mode`, and `status` in the
JSON result; exit code 2 when something was skipped. `python -m local_spark_mcp.warm [--ivy DIR]` resolves a
profile's Spark packages ahead of the first session.
