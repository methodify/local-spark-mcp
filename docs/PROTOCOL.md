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
  `interrupt`, `ping`, `status`, and `preload_status`. `status` returns
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
| `run_code` | `code`, `stream?`, `capture_result?`, `job_description?` | `ExecResult`: `ok`, `stdout`, `stderr`, `error`, `traceback`, `execution_count`, `notices`, `interrupted`, `displays` (one `{kind: "arrow", source, columns, row_count, truncated, limit, arrow_bytes}` per `display(df)` (`source: "display"`) and, with `capture_result: true`, for a Spark or pandas DataFrame that is the cell's last expression (`source: "result"`, same row cap; stdout still carries the repr); blobs in the same order) |
| `run_sql` | `sql`, `limit?`, `arrow?`, `job_description?` | `SqlResult`: `columns`, `rows`, `row_count`, `truncated`, `limit`, `notices`; with `arrow: true`, `rows` is empty and `arrow` = `{arrow_bytes, row_count, truncated, limit, columns}` with one blob following |
| `run_notebook` | `path`, `cells?`, `stop_on_error?`, `default_lakehouse?`, `parameters?` | per-cell results (see `engine.run_notebook`) |
| `info` | | session snapshot: versions, databases, lakehouses, write mode, shadows, profile, `java_home`, `python`, `hadoop_home`, `ivy_dir`, `preload`, `started_at`, `protocol_version`, … |
| `mount_table` | `lakehouse`, `table` | materialize one table now |
| `mount_tables` | `lakehouse`, `tables` | materialize many in parallel; `mounted`, `failed`, `seconds` per table |
| `preload` | `lakehouses?` (names, `["all"]`, or `{"lakehouse": ["t1", "dbo/t2"]}` for explicit tables with no listing), `workers?` | start background eager population; returns status at once. Failures appear in `preload_status` and on the worker's stderr, never as a cell notice |
| `register_lakehouse` | `lakehouse` (`{name, id, workspace_id, schemas?, default_schema?, detect_schemas?}`, as in `init`) | attach a lakehouse after start (its own workspace is fine): session database, schema catalog, first-touch resolution, shadows under the shared root |
| `unregister_lakehouse` | `name` | detach it: databases dropped from the session catalog (shadow files stay and re-link on re-registration), confs removed |
| `list_tables` | `lakehouse` | table entries from OneLake storage (`t` for `Tables/t`, `schema/t` for `Tables/schema/t`), not the Fabric REST endpoint, so schema-enabled lakehouses work |
| `preload_status` | | `state` (`idle` / `running` / `done` / `failed`), per-lakehouse progress, counts, `elapsed_s` |
| `wait_preload` | `timeout?` | block until done (or timeout); returns status |
| `table_features` | `lakehouse`, `tables` | Delta protocol features per table |
| `sync_files` | `paths?`, `direction?`, `lakehouse?` | Files mirror pull/push |
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
`extra_packages`. Each `lakehouses` entry may add `schemas` (`["dbo", …]`),
`default_schema` (default `dbo`), and `detect_schemas` (default true: the worker
lists `Tables/` once at start and treats folders of Delta tables as schemas).

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

## Versions and profiles

`profiles.json` at the repo root (and `python -m local_spark_mcp.profiles
--json` for the installed package) describes every runtime profile: pins,
Python per platform, Java majors and preference, Scala line, hadoop-azure,
session confs. `python -m local_spark_mcp.warm [--ivy DIR]` resolves a
profile's Spark packages ahead of the first session.
