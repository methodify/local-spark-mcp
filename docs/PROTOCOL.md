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
  `interrupt`, `ping`, `status` (`{cell_running, initialized}`), and
  `preload_status`. Replies never carry blobs or events.

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
| `healthcheck` | `profile?` | Works before `init`: versions, profile verdict, JDK and winutils resolution, jar validity (`healthcheck.healthcheck()` shape). |
| `init` | `SparkEngine` keyword arguments (below) plus `profile?` | The `info` dict plus `profile_warnings`. Runs `check_profile` first and fails with its message when the installed stack cannot serve the declared profile or would crash its Python workers on this platform. |
| `ping` | | `{}` |
| `run_code` | `code`, `stream?` | `ExecResult`: `ok`, `stdout`, `stderr`, `error`, `traceback`, `execution_count`, `notices`, `interrupted`, `displays` (one `{kind: "arrow", columns, row_count, truncated, limit, arrow_bytes}` per `display(df)`, blobs in the same order) |
| `run_sql` | `sql`, `limit?`, `arrow?` | `SqlResult`: `columns`, `rows`, `row_count`, `truncated`, `limit`, `notices`; with `arrow: true`, `rows` is empty and `arrow` = `{arrow_bytes, row_count, truncated, limit, columns}` with one blob following |
| `run_notebook` | `path`, `cells?`, `stop_on_error?`, `default_lakehouse?`, `parameters?` | per-cell results (see `engine.run_notebook`) |
| `info` | | session snapshot: versions, databases, lakehouses, write mode, shadows, profile, `java_home`, `python`, `hadoop_home`, `ivy_dir`, `preload`, `started_at`, `protocol_version`, … |
| `mount_table` | `lakehouse`, `table` | materialize one table now |
| `mount_tables` | `lakehouse`, `tables` | materialize many in parallel; `mounted`, `failed`, `seconds` per table |
| `preload` | `lakehouses?` (names or `["all"]`), `workers?` | start background eager population; returns status at once |
| `preload_status` | | `state` (`idle` / `running` / `done` / `failed`), per-lakehouse progress, counts, `elapsed_s` |
| `wait_preload` | `timeout?` | block until done (or timeout); returns status |
| `table_features` | `lakehouse`, `tables` | Delta protocol features per table |
| `sync_files` | `paths?`, `direction?`, `lakehouse?` | Files mirror pull/push |
| `shadow_status` | | write mode and shadowed tables with `state` and `version` |
| `discard_shadow` | `only?` (`read` / `written`), `table?` | drop shadows |
| `restore_shadow` | `table`, `version?` | rewind one shadow's local log |
| `shutdown` | | reply `{}` then exit |

`init` keyword arguments (all optional): `driver_memory`, `extra_configs`,
`env`, `java_home`, `hadoop_home`, `onelake` (`{endpoint, secret, jar_path}`:
the host must run a token endpoint, see `token_server.py`), `lakehouses`
(`[{name, id, workspace_id}]`), `default_lakehouse`, `write_mode`,
`persist_shadow`, `state_root`, `notebooks_root`, `files_sync`, `mirror_root`,
`default_sql_limit`, `preload` (list of lakehouse names or `["all"]`),
`preload_workers` (default 32), `extra_jars`, `extra_packages`.

## Interrupt (control socket)

`interrupt` cancels every Spark job (`SparkContext.cancelAllJobs`) and raises
`KeyboardInterrupt` in the cell's thread. The in-flight `run_code` /
`run_sql` then returns `ok: false`, `interrupted: true`, `error` starting with
`KeyboardInterrupt`; session state (variables, shadows) is kept. When nothing is
running the result is `{"interrupted": false, "reason": "idle: …"}`. After the
first cancel a watchdog keeps cancelling until the cell ends, so a job that was
still being planned when you interrupted does not run to completion. Limits: a
cell inside a long JVM call returns when its job is cancelled (seconds); a tight
loop inside a C extension cannot be interrupted; and on **Windows** a cell that
is sleeping or blocked in pure Python (not in Spark) returns only when that
blocking call ends, since the interrupt there is queued rather than delivered
(on Linux a real `SIGINT` wakes it at once). Spark work, the case that matters,
stops within seconds on both platforms.

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
