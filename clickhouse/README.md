# decision-query for ClickHouse

The same typed decisions as the SQLite and PostgreSQL modules, as ClickHouse
functions. A worker process, `decision-query-udf`, links the shared engine and
holds one resident model; ClickHouse starts it from an XML declaration, and the
public functions are SQL wrappers around it.

```sql
SELECT id, subject FROM tickets
 WHERE noul(body, 'Does the customer request a refund?', '') > 0.5;

SELECT id, choice(body, 'Which department should handle this?',
                  '["billing", "technical", "sales"]') AS department
  FROM tickets;

SELECT id, JSONExtractFloat(decide(map('subject', subject, 'body', body),
             '{"urgency": {"type": "score", "instructions": "How urgent is the request?",
                           "criteria": ["not urgent", "soon", "immediate"]}}'),
             'urgency', 'score') AS urgency
  FROM tickets;
```

Requires ClickHouse 25.3 or later on a server you manage. ClickHouse Cloud does
not run external executables.

## Install

Four steps; `make clickhouse-install` does the middle two.

1. Build: `make clickhouse DQ_MODEL_DIR=/srv/models/laya` produces
   `build/clickhouse/decision-query-udf`, `decision_query_function.xml` and
   `decision_query.sql`. `DQ_MODEL_DIR` may also be the URL of a System One
   endpoint; `DQ_OPTIONS='{"cuda":false}'` sets the load options (no spaces).
2. Copy the worker into ClickHouse's scripts directory,
   `/var/lib/clickhouse/user_scripts/`, executable by the `clickhouse` user.
3. Copy the XML into `/etc/clickhouse-server/`. ClickHouse reads every
   `*_function.xml` there. The one line to edit later is the `--backend=` value
   in `<command>`.
4. Create the wrappers once: `clickhouse-client --queries-file build/clickhouse/decision_query.sql`.

Then `SELECT dq_backend()` reports `CPU`, `CUDA0` or `remote`. It starts a
worker, loads the model and exits, so it takes a few seconds.

`sudo make clickhouse-install` does steps 2 and 3 under `/` (`CLICKHOUSE_PREFIX`
overrides it). To try the module without installing anything, mount the three
files into the official image:

```sh
docker run --rm -p 9000:9000 \
  -v "$PWD/build/clickhouse/decision-query-udf:/var/lib/clickhouse/user_scripts/decision-query-udf" \
  -v "$PWD/build/clickhouse/decision_query_function.xml:/etc/clickhouse-server/decision_query_function.xml" \
  -v "$PWD/models/laya:/srv/models/laya" clickhouse/clickhouse-server:25.3
clickhouse-client --queries-file build/clickhouse/decision_query.sql
```

The worker links ICU dynamically, so the container needs the ICU major version
of the machine that built it; build inside the same image if it does not start.

## SQL reference

| Function | Returns | Description |
|---|---|---|
| `dq_version()` | String | Module version, e.g. `v0.0.1`. |
| `dq_backend()` | String | Backend of a freshly loaded model (`CPU`, `CUDA0`, `remote`). Slow; for checking a setup. |
| `noul(state, instructions, criteria)` | Nullable(Float64) | Probability that the statement holds. `criteria` is `''` or a JSON object `{"true": "...", "false": "..."}`. |
| `choice(state, instructions, criteria)` | Nullable(String) | The selected option. `criteria` is a JSON array of names or an object of name to description. |
| `score(state, instructions, criteria)` | Nullable(Float64) | Expected ordinal score, 0 through n-1 over a JSON array of level descriptions. |
| `decide(state, questions)` | Nullable(String) | The full answers object as JSON text for a questions object. Use `JSONExtract*` on it. |

Two differences from the other modules:

- There is no `dq_load`. The model is chosen by the `--backend=` value in the
  XML, once per server, because ClickHouse starts the workers itself.
- `noul` always takes three arguments. ClickHouse has no overloading or default
  arguments, so pass `''` for no criteria. A two-argument call fails with
  "expect 3 arguments".

`state` is text, or a `map(...)` or `JSON` value for structured state, which the
model sees as labelled fields. `criteria` and `questions` are JSON text. Any NULL
argument yields NULL without running the model. Invalid JSON raises the same
messages as the other modules, from the wrapper, and the worker is handed NULL
instead of the bad text. Worker failures, such as an unreachable endpoint or a
wrong checkpoint path, fail the query with the worker's message.

## Settings in the XML

| Setting | Shipped value | Why |
|---|---|---|
| `pool_size` | 1 | Each worker holds a model copy. Raise it only with the memory to match. |
| `command_read_timeout` | 600000 ms | Must cover a cold model load. ClickHouse's default is 10 s. |
| `command_write_timeout` | 600000 ms | Symmetric with the read timeout. |
| `max_command_execution_time` | 3600 s | Per block of rows. |
| `stderr_reaction` | throw | The worker writes only error messages to stderr; this makes them the query's error text. Needed on 25.3, whose exit-code error carries no text. |

CPU inference runs at about one row per second and a block is 65,409 rows by
default, so add `SETTINGS max_block_size = 64` to table-scale queries on the CPU.

Two models side by side: duplicate the `dq_decide` block under another name with
another `--backend=`, and wrap it as the SQL file does.

## Tests

```sh
make clickhouse test-clickhouse                          # needs a clickhouse binary on PATH
DQ_MODEL_DIR=models/laya make model cli test-clickhouse  # adds the model-backed and parity tests
```

The suite drives `clickhouse local` against a local fake System One endpoint, so
the model-free tests need no checkpoint. Get a binary with `curl https://clickhouse.com/ | sh`.

## Limitations

- One model copy per pool worker; a pool of one serializes all queries.
- Scalar functions run one forward pass per row; `decide()` batches several
  questions about the same row. Cross-row batching is not available.
- A worker error fails the query and restarts the worker, which reloads the
  model.
- After editing the XML, `SYSTEM RELOAD FUNCTIONS` (or a restart) makes the
  change take effect.
