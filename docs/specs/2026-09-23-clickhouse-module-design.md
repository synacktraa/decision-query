# ClickHouse module design

Date: 2026-09-23. Status: approved in review, awaiting written-spec sign-off.

## Goal

Expose the same typed decisions as the SQLite and PostgreSQL modules from ClickHouse:
`noul`, `choice`, `score` and `decide` over a local checkpoint or an HTTP System One
endpoint, computed by the shared engine so the numbers are identical across databases.

## Decisions

| Decision | Choice | Why |
|---|---|---|
| Integration mechanism | An executable user-defined function (`executable_pool`) | ClickHouse has no loadable plugin API for functions. External executables declared in XML are the supported route. |
| Wiring | One pooled executable function, `dq_decide`, plus SQL wrappers for the public names | Each XML function owns a process pool and each worker holds a model copy. One function keeps one copy resident. |
| `noul` arity | Always three arguments; `''` means no criteria | ClickHouse cannot overload by argument count and has no default arguments. A wrong count fails with "expect 3 arguments. Actual 2", which points at the fix. |
| `criteria` and `questions` type | JSON text in a `String`, as in SQLite | One rule, identical call text across all three databases. |
| `state` type at the XML boundary | `Dynamic` | Text arrives at the worker as a JSON string and a `map(...)` or `JSON` value as an object, with no parsing of strings, so the worker distinguishes them the way the SQLite module distinguishes its JSON subtype. Verified on ClickHouse 26.10.1. |
| Minimum ClickHouse version | 25.3 | `Dynamic` became production-ready and enabled by default in 25.3. From 24.5 to 25.2 it needs `allow_experimental_dynamic_type = 1`, which is documented but not tested. |
| `dq_load` | Not provided | Workers are started by ClickHouse from the XML, so the model is chosen in the XML, not in a query. PostgreSQL's `decision_query.model_dir` setting already offers this style. |
| Setup command line tool | Deferred | Deserves its own effort. This module is designed so that a tool can wrap it later. |

## ClickHouse facts this design relies on

All verified on ClickHouse 26.10.1 with `clickhouse local` unless noted.

- Executable functions cast every argument to the type declared in the XML before writing it to the worker (`castColumnAccurate` in the loader). The argument count is fixed.
- A `Dynamic` argument serializes as the value's own JSON: a string, an object for `Map` and `JSON` values, `null` for NULL. A string that looks like JSON or like a map literal stays a string. `cast_string_to_dynamic_use_inference` defaults to 0.
- `Variant` is not used because `cast_string_to_variant_use_inference` defaults to 1, which parses strings.
- With `executable_pool` and `send_chunk_header = 0`, a worker that answers one line per input line and flushes after each line serves a multi-row block correctly.
- The XML `<command>` may carry flags after the script name with `execute_direct = 1`; the flags arrive in `argv` intact as long as they contain no spaces.
- `CREATE FUNCTION` lambdas take untyped parameters and may call executable functions and other lambdas. None of `noul`, `choice`, `score`, `decide`, `dq_version`, `dq_backend`, `dq_decide`, `dq_question` collides with a built-in.
- `throwIf(condition, 'constant message')` inside `if(...)` raises the message to the client, and a NULL condition does not throw. `format()` does not escape `{{`, so JSON is built with `concat`.
- An executable function is evaluated eagerly, before `if()` picks a branch, so guarding a call with `throwIf` does not stop the worker from receiving the argument. The wrappers hand the worker NULL for any unusable `questions` or `criteria`, which it answers with null and no request, so the `throwIf` message is the only error.
- ClickHouse 26.10 appends a failed worker's stderr to its exit-code error; 25.3 reports only the exit code. `stderr_reaction = throw` makes the message reach the client on both.
- ClickHouse's message for a wrong argument count to a SQL-defined function is "expect 3 arguments. Actual 2" on 25.3 and "Actual: 2" on 26.10, not the built-ins' "passed 2, should be 3".
- `clickhouse local` reads INSERT data from stdin when stdin is a pipe and waits for it to close; the test harness gives it an empty stdin.
- `allow_experimental_dynamic_type`, `allow_experimental_variant_type` and `allow_experimental_json_type` flipped to `true` in 25.3 (settings change history, "production-ready").

## Architecture

```
SELECT noul(body, 'Is this a refund request?', '') FROM tickets
        |  CREATE FUNCTION wrapper, inlined by ClickHouse
        v
dq_decide(body, '{"q":{"type":"noul","instructions":"Is this a refund request?"}}')
        |  executable_pool: one line per row on stdin, JSONEachRow
        v
decision-query-udf --backend=/srv/models/laya          (one process, model resident)
        |  dq::engine::answers(state, questions)         (engine/decision_engine.hpp)
        v
{"result": "{\"q\":{\"noul\":0.93,...}}"}               (one line per row on stdout)
        |  JSONExtractFloat(..., 'q', 'noul') in the wrapper
        v
0.93
```

The worker is an ordinary executable linking `decision-query-engine`. It needs no ClickHouse
headers or libraries, so it builds wherever the SQLite module builds.

## Components

### Directory layout

```
clickhouse/
  CMakeLists.txt                          target decision-query-udf; configures the two templates
  src/main.cpp                            the worker
  config/decision_query_function.xml.in   declares dq_decide and dq_backend_raw
  sql/decision_query.sql.in               CREATE OR REPLACE FUNCTION wrappers
  tests/test-clickhouse.py                unittest suite driving clickhouse local
  README.md
```

Root `CMakeLists.txt` gains `option(DQ_CLICKHOUSE ... ON)` and `add_subdirectory(clickhouse)`.
The Makefile gains `clickhouse`, `clickhouse-install` and `test-clickhouse`, mirroring the
PostgreSQL targets.

### The worker: `decision-query-udf`

Started by ClickHouse, never by a person. Command line:

| Flag | Meaning |
|---|---|
| `--backend=<dir-or-url>` | Required. A checkpoint directory or an `http://` / `https://` endpoint, exactly the first argument of `dq_load`. |
| `--options=<json>` | Optional. The `dq_load` options object. No spaces, because ClickHouse splits the command on spaces. |
| `--print-backend` | Load the model, then answer every input line with the backend name instead of running inference. Used by `dq_backend()`. |

Startup: parse flags, call `dq::engine::instance().load(backend, options)` once. While loading,
file descriptor 2 is redirected to `/dev/null` and restored afterwards, so library chatter
during model load never reaches stderr. A load failure writes the engine's message to stderr
and exits 1.

Loop: read one line from stdin. Parse it as JSON with two keys, `state` and `questions`.

- If `state` or `questions` is `null`: write `{"result": null}`.
- Otherwise: `state` is passed to the engine as received (a JSON string or a JSON object);
  `questions` is JSON text and is parsed. Write `{"result": <the answers object as a JSON string>}`.
- Flush after every line.

Any exception writes `e.what()` as one line to stderr and exits 1. The worker writes nothing
else to stderr. On end of input it exits 0.

### The XML declaration

Two functions, generated from `config/decision_query_function.xml.in` by CMake. The only line
a person edits after install is the `--backend=` value; `make clickhouse DQ_MODEL_DIR=...`
prefills it.

```xml
<functions>
  <function>
    <type>executable_pool</type>
    <name>dq_decide</name>
    <return_type>Nullable(String)</return_type>
    <return_name>result</return_name>
    <argument><type>Dynamic</type><name>state</name></argument>
    <argument><type>Nullable(String)</type><name>questions</name></argument>
    <format>JSONEachRow</format>
    <command>decision-query-udf --backend=@DQ_MODEL_DIR@</command>
    <pool_size>1</pool_size>
    <send_chunk_header>0</send_chunk_header>
    <command_read_timeout>600000</command_read_timeout>
    <command_write_timeout>600000</command_write_timeout>
    <max_command_execution_time>3600</max_command_execution_time>
    <stderr_reaction>throw</stderr_reaction>
  </function>
  <function>
    <type>executable</type>
    <name>dq_backend_raw</name>
    <return_type>String</return_type>
    <return_name>result</return_name>
    <argument><type>UInt8</type><name>dummy</name></argument>
    <format>JSONEachRow</format>
    <command>decision-query-udf --backend=@DQ_MODEL_DIR@ --print-backend</command>
    <command_read_timeout>600000</command_read_timeout>
    <stderr_reaction>throw</stderr_reaction>
  </function>
</functions>
```

`dq_backend_raw` takes a dummy argument so that ClickHouse sends it one input line per
result row; `dq_backend()` is a wrapper that supplies it. It is `executable`, not pooled: each
call starts a process, loads the model, prints the backend name and exits. It costs a few
seconds and holds no memory afterwards. It exists for the "is CUDA in use" check after install.

### The SQL wrappers

Generated from `sql/decision_query.sql.in`; CMake fills in the version. Running the file is
idempotent. The helpers `dq_questions`, `dq_question_with` and `dq_question` return NULL
for unusable input, which is what keeps bad text away from the eagerly evaluated worker.

```sql
CREATE OR REPLACE FUNCTION dq_version AS () -> 'v@DQ_VERSION_TEXT@';

CREATE OR REPLACE FUNCTION dq_backend AS () -> dq_backend_raw(1);

-- The one-question object the scalar functions send. criteria = '' omits the key.
CREATE OR REPLACE FUNCTION dq_question AS (type, instructions, criteria) ->
  if(criteria = '',
     concat('{"q":{"type":"', type, '","instructions":', toJSONString(instructions), '}}'),
     concat('{"q":{"type":"', type, '","instructions":', toJSONString(instructions),
            ',"criteria":', criteria, '}}'));

CREATE OR REPLACE FUNCTION noul AS (state, instructions, criteria) ->
  if(throwIf(criteria != '' AND NOT isValidJSON(criteria), 'noul criteria must be valid JSON'),
     NULL,
     JSONExtractFloat(dq_decide(state, dq_question('noul', instructions, criteria)), 'q', 'noul'));

CREATE OR REPLACE FUNCTION choice AS (state, instructions, criteria) ->
  if(throwIf(NOT isValidJSON(criteria), 'choice criteria must be valid JSON'),
     NULL,
     JSONExtractString(dq_decide(state, dq_question('choice', instructions, criteria)), 'q', 'choice'));

CREATE OR REPLACE FUNCTION score AS (state, instructions, criteria) ->
  if(throwIf(NOT isValidJSON(criteria), 'score criteria must be valid JSON'),
     NULL,
     JSONExtractFloat(dq_decide(state, dq_question('score', instructions, criteria)), 'q', 'score'));

CREATE OR REPLACE FUNCTION decide AS (state, questions) ->
  if(throwIf(NOT isValidJSON(questions), 'questions must be valid JSON')
     OR throwIf(JSONType(questions) != 'Object' OR JSONLength(questions) = 0,
                'questions must be a nonempty JSON object'),
     NULL,
     dq_decide(state, questions));
```

Public surface, compared with the other modules:

| Function | SQLite / PostgreSQL | ClickHouse |
|---|---|---|
| `dq_version()` | same | same |
| `dq_backend()` | resident model's backend | loads a throwaway copy and reports its backend; slow |
| `dq_load(dir[, options])` | present | absent; the model is set in the XML |
| `noul(state, instructions[, criteria])` | 2 or 3 arguments | always 3; `''` for no criteria |
| `choice(state, instructions, criteria)` | same | same |
| `score(state, instructions, criteria)` | same | same |
| `decide(state, questions)` | same; returns JSON | same; returns JSON text |

`state` is text, or a `map(...)` or `JSON` value for structured state. `instructions` is text.
`criteria` and `questions` are JSON text.

### Build and install

- `make clickhouse` configures the tree and builds `build/clickhouse/decision-query-udf`,
  `decision_query_function.xml` and `decision_query.sql`. `DQ_MODEL_DIR` and `DQ_OPTIONS`,
  the CMake variables the PostgreSQL tests already use, prefill the XML command line.
- `make clickhouse-install` copies the binary into ClickHouse's scripts directory
  (`/var/lib/clickhouse/user_scripts/` by default, `DQ_CLICKHOUSE_SCRIPTS_DIR` overrides) and
  the XML into the server config directory (`/etc/clickhouse-server/` by default,
  `DQ_CLICKHOUSE_CONFIG_DIR` overrides), makes the binary executable, then prints the
  `clickhouse-client --queries-file build/clickhouse/decision_query.sql` command to run.
- The worker links ICU dynamically like the other modules, so a container running it needs
  the same ICU major version as the build machine. The README says so beside the Docker
  quick start.

## Data flow for one row

Take `SELECT noul(body, 'Is this a refund request?', '') FROM tickets`.

1. ClickHouse inlines the wrapper. `criteria` is `''`, so `dq_question` builds
   `{"q":{"type":"noul","instructions":"Is this a refund request?"}}` with `toJSONString`
   escaping the question text.
2. `body` is cast to `Dynamic`. ClickHouse writes one line to the worker:
   `{"state":"I was charged twice...","questions":"{\"q\":...}"}`.
3. The worker calls `engine.answers(state, questions)` and writes
   `{"result":"{\"q\":{\"noul\":0.93,...}}"}`, then flushes.
4. ClickHouse reads it into the `Nullable(String)` result and the wrapper extracts `q.noul`.

Rows in a block are processed in order, one forward pass each, as in the other modules. With
`pool_size` 1, blocks from concurrent queries wait their turn, which is the SQLite module's
"calls are serialized" rule.

## NULL semantics

Any NULL argument yields NULL without running the model, the existing contract.

- NULL `state` reaches the worker as `null`; it answers `null`.
- NULL `instructions` or `criteria` makes `dq_question` return NULL through `concat`, so
  `questions` reaches the worker as `null`; it answers `null`. `throwIf` does not throw on a
  NULL condition, so the validation step passes NULL through.
- NULL `questions` to `decide` behaves the same way.

## Errors, in three layers

1. **In the wrapper, before the worker.** Invalid JSON in `criteria` or `questions`, or
   `questions` that is empty or not an object, fails the query with the messages the other
   modules emit: `noul criteria must be valid JSON`, `choice criteria must be valid JSON`,
   `score criteria must be valid JSON`, `questions must be valid JSON`,
   `questions must be a nonempty JSON object`. The worker is not contacted.
2. **In the worker, per request.** Engine errors such as an unknown question type, a
   malformed request or `Decision endpoint returned HTTP 500` are written to stderr and the
   worker exits 1. With `stderr_reaction = throw`, ClickHouse fails the query with that text.
   The pool starts a fresh worker on the next query, which costs one model load. These are
   configuration or usage mistakes, so the reload is acceptable.
3. **At startup.** A wrong `--backend=` fails the first query with
   `Not a checkpoint directory: <path>`, the existing message.

## Configuration and operations

| Setting in the shipped XML | Value | Why |
|---|---|---|
| `pool_size` | 1 | Each worker is a model copy (0.6 to 0.9 GB, or GPU memory). Raise it only with the memory to match. |
| `command_read_timeout` | 600000 ms | Must cover a cold model load plus one row. ClickHouse's default is 10 s. |
| `command_write_timeout` | 600000 ms | Symmetric with the read timeout. |
| `max_command_execution_time` | 3600 s | Per block. ClickHouse's default is 10 s. |
| `send_chunk_header` | 0 | The worker answers line by line. |
| `stderr_reaction` | throw | The worker's only stderr output is an error message, which then reaches the client. |

CPU inference runs at roughly one row per second and a default block is 65,409 rows, so the
README tells CPU users to add `SETTINGS max_block_size = 64` to table-scale queries.

Changing the model means editing the `--backend=` line. Two function sets pointing at two
models, for example English and multilingual, are possible by duplicating the XML block under
different names; the README mentions this in one sentence.

## Testing

`clickhouse/tests/test-clickhouse.py` is a unittest file shaped like `sqlite/tests/test-loadable.py`.
It requires a `clickhouse` binary (`CLICKHOUSE` environment variable, default `clickhouse` on
`PATH`) and fails with instructions if none is found; it never skips for that reason.

The harness starts a fake System One endpoint on localhost that records every request and
answers with fixed values, the same 25-line handler PR #10 added to the SQLite suite (its own
copy; consolidation into a shared helper is a follow-up once that PR merges). It writes a
per-run XML from the template with `--backend=http://127.0.0.1:<port>/v1/systemone`, writes a
`clickhouse local` config pointing at that XML and at the built worker, and runs each test's
SQL through `clickhouse local --multiquery` with the wrapper SQL file prepended.

Worker-only tests, run first and needing no ClickHouse: pipe lines to `decision-query-udf`
and check the lines it writes, for the null case, a normal request against the fake endpoint,
and an error exit.

Model-free tests through ClickHouse, always run:

1. Request shape: each of `noul`, `choice`, `score`, `decide` produces the exact System One
   body at the endpoint, and each wrapper extracts the right field. `noul` with `''` omits
   criteria; with JSON text includes it.
2. Errors before the worker: the five wrapper messages, with zero requests recorded.
3. NULL in, NULL out for every function, with zero requests recorded.
4. Structured state: text arrives as a string, `map(...)` as an object, JSON-looking text as
   a string.
5. Worker errors reach the client: HTTP 500 from the endpoint fails the query with
   `Decision endpoint returned HTTP 500`; a bad `--backend=` path fails with
   `Not a checkpoint directory`.
6. Registration of the five wrappers and two XML functions; `dq_version()` starts with `v`;
   `dq_backend()` returns `remote` against the endpoint; a multi-row scan through one worker
   returns rows in order.

Model-backed tests, when `DQ_MODEL_DIR` is set: the billing-ticket assertions of the other
suites, and parity with `laya-cli` within 0.0001 on `laya.cpp/benchmarks/cases/smoke.json`,
as the SQLite suite does.

## CI

`build.yml` gains `clickhouse-linux-x86_64`: the SQLite job's apt packages, a pinned
ClickHouse 25.3 LTS binary from the ClickHouse release downloads so the tests prove the
minimum version, `make clickhouse`, `make test-clickhouse`, and artifacts for the binary,
XML and SQL. `integration-local.yml` gains one step running the model-backed ClickHouse tests
with its cached checkpoint.

## Documentation

`clickhouse/README.md`, in the shape of `postgres/README.md`:

1. What it is, with the three example queries.
2. Install in four steps: build, copy the binary, copy the XML and edit its one line, run the
   SQL file. Then `SELECT dq_backend()` as the check.
3. A Docker quick start: one `docker run` with the binary, the XML and the model mounted.
4. The function table above, with the two differences called out.
5. The settings table above and the `max_block_size` note for CPU.
6. Limitations: ClickHouse 25.3 or later; self-managed servers only, not ClickHouse Cloud;
   one model copy per pool worker; no `dq_load`; the ICU note for containers.

Root `README.md`: a Layout row, a ClickHouse example beside the SQLite and PostgreSQL ones,
`make clickhouse` in the build section, "all three modules" where it says "both".

## Out of scope

- The setup command line tool.
- Native array or map `criteria` and `questions`.
- `dq_load`, ClickHouse Cloud, ClickHouse before 25.3.

## Items the implementation verifies first

Small facts the design assumes that only a running server can confirm. Each has a fallback.

| Item | Fallback if it fails |
|---|---|
| Redirecting fd 2 during model load silences all library output, so `stderr_reaction = throw` is safe (confirmed by the model-backed silence test) | `stderr_reaction = log_last`; the README points to the server log for worker errors |
| `max_command_execution_time` accepts 3600, or 0 means unlimited (a 2 s answer passed with the limit at 1 s on 26.10, so it is not the hazard first feared) | Keep 3600 and document raising it |
| An edited XML is reloaded without a server restart | Document `SYSTEM RELOAD FUNCTIONS` or a restart |
| `JSONType` on invalid text does not throw before `isValidJSON` is checked (confirmed: it returns `Null`) | Reorder the `decide` validation into nested `if`s |
