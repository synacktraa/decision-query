# ClickHouse module implementation plan

**Goal:** Expose `noul`, `choice`, `score` and `decide` from ClickHouse through the shared engine, per [the design](../specs/2026-09-23-clickhouse-module-design.md).

**Architecture:** One executable, `decision-query-udf`, links `decision-query-engine` and speaks ClickHouse's JSONEachRow line protocol on stdin and stdout. An XML file declares it to ClickHouse as the pooled function `dq_decide` (plus an unpooled `dq_backend_raw`), and a SQL file creates the public wrappers. Tests drive `clickhouse local` against a fake System One endpoint, so nothing here needs a checkpoint until the model-backed tests at the end.

**Tech stack:** C++20 (the worker), CMake 3.24 (build and templates), ClickHouse 25.3 or later (`Dynamic` argument type, `executable_pool`, `CREATE FUNCTION`), Python 3 unittest (tests), GitHub Actions (CI).

**Conventions for every task:**

- Work on branch `feat/clickhouse`. Build and test in the WSL Ubuntu-22.04 clone at `~/decision-query`, whose `win` remote points at the Windows checkout: `git fetch win && git checkout -B feat/clickhouse win/feat/clickhouse`, with `PATH=$HOME/.local/bin:$PATH`, `CMAKE_GENERATOR=Ninja` and `CMAKE_PREFIX_PATH=$HOME/.local` exported (pip CMake 3.31 and nlohmann-json 3.11 live under `~/.local`; Ubuntu's own are too old). `clickhouse` must be on `PATH` (the spike binary is at `~/chspike/clickhouse`; `export PATH=$HOME/chspike:$PATH`).
- Every task is test first: commit the failing test (`test(clickhouse): ...`), watch it fail for the stated reason, then commit the smallest code that passes (`feat(clickhouse): ...`, `build(...)`, `docs(...)`). One idea per commit, structured body, attribution trailer.
- Run the suite with `make test-clickhouse` (it runs `python3 clickhouse/tests/test-clickhouse.py`). Report full counts: passed, failed, skipped.
- Python style: two-space indent, as in `sqlite/tests/test-loadable.py`. C++ style: the root `.clang-format` (Google base, 2 spaces, 100 columns, namespaces indented).
- Never open a pull request; the user decides when.

---

### Task 1: Worker skeleton, null in null out, startup errors

**Files:**
- Create: `clickhouse/tests/test-clickhouse.py`
- Create: `clickhouse/src/main.cpp`
- Create: `clickhouse/CMakeLists.txt`
- Modify: `CMakeLists.txt` (root)
- Modify: `Makefile`

- [ ] **Step 1: Write the failing test**

Create `clickhouse/tests/test-clickhouse.py`:

```python
import http.server
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BUILD = ROOT / os.environ.get("DQ_BUILD_DIR", "build") / "clickhouse"
WORKER = BUILD / "decision-query-udf"
XML = BUILD / "decision_query_function.xml"
WRAPPERS = BUILD / "decision_query.sql"
CLICKHOUSE = os.environ.get("CLICKHOUSE") or shutil.which("clickhouse")
MODEL_DIR = os.environ.get("DQ_MODEL_DIR")
DQ_OPTIONS = os.environ.get("DQ_OPTIONS", "")
VERSION = "v" + (ROOT / "VERSION").read_text().strip()

QUESTION = '{"q": {"type": "noul", "instructions": "Does the customer request a refund?"}}'


class SystemOne:
  """A fake System One endpoint: records every request and answers with fixed values."""

  def __init__(self):
    self.requests = []
    self.status = 200
    self.answers = {"q": {"noul": 0.25, "choice": "billing", "score": 1.5}}
    endpoint = self

    class Handler(http.server.BaseHTTPRequestHandler):
      def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        endpoint.requests.append({"path": self.path, "user_agent": self.headers.get("User-Agent"),
                                  "authorization": self.headers.get("Authorization"), "body": body})
        reply = json.dumps({"model": "fake-model", "answers": endpoint.answers}).encode()
        self.send_response(endpoint.status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(reply)))
        self.end_headers()
        self.wfile.write(reply)

      def log_message(self, *args):
        pass

    self.server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=self.server.serve_forever, daemon=True).start()
    self.url = f"http://127.0.0.1:{self.server.server_port}/v1/systemone"

  def close(self):
    self.server.shutdown()
    self.server.server_close()


def run_worker(rows, *flags):
  """Feeds JSON rows to the worker on stdin; returns (results, stderr, exit code)."""
  stdin = "".join(json.dumps(row) + "\n" for row in rows)
  completed = subprocess.run([str(WORKER), *flags], input=stdin, capture_output=True, text=True, timeout=300)
  results = [json.loads(line)["result"] for line in completed.stdout.splitlines() if line.strip()]
  return results, completed.stderr, completed.returncode


class TestWorker(unittest.TestCase):
  """The worker on its own, fed lines the way ClickHouse feeds them."""

  @classmethod
  def setUpClass(cls):
    if not WORKER.exists():
      raise AssertionError(f"{WORKER} is missing; build it with: make clickhouse")
    cls.endpoint = SystemOne()

  @classmethod
  def tearDownClass(cls):
    cls.endpoint.close()

  def setUp(self):
    self.endpoint.requests.clear()
    self.endpoint.status = 200

  def test_null_row_answers_null_without_a_request(self):
    results, stderr, code = run_worker([{"state": None, "questions": QUESTION},
                                        {"state": "text", "questions": None}],
                                       f"--backend={self.endpoint.url}")
    self.assertEqual((results, stderr, code), ([None, None], "", 0))
    self.assertEqual(self.endpoint.requests, [])

  def test_missing_backend_flag_is_an_error(self):
    results, stderr, code = run_worker([])
    self.assertEqual(results, [])
    self.assertIn("--backend=", stderr)
    self.assertEqual(code, 1)

  def test_bad_checkpoint_path_is_an_error(self):
    results, stderr, code = run_worker([{"state": "text", "questions": QUESTION}],
                                       "--backend=/nonexistent/checkpoint")
    self.assertEqual(results, [])
    self.assertIn("Not a checkpoint directory: /nonexistent/checkpoint", stderr)
    self.assertEqual(code, 1)


if __name__ == "__main__":
  unittest.main()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python3 clickhouse/tests/test-clickhouse.py`
Expected: `AssertionError: .../build/clickhouse/decision-query-udf is missing; build it with: make clickhouse` (the worker does not exist yet).

Commit the test: `test(clickhouse): pin the worker's null handling and startup errors`

- [ ] **Step 3: Write the minimal implementation**

Create `clickhouse/src/main.cpp`:

```cpp
// ClickHouse executable user-defined function worker.
//
// Started by ClickHouse from decision_query_function.xml, never by hand. Reads
// one JSON line per row from stdin ({"state": ..., "questions": ...}), answers
// through the shared engine and writes one JSON line per row ({"result": ...}).
#include <fcntl.h>
#include <unistd.h>

#include <cstdio>
#include <iostream>
#include <stdexcept>
#include <string>

#include "decision_engine.hpp"

namespace {
  using json = laya::json;

  struct arguments {
    std::string backend, options;
  };

  arguments parse_arguments(int argc, char **argv) {
    arguments parsed;
    for (int i = 1; i < argc; ++i) {
      const std::string argument = argv[i];
      if (argument.rfind("--backend=", 0) == 0)
        parsed.backend = argument.substr(10);
      else if (argument.rfind("--options=", 0) == 0)
        parsed.options = argument.substr(10);
      else
        throw std::invalid_argument("Unknown argument: " + argument);
    }
    if (parsed.backend.empty())
      throw std::invalid_argument("--backend=<checkpoint directory or URL> is required");
    return parsed;
  }

  json parse_options(const std::string &text) {
    if (text.empty()) return json();
    try {
      return json::parse(text);
    } catch (const json::exception &e) {
      throw std::invalid_argument(std::string("Model options must be valid JSON: ") + e.what());
    }
  }

  // Sends stdout and stderr to /dev/null for its lifetime. ClickHouse reads
  // results from stdout and treats any stderr text as a failure, so nothing a
  // library prints while the model loads may reach either.
  struct silenced_output {
    int saved_stdout = dup(1), saved_stderr = dup(2);
    silenced_output() {
      std::fflush(stdout);
      std::fflush(stderr);
      const int null = open("/dev/null", O_WRONLY);
      if (null >= 0) {
        dup2(null, 1);
        dup2(null, 2);
        close(null);
      }
    }
    ~silenced_output() {
      std::fflush(stdout);
      std::fflush(stderr);
      dup2(saved_stdout, 1);
      dup2(saved_stderr, 2);
      close(saved_stdout);
      close(saved_stderr);
    }
  };

  void write_result(const json &result) {
    json line;
    line["result"] = result;
    std::cout << line.dump() << '\n' << std::flush;
  }
}  // namespace

int main(int argc, char **argv) {
  try {
    const arguments args = parse_arguments(argc, argv);
    const json options = parse_options(args.options);
    {
      silenced_output quiet;
      dq::engine::instance().load(args.backend, options);
    }
    std::string line;
    while (std::getline(std::cin, line)) {
      if (line.empty()) continue;
      json::parse(line);
      write_result(nullptr);
    }
    return 0;
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
```

Create `clickhouse/CMakeLists.txt`:

```cmake
# ClickHouse executable user-defined function: the worker binary that ClickHouse
# starts from decision_query_function.xml.
cmake_minimum_required(VERSION 3.24)
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  project(decision-query-clickhouse LANGUAGES C CXX)
endif()
if(NOT TARGET decision-query-engine)
  add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/../engine" engine)
endif()

add_executable(decision-query-udf src/main.cpp)
target_compile_options(decision-query-udf PRIVATE -Wall -Wextra -Wpedantic)
target_link_libraries(decision-query-udf PRIVATE decision-query-engine)
```

In the root `CMakeLists.txt`, append after the PostgreSQL block:

```cmake
# The ClickHouse worker needs no ClickHouse headers, so it builds wherever the
# SQLite module builds.
option(DQ_CLICKHOUSE "Build the ClickHouse executable function" ON)
if(DQ_CLICKHOUSE)
  add_subdirectory(clickhouse)
endif()
```

In the `Makefile`, after the `test-postgres` target add:

```make
# ClickHouse executable function (clickhouse/), built in the same tree.
# DQ_MODEL_DIR may be a checkpoint directory or an http(s):// URL.
CLICKHOUSE_BACKEND=$(if $(filter http://% https://%,$(DQ_MODEL_DIR)),$(DQ_MODEL_DIR),$(abspath $(DQ_MODEL_DIR)))
CLICKHOUSE_CMAKE_FLAGS=$(if $(DQ_MODEL_DIR),-DDQ_MODEL_DIR=$(CLICKHOUSE_BACKEND)) $(if $(DQ_OPTIONS),'-DDQ_OPTIONS=$(DQ_OPTIONS)')

clickhouse:
	cmake -S . -B $(BUILD) $(CMAKE_FLAGS) $(CLICKHOUSE_CMAKE_FLAGS) && cmake --build $(BUILD) --parallel --target decision-query-udf

test-clickhouse:
	$(PYTHON) clickhouse/tests/test-clickhouse.py
```

and add `clickhouse test-clickhouse` to the `.PHONY` list.

- [ ] **Step 4: Build and run the test to verify it passes**

Run: `make clickhouse && make test-clickhouse`
Expected: `Ran 3 tests ... OK`

- [ ] **Step 5: Commit**

`feat(clickhouse): add the worker skeleton and its build targets` with a body explaining the line protocol, the startup load, the stdout/stderr silencing and the new Makefile targets.

---

### Task 2: Worker answers requests through the engine

**Files:**
- Modify: `clickhouse/tests/test-clickhouse.py`
- Modify: `clickhouse/src/main.cpp`

- [ ] **Step 1: Write the failing tests**

Add to `TestWorker`:

```python
  def test_answers_a_request_through_the_endpoint(self):
    results, stderr, code = run_worker([
      {"state": "I was charged twice", "questions": QUESTION},
      {"state": {"subject": "Duplicate invoice", "body": "I was charged twice"}, "questions": QUESTION},
      {"state": '{"subject": "looks like JSON"}', "questions": QUESTION},
    ], f"--backend={self.endpoint.url}")
    self.assertEqual((stderr, code), ("", 0))
    self.assertEqual([json.loads(result)["q"]["noul"] for result in results], [0.25, 0.25, 0.25])
    self.assertEqual([request["body"]["state"] for request in self.endpoint.requests], [
      "I was charged twice",
      {"subject": "Duplicate invoice", "body": "I was charged twice"},
      '{"subject": "looks like JSON"}'])
    self.assertEqual(self.endpoint.requests[0]["path"], "/v1/systemone")
    self.assertEqual(self.endpoint.requests[0]["body"],
                     {"state": "I was charged twice", "questions": json.loads(QUESTION), "model": "jev-latest"})

  def test_endpoint_failure_stops_the_worker_with_the_message(self):
    self.endpoint.status = 500
    results, stderr, code = run_worker([{"state": "text", "questions": QUESTION}],
                                       f"--backend={self.endpoint.url}")
    self.assertEqual(results, [])
    self.assertIn("Decision endpoint returned HTTP 500", stderr)
    self.assertEqual(code, 1)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test-clickhouse`
Expected: two failures. `test_answers_a_request_through_the_endpoint` fails with `TypeError: the JSON object must be str ... not 'NoneType'` (the skeleton answers null to everything); `test_endpoint_failure_stops_the_worker_with_the_message` fails with `AssertionError: [None] != []`.

Commit: `test(clickhouse): pin the request the worker sends and its error exit`

- [ ] **Step 3: Write the minimal implementation**

In `main.cpp`, replace the body of `main`'s loop:

```cpp
    auto &engine = dq::engine::instance();
    {
      silenced_output quiet;
      engine.load(args.backend, options);
    }
    std::string line;
    while (std::getline(std::cin, line)) {
      if (line.empty()) continue;
      const json row = json::parse(line);
      const json &state = row.at("state");
      const json &questions = row.at("questions");
      if (state.is_null() || questions.is_null()) {
        write_result(nullptr);
        continue;
      }
      // state is forwarded as received: a JSON string stays text, an object
      // stays structured, which is what the SQLite module's JSON subtype does.
      write_result(engine.answers(state, json::parse(questions.get<std::string>())).dump());
    }
    return 0;
```

(The `dq::engine::instance().load(...)` line inside the silenced block becomes `engine.load(...)`.)

- [ ] **Step 4: Build and run the tests to verify they pass**

Run: `make clickhouse && make test-clickhouse`
Expected: `Ran 5 tests ... OK`

- [ ] **Step 5: Commit**

`feat(clickhouse): answer requests through the shared engine`

---

### Task 3: The worker identifies itself by project and version

**Files:**
- Modify: `clickhouse/tests/test-clickhouse.py`
- Modify: `engine/decision_engine.hpp`
- Modify: `clickhouse/CMakeLists.txt`

- [ ] **Step 1: Write the failing test**

Add to `TestWorker`:

```python
  def test_identifies_itself_and_sends_the_key_option(self):
    run_worker([{"state": "text", "questions": QUESTION}],
               f"--backend={self.endpoint.url}", '--options={"key":"test-key"}')
    self.assertEqual(self.endpoint.requests[0]["user_agent"], "decision-query/" + VERSION)
    self.assertEqual(self.endpoint.requests[0]["authorization"], "Bearer test-key")
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test-clickhouse`
Expected: `AssertionError: 'decision-query/dev' != 'decision-query/v0.0.1'` (the engine header only knows the SQLite and PostgreSQL version macros).

Commit: `test(clickhouse): pin the worker's User-Agent and bearer token`

- [ ] **Step 3: Write the minimal implementation**

In `engine/decision_engine.hpp`, replace the version macro block:

```cpp
// Each target defines its own version macro; the shared engine belongs to all.
#if defined(DQ_VERSION)
#define DQ_UA_VERSION DQ_VERSION
#elif defined(SQLITE_DQ_VERSION)
#define DQ_UA_VERSION SQLITE_DQ_VERSION
#elif defined(PGDQ_VERSION)
#define DQ_UA_VERSION PGDQ_VERSION
#else
#define DQ_UA_VERSION "dev"
#endif
```

In `clickhouse/CMakeLists.txt`, after `target_compile_options` add:

```cmake
target_compile_definitions(decision-query-udf PRIVATE DQ_VERSION="v${DQ_VERSION_TEXT}")
```

- [ ] **Step 4: Build and run the tests to verify they pass**

Run: `make clickhouse && make test-clickhouse`
Expected: `Ran 6 tests ... OK`

- [ ] **Step 5: Commit**

`build(engine): let any module define DQ_VERSION for the User-Agent`

---

### Task 4: `--print-backend`

**Files:**
- Modify: `clickhouse/tests/test-clickhouse.py`
- Modify: `clickhouse/src/main.cpp`

- [ ] **Step 1: Write the failing test**

Add to `TestWorker`:

```python
  def test_print_backend_reports_the_backend_name(self):
    results, stderr, code = run_worker([{"dummy": 1}], f"--backend={self.endpoint.url}", "--print-backend")
    self.assertEqual((results, stderr, code), (["remote"], "", 0))
    self.assertEqual(self.endpoint.requests, [])
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test-clickhouse`
Expected: `AssertionError: ([], 'Unknown argument: --print-backend\n', 1) != (['remote'], '', 0)`

Commit: `test(clickhouse): pin the --print-backend mode`

- [ ] **Step 3: Write the minimal implementation**

In `main.cpp`: add `bool print_backend = false;` to `struct arguments`; in `parse_arguments` add the branch `else if (argument == "--print-backend") parsed.print_backend = true;` before the `else throw`; and in the loop, right after `const json row = json::parse(line);`:

```cpp
      if (args.print_backend) {
        write_result(engine.backend_name());
        continue;
      }
```

- [ ] **Step 4: Build and run the tests to verify they pass**

Run: `make clickhouse && make test-clickhouse`
Expected: `Ran 7 tests ... OK`

- [ ] **Step 5: Commit**

`feat(clickhouse): add --print-backend for the dq_backend() diagnostic`

---

### Task 5: The XML declaration, generated by CMake

**Files:**
- Modify: `clickhouse/tests/test-clickhouse.py`
- Create: `clickhouse/config/decision_query_function.xml.in`
- Modify: `clickhouse/CMakeLists.txt`

- [ ] **Step 1: Write the failing test**

Add a new class after `TestWorker`:

```python
class TestBuild(unittest.TestCase):
  """Files that make clickhouse generates beside the worker."""

  def test_xml_declares_both_functions_with_the_worker_command(self):
    self.assertTrue(XML.exists(), f"{XML} is missing; build it with: make clickhouse")
    xml = XML.read_text()
    self.assertIn("<name>dq_decide</name>", xml)
    self.assertIn("<name>dq_backend_raw</name>", xml)
    self.assertEqual(xml.count("<command>decision-query-udf --backend="), 2)
    self.assertIn(" --print-backend</command>", xml)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test-clickhouse`
Expected: `AssertionError: .../build/clickhouse/decision_query_function.xml is missing; build it with: make clickhouse`

Commit: `test(clickhouse): pin the generated XML declaration`

- [ ] **Step 3: Write the minimal implementation**

Create `clickhouse/config/decision_query_function.xml.in`:

```xml
<!-- Declares the decision-query worker to ClickHouse. Generated from
     decision_query_function.xml.in by CMake. After installing, the only line
     to edit is the command's backend path or URL. -->
<functions>
  <!-- The pooled worker every public function goes through. One worker holds
       one resident model; raise pool_size only with the memory to match. -->
  <function>
    <type>executable_pool</type>
    <name>dq_decide</name>
    <return_type>Nullable(String)</return_type>
    <return_name>result</return_name>
    <argument><type>Dynamic</type><name>state</name></argument>
    <argument><type>Nullable(String)</type><name>questions</name></argument>
    <format>JSONEachRow</format>
    <command>@DQ_CLICKHOUSE_COMMAND@</command>
    <pool_size>1</pool_size>
    <send_chunk_header>0</send_chunk_header>
    <!-- ClickHouse's defaults assume workers that answer in milliseconds; a
         model takes seconds to load and about a second per row on the CPU. -->
    <command_read_timeout>600000</command_read_timeout>
    <command_write_timeout>600000</command_write_timeout>
    <max_command_execution_time>3600</max_command_execution_time>
  </function>
  <!-- dq_backend(): starts a worker, loads the model, reports the backend
       name and exits. A few seconds per call, no memory kept afterwards. -->
  <function>
    <type>executable</type>
    <name>dq_backend_raw</name>
    <return_type>String</return_type>
    <return_name>result</return_name>
    <argument><type>UInt8</type><name>dummy</name></argument>
    <format>JSONEachRow</format>
    <command>@DQ_CLICKHOUSE_COMMAND@ --print-backend</command>
    <command_read_timeout>600000</command_read_timeout>
  </function>
</functions>
```

In `clickhouse/CMakeLists.txt`, append:

```cmake
# The XML command line. DQ_MODEL_DIR and DQ_OPTIONS are the same cache variables
# the PostgreSQL tests use; DQ_MODEL_DIR may also be an http(s):// URL.
set(DQ_MODEL_DIR "" CACHE PATH "Checkpoint directory or URL written into the XML command line")
set(DQ_OPTIONS "" CACHE STRING "JSON load options written into the XML command line (no spaces)")
if(DQ_MODEL_DIR)
  set(DQ_CLICKHOUSE_COMMAND "decision-query-udf --backend=${DQ_MODEL_DIR}")
else()
  set(DQ_CLICKHOUSE_COMMAND "decision-query-udf --backend=/var/lib/clickhouse/models/laya")
endif()
if(DQ_OPTIONS AND NOT DQ_OPTIONS STREQUAL "{}")
  string(APPEND DQ_CLICKHOUSE_COMMAND " --options=${DQ_OPTIONS}")
endif()
configure_file(config/decision_query_function.xml.in decision_query_function.xml @ONLY)
```

- [ ] **Step 4: Build and run the tests to verify they pass**

Run: `make clickhouse && make test-clickhouse`
Expected: `Ran 8 tests ... OK`. Also check the command line by hand: `grep "<command>" build/clickhouse/decision_query_function.xml` shows `decision-query-udf --backend=/var/lib/clickhouse/models/laya` twice, the second with ` --print-backend`.

- [ ] **Step 5: Commit**

`feat(clickhouse): generate the XML that declares the worker`

---

### Task 6: The ClickHouse harness, `dq_version()` and `dq_backend()`

**Files:**
- Modify: `clickhouse/tests/test-clickhouse.py`
- Create: `clickhouse/sql/decision_query.sql.in`
- Modify: `clickhouse/CMakeLists.txt`

- [ ] **Step 1: Write the failing test**

Add after `run_worker`:

```python
def clickhouse(sql, backend, options=""):
  """Runs SQL in a fresh clickhouse local with the wrappers loaded and the worker
  pointed at backend. Returns (rows, stderr, exit code); rows come from
  JSONCompactEachRow output, so NULL is None and numbers are numbers."""
  with tempfile.TemporaryDirectory() as directory:
    directory = Path(directory)
    scripts = directory / "scripts"
    scripts.mkdir()
    shutil.copy2(WORKER, scripts / "decision-query-udf")
    command = f"decision-query-udf --backend={backend}" + (f" --options={options}" if options else "")
    xml = re.sub(r"<command>decision-query-udf[^<]*</command>",
                 lambda m: f"<command>{command}{' --print-backend' if '--print-backend' in m.group(0) else ''}</command>",
                 XML.read_text())
    (directory / "decision_query_function.xml").write_text(xml)
    (directory / "config.xml").write_text(
      f"<clickhouse><user_scripts_path>{scripts}/</user_scripts_path>"
      f"<user_defined_executable_functions_config>{directory}/*_function.xml</user_defined_executable_functions_config>"
      "</clickhouse>")
    wrappers = WRAPPERS.read_text() if WRAPPERS.exists() else ""
    completed = subprocess.run([CLICKHOUSE, "local", "-C", str(directory / "config.xml"),
                                "--output-format", "JSONCompactEachRow", "--query", wrappers + "\n" + sql],
                               capture_output=True, text=True, timeout=600)
    rows = [json.loads(line) for line in completed.stdout.splitlines() if line.strip()]
    return rows, completed.stderr, completed.returncode
```

Add a new class after `TestBuild`:

```python
class TestClickHouse(unittest.TestCase):
  """The SQL surface, through clickhouse local and the fake endpoint."""

  @classmethod
  def setUpClass(cls):
    if not WORKER.exists():
      raise AssertionError(f"{WORKER} is missing; build it with: make clickhouse")
    if not CLICKHOUSE:
      raise AssertionError("no clickhouse binary found; install one (curl https://clickhouse.com/ | sh) or set CLICKHOUSE")
    cls.endpoint = SystemOne()

  @classmethod
  def tearDownClass(cls):
    cls.endpoint.close()

  def setUp(self):
    self.endpoint.requests.clear()
    self.endpoint.status = 200

  def query(self, sql):
    return clickhouse(sql, self.endpoint.url)

  def test_dq_version_and_dq_backend(self):
    rows, stderr, code = self.query("SELECT dq_version(), dq_backend()")
    self.assertEqual((rows, code), ([[VERSION, "remote"]], 0), stderr)
    self.assertEqual(self.endpoint.requests, [])
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test-clickhouse`
Expected: `AssertionError: ([], 60) != ([['v0.0.1', 'remote']], 0) : Code: 46. DB::Exception: Unknown function dq_version ...`

Commit: `test(clickhouse): add the clickhouse local harness and pin dq_version, dq_backend`

- [ ] **Step 3: Write the minimal implementation**

Create `clickhouse/sql/decision_query.sql.in`:

```sql
-- Public functions, as wrappers over the executable functions declared in
-- decision_query_function.xml. Generated from decision_query.sql.in by CMake.
-- Run once per server: clickhouse-client --queries-file decision_query.sql
CREATE OR REPLACE FUNCTION dq_version AS () -> 'v@DQ_VERSION_TEXT@';

-- Slow by design: starts a worker, loads the model, reports its backend.
CREATE OR REPLACE FUNCTION dq_backend AS () -> dq_backend_raw(1);
```

In `clickhouse/CMakeLists.txt`, after the XML `configure_file` add:

```cmake
configure_file(sql/decision_query.sql.in decision_query.sql @ONLY)
```

- [ ] **Step 4: Build and run the tests to verify they pass**

Run: `make clickhouse && make test-clickhouse`
Expected: `Ran 9 tests ... OK`

- [ ] **Step 5: Commit**

`feat(clickhouse): add the SQL wrappers file with dq_version and dq_backend`

---

### Task 7: `decide` pass-through, structured state, table scan

**Files:**
- Modify: `clickhouse/tests/test-clickhouse.py`
- Modify: `clickhouse/sql/decision_query.sql.in`

- [ ] **Step 1: Write the failing tests**

Add to `TestClickHouse`:

```python
  def test_decide_sends_the_request_shape_and_returns_the_answers(self):
    rows, stderr, code = self.query(f"SELECT decide('I was charged twice', '{QUESTION}')")
    self.assertEqual(code, 0, stderr)
    self.assertEqual(json.loads(rows[0][0]), self.endpoint.answers)
    self.assertEqual(self.endpoint.requests[0]["body"],
                     {"state": "I was charged twice", "questions": json.loads(QUESTION), "model": "jev-latest"})

  def test_structured_state_reaches_the_endpoint_as_an_object(self):
    rows, stderr, code = self.query(
      f"SELECT decide(map('subject', 'Duplicate invoice', 'body', 'I was charged twice'), '{QUESTION}'),"
      f" decide('{{\"subject\": \"looks like JSON\"}}', '{QUESTION}')")
    self.assertEqual(code, 0, stderr)
    self.assertCountEqual([request["body"]["state"] for request in self.endpoint.requests],
                          [{"subject": "Duplicate invoice", "body": "I was charged twice"},
                           '{"subject": "looks like JSON"}'])

  def test_a_table_scan_goes_through_one_worker_in_order(self):
    rows, stderr, code = self.query(f"SELECT decide(x, '{QUESTION}') FROM (SELECT arrayJoin(['a', 'b', 'c']) AS x)")
    self.assertEqual(code, 0, stderr)
    self.assertEqual(len(rows), 3)
    self.assertEqual([request["body"]["state"] for request in self.endpoint.requests], ["a", "b", "c"])
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test-clickhouse`
Expected: three failures, each `AssertionError: 60 != 0 : Code: 46. DB::Exception: Unknown function decide ...`

Commit: `test(clickhouse): pin decide's request shape, structured state and table scans`

- [ ] **Step 3: Write the minimal implementation**

Append to `clickhouse/sql/decision_query.sql.in`:

```sql
CREATE OR REPLACE FUNCTION decide AS (state, questions) -> dq_decide(state, questions);
```

- [ ] **Step 4: Build and run the tests to verify they pass**

Run: `make clickhouse && make test-clickhouse`
Expected: `Ran 12 tests ... OK`

- [ ] **Step 5: Commit**

`feat(clickhouse): add the decide wrapper`

---

### Task 8: `decide` validates its questions and passes NULL through

**Files:**
- Modify: `clickhouse/tests/test-clickhouse.py`
- Modify: `clickhouse/sql/decision_query.sql.in`

- [ ] **Step 1: Write the failing tests**

Add to `TestClickHouse`:

```python
  def test_decide_rejects_bad_questions_before_the_worker(self):
    for questions, message in (("nope", "questions must be valid JSON"),
                               ("{}", "questions must be a nonempty JSON object"),
                               ("[1]", "questions must be a nonempty JSON object")):
      rows, stderr, code = self.query(f"SELECT decide('state', '{questions}')")
      self.assertNotEqual(code, 0, questions)
      self.assertIn(message, stderr)
      self.assertNotIn("Executable generates stderr", stderr, "the wrapper, not the worker, must reject it")
    self.assertEqual(self.endpoint.requests, [])

  def test_decide_null_in_null_out(self):
    rows, stderr, code = self.query(
      f"SELECT decide(CAST(NULL AS Nullable(String)), '{QUESTION}'), decide('state', CAST(NULL AS Nullable(String)))")
    self.assertEqual((rows, code), ([[None, None]], 0), stderr)
    self.assertEqual(self.endpoint.requests, [])
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test-clickhouse`
Expected: `test_decide_rejects_bad_questions_before_the_worker` fails on `'{}'`: the pass-through sends it to the worker, which answers rather than erroring, so `AssertionError: 0 == 0`. `test_decide_null_in_null_out` passes already (the worker answers null to null); that is expected and it stays as the regression pin.

Commit: `test(clickhouse): pin decide's validation messages and NULL handling`

- [ ] **Step 3: Write the minimal implementation**

Replace the `decide` line in `clickhouse/sql/decision_query.sql.in` with:

```sql
-- Validation happens here, before a worker is involved, with the messages the
-- other modules emit. throwIf ignores a NULL condition, so NULL passes through.
CREATE OR REPLACE FUNCTION decide AS (state, questions) ->
  if(throwIf(NOT isValidJSON(questions), 'questions must be valid JSON')
     OR throwIf(JSONType(questions) != 'Object' OR JSONLength(questions) = 0,
                'questions must be a nonempty JSON object'),
     NULL,
     dq_decide(state, questions));
```

- [ ] **Step 4: Build and run the tests to verify they pass**

Run: `make clickhouse && make test-clickhouse`
Expected: `Ran 14 tests ... OK`

- [ ] **Step 5: Commit**

`feat(clickhouse): validate decide's questions in the wrapper`

---

### Task 9: `noul`

**Files:**
- Modify: `clickhouse/tests/test-clickhouse.py`
- Modify: `clickhouse/sql/decision_query.sql.in`

- [ ] **Step 1: Write the failing tests**

Add to `TestClickHouse`:

```python
  def test_noul_builds_the_question_with_and_without_criteria(self):
    rows, stderr, code = self.query(
      "SELECT noul('body', 'Is this a \"refund\"?', ''),"
      " noul('body', 'Is this a refund?', '{\"true\": \"a refund is requested\", \"false\": \"no refund is requested\"}')")
    self.assertEqual((rows, code), ([[0.25, 0.25]], 0), stderr)
    self.assertCountEqual([request["body"]["questions"] for request in self.endpoint.requests], [
      {"q": {"type": "noul", "instructions": 'Is this a "refund"?'}},
      {"q": {"type": "noul", "instructions": "Is this a refund?",
             "criteria": {"true": "a refund is requested", "false": "no refund is requested"}}}])

  def test_noul_rejects_bad_criteria_before_the_worker(self):
    rows, stderr, code = self.query("SELECT noul('body', 'question', 'nope')")
    self.assertNotEqual(code, 0)
    self.assertIn("noul criteria must be valid JSON", stderr)
    self.assertNotIn("Executable generates stderr", stderr)
    self.assertEqual(self.endpoint.requests, [])

  def test_noul_null_in_null_out(self):
    rows, stderr, code = self.query(
      "SELECT noul(CAST(NULL AS Nullable(String)), 'question', ''),"
      " noul('body', CAST(NULL AS Nullable(String)), ''),"
      " noul('body', 'question', CAST(NULL AS Nullable(String)))")
    self.assertEqual((rows, code), ([[None, None, None]], 0), stderr)
    self.assertEqual(self.endpoint.requests, [])

  def test_noul_takes_exactly_three_arguments(self):
    rows, stderr, code = self.query("SELECT noul('body', 'question')")
    self.assertNotEqual(code, 0)
    self.assertIn("passed 2, should be 3", stderr)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test-clickhouse`
Expected: four failures, each mentioning `Unknown function noul`.

Commit: `test(clickhouse): pin noul's question, criteria validation, NULLs and arity`

- [ ] **Step 3: Write the minimal implementation**

Append to `clickhouse/sql/decision_query.sql.in`:

```sql
-- The one-question object the scalar functions send. criteria = '' omits the
-- key. concat and toJSONString return NULL for a NULL input, so a NULL
-- instructions or criteria makes the worker receive null and answer NULL.
CREATE OR REPLACE FUNCTION dq_question AS (type, instructions, criteria) ->
  if(criteria = '',
     concat('{"q":{"type":"', type, '","instructions":', toJSONString(instructions), '}}'),
     concat('{"q":{"type":"', type, '","instructions":', toJSONString(instructions),
            ',"criteria":', criteria, '}}'));

-- noul always takes three arguments: ClickHouse cannot overload by count.
CREATE OR REPLACE FUNCTION noul AS (state, instructions, criteria) ->
  if(throwIf(criteria != '' AND NOT isValidJSON(criteria), 'noul criteria must be valid JSON'),
     NULL,
     JSONExtractFloat(dq_decide(state, dq_question('noul', instructions, criteria)), 'q', 'noul'));
```

- [ ] **Step 4: Build and run the tests to verify they pass**

Run: `make clickhouse && make test-clickhouse`
Expected: `Ran 18 tests ... OK`

- [ ] **Step 5: Commit**

`feat(clickhouse): add the noul wrapper`

---

### Task 10: `choice` and `score`

**Files:**
- Modify: `clickhouse/tests/test-clickhouse.py`
- Modify: `clickhouse/sql/decision_query.sql.in`

- [ ] **Step 1: Write the failing tests**

Add to `TestClickHouse`:

```python
  def test_choice_and_score_extract_their_answers(self):
    rows, stderr, code = self.query(
      "SELECT choice('body', 'Which department?', '{\"billing\": \"payments\", \"technical\": \"bugs\"}'),"
      " choice('body', 'Which department?', '[\"billing\", \"technical\"]'),"
      " score('body', 'How urgent?', '[\"not urgent\", \"soon\", \"immediate\"]')")
    self.assertEqual((rows, code), ([["billing", "billing", 1.5]], 0), stderr)
    questions = [request["body"]["questions"] for request in self.endpoint.requests]
    self.assertCountEqual([question["q"]["type"] for question in questions], ["choice", "choice", "score"])
    self.assertIn({"q": {"type": "score", "instructions": "How urgent?",
                         "criteria": ["not urgent", "soon", "immediate"]}}, questions)

  def test_choice_and_score_reject_bad_criteria_before_the_worker(self):
    for sql, message in (("choice('body', 'q', '')", "choice criteria must be valid JSON"),
                         ("score('body', 'q', '{')", "score criteria must be valid JSON")):
      rows, stderr, code = self.query(f"SELECT {sql}")
      self.assertNotEqual(code, 0, sql)
      self.assertIn(message, stderr)
      self.assertNotIn("Executable generates stderr", stderr)
    self.assertEqual(self.endpoint.requests, [])

  def test_choice_and_score_null_in_null_out(self):
    rows, stderr, code = self.query(
      "SELECT choice(CAST(NULL AS Nullable(String)), 'q', '[\"a\"]'),"
      " score('body', 'q', CAST(NULL AS Nullable(String)))")
    self.assertEqual((rows, code), ([[None, None]], 0), stderr)
    self.assertEqual(self.endpoint.requests, [])
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test-clickhouse`
Expected: three failures mentioning `Unknown function choice`.

Commit: `test(clickhouse): pin choice and score`

- [ ] **Step 3: Write the minimal implementation**

Append to `clickhouse/sql/decision_query.sql.in`:

```sql
CREATE OR REPLACE FUNCTION choice AS (state, instructions, criteria) ->
  if(throwIf(NOT isValidJSON(criteria), 'choice criteria must be valid JSON'),
     NULL,
     JSONExtractString(dq_decide(state, dq_question('choice', instructions, criteria)), 'q', 'choice'));

CREATE OR REPLACE FUNCTION score AS (state, instructions, criteria) ->
  if(throwIf(NOT isValidJSON(criteria), 'score criteria must be valid JSON'),
     NULL,
     JSONExtractFloat(dq_decide(state, dq_question('score', instructions, criteria)), 'q', 'score'));
```

- [ ] **Step 4: Build and run the tests to verify they pass**

Run: `make clickhouse && make test-clickhouse`
Expected: `Ran 21 tests ... OK`

- [ ] **Step 5: Commit**

`feat(clickhouse): add the choice and score wrappers`

---

### Task 11: Worker errors reach the client

**Files:**
- Modify: `clickhouse/tests/test-clickhouse.py`
- Modify: `clickhouse/config/decision_query_function.xml.in`

- [ ] **Step 1: Write the failing tests**

Add to `TestClickHouse`:

```python
  def test_endpoint_errors_reach_the_client(self):
    self.endpoint.status = 500
    rows, stderr, code = self.query(f"SELECT decide('state', '{QUESTION}')")
    self.assertNotEqual(code, 0)
    self.assertIn("Decision endpoint returned HTTP 500", stderr)

  def test_bad_backend_path_reaches_the_client(self):
    rows, stderr, code = clickhouse(f"SELECT decide('state', '{QUESTION}')", "/nonexistent/checkpoint")
    self.assertNotEqual(code, 0)
    self.assertIn("Not a checkpoint directory: /nonexistent/checkpoint", stderr)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make test-clickhouse`
Expected: both fail with `AssertionError: 'Decision endpoint returned HTTP 500' not found in ...` (and the same for the path): without `stderr_reaction`, ClickHouse logs the worker's stderr line on the server side and reports only a generic child-process failure.

Commit: `test(clickhouse): pin that worker errors reach the client`

- [ ] **Step 3: Write the minimal implementation**

In `clickhouse/config/decision_query_function.xml.in`, add to both `<function>` blocks, after `<command_read_timeout>`:

```xml
    <!-- The worker's only stderr output is an error message; make it the query's error. -->
    <stderr_reaction>throw</stderr_reaction>
```

- [ ] **Step 4: Build and run the tests to verify they pass**

Run: `make clickhouse && make test-clickhouse`
Expected: `Ran 23 tests ... OK`

- [ ] **Step 5: Commit**

`feat(clickhouse): surface worker errors as the query's error`

---

### Task 12: Install targets

**Files:**
- Modify: `clickhouse/tests/test-clickhouse.py`
- Modify: `clickhouse/CMakeLists.txt`
- Modify: `Makefile`

- [ ] **Step 1: Write the failing test**

Add to `TestBuild`:

```python
  def test_install_places_the_worker_and_the_xml_under_a_prefix(self):
    with tempfile.TemporaryDirectory() as prefix:
      completed = subprocess.run(["cmake", "--install", str(BUILD.parent), "--component", "clickhouse",
                                  "--prefix", prefix], capture_output=True, text=True)
      self.assertEqual(completed.returncode, 0, completed.stderr)
      worker = Path(prefix) / "var/lib/clickhouse/user_scripts/decision-query-udf"
      self.assertTrue(os.access(worker, os.X_OK), f"{worker} is missing or not executable")
      self.assertTrue((Path(prefix) / "etc/clickhouse-server/decision_query_function.xml").exists())
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test-clickhouse`
Expected: `AssertionError: .../var/lib/clickhouse/user_scripts/decision-query-udf is missing or not executable` (there are no install rules for the component, so the install does nothing).

Commit: `test(clickhouse): pin the install layout`

- [ ] **Step 3: Write the minimal implementation**

Append to `clickhouse/CMakeLists.txt`:

```cmake
# Install layout, relative to the install prefix (make clickhouse-install uses /).
set(DQ_CLICKHOUSE_SCRIPTS_DIR "var/lib/clickhouse/user_scripts" CACHE PATH
  "ClickHouse's user_scripts_path, where executable functions must live")
set(DQ_CLICKHOUSE_CONFIG_DIR "etc/clickhouse-server" CACHE PATH
  "ClickHouse's server config directory, where *_function.xml files are read")
install(TARGETS decision-query-udf RUNTIME DESTINATION "${DQ_CLICKHOUSE_SCRIPTS_DIR}" COMPONENT clickhouse)
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/decision_query_function.xml"
  DESTINATION "${DQ_CLICKHOUSE_CONFIG_DIR}" COMPONENT clickhouse)
```

In the `Makefile`, after the `clickhouse:` target add:

```make
CLICKHOUSE_PREFIX?=/

# Installs the worker and the XML under CLICKHOUSE_PREFIX (may need sudo), then
# names the SQL file to run once with clickhouse-client.
clickhouse-install: clickhouse
	cmake --install $(BUILD) --component clickhouse --prefix $(CLICKHOUSE_PREFIX)
	@echo "Now run: clickhouse-client --queries-file $(BUILD)/clickhouse/decision_query.sql"
```

and add `clickhouse-install` to `.PHONY`.

- [ ] **Step 4: Build and run the tests to verify they pass**

Run: `make clickhouse && make test-clickhouse`
Expected: `Ran 24 tests ... OK`

- [ ] **Step 5: Commit**

`build(clickhouse): add the install rules and make clickhouse-install`

---

### Task 13: Model-backed tests

**Files:**
- Modify: `clickhouse/tests/test-clickhouse.py`

Needs a checkpoint and the CLI in the WSL clone: `pip3 install --user huggingface_hub && make model && make cli` (about 840 MB, cached afterwards).

- [ ] **Step 1: Write the tests**

Add near the top of the file, after `VERSION`:

```python
CLI_PATH = os.environ.get("LAYA_CLI", str(ROOT / "build/bin/laya-cli"))
CLI_FLAGS = os.environ.get("LAYA_CLI_FLAGS", "--cpu").split()
SMOKE_CASES = ROOT / "laya.cpp/benchmarks/cases/smoke.json"
TOLERANCE = 1e-4  # laya.cpp's own acceptance tolerance for public numbers.
```

Add a new class before `if __name__ == "__main__":`:

```python
@unittest.skipUnless(MODEL_DIR, "set DQ_MODEL_DIR to a checkpoint directory to run model-backed tests")
class TestModel(unittest.TestCase):
  """Against a real checkpoint: behaviour, silence on stderr, parity with laya-cli."""

  BODY = "I was charged twice. Please refund the extra charge today."
  DEPARTMENTS = '{"billing": "payments and refunds", "technical": "bugs and outages", "sales": "new contracts"}'

  @classmethod
  def setUpClass(cls):
    cls.flags = [f"--backend={MODEL_DIR}"] + ([f"--options={DQ_OPTIONS}"] if DQ_OPTIONS else [])

  def query(self, sql):
    return clickhouse(sql, str(Path(MODEL_DIR).resolve()), DQ_OPTIONS)

  def test_worker_is_silent_after_loading_a_checkpoint(self):
    results, stderr, code = run_worker([{"state": "Please refund the duplicate charge.", "questions": QUESTION}],
                                       *self.flags)
    self.assertEqual((stderr, code), ("", 0))
    self.assertGreater(json.loads(results[0])["q"]["noul"], 0.5)

  def test_decisions_about_a_ticket(self):
    rows, stderr, code = self.query(f"""
      SELECT dq_backend(),
             noul('{self.BODY}', 'Does the customer request a refund?', ''),
             noul('The service works well. Thank you!', 'Is this a complaint?', ''),
             noul('{self.BODY}', 'Does the customer request a refund?',
                  '{{"true": "a refund is requested", "false": "no refund is requested"}}'),
             choice('{self.BODY}', 'Which department should handle this?', '{self.DEPARTMENTS}'),
             choice(map('subject', 'Duplicate invoice', 'body', '{self.BODY}'),
                    'Which department should handle this?', '["billing", "technical", "sales"]'),
             score('{self.BODY}', 'How urgent is the request?', '["not urgent", "soon", "immediate"]')""")
    self.assertEqual(code, 0, stderr)
    backend, refund, complaint, described, department, department_from_fields, urgency = rows[0]
    self.assertTrue(backend)
    self.assertGreater(refund, 0.5)
    self.assertLess(complaint, 0.5)
    self.assertGreater(described, 0.5)
    self.assertEqual((department, department_from_fields), ("billing", "billing"))
    self.assertTrue(0.0 <= urgency <= 2.0, urgency)

  def test_decide_answers_several_questions_in_one_pass(self):
    questions = json.dumps({
      "department": {"type": "choice", "instructions": "Which department should handle this?",
                     "criteria": json.loads(self.DEPARTMENTS)},
      "urgency": {"type": "score", "instructions": "How urgent is the request?",
                  "criteria": ["not urgent", "soon", "immediate"]},
      "refund": {"type": "noul", "instructions": "Does the customer request a refund?"}})
    rows, stderr, code = self.query(
      f"SELECT decide(map('subject', 'Duplicate invoice', 'body', '{self.BODY}'), '{questions}')")
    self.assertEqual(code, 0, stderr)
    answers = json.loads(rows[0][0])
    self.assertEqual(list(answers), ["department", "urgency", "refund"])
    self.assertEqual(answers["department"]["choice"], "billing")
    self.assertGreater(answers["department"]["probabilities"]["billing"], 0.5)
    self.assertGreater(answers["refund"]["noul"], 0.5)
    self.assertTrue(0.0 <= answers["urgency"]["score"] <= 2.0)

  def test_table_scan(self):
    rows, stderr, code = self.query(f"""
      CREATE TABLE tickets (id UInt8, body String) ENGINE = Memory;
      INSERT INTO tickets VALUES (1, '{self.BODY}'), (2, 'The service works well. Thank you!'),
                                 (3, 'The login page returns a 500 error since this morning.');
      SELECT id, choice(body, 'Which department should handle this?', '["billing", "technical", "sales"]')
        FROM tickets WHERE noul(body, 'Does the customer request a refund?', '') > 0.5 ORDER BY id""")
    self.assertEqual((rows, code), ([[1, "billing"]], 0), stderr)

  @unittest.skipUnless(os.path.exists(CLI_PATH), "build laya-cli (make cli) to run the parity test")
  def test_cli_parity(self):
    cases = json.loads(SMOKE_CASES.read_text())
    stdin = "\n".join(json.dumps({"state": case["state"], "questions": case["questions"]}) for case in cases) + "\n"
    cli = subprocess.run([CLI_PATH, "--model", MODEL_DIR] + CLI_FLAGS, input=stdin,
                         capture_output=True, text=True, check=True)
    expected = [json.loads(line)["results"][0]["answers"] for line in cli.stdout.splitlines() if line.strip()]
    results, stderr, code = run_worker(
      [{"state": case["state"], "questions": json.dumps(case["questions"])} for case in cases], *self.flags)
    self.assertEqual((stderr, code), ("", 0))
    self.assertEqual(len(results), len(cases))
    for case, want, got in zip(cases, expected, results):
      self.assert_close(want, json.loads(got), case["id"])

  def assert_close(self, expected, actual, path):
    if isinstance(expected, dict):
      self.assertEqual(list(expected), list(actual), path)
      for key in expected:
        self.assert_close(expected[key], actual[key], f"{path}.{key}")
    elif isinstance(expected, (int, float)) and not isinstance(expected, bool):
      self.assertAlmostEqual(expected, actual, delta=TOLERANCE, msg=path)
    else:
      self.assertEqual(expected, actual, path)
```

- [ ] **Step 2: Run the tests**

Run: `DQ_MODEL_DIR=models/laya DQ_OPTIONS='{"cuda":false}' make test-clickhouse`
Expected: `Ran 29 tests ... OK`. These tests pin behaviour the worker already has; the one that can fail is `test_worker_is_silent_after_loading_a_checkpoint`. If it does, the checkpoint's runtime prints outside the silenced load; extend `silenced_output` to wrap `engine.answers` as well, in a separate `fix(clickhouse):` commit, and rerun.

Without a checkpoint, `make test-clickhouse` reports `Ran 29 tests ... OK (skipped=5)`; report the skip count every time.

- [ ] **Step 3: Commit**

`test(clickhouse): add the model-backed tests and laya-cli parity`

---

### Task 14: CI

**Files:**
- Modify: `.github/workflows/build.yml`
- Modify: `.github/workflows/integration-local.yml`

- [ ] **Step 1: Add the build job**

Append to `.github/workflows/build.yml`:

```yaml
  clickhouse-linux-x86_64:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          submodules: recursive
      - run: sudo apt-get update && sudo apt-get install -y cmake ninja-build libicu-dev nlohmann-json3-dev libssl-dev
      # The oldest supported release, so the suite proves the minimum-version claim.
      - name: Install ClickHouse 25.3 LTS
        run: |
          curl -fsSL -o clickhouse.tgz https://github.com/ClickHouse/ClickHouse/releases/download/v25.3.14.14-lts/clickhouse-common-static-25.3.14.14-amd64.tgz
          tar -xzf clickhouse.tgz
          sudo install -m 755 "$(find clickhouse-common-static-25.3.14.14 -type f -name clickhouse | head -1)" /usr/local/bin/clickhouse
          clickhouse local --version
      - run: make clickhouse
      - run: make test-clickhouse
      - uses: actions/upload-artifact@v4
        with:
          name: decision-query-clickhouse-linux-x86_64
          path: |
            build/clickhouse/decision-query-udf
            build/clickhouse/decision_query_function.xml
            build/clickhouse/decision_query.sql
```

- [ ] **Step 2: Add the model-backed step**

In `.github/workflows/integration-local.yml`, after the `make loadable` step add:

```yaml
      - name: Install ClickHouse 25.3 LTS
        run: |
          curl -fsSL -o clickhouse.tgz https://github.com/ClickHouse/ClickHouse/releases/download/v25.3.14.14-lts/clickhouse-common-static-25.3.14.14-amd64.tgz
          tar -xzf clickhouse.tgz
          sudo install -m 755 "$(find clickhouse-common-static-25.3.14.14 -type f -name clickhouse | head -1)" /usr/local/bin/clickhouse
      - run: make clickhouse cli
      - name: Model-backed checks through ClickHouse
        run: DQ_MODEL_DIR=models/laya DQ_OPTIONS='{"cuda":false}' make test-clickhouse
```

- [ ] **Step 3: Verify**

Commit `ci(clickhouse): build and test the module on ClickHouse 25.3`, push the branch (no pull request), then run `gh run list --branch feat/clickhouse --limit 3` and `gh run watch` until the `clickhouse-linux-x86_64` job is green. If the tarball layout differs from the `find` assumption, fix the install step in a follow-up `ci(clickhouse):` commit. The `integration-local` workflow only runs on a pull request or by hand: trigger it with `gh workflow run integration-local.yml --ref feat/clickhouse` and watch it.

---

### Task 15: Module README

**Files:**
- Create: `clickhouse/README.md`

- [ ] **Step 1: Write it**

```markdown
# decision-query for ClickHouse

The same typed decisions as the SQLite and PostgreSQL modules, as ClickHouse
functions. A worker process, `decision-query-udf`, links the shared engine and
holds one resident model; ClickHouse starts it from an XML declaration and the
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

Four steps. The first three are `make clickhouse-install`.

1. Build: `make clickhouse DQ_MODEL_DIR=/srv/models/laya` produces
   `build/clickhouse/decision-query-udf`, `decision_query_function.xml` and
   `decision_query.sql`. `DQ_MODEL_DIR` may also be the URL of a System One
   endpoint; `DQ_OPTIONS='{"cuda":false}'` sets the load options (no spaces).
2. Copy the worker into ClickHouse's scripts directory,
   `/var/lib/clickhouse/user_scripts/`, executable by the `clickhouse` user.
3. Copy the XML into `/etc/clickhouse-server/`. ClickHouse reads every
   `*_function.xml` there. The one line to edit later is the `--backend=` path
   in `<command>`.
4. Create the wrappers once: `clickhouse-client --queries-file build/clickhouse/decision_query.sql`.

Then `SELECT dq_backend()` reports `CPU`, `CUDA0` or `remote`. It starts a
worker, loads the model and exits, so it takes a few seconds.

`sudo make clickhouse-install` does steps 2 and 3 with `CLICKHOUSE_PREFIX=/`.
To try it without installing, mount the three files into the official image:

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
  "passed 2, should be 3".

`state` is text, or a `map(...)` or `JSON` value for structured state, which the
model sees as labelled fields. `criteria` and `questions` are JSON text. Any NULL
argument yields NULL without running the model. Invalid JSON raises the same
messages as the other modules, before a worker is involved.

## Settings in the XML

| Setting | Shipped value | Why |
|---|---|---|
| `pool_size` | 1 | Each worker holds a model copy. Raise it only with the memory to match. |
| `command_read_timeout` | 600000 ms | Must cover a cold model load. ClickHouse's default is 10 s. |
| `command_write_timeout` | 600000 ms | Symmetric with the read timeout. |
| `max_command_execution_time` | 3600 s | Per block of rows. |
| `stderr_reaction` | throw | The worker's only stderr output is an error message, which becomes the query's error. |

CPU inference runs at about one row per second and a block is 65,409 rows by
default, so add `SETTINGS max_block_size = 64` to table-scale queries on the CPU.

Two models side by side: duplicate the `dq_decide` block under another name with
another `--backend=`, and wrap it as the SQL file does.

## Tests

```sh
make clickhouse test-clickhouse                             # needs a clickhouse binary on PATH
DQ_MODEL_DIR=models/laya make model cli test-clickhouse     # adds the model-backed and parity tests
```

The suite drives `clickhouse local` against a local fake System One endpoint, so
the model-free tests need no checkpoint. Get a binary with `curl https://clickhouse.com/ | sh`.

## Limitations

- One model copy per pool worker; a pool of one serializes all queries.
- Scalar functions run one forward pass per row; `decide()` batches several
  questions about the same row. Cross-row batching is not available.
- A worker error (unreachable endpoint, bad checkpoint path) fails the query and
  restarts the worker, which reloads the model.
- Editing the XML: ClickHouse reloads `*_function.xml` files on its own schedule;
  `SYSTEM RELOAD FUNCTIONS` or a restart makes the change immediate.
```

- [ ] **Step 2: Verify**

Check every command named in it exists: `make -n clickhouse clickhouse-install test-clickhouse` runs without "No rule to make target". Check the SQL examples parse by running the first two through the harness: `python3 -c` is not needed; the `TestModel.test_table_scan` query already covers the shape.

- [ ] **Step 3: Commit**

`docs(clickhouse): add the module README`

---

### Task 16: Root README

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Edit**

1. First paragraph: "Typed decisions as SQL functions, for SQLite and PostgreSQL." becomes "Typed decisions as SQL functions, for SQLite, PostgreSQL and ClickHouse."
2. After the PostgreSQL example in the first code block, add:

```sql
-- ClickHouse
SELECT id FROM tickets WHERE noul(body, 'Does the customer request a refund?', '') > 0.5;
```

3. "Both modules provide ..." becomes "All three modules provide `dq_version`, `dq_backend`, `noul`, `choice`, `score` and `decide()`, which returns the full answers object as JSON; SQLite and PostgreSQL also provide `dq_load`, and ClickHouse sets the model in its XML instead."
4. Layout table, new row after `postgres/`:

```markdown
| [`clickhouse/`](clickhouse/README.md) | ClickHouse executable function: the `decision-query-udf` worker, the XML that declares it and the SQL wrappers, with tests driven through `clickhouse local`. |
```

5. Build section, after `sudo make postgres-install`:

```sh
make clickhouse             # ClickHouse: build/clickhouse/decision-query-udf, decision_query_function.xml, decision_query.sql
sudo make clickhouse-install
```

and after the CUDA paragraph: "The ClickHouse worker needs no ClickHouse headers and always builds; `-DDQ_CLICKHOUSE=OFF` skips it."

6. Tests section, add: `make test-clickhouse                                   # ClickHouse, needs a clickhouse binary on PATH`.
7. Limitations, last bullet: "SQLite keeps one model per process; PostgreSQL backends and ClickHouse pool workers each load their own copy. See the module READMEs."

- [ ] **Step 2: Verify**

`grep -n "clickhouse" README.md` shows all seven edits; `make -n clickhouse clickhouse-install test-clickhouse` resolves.

- [ ] **Step 3: Commit**

`docs: add the ClickHouse module to the root README`

---

## Self-review

**Spec coverage.** Worker and line protocol: Tasks 1, 2, 4. Version and User-Agent: Task 3. XML with both functions and the timeouts: Tasks 5, 11. SQL wrappers, `''` criteria, validation messages, NULL: Tasks 6 to 10. Structured state through `Dynamic`: Tasks 2, 7. Errors in three layers: Tasks 1, 2, 8, 9, 10, 11. Install targets: Task 12. Model-backed and parity: Task 13. CI on 25.3: Task 14. Docs: Tasks 15, 16. The spec's "verify first" items: stderr silence is Task 13's first test; the per-block timeout was measured in the spike (a 2 s answer passed with the limit at 1 s); XML reload is documented in Task 15's last limitation; `JSONType` on invalid text returns `Null` (spike), so Task 8's ordering is safe.

**Names.** `decision-query-udf`, `dq_decide`, `dq_backend_raw`, `dq_question`, `DQ_CLICKHOUSE_COMMAND`, `DQ_CLICKHOUSE_SCRIPTS_DIR`, `DQ_CLICKHOUSE_CONFIG_DIR`, `CLICKHOUSE_PREFIX` are spelled the same in every task that uses them.

**Known passes-immediately tests.** Task 8's NULL test and Task 13's tests pin behaviour that earlier tasks produce; the plan says so where it happens.

## Execution notes

Where the executed branch departs from the tasks above, and why:

- **Eager evaluation (Tasks 8 to 10).** ClickHouse evaluates the executable function before `if()` picks a branch, so `throwIf` alone did not keep bad text away from the worker; its error won. The wrappers gained `dq_questions` and `dq_question_with`, which return NULL for unusable input, and the worker answers null to that.
- **Arity message (Task 9).** ClickHouse reports a wrong argument count to a SQL-defined function as "expect 3 arguments. Actual 2" (25.3) or "Actual: 2" (26.10), not "passed 2, should be 3". The test asserts on the shared part.
- **stderr on 25.3 (Task 11).** 26.10 already appends a failed worker's stderr to its error, so the tests passed there before the XML change; the red was observed on 25.3, which is why the suite is also run against the pinned minimum version.
- **Task 3.** The User-Agent name lives on another branch, so the test pins only the version suffix.
- **Harness (Task 13).** `clickhouse local` reads INSERT data from stdin when stdin is a pipe; the harness passes an empty stdin, otherwise table tests hang.
- **Checkpoint.** `make model` moves a directory whose files are relative symlinks into the Hugging Face cache, which breaks them; the links were replaced by copies by hand. Worth its own fix outside this branch.
