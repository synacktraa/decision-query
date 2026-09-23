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

  def test_identifies_itself_and_sends_the_key_option(self):
    run_worker([{"state": "text", "questions": QUESTION}],
               f"--backend={self.endpoint.url}", '--options={"key":"test-key"}')
    # The name before the slash is the engine's; the version must be dq_version()'s.
    user_agent = self.endpoint.requests[0]["user_agent"]
    self.assertTrue(user_agent.endswith("/" + VERSION), user_agent)
    self.assertEqual(self.endpoint.requests[0]["authorization"], "Bearer test-key")

  def test_print_backend_reports_the_backend_name(self):
    results, stderr, code = run_worker([{"dummy": 1}], f"--backend={self.endpoint.url}", "--print-backend")
    self.assertEqual((results, stderr, code), (["remote"], "", 0))
    self.assertEqual(self.endpoint.requests, [])


class TestBuild(unittest.TestCase):
  """Files that make clickhouse generates beside the worker."""

  def test_xml_declares_both_functions_with_the_worker_command(self):
    self.assertTrue(XML.exists(), f"{XML} is missing; build it with: make clickhouse")
    xml = XML.read_text()
    self.assertIn("<name>dq_decide</name>", xml)
    self.assertIn("<name>dq_backend_raw</name>", xml)
    self.assertEqual(xml.count("<command>decision-query-udf --backend="), 2)
    self.assertIn(" --print-backend</command>", xml)

  def test_install_places_the_worker_and_the_xml_under_a_prefix(self):
    with tempfile.TemporaryDirectory() as prefix:
      completed = subprocess.run(["cmake", "--install", str(BUILD.parent), "--component", "clickhouse",
                                  "--prefix", prefix], capture_output=True, text=True)
      self.assertEqual(completed.returncode, 0, completed.stderr)
      worker = Path(prefix) / "var/lib/clickhouse/user_scripts/decision-query-udf"
      self.assertTrue(os.access(worker, os.X_OK), f"{worker} is missing or not executable")
      self.assertTrue((Path(prefix) / "etc/clickhouse-server/decision_query_function.xml").exists())



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

  def test_decide_sends_the_request_shape_and_returns_the_answers(self):
    rows, stderr, code = self.query(f"SELECT decide('I was charged twice', '{QUESTION}')")
    self.assertEqual(code, 0, stderr)
    self.assertEqual(json.loads(rows[0][0]), self.endpoint.answers)
    self.assertEqual(self.endpoint.requests[0]["body"],
                     {"state": "I was charged twice", "questions": json.loads(QUESTION), "model": "jev-latest"})

  def test_structured_state_reaches_the_endpoint_as_an_object(self):
    rows, stderr, code = self.query(f"""SELECT
      decide(map('subject', 'Duplicate invoice', 'body', 'I was charged twice'), '{QUESTION}'),
      decide('{{"subject": "looks like JSON"}}', '{QUESTION}')""")
    self.assertEqual(code, 0, stderr)
    self.assertCountEqual([request["body"]["state"] for request in self.endpoint.requests],
                          [{"subject": "Duplicate invoice", "body": "I was charged twice"},
                           '{"subject": "looks like JSON"}'])

  def test_a_table_scan_goes_through_one_worker_in_order(self):
    rows, stderr, code = self.query(f"SELECT decide(x, '{QUESTION}') FROM (SELECT arrayJoin(['a', 'b', 'c']) AS x)")
    self.assertEqual(code, 0, stderr)
    self.assertEqual(len(rows), 3)
    self.assertEqual([request["body"]["state"] for request in self.endpoint.requests], ["a", "b", "c"])

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

  def test_noul_builds_the_question_with_and_without_criteria(self):
    rows, stderr, code = self.query("""SELECT
      noul('body', 'Is this a "refund"?', ''),
      noul('body', 'Is this a refund?', '{"true": "a refund is requested", "false": "no refund is requested"}')""")
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
    self.assertNotIn("Child process", stderr, "the wrapper, not the worker, must reject it")
    self.assertEqual(self.endpoint.requests, [])

  def test_noul_null_in_null_out(self):
    rows, stderr, code = self.query("""SELECT
      noul(CAST(NULL AS Nullable(String)), 'question', ''),
      noul('body', CAST(NULL AS Nullable(String)), ''),
      noul('body', 'question', CAST(NULL AS Nullable(String)))""")
    self.assertEqual((rows, code), ([[None, None, None]], 0), stderr)
    self.assertEqual(self.endpoint.requests, [])

  def test_noul_takes_exactly_three_arguments(self):
    # ClickHouse words this differently for SQL-defined functions than for built-ins.
    rows, stderr, code = self.query("SELECT noul('body', 'question')")
    self.assertNotEqual(code, 0)
    self.assertIn("expect 3 arguments. Actual: 2", stderr)

  def test_choice_and_score_extract_their_answers(self):
    rows, stderr, code = self.query("""SELECT
      choice('body', 'Which department?', '{"billing": "payments", "technical": "bugs"}'),
      choice('body', 'Which department?', '["billing", "technical"]'),
      score('body', 'How urgent?', '["not urgent", "soon", "immediate"]')""")
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
      self.assertNotIn("Child process", stderr, "the wrapper, not the worker, must reject it")
    self.assertEqual(self.endpoint.requests, [])

  def test_choice_and_score_null_in_null_out(self):
    rows, stderr, code = self.query("""SELECT
      choice(CAST(NULL AS Nullable(String)), 'q', '["a"]'),
      score('body', 'q', CAST(NULL AS Nullable(String)))""")
    self.assertEqual((rows, code), ([[None, None]], 0), stderr)
    self.assertEqual(self.endpoint.requests, [])

  def test_endpoint_errors_reach_the_client(self):
    self.endpoint.status = 500
    rows, stderr, code = self.query(f"SELECT decide('state', '{QUESTION}')")
    self.assertNotEqual(code, 0)
    self.assertIn("Decision endpoint returned HTTP 500", stderr)

  def test_bad_backend_path_reaches_the_client(self):
    rows, stderr, code = clickhouse(f"SELECT decide('state', '{QUESTION}')", "/nonexistent/checkpoint")
    self.assertNotEqual(code, 0)
    self.assertIn("Not a checkpoint directory: /nonexistent/checkpoint", stderr)


if __name__ == "__main__":
  unittest.main()
