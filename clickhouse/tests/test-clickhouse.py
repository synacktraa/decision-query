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
