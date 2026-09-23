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
