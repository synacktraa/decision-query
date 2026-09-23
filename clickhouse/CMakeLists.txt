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
target_compile_definitions(decision-query-udf PRIVATE DQ_VERSION="v${DQ_VERSION_TEXT}")
target_link_libraries(decision-query-udf PRIVATE decision-query-engine)

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
configure_file(sql/decision_query.sql.in decision_query.sql @ONLY)

# Install layout, relative to the install prefix (make clickhouse-install uses /).
set(DQ_CLICKHOUSE_SCRIPTS_DIR "var/lib/clickhouse/user_scripts" CACHE PATH
  "ClickHouse's user_scripts_path, where executable functions must live")
set(DQ_CLICKHOUSE_CONFIG_DIR "etc/clickhouse-server" CACHE PATH
  "ClickHouse's server config directory, where *_function.xml files are read")
install(TARGETS decision-query-udf RUNTIME DESTINATION "${DQ_CLICKHOUSE_SCRIPTS_DIR}" COMPONENT clickhouse)
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/decision_query_function.xml"
  DESTINATION "${DQ_CLICKHOUSE_CONFIG_DIR}" COMPONENT clickhouse)
