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
