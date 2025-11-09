cmake_minimum_required(VERSION 3.15)
project(natch_fine)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Find Erlang/OTP for NIF headers
# Get Erlang root directory from environment or auto-detect
if(DEFINED ENV{ERL_ROOT})
  set(ERLANG_ROOT_DIR $ENV{ERL_ROOT})
else()
  execute_process(
    COMMAND erl -noshell -eval "io:format(\"~s~n\", [code:root_dir()]), halt()."
    OUTPUT_VARIABLE ERLANG_ROOT_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )
endif()

set(ERLANG_INCLUDE_DIR "${ERLANG_ROOT_DIR}/usr/include")

# Find clickhouse-cpp library
# Try in order of preference:
# 1. Environment variable CLICKHOUSE_CPP_DIR (for custom builds/CI)
# 2. Git submodule at ../clickhouse-cpp (standard location)

if(DEFINED ENV{CLICKHOUSE_CPP_DIR})
  set(CLICKHOUSE_CPP_DIR $ENV{CLICKHOUSE_CPP_DIR})
  message(STATUS "Using clickhouse-cpp from environment: ${CLICKHOUSE_CPP_DIR}")
elseif(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/../clickhouse-cpp/CMakeLists.txt")
  set(CLICKHOUSE_CPP_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../clickhouse-cpp")
  message(STATUS "Using clickhouse-cpp from submodule: ${CLICKHOUSE_CPP_DIR}")
else()
  message(FATAL_ERROR
    "clickhouse-cpp not found! Please either:\n"
    "  1. Initialize git submodules: git submodule update --init --recursive\n"
    "  2. Set CLICKHOUSE_CPP_DIR environment variable")
endif()

# Enable SSL support in clickhouse-cpp
set(WITH_OPENSSL ON CACHE BOOL "Enable OpenSSL support" FORCE)

add_subdirectory(${CLICKHOUSE_CPP_DIR} ${CMAKE_BINARY_DIR}/clickhouse-cpp EXCLUDE_FROM_ALL)

# FINE headers - find in deps
if(DEFINED ENV{MIX_DEPS_PATH})
  set(FINE_DIR "$ENV{MIX_DEPS_PATH}/fine")
else()
  # Default Mix deps location
  get_filename_component(FINE_DIR "${CMAKE_SOURCE_DIR}/../../deps/fine" ABSOLUTE)
endif()

# Build NIF shared library
add_library(natch_fine SHARED
  src/minimal.cpp
  src/column.cpp
  src/block.cpp
  src/select.cpp
  src/query.cpp
)

# Link against clickhouse-cpp
target_link_libraries(natch_fine
  PRIVATE
    clickhouse-cpp-lib
)

# Include directories
target_include_directories(natch_fine
  PRIVATE
    ${CLICKHOUSE_CPP_DIR}
    ${ERLANG_INCLUDE_DIR}
    ${FINE_DIR}/c_include
)

# Set visibility and naming
set_target_properties(natch_fine PROPERTIES
  CXX_VISIBILITY_PRESET hidden
  PREFIX ""
  SUFFIX ".so"
)

# Output to priv/ directory
# Use MIX_APP_PATH if set (for precompilation), otherwise use project priv
if(DEFINED ENV{MIX_APP_PATH})
  set(PRIV_OUTPUT_DIR $ENV{MIX_APP_PATH}/priv)
else()
  set(PRIV_OUTPUT_DIR ${CMAKE_SOURCE_DIR}/../../priv)
endif()

set_target_properties(natch_fine PROPERTIES
  LIBRARY_OUTPUT_DIRECTORY ${PRIV_OUTPUT_DIR}
)

# Apple-specific settings
if(APPLE)
  set_target_properties(natch_fine PROPERTIES
    SUFFIX ".so"
    LINK_FLAGS "-undefined dynamic_lookup"
  )
endif()
