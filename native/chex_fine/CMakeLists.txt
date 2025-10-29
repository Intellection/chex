cmake_minimum_required(VERSION 3.15)
project(chex_fine)

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

# Add clickhouse-cpp from external directory
set(CLICKHOUSE_CPP_DIR "/Users/brendon/work/clickhouse-cpp")
add_subdirectory(${CLICKHOUSE_CPP_DIR} ${CMAKE_BINARY_DIR}/clickhouse-cpp EXCLUDE_FROM_ALL)

# FINE headers - find in deps
if(DEFINED ENV{MIX_DEPS_PATH})
  set(FINE_DIR "$ENV{MIX_DEPS_PATH}/fine")
else()
  # Default Mix deps location
  get_filename_component(FINE_DIR "${CMAKE_SOURCE_DIR}/../../deps/fine" ABSOLUTE)
endif()

# Build NIF shared library
add_library(chex_fine SHARED
  src/minimal.cpp
  src/column.cpp
  src/block.cpp
  src/select.cpp
)

# Link against clickhouse-cpp
target_link_libraries(chex_fine
  PRIVATE
    clickhouse-cpp-lib
)

# Include directories
target_include_directories(chex_fine
  PRIVATE
    ${CLICKHOUSE_CPP_DIR}
    ${ERLANG_INCLUDE_DIR}
    ${FINE_DIR}/c_include
)

# Set visibility and naming
set_target_properties(chex_fine PROPERTIES
  CXX_VISIBILITY_PRESET hidden
  PREFIX ""
  SUFFIX ".so"
)

# Output to priv/ directory
set_target_properties(chex_fine PROPERTIES
  LIBRARY_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/../../priv
)

# Apple-specific settings
if(APPLE)
  set_target_properties(chex_fine PROPERTIES
    SUFFIX ".so"
    LINK_FLAGS "-undefined dynamic_lookup"
  )
endif()
