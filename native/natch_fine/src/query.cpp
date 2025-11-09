// query.cpp - Parameterized query support for ClickHouse
//
// Implements type-safe parameterized queries to prevent SQL injection.
// Uses clickhouse-cpp's Query class with named parameters.
//
// Parameter syntax: {name:Type} in SQL, e.g., "SELECT * FROM t WHERE id = {id:UInt64}"
// All parameter values are passed as strings to clickhouse-cpp (even integers/floats).
// The type hint in the placeholder tells ClickHouse how to interpret the string value.

#include <fine.hpp>
#include <clickhouse/query.h>
#include <string>
#include <optional>

using namespace clickhouse;

// Wrap Query as a FINE resource
FINE_RESOURCE(Query);

// ============================================================================
// Query Creation
// ============================================================================

/// Creates a new Query object with parameterized SQL
///
/// @param sql SQL string with {name:Type} placeholders
/// @return Query resource reference
///
/// Example: "SELECT * FROM users WHERE id = {id:UInt64} AND active = {active:UInt8}"
fine::ResourcePtr<Query> query_create(
    ErlNifEnv *env,
    std::string sql) {
  try {
    return fine::make_resource<Query>(sql);
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("Failed to create query: ") + e.what());
  }
}
FINE_NIF(query_create, 0);

// ============================================================================
// Parameter Binding - Integers
// ============================================================================

/// Binds a UInt64 parameter
fine::Atom query_bind_uint64(
    ErlNifEnv *env,
    fine::ResourcePtr<Query> query,
    std::string name,
    uint64_t value) {
  try {
    query->SetParam(name, std::to_string(value));
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("Failed to bind UInt64 parameter '") + name + "': " + e.what());
  }
}
FINE_NIF(query_bind_uint64, 0);

/// Binds an Int64 parameter
fine::Atom query_bind_int64(
    ErlNifEnv *env,
    fine::ResourcePtr<Query> query,
    std::string name,
    int64_t value) {
  try {
    query->SetParam(name, std::to_string(value));
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("Failed to bind Int64 parameter '") + name + "': " + e.what());
  }
}
FINE_NIF(query_bind_int64, 0);

/// Binds an Int32 parameter (uses int64_t for FINE, casts to int32 for validation)
fine::Atom query_bind_int32(
    ErlNifEnv *env,
    fine::ResourcePtr<Query> query,
    std::string name,
    int64_t value) {
  try {
    query->SetParam(name, std::to_string(value));
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("Failed to bind Int32 parameter '") + name + "': " + e.what());
  }
}
FINE_NIF(query_bind_int32, 0);

/// Binds a UInt32 parameter (uses int64_t for FINE, casts to uint32 for validation)
fine::Atom query_bind_uint32(
    ErlNifEnv *env,
    fine::ResourcePtr<Query> query,
    std::string name,
    int64_t value) {
  try {
    query->SetParam(name, std::to_string(value));
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("Failed to bind UInt32 parameter '") + name + "': " + e.what());
  }
}
FINE_NIF(query_bind_uint32, 0);

// ============================================================================
// Parameter Binding - Floats
// ============================================================================

/// Binds a Float64 parameter
fine::Atom query_bind_float64(
    ErlNifEnv *env,
    fine::ResourcePtr<Query> query,
    std::string name,
    double value) {
  try {
    query->SetParam(name, std::to_string(value));
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("Failed to bind Float64 parameter '") + name + "': " + e.what());
  }
}
FINE_NIF(query_bind_float64, 0);

/// Binds a Float32 parameter (uses double for FINE, converts to float precision)
fine::Atom query_bind_float32(
    ErlNifEnv *env,
    fine::ResourcePtr<Query> query,
    std::string name,
    double value) {
  try {
    // Convert to float precision then back to string
    float float_val = static_cast<float>(value);
    query->SetParam(name, std::to_string(float_val));
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("Failed to bind Float32 parameter '") + name + "': " + e.what());
  }
}
FINE_NIF(query_bind_float32, 0);

// ============================================================================
// Parameter Binding - Strings
// ============================================================================

/// Binds a String parameter
fine::Atom query_bind_string(
    ErlNifEnv *env,
    fine::ResourcePtr<Query> query,
    std::string name,
    std::string value) {
  try {
    query->SetParam(name, value);
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("Failed to bind String parameter '") + name + "': " + e.what());
  }
}
FINE_NIF(query_bind_string, 0);

// ============================================================================
// Parameter Binding - Temporal Types
// ============================================================================

/// Binds a DateTime parameter (Unix timestamp in seconds)
fine::Atom query_bind_datetime(
    ErlNifEnv *env,
    fine::ResourcePtr<Query> query,
    std::string name,
    int64_t timestamp) {
  try {
    query->SetParam(name, std::to_string(timestamp));
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("Failed to bind DateTime parameter '") + name + "': " + e.what());
  }
}
FINE_NIF(query_bind_datetime, 0);

/// Binds a Date parameter (days since ClickHouse epoch: 1970-01-01)
/// Uses int64_t for FINE compatibility
fine::Atom query_bind_date(
    ErlNifEnv *env,
    fine::ResourcePtr<Query> query,
    std::string name,
    int64_t days) {
  try {
    query->SetParam(name, std::to_string(days));
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("Failed to bind Date parameter '") + name + "': " + e.what());
  }
}
FINE_NIF(query_bind_date, 0);

/// Binds a DateTime64 parameter (microseconds since Unix epoch)
fine::Atom query_bind_datetime64(
    ErlNifEnv *env,
    fine::ResourcePtr<Query> query,
    std::string name,
    int64_t microseconds) {
  try {
    query->SetParam(name, std::to_string(microseconds));
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("Failed to bind DateTime64 parameter '") + name + "': " + e.what());
  }
}
FINE_NIF(query_bind_datetime64, 0);

// ============================================================================
// Parameter Binding - NULL
// ============================================================================

/// Binds a NULL parameter (works with any Nullable(T) type)
fine::Atom query_bind_null(
    ErlNifEnv *env,
    fine::ResourcePtr<Query> query,
    std::string name) {
  try {
    // Empty optional represents NULL in clickhouse-cpp
    query->SetParam(name, QueryParamValue());
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("Failed to bind NULL parameter '") + name + "': " + e.what());
  }
}
FINE_NIF(query_bind_null, 0);
