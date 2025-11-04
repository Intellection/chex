#include <fine.hpp>
#include <clickhouse/client.h>
#include <clickhouse/columns/factory.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/uuid.h>
#include <clickhouse/columns/decimal.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/array.h>
#include <clickhouse/columns/tuple.h>
#include <clickhouse/columns/map.h>
#include <clickhouse/columns/lowcardinality.h>
#include <string>
#include <memory>
#include <stdexcept>
#include "error_encoding.h"

using namespace clickhouse;

// Wrapper to hold shared_ptr<Column> since FINE uses ResourcePtr
struct ColumnResource {
  std::shared_ptr<Column> ptr;

  ColumnResource(std::shared_ptr<Column> p) : ptr(p) {}
};

// Declare ColumnResource as a FINE resource
FINE_RESOURCE(ColumnResource);

// Create a column by type name
// Uses clickhouse-cpp's CreateColumnByType for dynamic type creation
fine::ResourcePtr<ColumnResource> column_create(
    ErlNifEnv *env,
    std::string type_name) {
  try {
    auto col = CreateColumnByType(type_name);
    if (!col) {
      throw std::runtime_error("Failed to create column of type: " + type_name);
    }
    return fine::make_resource<ColumnResource>(col);
  } catch (const std::exception& e) {
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(column_create, 0);

// Append UInt64 value
// DEPRECATED: Use column_uint64_append_bulk for better performance
fine::Atom column_uint64_append(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res,
    uint64_t value) {
  try {
    auto typed = std::static_pointer_cast<ColumnUInt64>(col_res->ptr);
    typed->Append(value);
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(column_uint64_append, 0);

// Append Int64 value
// DEPRECATED: Use column_int64_append_bulk for better performance
fine::Atom column_int64_append(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res,
    int64_t value) {
  try {
    auto typed = std::static_pointer_cast<ColumnInt64>(col_res->ptr);
    typed->Append(value);
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(column_int64_append, 0);

// Append String value
// DEPRECATED: Use column_string_append_bulk for better performance
fine::Atom column_string_append(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res,
    std::string value) {
  try {
    auto typed = std::static_pointer_cast<ColumnString>(col_res->ptr);
    typed->Append(value);
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(column_string_append, 0);

// Append Float64 value
// DEPRECATED: Use column_float64_append_bulk for better performance
fine::Atom column_float64_append(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res,
    double value) {
  try {
    auto typed = std::static_pointer_cast<ColumnFloat64>(col_res->ptr);
    typed->Append(value);
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(column_float64_append, 0);

// Append DateTime value (Unix timestamp as uint64)
// DEPRECATED: Use column_datetime_append_bulk for better performance
fine::Atom column_datetime_append(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res,
    uint64_t timestamp) {
  try {
    auto typed = std::static_pointer_cast<ColumnDateTime>(col_res->ptr);
    typed->Append(static_cast<time_t>(timestamp));
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(column_datetime_append, 0);

// Get column size (number of rows)
uint64_t column_size(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res) {
  return col_res->ptr->Size();
}
FINE_NIF(column_size, 0);

//
// BULK APPEND OPERATIONS
// These functions accept vectors of values for efficient bulk insertion
// Reduces NIF boundary crossings from N (one per value) to 1 (one per column)
//

// Bulk append UInt64 values
fine::Atom column_uint64_append_bulk(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res,
    std::vector<uint64_t> values) {
  try {
    auto typed = std::static_pointer_cast<ColumnUInt64>(col_res->ptr);
    for (const auto& value : values) {
      typed->Append(value);
    }
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(column_uint64_append_bulk, 0);

// Bulk append Int64 values
fine::Atom column_int64_append_bulk(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res,
    std::vector<int64_t> values) {
  try {
    auto typed = std::static_pointer_cast<ColumnInt64>(col_res->ptr);
    for (const auto& value : values) {
      typed->Append(value);
    }
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(column_int64_append_bulk, 0);

// Bulk append String values
fine::Atom column_string_append_bulk(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res,
    std::vector<std::string> values) {
  try {
    auto typed = std::static_pointer_cast<ColumnString>(col_res->ptr);
    for (const auto& value : values) {
      typed->Append(value);
    }
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(column_string_append_bulk, 0);

// Bulk append Float64 values
fine::Atom column_float64_append_bulk(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res,
    std::vector<double> values) {
  try {
    auto typed = std::static_pointer_cast<ColumnFloat64>(col_res->ptr);
    for (const auto& value : values) {
      typed->Append(value);
    }
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(column_float64_append_bulk, 0);

// Bulk append DateTime values (Unix timestamps as uint64)
fine::Atom column_datetime_append_bulk(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res,
    std::vector<uint64_t> timestamps) {
  try {
    auto typed = std::static_pointer_cast<ColumnDateTime>(col_res->ptr);
    for (const auto& timestamp : timestamps) {
      typed->Append(static_cast<time_t>(timestamp));
    }
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(column_datetime_append_bulk, 0);

// Bulk append DateTime64 values (microsecond timestamps as int64)
fine::Atom column_datetime64_append_bulk(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res,
    std::vector<int64_t> ticks) {
  try {
    auto typed = std::static_pointer_cast<ColumnDateTime64>(col_res->ptr);
    for (const auto& tick : ticks) {
      typed->Append(tick);
    }
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(column_datetime64_append_bulk, 0);

// Bulk append Decimal64 values (scaled int64 values)
fine::Atom column_decimal_append_bulk(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res,
    std::vector<int64_t> scaled_values) {
  try {
    auto typed = std::static_pointer_cast<ColumnDecimal>(col_res->ptr);
    for (const auto& value : scaled_values) {
      // Convert int64 to Int128 for ColumnDecimal
      typed->Append(Int128(value));
    }
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(column_decimal_append_bulk, 0);

// Bulk append Nullable(UInt64) values
fine::Atom column_nullable_uint64_append_bulk(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res,
    std::vector<uint64_t> values,
    std::vector<uint64_t> nulls) {
  try {
    auto nullable_col = std::static_pointer_cast<ColumnNullable>(col_res->ptr);
    auto nested = nullable_col->Nested()->As<ColumnUInt64>();
    auto null_map = nullable_col->Nulls()->As<ColumnUInt8>();

    for (size_t i = 0; i < values.size(); i++) {
      nested->Append(values[i]);
      null_map->Append(static_cast<uint8_t>(nulls[i]));
    }
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(column_nullable_uint64_append_bulk, 0);

// Bulk append Nullable(Int64) values
fine::Atom column_nullable_int64_append_bulk(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res,
    std::vector<int64_t> values,
    std::vector<uint64_t> nulls) {
  try {
    auto nullable_col = std::static_pointer_cast<ColumnNullable>(col_res->ptr);
    auto nested = nullable_col->Nested()->As<ColumnInt64>();
    auto null_map = nullable_col->Nulls()->As<ColumnUInt8>();

    for (size_t i = 0; i < values.size(); i++) {
      nested->Append(values[i]);
      null_map->Append(static_cast<uint8_t>(nulls[i]));
    }
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(column_nullable_int64_append_bulk, 0);

// Bulk append Nullable(String) values
fine::Atom column_nullable_string_append_bulk(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res,
    std::vector<std::string> values,
    std::vector<uint64_t> nulls) {
  try {
    auto nullable_col = std::static_pointer_cast<ColumnNullable>(col_res->ptr);
    auto nested = nullable_col->Nested()->As<ColumnString>();
    auto null_map = nullable_col->Nulls()->As<ColumnUInt8>();

    for (size_t i = 0; i < values.size(); i++) {
      nested->Append(values[i]);
      null_map->Append(static_cast<uint8_t>(nulls[i]));
    }
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(column_nullable_string_append_bulk, 0);

// Bulk append Nullable(Float64) values
fine::Atom column_nullable_float64_append_bulk(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res,
    std::vector<double> values,
    std::vector<uint64_t> nulls) {
  try {
    auto nullable_col = std::static_pointer_cast<ColumnNullable>(col_res->ptr);
    auto nested = nullable_col->Nested()->As<ColumnFloat64>();
    auto null_map = nullable_col->Nulls()->As<ColumnUInt8>();

    for (size_t i = 0; i < values.size(); i++) {
      nested->Append(values[i]);
      null_map->Append(static_cast<uint8_t>(nulls[i]));
    }
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(column_nullable_float64_append_bulk, 0);

//
// PHASE 5C - ADDITIONAL TYPE SUPPORT
// Bulk append operations for Bool, Date, Float32, and additional integer types
//

// Bulk append Date values (days since epoch as uint16)
fine::Atom column_date_append_bulk(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res,
    std::vector<uint64_t> days) {
  try {
    auto typed = std::static_pointer_cast<ColumnDate>(col_res->ptr);
    for (const auto& day : days) {
      typed->AppendRaw(static_cast<uint16_t>(day));
    }
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(column_date_append_bulk, 0);

// Bulk append UInt8 values (used for Bool)
fine::Atom column_uint8_append_bulk(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res,
    std::vector<uint64_t> values) {
  try {
    auto typed = std::static_pointer_cast<ColumnUInt8>(col_res->ptr);
    for (const auto& value : values) {
      typed->Append(static_cast<uint8_t>(value));
    }
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(column_uint8_append_bulk, 0);

// Bulk append UInt32 values
fine::Atom column_uint32_append_bulk(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res,
    std::vector<uint64_t> values) {
  try {
    auto typed = std::static_pointer_cast<ColumnUInt32>(col_res->ptr);
    for (const auto& value : values) {
      typed->Append(static_cast<uint32_t>(value));
    }
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(column_uint32_append_bulk, 0);

// Bulk append UInt16 values
fine::Atom column_uint16_append_bulk(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res,
    std::vector<uint64_t> values) {
  try {
    auto typed = std::static_pointer_cast<ColumnUInt16>(col_res->ptr);
    for (const auto& value : values) {
      typed->Append(static_cast<uint16_t>(value));
    }
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(column_uint16_append_bulk, 0);

// Bulk append Int32 values
fine::Atom column_int32_append_bulk(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res,
    std::vector<int64_t> values) {
  try {
    auto typed = std::static_pointer_cast<ColumnInt32>(col_res->ptr);
    for (const auto& value : values) {
      typed->Append(static_cast<int32_t>(value));
    }
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(column_int32_append_bulk, 0);

// Bulk append Int16 values
fine::Atom column_int16_append_bulk(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res,
    std::vector<int64_t> values) {
  try {
    auto typed = std::static_pointer_cast<ColumnInt16>(col_res->ptr);
    for (const auto& value : values) {
      typed->Append(static_cast<int16_t>(value));
    }
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(column_int16_append_bulk, 0);

// Bulk append Int8 values
fine::Atom column_int8_append_bulk(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res,
    std::vector<int64_t> values) {
  try {
    auto typed = std::static_pointer_cast<ColumnInt8>(col_res->ptr);
    for (const auto& value : values) {
      typed->Append(static_cast<int8_t>(value));
    }
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(column_int8_append_bulk, 0);

// Bulk append Float32 values
fine::Atom column_float32_append_bulk(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res,
    std::vector<double> values) {
  try {
    auto typed = std::static_pointer_cast<ColumnFloat32>(col_res->ptr);
    for (const auto& value : values) {
      typed->Append(static_cast<float>(value));
    }
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(column_float32_append_bulk, 0);

// Bulk append UUID values (separate lists of high and low 64-bit values)
fine::Atom column_uuid_append_bulk(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res,
    std::vector<uint64_t> highs,
    std::vector<uint64_t> lows) {
  try {
    if (highs.size() != lows.size()) {
      throw std::runtime_error("UUID highs and lows lists must be same length");
    }

    auto typed = std::static_pointer_cast<ColumnUUID>(col_res->ptr);
    for (size_t i = 0; i < highs.size(); i++) {
      // UUID is std::pair<uint64_t, uint64_t> where first=high, second=low
      typed->Append(UUID{highs[i], lows[i]});
    }
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(column_uuid_append_bulk, 0);

// ============================================================================
// Array Column Support
// ============================================================================

// Append pre-built nested column to array
// Works for ANY nested column type (Date, UUID, Nullable(T), Array(T), etc.)
// Supports arbitrary nesting: Array(Array(Array(T))) works via recursion
fine::Atom column_array_append_from_column(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> array_col_res,
    fine::ResourcePtr<ColumnResource> nested_col_res,
    std::vector<uint64_t> offsets) {
  try {
    if (!array_col_res->ptr) {
      throw std::runtime_error("Array column pointer is null");
    }
    if (!nested_col_res->ptr) {
      throw std::runtime_error("Nested column pointer is null");
    }

    auto array_col = std::static_pointer_cast<ColumnArray>(array_col_res->ptr);
    if (!array_col) {
      throw std::runtime_error("Failed to cast to ColumnArray");
    }

    ColumnRef nested_col = nested_col_res->ptr;
    size_t nested_size = nested_col->Size();

    size_t prev = 0;
    for (size_t offset : offsets) {
      if (offset < prev) {
        throw std::runtime_error("Offsets must be monotonically increasing");
      }
      if (offset > nested_size) {
        throw std::runtime_error("Offset " + std::to_string(offset) + " exceeds nested column size " + std::to_string(nested_size));
      }

      size_t count = offset - prev;

      // Slice nested column and append to array
      // This is type-agnostic - works for ANY column type!
      auto slice = nested_col->Slice(prev, count);
      if (!slice) {
        throw std::runtime_error("Slice returned null pointer");
      }

      array_col->AppendAsColumn(slice);
      prev = offset;
    }
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(column_array_append_from_column, 0);

// ============================================================================
// Tuple Type Support - Columnar API
// ============================================================================

// Append pre-built nested columns to tuple
// Columnar API: accepts pre-separated columns for maximum performance
// Works for ANY combination of column types
fine::Atom column_tuple_append_from_columns(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> tuple_col_res,
    std::vector<fine::ResourcePtr<ColumnResource>> nested_col_resources) {
  try {
    if (!tuple_col_res->ptr) {
      throw std::runtime_error("Tuple column pointer is null");
    }

    auto tuple_col = std::static_pointer_cast<ColumnTuple>(tuple_col_res->ptr);
    if (!tuple_col) {
      throw std::runtime_error("Failed to cast to ColumnTuple");
    }

    // Check that the number of columns matches
    if (nested_col_resources.size() != tuple_col->TupleSize()) {
      throw std::runtime_error("Column count mismatch: expected " +
                               std::to_string(tuple_col->TupleSize()) +
                               ", got " + std::to_string(nested_col_resources.size()));
    }

    // Validate all columns have the same size
    if (!nested_col_resources.empty()) {
      size_t expected_size = nested_col_resources[0]->ptr->Size();
      for (size_t i = 1; i < nested_col_resources.size(); i++) {
        if (nested_col_resources[i]->ptr->Size() != expected_size) {
          throw std::runtime_error("All columns must have the same size");
        }
      }
    }

    // Build vector of column refs
    std::vector<ColumnRef> columns;
    columns.reserve(nested_col_resources.size());
    for (auto& col_res : nested_col_resources) {
      if (!col_res->ptr) {
        throw std::runtime_error("Nested column pointer is null");
      }
      columns.push_back(col_res->ptr);
    }

    // Create temporary tuple column and append to main tuple
    auto temp_tuple = std::make_shared<ColumnTuple>(columns);
    tuple_col->Append(temp_tuple);

    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(column_tuple_append_from_columns, 0);

// ============================================================================
// Map Type Support - Columnar API
// ============================================================================

// Append pre-built Array(Tuple(K,V)) column to map
// Map is just syntax for Array(Tuple(K,V)), so we build the array and append it
fine::Atom column_map_append_from_array(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> map_col_res,
    fine::ResourcePtr<ColumnResource> array_tuple_col_res) {
  try {
    if (!map_col_res->ptr) {
      throw std::runtime_error("Map column pointer is null");
    }
    if (!array_tuple_col_res->ptr) {
      throw std::runtime_error("Array column pointer is null");
    }

    // ColumnMap wraps an Array(Tuple(K,V)) internally
    // We need to create a temporary ColumnMap from our array and append it
    auto array_col = std::static_pointer_cast<ColumnArray>(array_tuple_col_res->ptr);
    if (!array_col) {
      throw std::runtime_error("Failed to cast to ColumnArray");
    }

    // Create a ColumnMap from the array
    auto temp_map = std::make_shared<ColumnMap>(array_tuple_col_res->ptr);

    // Now append the ColumnMap
    map_col_res->ptr->Append(temp_map);

    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(column_map_append_from_array, 0);

// ============================================================================
// LowCardinality Type Support
// ============================================================================

// Append values to LowCardinality column via a temporary column
// LowCardinality automatically builds dictionary and handles deduplication
fine::Atom column_lowcardinality_append_from_column(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> lc_col_res,
    fine::ResourcePtr<ColumnResource> source_col_res) {
  try {
    if (!lc_col_res->ptr) {
      throw std::runtime_error("LowCardinality column pointer is null");
    }
    if (!source_col_res->ptr) {
      throw std::runtime_error("Source column pointer is null");
    }

    // Create a temporary LowCardinality column from the source data
    auto temp_lc = std::make_shared<ColumnLowCardinality>(source_col_res->ptr);

    // Append to the main LowCardinality column
    // This merges the dictionaries and updates indices
    lc_col_res->ptr->Append(temp_lc);

    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(column_lowcardinality_append_from_column, 0);
