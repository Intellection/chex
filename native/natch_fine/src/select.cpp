#include <fine.hpp>
#include <clickhouse/client.h>
#include <clickhouse/query.h>
#include <clickhouse/block.h>
#include <clickhouse/columns/column.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/uuid.h>
#include <clickhouse/columns/array.h>
#include <clickhouse/columns/tuple.h>
#include <clickhouse/columns/map.h>
#include <clickhouse/columns/lowcardinality.h>
#include <clickhouse/columns/enum.h>
#include <clickhouse/types/types.h>
#include <string>
#include <vector>
#include <memory>
#include <sstream>
#include <iomanip>

using namespace clickhouse;

// Forward declaration
ERL_NIF_TERM column_to_elixir_list(ErlNifEnv *env, ColumnRef col);

// Helper to format UUID to string (much faster than ostringstream)
inline void format_uuid_to_buffer(const UUID& uuid, char* buffer) {
  uint64_t high = uuid.first;
  uint64_t low = uuid.second;
  snprintf(buffer, 37,  // 36 chars + null terminator
           "%08llx-%04llx-%04llx-%04llx-%012llx",
           (unsigned long long)((high >> 32) & 0xFFFFFFFF),
           (unsigned long long)((high >> 16) & 0xFFFF),
           (unsigned long long)(high & 0xFFFF),
           (unsigned long long)((low >> 48) & 0xFFFF),
           (unsigned long long)(low & 0xFFFFFFFFFFFF));
}

// Helper to recursively convert a column to an Elixir list
// This handles all column types including nested arrays
ERL_NIF_TERM column_to_elixir_list(ErlNifEnv *env, ColumnRef col) {
  size_t count = col->Size();
  std::vector<ERL_NIF_TERM> values;
  values.reserve(count);

  // Optimized: Use Type::Code for O(1) type dispatch instead of cascade of As<T>() calls
  Type::Code type_code = col->GetType().GetCode();

  switch (type_code) {
  case Type::UInt64: {
    auto uint64_col = col->As<ColumnUInt64>();
    for (size_t i = 0; i < count; i++) {
      values.push_back(enif_make_uint64(env, uint64_col->At(i)));
    }
    break;
  }
  case Type::UInt32: {
    auto uint32_col = col->As<ColumnUInt32>();
    for (size_t i = 0; i < count; i++) {
      values.push_back(enif_make_uint64(env, uint32_col->At(i)));
    }
    break;
  }
  case Type::UInt16: {
    auto uint16_col = col->As<ColumnUInt16>();
    for (size_t i = 0; i < count; i++) {
      values.push_back(enif_make_uint64(env, uint16_col->At(i)));
    }
    break;
  }
  case Type::UInt8: {
    auto uint8_col = col->As<ColumnUInt8>();
    for (size_t i = 0; i < count; i++) {
      values.push_back(enif_make_uint64(env, uint8_col->At(i)));
    }
    break;
  }
  case Type::Int64: {
    auto int64_col = col->As<ColumnInt64>();
    for (size_t i = 0; i < count; i++) {
      values.push_back(enif_make_int64(env, int64_col->At(i)));
    }
    break;
  }
  case Type::Int32: {
    auto int32_col = col->As<ColumnInt32>();
    for (size_t i = 0; i < count; i++) {
      values.push_back(enif_make_int64(env, int32_col->At(i)));
    }
    break;
  }
  case Type::Int16: {
    auto int16_col = col->As<ColumnInt16>();
    for (size_t i = 0; i < count; i++) {
      values.push_back(enif_make_int64(env, int16_col->At(i)));
    }
    break;
  }
  case Type::Int8: {
    auto int8_col = col->As<ColumnInt8>();
    for (size_t i = 0; i < count; i++) {
      values.push_back(enif_make_int64(env, int8_col->At(i)));
    }
    break;
  }
  case Type::Float64: {
    auto float64_col = col->As<ColumnFloat64>();
    for (size_t i = 0; i < count; i++) {
      values.push_back(enif_make_double(env, float64_col->At(i)));
    }
    break;
  }
  case Type::Float32: {
    auto float32_col = col->As<ColumnFloat32>();
    for (size_t i = 0; i < count; i++) {
      values.push_back(enif_make_double(env, float32_col->At(i)));
    }
    break;
  }
  case Type::String: {
    auto string_col = col->As<ColumnString>();
    for (size_t i = 0; i < count; i++) {
      std::string_view val_view = string_col->At(i);
      ErlNifBinary bin;
      enif_alloc_binary(val_view.size(), &bin);
      std::memcpy(bin.data, val_view.data(), val_view.size());
      values.push_back(enif_make_binary(env, &bin));
    }
    break;
  }
  case Type::DateTime: {
    auto datetime_col = col->As<ColumnDateTime>();
    for (size_t i = 0; i < count; i++) {
      values.push_back(enif_make_uint64(env, datetime_col->At(i)));
    }
    break;
  }
  case Type::DateTime64: {
    auto datetime64_col = col->As<ColumnDateTime64>();
    for (size_t i = 0; i < count; i++) {
      values.push_back(enif_make_int64(env, datetime64_col->At(i)));
    }
    break;
  }
  case Type::Date: {
    auto date_col = col->As<ColumnDate>();
    for (size_t i = 0; i < count; i++) {
      values.push_back(enif_make_uint64(env, date_col->RawAt(i)));
    }
    break;
  }
  case Type::UUID: {
    auto uuid_col = col->As<ColumnUUID>();
    for (size_t i = 0; i < count; i++) {
      UUID uuid = uuid_col->At(i);
      char uuid_buf[37];
      format_uuid_to_buffer(uuid, uuid_buf);
      ErlNifBinary bin;
      enif_alloc_binary(36, &bin);
      std::memcpy(bin.data, uuid_buf, 36);
      values.push_back(enif_make_binary(env, &bin));
    }
    break;
  }
  case Type::Decimal:
  case Type::Decimal32:
  case Type::Decimal64:
  case Type::Decimal128: {
    auto decimal_col = col->As<ColumnDecimal>();
    for (size_t i = 0; i < count; i++) {
      Int128 value = decimal_col->At(i);
      int64_t scaled_value = static_cast<int64_t>(value);
      values.push_back(enif_make_int64(env, scaled_value));
    }
    break;
  }
  case Type::Array: {
    auto array_col = col->As<ColumnArray>();
    // Recursively handle nested arrays
    for (size_t i = 0; i < count; i++) {
      auto nested = array_col->GetAsColumn(i);
      values.push_back(column_to_elixir_list(env, nested));
    }
    break;
  }
  case Type::Tuple: {
    auto tuple_col = col->As<ColumnTuple>();
    // Handle tuple columns - return Elixir tuples
    size_t tuple_size = tuple_col->TupleSize();

    // Optimized: Pre-convert each element column ONCE, then index directly
    std::vector<std::vector<ERL_NIF_TERM>> element_columns;
    element_columns.reserve(tuple_size);

    for (size_t j = 0; j < tuple_size; j++) {
      auto element_col = tuple_col->At(j);
      // Convert entire element column to Elixir list, then extract to vector
      ERL_NIF_TERM elem_list = column_to_elixir_list(env, element_col);
      std::vector<ERL_NIF_TERM> elem_vec;
      elem_vec.reserve(count);
      ERL_NIF_TERM tail = elem_list;
      for (size_t i = 0; i < count; i++) {
        ERL_NIF_TERM head;
        if (enif_get_list_cell(env, tail, &head, &tail)) {
          elem_vec.push_back(head);
        } else {
          elem_vec.push_back(enif_make_atom(env, "error"));
        }
      }
      element_columns.push_back(std::move(elem_vec));
    }

    // Now build tuples by indexing pre-converted columns
    for (size_t i = 0; i < count; i++) {
      std::vector<ERL_NIF_TERM> tuple_elements;
      tuple_elements.reserve(tuple_size);
      for (size_t j = 0; j < tuple_size; j++) {
        tuple_elements.push_back(element_columns[j][i]);
      }
      values.push_back(enif_make_tuple_from_array(env, tuple_elements.data(), tuple_elements.size()));
    }
    break;
  }
  case Type::Map: {
    auto map_col = col->As<ColumnMap>();
    // Handle map columns - return Elixir maps
    // Map is stored as Array(Tuple(K, V)) where Tuple is columnar
    for (size_t i = 0; i < count; i++) {
      // Get the i-th map's tuples as a ColumnTuple
      auto kv_tuples = map_col->GetAsColumn(i);

      // This is a ColumnTuple with 2 columns: keys and values
      if (auto tuple_col = kv_tuples->As<ColumnTuple>()) {
        // Get the keys and values columns
        auto keys_col = tuple_col->At(0);
        auto values_col = tuple_col->At(1);

        // The number of key-value pairs is the size of the keys/values columns
        size_t map_size = keys_col->Size();

        // Optimized: Convert columns to vectors directly, then build map in O(M)
        std::vector<ERL_NIF_TERM> key_terms;
        std::vector<ERL_NIF_TERM> value_terms;
        key_terms.reserve(map_size);
        value_terms.reserve(map_size);

        // Convert keys column to vector
        ERL_NIF_TERM keys_list = column_to_elixir_list(env, keys_col);
        ERL_NIF_TERM key_tail = keys_list;
        for (size_t j = 0; j < map_size; j++) {
          ERL_NIF_TERM key;
          if (enif_get_list_cell(env, key_tail, &key, &key_tail)) {
            key_terms.push_back(key);
          }
        }

        // Convert values column to vector
        ERL_NIF_TERM values_list = column_to_elixir_list(env, values_col);
        ERL_NIF_TERM value_tail = values_list;
        for (size_t j = 0; j < map_size; j++) {
          ERL_NIF_TERM value;
          if (enif_get_list_cell(env, value_tail, &value, &value_tail)) {
            value_terms.push_back(value);
          }
        }

        // Build map in O(M) with enif_make_map_from_arrays
        ERL_NIF_TERM elixir_map;
        enif_make_map_from_arrays(env, key_terms.data(), value_terms.data(), map_size, &elixir_map);

        values.push_back(elixir_map);
      } else {
        // Fallback for unexpected structure
        values.push_back(enif_make_new_map(env));
      }
    }
    break;
  }
  case Type::Enum8: {
    auto enum8_col = col->As<ColumnEnum8>();
    // Handle Enum8 columns - return string names
    for (size_t i = 0; i < count; i++) {
      std::string_view name = enum8_col->NameAt(i);
      ErlNifBinary bin;
      enif_alloc_binary(name.size(), &bin);
      std::memcpy(bin.data, name.data(), name.size());
      values.push_back(enif_make_binary(env, &bin));
    }
    break;
  }
  case Type::Enum16: {
    auto enum16_col = col->As<ColumnEnum16>();
    // Handle Enum16 columns - return string names
    for (size_t i = 0; i < count; i++) {
      std::string_view name = enum16_col->NameAt(i);
      ErlNifBinary bin;
      enif_alloc_binary(name.size(), &bin);
      std::memcpy(bin.data, name.data(), name.size());
      values.push_back(enif_make_binary(env, &bin));
    }
    break;
  }
  case Type::LowCardinality: {
    auto lc_col = col->As<ColumnLowCardinality>();
    // Handle LowCardinality columns - decode values from dictionary
    // For each row, get the decoded value by calling GetItem
    // GetItem internally looks up the dictionary index and returns the value
    for (size_t i = 0; i < count; i++) {
      auto item = lc_col->GetItem(i);

      // Convert ItemView to Elixir term based on type
      if (item.type == Type::String) {
        auto val = item.get<std::string_view>();
        ErlNifBinary bin;
        enif_alloc_binary(val.size(), &bin);
        std::memcpy(bin.data, val.data(), val.size());
        values.push_back(enif_make_binary(env, &bin));
      } else if (item.type == Type::Void) {
        // Null value
        values.push_back(enif_make_atom(env, "nil"));
      } else {
        // For other types, would need more handling
        // For now, throw an error
        throw std::runtime_error("Unsupported LowCardinality inner type");
      }
    }
    break;
  }
  case Type::Nullable: {
    auto nullable_col = col->As<ColumnNullable>();
    auto nested = nullable_col->Nested();

    // Optimized: Check nested type ONCE outside loop, then direct extraction
    if (auto uint64_col = nested->As<ColumnUInt64>()) {
      for (size_t i = 0; i < count; i++) {
        if (nullable_col->IsNull(i)) {
          values.push_back(enif_make_atom(env, "nil"));
        } else {
          values.push_back(enif_make_uint64(env, uint64_col->At(i)));
        }
      }
    } else if (auto int64_col = nested->As<ColumnInt64>()) {
      for (size_t i = 0; i < count; i++) {
        if (nullable_col->IsNull(i)) {
          values.push_back(enif_make_atom(env, "nil"));
        } else {
          values.push_back(enif_make_int64(env, int64_col->At(i)));
        }
      }
    } else if (auto float64_col = nested->As<ColumnFloat64>()) {
      for (size_t i = 0; i < count; i++) {
        if (nullable_col->IsNull(i)) {
          values.push_back(enif_make_atom(env, "nil"));
        } else {
          values.push_back(enif_make_double(env, float64_col->At(i)));
        }
      }
    } else if (auto string_col = nested->As<ColumnString>()) {
      for (size_t i = 0; i < count; i++) {
        if (nullable_col->IsNull(i)) {
          values.push_back(enif_make_atom(env, "nil"));
        } else {
          std::string_view val_view = string_col->At(i);
          ErlNifBinary bin;
          enif_alloc_binary(val_view.size(), &bin);
          std::memcpy(bin.data, val_view.data(), val_view.size());
          values.push_back(enif_make_binary(env, &bin));
        }
      }
    } else {
      // Fallback for complex/uncommon types: use Slice approach
      for (size_t i = 0; i < count; i++) {
        if (nullable_col->IsNull(i)) {
          values.push_back(enif_make_atom(env, "nil"));
        } else {
          auto single_value_col = nested->Slice(i, 1);
          ERL_NIF_TERM elem_list = column_to_elixir_list(env, single_value_col);
          ERL_NIF_TERM head, tail;
          if (enif_get_list_cell(env, elem_list, &head, &tail)) {
            values.push_back(head);
          } else {
            values.push_back(enif_make_atom(env, "error"));
          }
        }
      }
    }
    break;
  }
  default:
    // Unsupported or unknown type
    throw std::runtime_error("Unsupported column type in column_to_elixir_list");
  }

  return enif_make_list_from_array(env, values.data(), values.size());
}

// Helper to convert Block to maps and append to output vector
void block_to_maps_impl(ErlNifEnv *env, std::shared_ptr<Block> block, std::vector<ERL_NIF_TERM>& out_maps) {
  size_t col_count = block->GetColumnCount();
  size_t row_count = block->GetRowCount();

  if (row_count == 0) {
    return;  // Nothing to add
  }

  // Extract column names and data
  std::vector<std::string> col_names;
  std::vector<std::vector<ERL_NIF_TERM>> col_data;

  for (size_t c = 0; c < col_count; c++) {
    col_names.push_back(block->GetColumnName(c));

    ColumnRef col = (*block)[c];
    std::vector<ERL_NIF_TERM> column_values;
    column_values.reserve(row_count);

    // Extract column data based on type
    if (auto uint64_col = col->As<ColumnUInt64>()) {
      for (size_t i = 0; i < row_count; i++) {
        column_values.push_back(enif_make_uint64(env, uint64_col->At(i)));
      }
    } else if (auto uint32_col = col->As<ColumnUInt32>()) {
      for (size_t i = 0; i < row_count; i++) {
        column_values.push_back(enif_make_uint64(env, uint32_col->At(i)));
      }
    } else if (auto uint16_col = col->As<ColumnUInt16>()) {
      for (size_t i = 0; i < row_count; i++) {
        column_values.push_back(enif_make_uint64(env, uint16_col->At(i)));
      }
    } else if (auto uint8_col = col->As<ColumnUInt8>()) {
      for (size_t i = 0; i < row_count; i++) {
        column_values.push_back(enif_make_uint64(env, uint8_col->At(i)));
      }
    } else if (auto int64_col = col->As<ColumnInt64>()) {
      for (size_t i = 0; i < row_count; i++) {
        column_values.push_back(enif_make_int64(env, int64_col->At(i)));
      }
    } else if (auto int32_col = col->As<ColumnInt32>()) {
      for (size_t i = 0; i < row_count; i++) {
        column_values.push_back(enif_make_int64(env, int32_col->At(i)));
      }
    } else if (auto int16_col = col->As<ColumnInt16>()) {
      for (size_t i = 0; i < row_count; i++) {
        column_values.push_back(enif_make_int64(env, int16_col->At(i)));
      }
    } else if (auto int8_col = col->As<ColumnInt8>()) {
      for (size_t i = 0; i < row_count; i++) {
        column_values.push_back(enif_make_int64(env, int8_col->At(i)));
      }
    } else if (auto float64_col = col->As<ColumnFloat64>()) {
      for (size_t i = 0; i < row_count; i++) {
        column_values.push_back(enif_make_double(env, float64_col->At(i)));
      }
    } else if (auto float32_col = col->As<ColumnFloat32>()) {
      for (size_t i = 0; i < row_count; i++) {
        column_values.push_back(enif_make_double(env, float32_col->At(i)));
      }
    } else if (auto string_col = col->As<ColumnString>()) {
      for (size_t i = 0; i < row_count; i++) {
        std::string_view val_view = string_col->At(i);
        ErlNifBinary bin;
        enif_alloc_binary(val_view.size(), &bin);
        std::memcpy(bin.data, val_view.data(), val_view.size());
        column_values.push_back(enif_make_binary(env, &bin));
      }
    } else if (auto datetime_col = col->As<ColumnDateTime>()) {
      for (size_t i = 0; i < row_count; i++) {
        column_values.push_back(enif_make_uint64(env, datetime_col->At(i)));
      }
    } else if (auto datetime64_col = col->As<ColumnDateTime64>()) {
      for (size_t i = 0; i < row_count; i++) {
        column_values.push_back(enif_make_int64(env, datetime64_col->At(i)));
      }
    } else if (auto date_col = col->As<ColumnDate>()) {
      for (size_t i = 0; i < row_count; i++) {
        column_values.push_back(enif_make_uint64(env, date_col->RawAt(i)));
      }
    } else if (auto uuid_col = col->As<ColumnUUID>()) {
      for (size_t i = 0; i < row_count; i++) {
        UUID uuid = uuid_col->At(i);
        char uuid_buf[37];
        format_uuid_to_buffer(uuid, uuid_buf);
        ErlNifBinary bin;
        enif_alloc_binary(36, &bin);
        std::memcpy(bin.data, uuid_buf, 36);
        column_values.push_back(enif_make_binary(env, &bin));
      }
    } else if (auto decimal_col = col->As<ColumnDecimal>()) {
      for (size_t i = 0; i < row_count; i++) {
        Int128 value = decimal_col->At(i);
        // Convert Int128 to int64 for Elixir (assumes value fits in int64)
        // Elixir will convert back to Decimal by dividing by 10^scale
        int64_t scaled_value = static_cast<int64_t>(value);
        column_values.push_back(enif_make_int64(env, scaled_value));
      }
    } else if (auto array_col = col->As<ColumnArray>()) {
      // Handle array columns - recursively converts nested arrays to Elixir lists
      for (size_t i = 0; i < row_count; i++) {
        auto nested = array_col->GetAsColumn(i);
        column_values.push_back(column_to_elixir_list(env, nested));
      }
    } else if (auto map_col = col->As<ColumnMap>()) {
      // Handle map columns - use column_to_elixir_list for complex nested structure
      for (size_t i = 0; i < row_count; i++) {
        auto kv_tuples = map_col->GetAsColumn(i);

        if (auto tuple_col = kv_tuples->As<ColumnTuple>()) {
          auto keys_col = tuple_col->At(0);
          auto values_col = tuple_col->At(1);
          size_t map_size = keys_col->Size();

          // Optimized: Convert columns to vectors directly, then build map in O(M)
          std::vector<ERL_NIF_TERM> key_terms;
          std::vector<ERL_NIF_TERM> value_terms;
          key_terms.reserve(map_size);
          value_terms.reserve(map_size);

          // Convert keys column to vector
          ERL_NIF_TERM keys_list = column_to_elixir_list(env, keys_col);
          ERL_NIF_TERM key_tail = keys_list;
          for (size_t j = 0; j < map_size; j++) {
            ERL_NIF_TERM key;
            if (enif_get_list_cell(env, key_tail, &key, &key_tail)) {
              key_terms.push_back(key);
            }
          }

          // Convert values column to vector
          ERL_NIF_TERM values_list = column_to_elixir_list(env, values_col);
          ERL_NIF_TERM value_tail = values_list;
          for (size_t j = 0; j < map_size; j++) {
            ERL_NIF_TERM value;
            if (enif_get_list_cell(env, value_tail, &value, &value_tail)) {
              value_terms.push_back(value);
            }
          }

          // Build map in O(M) with enif_make_map_from_arrays
          ERL_NIF_TERM elixir_map;
          enif_make_map_from_arrays(env, key_terms.data(), value_terms.data(), map_size, &elixir_map);

          column_values.push_back(elixir_map);
        } else {
          column_values.push_back(enif_make_new_map(env));
        }
      }
    } else if (auto tuple_col = col->As<ColumnTuple>()) {
      // Handle tuple columns - use column_to_elixir_list for complex logic
      size_t tuple_size = tuple_col->TupleSize();

      // Optimized: Pre-convert each element column ONCE, then index directly
      std::vector<std::vector<ERL_NIF_TERM>> element_columns;
      element_columns.reserve(tuple_size);

      for (size_t j = 0; j < tuple_size; j++) {
        auto element_col = tuple_col->At(j);
        // Convert entire element column to Elixir list, then extract to vector
        ERL_NIF_TERM elem_list = column_to_elixir_list(env, element_col);
        std::vector<ERL_NIF_TERM> elem_vec;
        elem_vec.reserve(row_count);
        ERL_NIF_TERM tail = elem_list;
        for (size_t i = 0; i < row_count; i++) {
          ERL_NIF_TERM head;
          if (enif_get_list_cell(env, tail, &head, &tail)) {
            elem_vec.push_back(head);
          } else {
            elem_vec.push_back(enif_make_atom(env, "error"));
          }
        }
        element_columns.push_back(std::move(elem_vec));
      }

      // Now build tuples by indexing pre-converted columns
      for (size_t i = 0; i < row_count; i++) {
        std::vector<ERL_NIF_TERM> tuple_elements;
        tuple_elements.reserve(tuple_size);
        for (size_t j = 0; j < tuple_size; j++) {
          tuple_elements.push_back(element_columns[j][i]);
        }
        column_values.push_back(enif_make_tuple_from_array(env, tuple_elements.data(), tuple_elements.size()));
      }
    } else if (auto enum8_col = col->As<ColumnEnum8>()) {
      // Handle Enum8 columns
      for (size_t i = 0; i < row_count; i++) {
        std::string_view name = enum8_col->NameAt(i);
        ErlNifBinary bin;
        enif_alloc_binary(name.size(), &bin);
        std::memcpy(bin.data, name.data(), name.size());
        column_values.push_back(enif_make_binary(env, &bin));
      }
    } else if (auto enum16_col = col->As<ColumnEnum16>()) {
      // Handle Enum16 columns
      for (size_t i = 0; i < row_count; i++) {
        std::string_view name = enum16_col->NameAt(i);
        ErlNifBinary bin;
        enif_alloc_binary(name.size(), &bin);
        std::memcpy(bin.data, name.data(), name.size());
        column_values.push_back(enif_make_binary(env, &bin));
      }
    } else if (auto lc_col = col->As<ColumnLowCardinality>()) {
      // Handle LowCardinality columns
      for (size_t i = 0; i < row_count; i++) {
        auto item = lc_col->GetItem(i);
        if (item.type == Type::String) {
          auto val = item.get<std::string_view>();
          ErlNifBinary bin;
          enif_alloc_binary(val.size(), &bin);
          std::memcpy(bin.data, val.data(), val.size());
          column_values.push_back(enif_make_binary(env, &bin));
        } else if (item.type == Type::Void) {
          column_values.push_back(enif_make_atom(env, "nil"));
        } else {
          throw std::runtime_error("Unsupported LowCardinality inner type");
        }
      }
    } else if (auto nullable_col = col->As<ColumnNullable>()) {
      auto nested = nullable_col->Nested();

      // Optimized: Check nested type ONCE outside loop, then direct extraction
      if (auto uint64_col = nested->As<ColumnUInt64>()) {
        for (size_t i = 0; i < row_count; i++) {
          if (nullable_col->IsNull(i)) {
            column_values.push_back(enif_make_atom(env, "nil"));
          } else {
            column_values.push_back(enif_make_uint64(env, uint64_col->At(i)));
          }
        }
      } else if (auto int64_col = nested->As<ColumnInt64>()) {
        for (size_t i = 0; i < row_count; i++) {
          if (nullable_col->IsNull(i)) {
            column_values.push_back(enif_make_atom(env, "nil"));
          } else {
            column_values.push_back(enif_make_int64(env, int64_col->At(i)));
          }
        }
      } else if (auto float64_col = nested->As<ColumnFloat64>()) {
        for (size_t i = 0; i < row_count; i++) {
          if (nullable_col->IsNull(i)) {
            column_values.push_back(enif_make_atom(env, "nil"));
          } else {
            column_values.push_back(enif_make_double(env, float64_col->At(i)));
          }
        }
      } else if (auto string_col = nested->As<ColumnString>()) {
        for (size_t i = 0; i < row_count; i++) {
          if (nullable_col->IsNull(i)) {
            column_values.push_back(enif_make_atom(env, "nil"));
          } else {
            std::string_view val_view = string_col->At(i);
            ErlNifBinary bin;
            enif_alloc_binary(val_view.size(), &bin);
            std::memcpy(bin.data, val_view.data(), val_view.size());
            column_values.push_back(enif_make_binary(env, &bin));
          }
        }
      } else {
        // Fallback for complex/uncommon types: use Slice approach
        for (size_t i = 0; i < row_count; i++) {
          if (nullable_col->IsNull(i)) {
            column_values.push_back(enif_make_atom(env, "nil"));
          } else {
            auto single_value_col = nested->Slice(i, 1);
            ERL_NIF_TERM elem_list = column_to_elixir_list(env, single_value_col);
            ERL_NIF_TERM head, tail;
            if (enif_get_list_cell(env, elem_list, &head, &tail)) {
              column_values.push_back(head);
            } else {
              column_values.push_back(enif_make_atom(env, "error"));
            }
          }
        }
      }
    }

    col_data.push_back(column_values);
  }

  // Pre-create column name atoms once (major optimization)
  std::vector<ERL_NIF_TERM> key_atoms;
  key_atoms.reserve(col_count);
  for (size_t c = 0; c < col_count; c++) {
    key_atoms.push_back(enif_make_atom(env, col_names[c].c_str()));
  }

  // Build maps in local vector first for better cache locality
  std::vector<ERL_NIF_TERM> rows;
  rows.reserve(row_count);

  // Build maps row by row, reusing the pre-created key atoms
  for (size_t r = 0; r < row_count; r++) {
    ERL_NIF_TERM values[col_count];

    for (size_t c = 0; c < col_count; c++) {
      values[c] = col_data[c][r];
    }

    ERL_NIF_TERM map;
    enif_make_map_from_arrays(env, key_atoms.data(), values, col_count, &map);
    rows.push_back(map);
  }

  // Append all rows at once to output vector
  out_maps.insert(out_maps.end(), rows.begin(), rows.end());
}

// Wrapper struct to return list of maps from FINE NIF
struct SelectResult {
  ERL_NIF_TERM maps;

  SelectResult(ERL_NIF_TERM m) : maps(m) {}
};

// FINE encoder/decoder for SelectResult
namespace fine {
  template <>
  struct Encoder<SelectResult> {
    static ERL_NIF_TERM encode(ErlNifEnv *env, const SelectResult &result) {
      return result.maps;
    }
  };

  template <>
  struct Decoder<SelectResult> {
    static bool decode(ErlNifEnv *env, ERL_NIF_TERM term, SelectResult &result) {
      // This should never be called since SelectResult is only used for return values
      return false;
    }
  };
}

// Execute SELECT query and return list of maps
SelectResult client_select(
    ErlNifEnv *env,
    fine::ResourcePtr<Client> client,
    std::string query) {

  // Collect all result maps immediately in the callback
  std::vector<ERL_NIF_TERM> all_maps;

  client->Select(query, [&](const Block &block) {
    // Convert this block to maps and append directly to all_maps
    auto block_ptr = std::make_shared<Block>(block);
    block_to_maps_impl(env, block_ptr, all_maps);
  });

  // Build final list from all maps
  if (all_maps.empty()) {
    return SelectResult(enif_make_list(env, 0));
  }

  return SelectResult(enif_make_list_from_array(env, all_maps.data(), all_maps.size()));
}

FINE_NIF(client_select, 0);

// Execute parameterized SELECT query and return list of maps
SelectResult client_select_parameterized(
    ErlNifEnv *env,
    fine::ResourcePtr<Client> client,
    fine::ResourcePtr<Query> query) {

  // Collect all result maps immediately in the callback
  std::vector<ERL_NIF_TERM> all_maps;

  // Set callback on the Query object before calling Select
  query->OnData([&](const Block &block) {
    // Convert this block to maps and append directly to all_maps
    auto block_ptr = std::make_shared<Block>(block);
    block_to_maps_impl(env, block_ptr, all_maps);
  });

  client->Select(*query);

  // Build final list from all maps
  if (all_maps.empty()) {
    return SelectResult(enif_make_list(env, 0));
  }

  return SelectResult(enif_make_list_from_array(env, all_maps.data(), all_maps.size()));
}

FINE_NIF(client_select_parameterized, 0);

// Wrapper struct to return columnar map from FINE NIF
struct ColumnarResult {
  ERL_NIF_TERM columns_map;

  ColumnarResult(ERL_NIF_TERM m) : columns_map(m) {}
};

// FINE encoder/decoder for ColumnarResult
namespace fine {
  template <>
  struct Encoder<ColumnarResult> {
    static ERL_NIF_TERM encode(ErlNifEnv *env, const ColumnarResult &result) {
      return result.columns_map;
    }
  };

  template <>
  struct Decoder<ColumnarResult> {
    static bool decode(ErlNifEnv *env, ERL_NIF_TERM term, ColumnarResult &result) {
      return false;  // Only used for return values
    }
  };
}

// Execute SELECT query and return columnar format: %{column_name => [values]}
ColumnarResult client_select_cols(
    ErlNifEnv *env,
    fine::ResourcePtr<Client> client,
    std::string query) {

  // Pre-create column structure on first block (indexed vectors for O(1) access)
  std::vector<std::string> col_names;
  std::vector<ERL_NIF_TERM> key_atoms;
  std::vector<std::vector<ERL_NIF_TERM>> all_columns;
  bool first_block = true;

  client->Select(query, [&](const Block &block) {
    size_t col_count = block.GetColumnCount();
    size_t row_count = block.GetRowCount();

    if (row_count == 0) {
      return;
    }

    // Initialize column structure on first block
    if (first_block) {
      col_names.reserve(col_count);
      key_atoms.reserve(col_count);
      all_columns.reserve(col_count);

      for (size_t c = 0; c < col_count; c++) {
        std::string col_name = block.GetColumnName(c);
        col_names.push_back(col_name);
        key_atoms.push_back(enif_make_atom(env, col_name.c_str()));

        // Estimate capacity: assume 10 blocks total (heuristic)
        std::vector<ERL_NIF_TERM> col_vec;
        col_vec.reserve(row_count * 10);
        all_columns.push_back(std::move(col_vec));
      }

      first_block = false;
    }

    // Extract each column's data using index-based access
    for (size_t c = 0; c < col_count; c++) {
      ColumnRef col = block[c];
      std::vector<ERL_NIF_TERM> column_values;
      column_values.reserve(row_count);

      // Extract column data based on type (reuse logic from block_to_maps_impl)
      if (auto uint64_col = col->As<ColumnUInt64>()) {
        for (size_t i = 0; i < row_count; i++) {
          column_values.push_back(enif_make_uint64(env, uint64_col->At(i)));
        }
      } else if (auto uint32_col = col->As<ColumnUInt32>()) {
        for (size_t i = 0; i < row_count; i++) {
          column_values.push_back(enif_make_uint64(env, uint32_col->At(i)));
        }
      } else if (auto uint16_col = col->As<ColumnUInt16>()) {
        for (size_t i = 0; i < row_count; i++) {
          column_values.push_back(enif_make_uint64(env, uint16_col->At(i)));
        }
      } else if (auto uint8_col = col->As<ColumnUInt8>()) {
        for (size_t i = 0; i < row_count; i++) {
          column_values.push_back(enif_make_uint64(env, uint8_col->At(i)));
        }
      } else if (auto int64_col = col->As<ColumnInt64>()) {
        for (size_t i = 0; i < row_count; i++) {
          column_values.push_back(enif_make_int64(env, int64_col->At(i)));
        }
      } else if (auto int32_col = col->As<ColumnInt32>()) {
        for (size_t i = 0; i < row_count; i++) {
          column_values.push_back(enif_make_int64(env, int32_col->At(i)));
        }
      } else if (auto int16_col = col->As<ColumnInt16>()) {
        for (size_t i = 0; i < row_count; i++) {
          column_values.push_back(enif_make_int64(env, int16_col->At(i)));
        }
      } else if (auto int8_col = col->As<ColumnInt8>()) {
        for (size_t i = 0; i < row_count; i++) {
          column_values.push_back(enif_make_int64(env, int8_col->At(i)));
        }
      } else if (auto float64_col = col->As<ColumnFloat64>()) {
        for (size_t i = 0; i < row_count; i++) {
          column_values.push_back(enif_make_double(env, float64_col->At(i)));
        }
      } else if (auto float32_col = col->As<ColumnFloat32>()) {
        for (size_t i = 0; i < row_count; i++) {
          column_values.push_back(enif_make_double(env, float32_col->At(i)));
        }
      } else if (auto string_col = col->As<ColumnString>()) {
        for (size_t i = 0; i < row_count; i++) {
          std::string_view val_view = string_col->At(i);
          ErlNifBinary bin;
          enif_alloc_binary(val_view.size(), &bin);
          std::memcpy(bin.data, val_view.data(), val_view.size());
          column_values.push_back(enif_make_binary(env, &bin));
        }
      } else if (auto datetime_col = col->As<ColumnDateTime>()) {
        for (size_t i = 0; i < row_count; i++) {
          column_values.push_back(enif_make_uint64(env, datetime_col->At(i)));
        }
      } else if (auto datetime64_col = col->As<ColumnDateTime64>()) {
        for (size_t i = 0; i < row_count; i++) {
          column_values.push_back(enif_make_int64(env, datetime64_col->At(i)));
        }
      } else if (auto date_col = col->As<ColumnDate>()) {
        for (size_t i = 0; i < row_count; i++) {
          column_values.push_back(enif_make_uint64(env, date_col->RawAt(i)));
        }
      } else if (auto uuid_col = col->As<ColumnUUID>()) {
        for (size_t i = 0; i < row_count; i++) {
          UUID uuid = uuid_col->At(i);
          char uuid_buf[37];
          format_uuid_to_buffer(uuid, uuid_buf);
          ErlNifBinary bin;
          enif_alloc_binary(36, &bin);
          std::memcpy(bin.data, uuid_buf, 36);
          column_values.push_back(enif_make_binary(env, &bin));
        }
      } else if (auto decimal_col = col->As<ColumnDecimal>()) {
        for (size_t i = 0; i < row_count; i++) {
          Int128 value = decimal_col->At(i);
          int64_t scaled_value = static_cast<int64_t>(value);
          column_values.push_back(enif_make_int64(env, scaled_value));
        }
      } else if (auto array_col = col->As<ColumnArray>()) {
        for (size_t i = 0; i < row_count; i++) {
          auto nested = array_col->GetAsColumn(i);
          column_values.push_back(column_to_elixir_list(env, nested));
        }
      } else if (auto map_col = col->As<ColumnMap>()) {
        for (size_t i = 0; i < row_count; i++) {
          auto kv_tuples = map_col->GetAsColumn(i);
          if (auto tuple_col = kv_tuples->As<ColumnTuple>()) {
            auto keys_col = tuple_col->At(0);
            auto values_col = tuple_col->At(1);
            size_t map_size = keys_col->Size();

            // Optimized: Convert columns to vectors directly, then build map in O(M)
            std::vector<ERL_NIF_TERM> key_terms;
            std::vector<ERL_NIF_TERM> value_terms;
            key_terms.reserve(map_size);
            value_terms.reserve(map_size);

            // Convert keys column to vector
            ERL_NIF_TERM keys_list = column_to_elixir_list(env, keys_col);
            ERL_NIF_TERM key_tail = keys_list;
            for (size_t j = 0; j < map_size; j++) {
              ERL_NIF_TERM key;
              if (enif_get_list_cell(env, key_tail, &key, &key_tail)) {
                key_terms.push_back(key);
              }
            }

            // Convert values column to vector
            ERL_NIF_TERM values_list = column_to_elixir_list(env, values_col);
            ERL_NIF_TERM value_tail = values_list;
            for (size_t j = 0; j < map_size; j++) {
              ERL_NIF_TERM value;
              if (enif_get_list_cell(env, value_tail, &value, &value_tail)) {
                value_terms.push_back(value);
              }
            }

            // Build map in O(M) with enif_make_map_from_arrays
            ERL_NIF_TERM elixir_map;
            enif_make_map_from_arrays(env, key_terms.data(), value_terms.data(), map_size, &elixir_map);

            column_values.push_back(elixir_map);
          } else {
            column_values.push_back(enif_make_new_map(env));
          }
        }
      } else if (auto tuple_col = col->As<ColumnTuple>()) {
        size_t tuple_size = tuple_col->TupleSize();

        // Optimized: Pre-convert each element column ONCE, then index directly
        std::vector<std::vector<ERL_NIF_TERM>> element_columns;
        element_columns.reserve(tuple_size);

        for (size_t j = 0; j < tuple_size; j++) {
          auto element_col = tuple_col->At(j);
          // Convert entire element column to Elixir list, then extract to vector
          ERL_NIF_TERM elem_list = column_to_elixir_list(env, element_col);
          std::vector<ERL_NIF_TERM> elem_vec;
          elem_vec.reserve(row_count);
          ERL_NIF_TERM tail = elem_list;
          for (size_t i = 0; i < row_count; i++) {
            ERL_NIF_TERM head;
            if (enif_get_list_cell(env, tail, &head, &tail)) {
              elem_vec.push_back(head);
            } else {
              elem_vec.push_back(enif_make_atom(env, "error"));
            }
          }
          element_columns.push_back(std::move(elem_vec));
        }

        // Now build tuples by indexing pre-converted columns
        for (size_t i = 0; i < row_count; i++) {
          std::vector<ERL_NIF_TERM> tuple_elements;
          tuple_elements.reserve(tuple_size);
          for (size_t j = 0; j < tuple_size; j++) {
            tuple_elements.push_back(element_columns[j][i]);
          }
          column_values.push_back(enif_make_tuple_from_array(env, tuple_elements.data(), tuple_elements.size()));
        }
      } else if (auto enum8_col = col->As<ColumnEnum8>()) {
        for (size_t i = 0; i < row_count; i++) {
          std::string_view name = enum8_col->NameAt(i);
          ErlNifBinary bin;
          enif_alloc_binary(name.size(), &bin);
          std::memcpy(bin.data, name.data(), name.size());
          column_values.push_back(enif_make_binary(env, &bin));
        }
      } else if (auto enum16_col = col->As<ColumnEnum16>()) {
        for (size_t i = 0; i < row_count; i++) {
          std::string_view name = enum16_col->NameAt(i);
          ErlNifBinary bin;
          enif_alloc_binary(name.size(), &bin);
          std::memcpy(bin.data, name.data(), name.size());
          column_values.push_back(enif_make_binary(env, &bin));
        }
      } else if (auto lc_col = col->As<ColumnLowCardinality>()) {
        for (size_t i = 0; i < row_count; i++) {
          auto item = lc_col->GetItem(i);
          if (item.type == Type::String) {
            auto val = item.get<std::string_view>();
            ErlNifBinary bin;
            enif_alloc_binary(val.size(), &bin);
            std::memcpy(bin.data, val.data(), val.size());
            column_values.push_back(enif_make_binary(env, &bin));
          } else if (item.type == Type::Void) {
            column_values.push_back(enif_make_atom(env, "nil"));
          } else {
            throw std::runtime_error("Unsupported LowCardinality inner type");
          }
        }
      } else if (auto nullable_col = col->As<ColumnNullable>()) {
        auto nested = nullable_col->Nested();

        // Optimized: Check nested type ONCE outside loop, then direct extraction
        if (auto uint64_col = nested->As<ColumnUInt64>()) {
          for (size_t i = 0; i < row_count; i++) {
            if (nullable_col->IsNull(i)) {
              column_values.push_back(enif_make_atom(env, "nil"));
            } else {
              column_values.push_back(enif_make_uint64(env, uint64_col->At(i)));
            }
          }
        } else if (auto int64_col = nested->As<ColumnInt64>()) {
          for (size_t i = 0; i < row_count; i++) {
            if (nullable_col->IsNull(i)) {
              column_values.push_back(enif_make_atom(env, "nil"));
            } else {
              column_values.push_back(enif_make_int64(env, int64_col->At(i)));
            }
          }
        } else if (auto float64_col = nested->As<ColumnFloat64>()) {
          for (size_t i = 0; i < row_count; i++) {
            if (nullable_col->IsNull(i)) {
              column_values.push_back(enif_make_atom(env, "nil"));
            } else {
              column_values.push_back(enif_make_double(env, float64_col->At(i)));
            }
          }
        } else if (auto string_col = nested->As<ColumnString>()) {
          for (size_t i = 0; i < row_count; i++) {
            if (nullable_col->IsNull(i)) {
              column_values.push_back(enif_make_atom(env, "nil"));
            } else {
              std::string_view val_view = string_col->At(i);
              ErlNifBinary bin;
              enif_alloc_binary(val_view.size(), &bin);
              std::memcpy(bin.data, val_view.data(), val_view.size());
              column_values.push_back(enif_make_binary(env, &bin));
            }
          }
        } else {
          // Fallback for complex/uncommon types: use Slice approach
          for (size_t i = 0; i < row_count; i++) {
            if (nullable_col->IsNull(i)) {
              column_values.push_back(enif_make_atom(env, "nil"));
            } else {
              auto single_value_col = nested->Slice(i, 1);
              ERL_NIF_TERM elem_list = column_to_elixir_list(env, single_value_col);
              ERL_NIF_TERM head, tail;
              if (enif_get_list_cell(env, elem_list, &head, &tail)) {
                column_values.push_back(head);
              } else {
                column_values.push_back(enif_make_atom(env, "error"));
              }
            }
          }
        }
      }

      // Append this block's column values to accumulated data (indexed access - O(1))
      all_columns[c].insert(
        all_columns[c].end(),
        column_values.begin(),
        column_values.end()
      );
    }
  });

  // Build Elixir map: %{column_name => [values]}
  // Atoms already created during first block processing
  size_t num_columns = all_columns.size();
  std::vector<ERL_NIF_TERM> values;
  values.reserve(num_columns);

  for (size_t c = 0; c < num_columns; c++) {
    values.push_back(enif_make_list_from_array(env, all_columns[c].data(), all_columns[c].size()));
  }

  ERL_NIF_TERM columns_map;
  enif_make_map_from_arrays(env, key_atoms.data(), values.data(), num_columns, &columns_map);

  return ColumnarResult(columns_map);
}

FINE_NIF(client_select_cols, 0);

// Execute parameterized SELECT query and return columnar format
ColumnarResult client_select_cols_parameterized(
    ErlNifEnv *env,
    fine::ResourcePtr<Client> client,
    fine::ResourcePtr<Query> query) {

  // Pre-create column structure on first block (indexed vectors for O(1) access)
  std::vector<std::string> col_names;
  std::vector<ERL_NIF_TERM> key_atoms;
  std::vector<std::vector<ERL_NIF_TERM>> all_columns;
  bool first_block = true;

  // Set callback on the Query object before calling Select
  query->OnData([&](const Block &block) {
    size_t col_count = block.GetColumnCount();
    size_t row_count = block.GetRowCount();

    if (row_count == 0) {
      return;
    }

    // Initialize column structure on first block
    if (first_block) {
      col_names.reserve(col_count);
      key_atoms.reserve(col_count);
      all_columns.reserve(col_count);

      for (size_t c = 0; c < col_count; c++) {
        std::string col_name = block.GetColumnName(c);
        col_names.push_back(col_name);
        key_atoms.push_back(enif_make_atom(env, col_name.c_str()));

        // Estimate capacity: assume 10 blocks total (heuristic)
        std::vector<ERL_NIF_TERM> col_vec;
        col_vec.reserve(row_count * 10);
        all_columns.push_back(std::move(col_vec));
      }

      first_block = false;
    }

    // Extract each column's data using index-based access
    for (size_t c = 0; c < col_count; c++) {
      ColumnRef col = block[c];
      std::vector<ERL_NIF_TERM> column_values;
      column_values.reserve(row_count);

      // Extract column data based on type (reuse logic from client_select_cols)
      if (auto uint64_col = col->As<ColumnUInt64>()) {
        for (size_t i = 0; i < row_count; i++) {
          column_values.push_back(enif_make_uint64(env, uint64_col->At(i)));
        }
      } else if (auto uint32_col = col->As<ColumnUInt32>()) {
        for (size_t i = 0; i < row_count; i++) {
          column_values.push_back(enif_make_uint64(env, uint32_col->At(i)));
        }
      } else if (auto uint16_col = col->As<ColumnUInt16>()) {
        for (size_t i = 0; i < row_count; i++) {
          column_values.push_back(enif_make_uint64(env, uint16_col->At(i)));
        }
      } else if (auto uint8_col = col->As<ColumnUInt8>()) {
        for (size_t i = 0; i < row_count; i++) {
          column_values.push_back(enif_make_uint64(env, uint8_col->At(i)));
        }
      } else if (auto int64_col = col->As<ColumnInt64>()) {
        for (size_t i = 0; i < row_count; i++) {
          column_values.push_back(enif_make_int64(env, int64_col->At(i)));
        }
      } else if (auto int32_col = col->As<ColumnInt32>()) {
        for (size_t i = 0; i < row_count; i++) {
          column_values.push_back(enif_make_int64(env, int32_col->At(i)));
        }
      } else if (auto int16_col = col->As<ColumnInt16>()) {
        for (size_t i = 0; i < row_count; i++) {
          column_values.push_back(enif_make_int64(env, int16_col->At(i)));
        }
      } else if (auto int8_col = col->As<ColumnInt8>()) {
        for (size_t i = 0; i < row_count; i++) {
          column_values.push_back(enif_make_int64(env, int8_col->At(i)));
        }
      } else if (auto float64_col = col->As<ColumnFloat64>()) {
        for (size_t i = 0; i < row_count; i++) {
          column_values.push_back(enif_make_double(env, float64_col->At(i)));
        }
      } else if (auto float32_col = col->As<ColumnFloat32>()) {
        for (size_t i = 0; i < row_count; i++) {
          column_values.push_back(enif_make_double(env, float32_col->At(i)));
        }
      } else if (auto string_col = col->As<ColumnString>()) {
        for (size_t i = 0; i < row_count; i++) {
          std::string_view val_view = string_col->At(i);
          ErlNifBinary bin;
          enif_alloc_binary(val_view.size(), &bin);
          std::memcpy(bin.data, val_view.data(), val_view.size());
          column_values.push_back(enif_make_binary(env, &bin));
        }
      } else if (auto datetime_col = col->As<ColumnDateTime>()) {
        for (size_t i = 0; i < row_count; i++) {
          column_values.push_back(enif_make_uint64(env, datetime_col->At(i)));
        }
      } else if (auto datetime64_col = col->As<ColumnDateTime64>()) {
        for (size_t i = 0; i < row_count; i++) {
          column_values.push_back(enif_make_int64(env, datetime64_col->At(i)));
        }
      } else if (auto date_col = col->As<ColumnDate>()) {
        for (size_t i = 0; i < row_count; i++) {
          column_values.push_back(enif_make_uint64(env, date_col->RawAt(i)));
        }
      } else if (auto uuid_col = col->As<ColumnUUID>()) {
        for (size_t i = 0; i < row_count; i++) {
          UUID uuid = uuid_col->At(i);
          char uuid_buf[37];
          format_uuid_to_buffer(uuid, uuid_buf);
          ErlNifBinary bin;
          enif_alloc_binary(36, &bin);
          std::memcpy(bin.data, uuid_buf, 36);
          column_values.push_back(enif_make_binary(env, &bin));
        }
      } else if (auto decimal_col = col->As<ColumnDecimal>()) {
        for (size_t i = 0; i < row_count; i++) {
          Int128 value = decimal_col->At(i);
          int64_t scaled_value = static_cast<int64_t>(value);
          column_values.push_back(enif_make_int64(env, scaled_value));
        }
      } else if (auto array_col = col->As<ColumnArray>()) {
        for (size_t i = 0; i < row_count; i++) {
          auto nested = array_col->GetAsColumn(i);
          column_values.push_back(column_to_elixir_list(env, nested));
        }
      } else if (auto map_col = col->As<ColumnMap>()) {
        for (size_t i = 0; i < row_count; i++) {
          auto kv_tuples = map_col->GetAsColumn(i);
          if (auto tuple_col = kv_tuples->As<ColumnTuple>()) {
            auto keys_col = tuple_col->At(0);
            auto values_col = tuple_col->At(1);
            size_t map_size = keys_col->Size();

            std::vector<ERL_NIF_TERM> key_terms;
            std::vector<ERL_NIF_TERM> value_terms;
            key_terms.reserve(map_size);
            value_terms.reserve(map_size);

            ERL_NIF_TERM keys_list = column_to_elixir_list(env, keys_col);
            ERL_NIF_TERM key_tail = keys_list;
            for (size_t j = 0; j < map_size; j++) {
              ERL_NIF_TERM key;
              if (enif_get_list_cell(env, key_tail, &key, &key_tail)) {
                key_terms.push_back(key);
              }
            }

            ERL_NIF_TERM values_list = column_to_elixir_list(env, values_col);
            ERL_NIF_TERM value_tail = values_list;
            for (size_t j = 0; j < map_size; j++) {
              ERL_NIF_TERM value;
              if (enif_get_list_cell(env, value_tail, &value, &value_tail)) {
                value_terms.push_back(value);
              }
            }

            ERL_NIF_TERM elixir_map;
            enif_make_map_from_arrays(env, key_terms.data(), value_terms.data(), map_size, &elixir_map);

            column_values.push_back(elixir_map);
          } else {
            column_values.push_back(enif_make_new_map(env));
          }
        }
      } else if (auto tuple_col = col->As<ColumnTuple>()) {
        size_t tuple_size = tuple_col->TupleSize();

        std::vector<std::vector<ERL_NIF_TERM>> element_columns;
        element_columns.reserve(tuple_size);

        for (size_t j = 0; j < tuple_size; j++) {
          auto element_col = tuple_col->At(j);
          ERL_NIF_TERM elem_list = column_to_elixir_list(env, element_col);
          std::vector<ERL_NIF_TERM> elem_vec;
          elem_vec.reserve(row_count);
          ERL_NIF_TERM tail = elem_list;
          for (size_t i = 0; i < row_count; i++) {
            ERL_NIF_TERM head;
            if (enif_get_list_cell(env, tail, &head, &tail)) {
              elem_vec.push_back(head);
            } else {
              elem_vec.push_back(enif_make_atom(env, "error"));
            }
          }
          element_columns.push_back(std::move(elem_vec));
        }

        for (size_t i = 0; i < row_count; i++) {
          std::vector<ERL_NIF_TERM> tuple_elements;
          tuple_elements.reserve(tuple_size);
          for (size_t j = 0; j < tuple_size; j++) {
            tuple_elements.push_back(element_columns[j][i]);
          }
          column_values.push_back(enif_make_tuple_from_array(env, tuple_elements.data(), tuple_elements.size()));
        }
      } else if (auto enum8_col = col->As<ColumnEnum8>()) {
        for (size_t i = 0; i < row_count; i++) {
          std::string_view name = enum8_col->NameAt(i);
          ErlNifBinary bin;
          enif_alloc_binary(name.size(), &bin);
          std::memcpy(bin.data, name.data(), name.size());
          column_values.push_back(enif_make_binary(env, &bin));
        }
      } else if (auto enum16_col = col->As<ColumnEnum16>()) {
        for (size_t i = 0; i < row_count; i++) {
          std::string_view name = enum16_col->NameAt(i);
          ErlNifBinary bin;
          enif_alloc_binary(name.size(), &bin);
          std::memcpy(bin.data, name.data(), name.size());
          column_values.push_back(enif_make_binary(env, &bin));
        }
      } else if (auto lc_col = col->As<ColumnLowCardinality>()) {
        for (size_t i = 0; i < row_count; i++) {
          auto item = lc_col->GetItem(i);
          if (item.type == Type::String) {
            auto val = item.get<std::string_view>();
            ErlNifBinary bin;
            enif_alloc_binary(val.size(), &bin);
            std::memcpy(bin.data, val.data(), val.size());
            column_values.push_back(enif_make_binary(env, &bin));
          } else if (item.type == Type::Void) {
            column_values.push_back(enif_make_atom(env, "nil"));
          } else {
            throw std::runtime_error("Unsupported LowCardinality inner type");
          }
        }
      } else if (auto nullable_col = col->As<ColumnNullable>()) {
        auto nested = nullable_col->Nested();

        if (auto uint64_col = nested->As<ColumnUInt64>()) {
          for (size_t i = 0; i < row_count; i++) {
            if (nullable_col->IsNull(i)) {
              column_values.push_back(enif_make_atom(env, "nil"));
            } else {
              column_values.push_back(enif_make_uint64(env, uint64_col->At(i)));
            }
          }
        } else if (auto int64_col = nested->As<ColumnInt64>()) {
          for (size_t i = 0; i < row_count; i++) {
            if (nullable_col->IsNull(i)) {
              column_values.push_back(enif_make_atom(env, "nil"));
            } else {
              column_values.push_back(enif_make_int64(env, int64_col->At(i)));
            }
          }
        } else if (auto float64_col = nested->As<ColumnFloat64>()) {
          for (size_t i = 0; i < row_count; i++) {
            if (nullable_col->IsNull(i)) {
              column_values.push_back(enif_make_atom(env, "nil"));
            } else {
              column_values.push_back(enif_make_double(env, float64_col->At(i)));
            }
          }
        } else if (auto string_col = nested->As<ColumnString>()) {
          for (size_t i = 0; i < row_count; i++) {
            if (nullable_col->IsNull(i)) {
              column_values.push_back(enif_make_atom(env, "nil"));
            } else {
              std::string_view val_view = string_col->At(i);
              ErlNifBinary bin;
              enif_alloc_binary(val_view.size(), &bin);
              std::memcpy(bin.data, val_view.data(), val_view.size());
              column_values.push_back(enif_make_binary(env, &bin));
            }
          }
        } else {
          // Fallback for complex/uncommon types
          for (size_t i = 0; i < row_count; i++) {
            if (nullable_col->IsNull(i)) {
              column_values.push_back(enif_make_atom(env, "nil"));
            } else {
              auto single_value_col = nested->Slice(i, 1);
              ERL_NIF_TERM elem_list = column_to_elixir_list(env, single_value_col);
              ERL_NIF_TERM head, tail;
              if (enif_get_list_cell(env, elem_list, &head, &tail)) {
                column_values.push_back(head);
              } else {
                column_values.push_back(enif_make_atom(env, "error"));
              }
            }
          }
        }
      }

      // Append this block's column values to accumulated data
      all_columns[c].insert(
        all_columns[c].end(),
        column_values.begin(),
        column_values.end()
      );
    }
  });
  // Execute the query with the configured callback
  client->Select(*query);


  // Build Elixir map: %{column_name => [values]}
  size_t num_columns = all_columns.size();
  std::vector<ERL_NIF_TERM> values;
  values.reserve(num_columns);

  for (size_t c = 0; c < num_columns; c++) {
    values.push_back(enif_make_list_from_array(env, all_columns[c].data(), all_columns[c].size()));
  }

  ERL_NIF_TERM columns_map;
  enif_make_map_from_arrays(env, key_atoms.data(), values.data(), num_columns, &columns_map);

  return ColumnarResult(columns_map);
}

FINE_NIF(client_select_cols_parameterized, 0);

