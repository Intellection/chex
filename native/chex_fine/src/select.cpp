#include <fine.hpp>
#include <clickhouse/client.h>
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
#include <string>
#include <vector>
#include <memory>
#include <sstream>
#include <iomanip>

using namespace clickhouse;

// Declare that Client is a FINE resource (defined in minimal.cpp)
extern "C" {
  FINE_RESOURCE(Client);
}

// Forward declaration
ERL_NIF_TERM column_to_elixir_list(ErlNifEnv *env, ColumnRef col);

// Helper to recursively convert a column to an Elixir list
// This handles all column types including nested arrays
ERL_NIF_TERM column_to_elixir_list(ErlNifEnv *env, ColumnRef col) {
  size_t count = col->Size();
  std::vector<ERL_NIF_TERM> values;

  if (auto uint64_col = col->As<ColumnUInt64>()) {
    for (size_t i = 0; i < count; i++) {
      values.push_back(enif_make_uint64(env, uint64_col->At(i)));
    }
  } else if (auto uint32_col = col->As<ColumnUInt32>()) {
    for (size_t i = 0; i < count; i++) {
      values.push_back(enif_make_uint64(env, uint32_col->At(i)));
    }
  } else if (auto uint16_col = col->As<ColumnUInt16>()) {
    for (size_t i = 0; i < count; i++) {
      values.push_back(enif_make_uint64(env, uint16_col->At(i)));
    }
  } else if (auto uint8_col = col->As<ColumnUInt8>()) {
    for (size_t i = 0; i < count; i++) {
      values.push_back(enif_make_uint64(env, uint8_col->At(i)));
    }
  } else if (auto int64_col = col->As<ColumnInt64>()) {
    for (size_t i = 0; i < count; i++) {
      values.push_back(enif_make_int64(env, int64_col->At(i)));
    }
  } else if (auto int32_col = col->As<ColumnInt32>()) {
    for (size_t i = 0; i < count; i++) {
      values.push_back(enif_make_int64(env, int32_col->At(i)));
    }
  } else if (auto int16_col = col->As<ColumnInt16>()) {
    for (size_t i = 0; i < count; i++) {
      values.push_back(enif_make_int64(env, int16_col->At(i)));
    }
  } else if (auto int8_col = col->As<ColumnInt8>()) {
    for (size_t i = 0; i < count; i++) {
      values.push_back(enif_make_int64(env, int8_col->At(i)));
    }
  } else if (auto float64_col = col->As<ColumnFloat64>()) {
    for (size_t i = 0; i < count; i++) {
      values.push_back(enif_make_double(env, float64_col->At(i)));
    }
  } else if (auto float32_col = col->As<ColumnFloat32>()) {
    for (size_t i = 0; i < count; i++) {
      values.push_back(enif_make_double(env, float32_col->At(i)));
    }
  } else if (auto string_col = col->As<ColumnString>()) {
    for (size_t i = 0; i < count; i++) {
      std::string_view val_view = string_col->At(i);
      std::string val(val_view);
      ErlNifBinary bin;
      enif_alloc_binary(val.size(), &bin);
      std::memcpy(bin.data, val.data(), val.size());
      values.push_back(enif_make_binary(env, &bin));
    }
  } else if (auto datetime_col = col->As<ColumnDateTime>()) {
    for (size_t i = 0; i < count; i++) {
      values.push_back(enif_make_uint64(env, datetime_col->At(i)));
    }
  } else if (auto datetime64_col = col->As<ColumnDateTime64>()) {
    for (size_t i = 0; i < count; i++) {
      values.push_back(enif_make_int64(env, datetime64_col->At(i)));
    }
  } else if (auto date_col = col->As<ColumnDate>()) {
    for (size_t i = 0; i < count; i++) {
      values.push_back(enif_make_uint64(env, date_col->RawAt(i)));
    }
  } else if (auto uuid_col = col->As<ColumnUUID>()) {
    for (size_t i = 0; i < count; i++) {
      UUID uuid = uuid_col->At(i);
      std::ostringstream oss;
      oss << std::hex << std::setfill('0');
      uint64_t high = uuid.first;
      oss << std::setw(8) << ((high >> 32) & 0xFFFFFFFF) << "-";
      oss << std::setw(4) << ((high >> 16) & 0xFFFF) << "-";
      oss << std::setw(4) << (high & 0xFFFF) << "-";
      uint64_t low = uuid.second;
      oss << std::setw(4) << ((low >> 48) & 0xFFFF) << "-";
      oss << std::setw(12) << (low & 0xFFFFFFFFFFFF);
      std::string uuid_str = oss.str();
      ErlNifBinary bin;
      enif_alloc_binary(uuid_str.size(), &bin);
      std::memcpy(bin.data, uuid_str.data(), uuid_str.size());
      values.push_back(enif_make_binary(env, &bin));
    }
  } else if (auto decimal_col = col->As<ColumnDecimal>()) {
    for (size_t i = 0; i < count; i++) {
      Int128 value = decimal_col->At(i);
      int64_t scaled_value = static_cast<int64_t>(value);
      values.push_back(enif_make_int64(env, scaled_value));
    }
  } else if (auto array_col = col->As<ColumnArray>()) {
    // Recursively handle nested arrays
    for (size_t i = 0; i < count; i++) {
      auto nested = array_col->GetAsColumn(i);
      values.push_back(column_to_elixir_list(env, nested));
    }
  } else if (auto tuple_col = col->As<ColumnTuple>()) {
    // Handle tuple columns - return Elixir tuples
    size_t tuple_size = tuple_col->TupleSize();
    for (size_t i = 0; i < count; i++) {
      std::vector<ERL_NIF_TERM> tuple_elements;
      tuple_elements.reserve(tuple_size);

      // Extract each element from the tuple
      for (size_t j = 0; j < tuple_size; j++) {
        auto element_col = tuple_col->At(j);
        // Get the i-th value from the j-th element column
        // We need to slice this column to get just one value
        auto single_value_col = element_col->Slice(i, 1);
        // Convert to Elixir and extract the first (only) element
        ERL_NIF_TERM elem_list = column_to_elixir_list(env, single_value_col);
        // Extract first element from the list
        ERL_NIF_TERM head, tail;
        if (enif_get_list_cell(env, elem_list, &head, &tail)) {
          tuple_elements.push_back(head);
        } else {
          // Handle empty case - shouldn't happen but be defensive
          tuple_elements.push_back(enif_make_atom(env, "error"));
        }
      }

      // Create Elixir tuple from elements
      values.push_back(enif_make_tuple_from_array(env, tuple_elements.data(), tuple_elements.size()));
    }
  } else if (auto map_col = col->As<ColumnMap>()) {
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

        // Build Elixir map
        ERL_NIF_TERM elixir_map = enif_make_new_map(env);

        // Convert both columns to Elixir lists
        ERL_NIF_TERM keys_list = column_to_elixir_list(env, keys_col);
        ERL_NIF_TERM values_list = column_to_elixir_list(env, values_col);

        // Iterate through both lists simultaneously to build the map
        for (size_t j = 0; j < map_size; j++) {
          ERL_NIF_TERM key, key_tail, value, value_tail;
          if (enif_get_list_cell(env, keys_list, &key, &key_tail) &&
              enif_get_list_cell(env, values_list, &value, &value_tail)) {
            enif_make_map_put(env, elixir_map, key, value, &elixir_map);
            keys_list = key_tail;
            values_list = value_tail;
          }
        }

        values.push_back(elixir_map);
      } else {
        // Fallback for unexpected structure
        values.push_back(enif_make_new_map(env));
      }
    }
  } else if (auto enum8_col = col->As<ColumnEnum8>()) {
    // Handle Enum8 columns - return string names
    for (size_t i = 0; i < count; i++) {
      std::string_view name = enum8_col->NameAt(i);
      ErlNifBinary bin;
      enif_alloc_binary(name.size(), &bin);
      std::memcpy(bin.data, name.data(), name.size());
      values.push_back(enif_make_binary(env, &bin));
    }
  } else if (auto enum16_col = col->As<ColumnEnum16>()) {
    // Handle Enum16 columns - return string names
    for (size_t i = 0; i < count; i++) {
      std::string_view name = enum16_col->NameAt(i);
      ErlNifBinary bin;
      enif_alloc_binary(name.size(), &bin);
      std::memcpy(bin.data, name.data(), name.size());
      values.push_back(enif_make_binary(env, &bin));
    }
  } else if (auto lc_col = col->As<ColumnLowCardinality>()) {
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
  } else if (auto nullable_col = col->As<ColumnNullable>()) {
    auto nested = nullable_col->Nested();
    for (size_t i = 0; i < count; i++) {
      if (nullable_col->IsNull(i)) {
        values.push_back(enif_make_atom(env, "nil"));
      } else {
        // Generic handling: slice the nested column to get just the i-th value,
        // then recursively convert it. This handles all types uniformly.
        auto single_value_col = nested->Slice(i, 1);
        ERL_NIF_TERM elem_list = column_to_elixir_list(env, single_value_col);
        // Extract first element from the list
        ERL_NIF_TERM head, tail;
        if (enif_get_list_cell(env, elem_list, &head, &tail)) {
          values.push_back(head);
        } else {
          // Should never happen, but be defensive
          values.push_back(enif_make_atom(env, "error"));
        }
      }
    }
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
        std::string val(val_view);
        ErlNifBinary bin;
        enif_alloc_binary(val.size(), &bin);
        std::memcpy(bin.data, val.data(), val.size());
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
        // Convert UUID to standard string format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        std::ostringstream oss;
        oss << std::hex << std::setfill('0');

        // high 64 bits
        uint64_t high = uuid.first;
        oss << std::setw(8) << ((high >> 32) & 0xFFFFFFFF) << "-";
        oss << std::setw(4) << ((high >> 16) & 0xFFFF) << "-";
        oss << std::setw(4) << (high & 0xFFFF) << "-";

        // low 64 bits
        uint64_t low = uuid.second;
        oss << std::setw(4) << ((low >> 48) & 0xFFFF) << "-";
        oss << std::setw(12) << (low & 0xFFFFFFFFFFFF);

        std::string uuid_str = oss.str();
        ErlNifBinary bin;
        enif_alloc_binary(uuid_str.size(), &bin);
        std::memcpy(bin.data, uuid_str.data(), uuid_str.size());
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

          ERL_NIF_TERM elixir_map = enif_make_new_map(env);
          ERL_NIF_TERM keys_list = column_to_elixir_list(env, keys_col);
          ERL_NIF_TERM values_list = column_to_elixir_list(env, values_col);

          for (size_t j = 0; j < map_size; j++) {
            ERL_NIF_TERM key, key_tail, value, value_tail;
            if (enif_get_list_cell(env, keys_list, &key, &key_tail) &&
                enif_get_list_cell(env, values_list, &value, &value_tail)) {
              enif_make_map_put(env, elixir_map, key, value, &elixir_map);
              keys_list = key_tail;
              values_list = value_tail;
            }
          }
          column_values.push_back(elixir_map);
        } else {
          column_values.push_back(enif_make_new_map(env));
        }
      }
    } else if (auto tuple_col = col->As<ColumnTuple>()) {
      // Handle tuple columns - use column_to_elixir_list for complex logic
      for (size_t i = 0; i < row_count; i++) {
        size_t tuple_size = tuple_col->TupleSize();
        std::vector<ERL_NIF_TERM> tuple_elements;
        tuple_elements.reserve(tuple_size);

        for (size_t j = 0; j < tuple_size; j++) {
          auto element_col = tuple_col->At(j);
          auto single_value_col = element_col->Slice(i, 1);
          ERL_NIF_TERM elem_list = column_to_elixir_list(env, single_value_col);
          ERL_NIF_TERM head, tail;
          if (enif_get_list_cell(env, elem_list, &head, &tail)) {
            tuple_elements.push_back(head);
          } else {
            tuple_elements.push_back(enif_make_atom(env, "error"));
          }
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
      // Handle nullable columns - use generic approach via column_to_elixir_list
      auto nested = nullable_col->Nested();

      for (size_t i = 0; i < row_count; i++) {
        if (nullable_col->IsNull(i)) {
          column_values.push_back(enif_make_atom(env, "nil"));
        } else {
          // Generic handling: slice the nested column to get just the i-th value,
          // then recursively convert it. This handles all types uniformly.
          auto single_value_col = nested->Slice(i, 1);
          ERL_NIF_TERM elem_list = column_to_elixir_list(env, single_value_col);
          // Extract first element from the list
          ERL_NIF_TERM head, tail;
          if (enif_get_list_cell(env, elem_list, &head, &tail)) {
            column_values.push_back(head);
          } else {
            // Should never happen, but be defensive
            column_values.push_back(enif_make_atom(env, "error"));
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
