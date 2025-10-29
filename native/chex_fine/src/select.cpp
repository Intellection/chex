#include <fine.hpp>
#include <clickhouse/client.h>
#include <clickhouse/block.h>
#include <clickhouse/columns/column.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/date.h>
#include <string>
#include <vector>
#include <memory>

using namespace clickhouse;

// Declare that Client is a FINE resource (defined in minimal.cpp)
extern "C" {
  FINE_RESOURCE(Client);
}

// Helper to convert Block to list of maps
ERL_NIF_TERM block_to_maps_impl(ErlNifEnv *env, std::shared_ptr<Block> block) {
  size_t col_count = block->GetColumnCount();
  size_t row_count = block->GetRowCount();

  if (row_count == 0) {
    return enif_make_list(env, 0);
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
    } else if (auto int64_col = col->As<ColumnInt64>()) {
      for (size_t i = 0; i < row_count; i++) {
        column_values.push_back(enif_make_int64(env, int64_col->At(i)));
      }
    } else if (auto float64_col = col->As<ColumnFloat64>()) {
      for (size_t i = 0; i < row_count; i++) {
        column_values.push_back(enif_make_double(env, float64_col->At(i)));
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
    }

    col_data.push_back(column_values);
  }

  // Build list of maps
  std::vector<ERL_NIF_TERM> rows;
  for (size_t r = 0; r < row_count; r++) {
    ERL_NIF_TERM keys[col_count];
    ERL_NIF_TERM values[col_count];

    for (size_t c = 0; c < col_count; c++) {
      keys[c] = enif_make_atom(env, col_names[c].c_str());
      values[c] = col_data[c][r];
    }

    ERL_NIF_TERM map;
    enif_make_map_from_arrays(env, keys, values, col_count, &map);
    rows.push_back(map);
  }

  return enif_make_list_from_array(env, rows.data(), rows.size());
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
    // Convert this block to maps immediately while data is valid
    auto block_ptr = std::make_shared<Block>(block);
    ERL_NIF_TERM maps_from_block = block_to_maps_impl(env, block_ptr);

    // Unpack the list and add to our collection
    unsigned int list_length;
    if (enif_get_list_length(env, maps_from_block, &list_length)) {
      ERL_NIF_TERM head, tail = maps_from_block;
      while (enif_get_list_cell(env, tail, &head, &tail)) {
        all_maps.push_back(head);
      }
    }
  });

  // Build final list from all maps
  if (all_maps.empty()) {
    return SelectResult(enif_make_list(env, 0));
  }

  return SelectResult(enif_make_list_from_array(env, all_maps.data(), all_maps.size()));
}

FINE_NIF(client_select, 0);
