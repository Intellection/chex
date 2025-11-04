#include <fine.hpp>
#include <clickhouse/client.h>
#include <clickhouse/block.h>
#include <string>
#include <memory>
#include <stdexcept>
#include "error_encoding.h"

using namespace clickhouse;

// Forward declare ColumnResource from column.cpp
struct ColumnResource {
  std::shared_ptr<Column> ptr;
  ColumnResource(std::shared_ptr<Column> p) : ptr(p) {}
};

// Declare ColumnResource as extern (defined in column.cpp)
extern "C" {
  // FINE will handle the resource type registration
}

// Wrapper for Block
struct BlockResource {
  std::shared_ptr<Block> ptr;

  BlockResource() : ptr(std::make_shared<Block>()) {}
  BlockResource(std::shared_ptr<Block> p) : ptr(p) {}
};

// Declare BlockResource as a FINE resource
FINE_RESOURCE(BlockResource);

// Also need to declare ColumnResource here for FINE to recognize it
FINE_RESOURCE(ColumnResource);

// Create a new empty block
fine::ResourcePtr<BlockResource> block_create(ErlNifEnv *env) {
  try {
    return fine::make_resource<BlockResource>();
  } catch (const std::exception& e) {
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(block_create, 0);

// Append a column to the block
// Note: Block takes ownership via shared_ptr
fine::Atom block_append_column(
    ErlNifEnv *env,
    fine::ResourcePtr<BlockResource> block_res,
    std::string name,
    fine::ResourcePtr<ColumnResource> col_res) {
  try {
    // Block::AppendColumn takes shared_ptr<Column>
    block_res->ptr->AppendColumn(name, col_res->ptr);
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(block_append_column, 0);

// Get the number of rows in the block
uint64_t block_row_count(
    ErlNifEnv *env,
    fine::ResourcePtr<BlockResource> block_res) {
  return block_res->ptr->GetRowCount();
}
FINE_NIF(block_row_count, 0);

// Get the number of columns in the block
uint64_t block_column_count(
    ErlNifEnv *env,
    fine::ResourcePtr<BlockResource> block_res) {
  return block_res->ptr->GetColumnCount();
}
FINE_NIF(block_column_count, 0);

// Forward declare Client (from minimal.cpp)
// We need this to avoid duplicate FINE_RESOURCE declarations
namespace clickhouse {
  class Client;
}

// Insert a block into a table
fine::Atom client_insert(
    ErlNifEnv *env,
    fine::ResourcePtr<Client> client,
    std::string table_name,
    fine::ResourcePtr<BlockResource> block_res) {
  try {
    // Block is copied by Insert
    client->Insert(table_name, *block_res->ptr);
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(client_insert, 0);
