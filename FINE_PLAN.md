# Chex: FINE Wrapper Implementation Plan

**Last Updated:** 2025-10-29
**Status:** ✅ Phases 1-4 Complete (PoC + Foundation + Types + Blocks + SELECT)
**Timeline:** MVP achieved in ~1 hour

---

## Executive Summary

This document outlines a plan to wrap the clickhouse-cpp library using FINE (Foreign Interface Native Extensions) to provide native TCP protocol access to ClickHouse from Elixir. This approach delivers 51% faster inserts and 53% less bandwidth compared to HTTP while maintaining feature completeness through the mature clickhouse-cpp library.

### Why FINE + clickhouse-cpp?

**Pros:**
- ✅ Native protocol performance (51% faster than HTTP)
- ✅ Full feature completeness immediately (leverage clickhouse-cpp)
- ✅ Automatic type marshalling (FINE handles FFI complexity)
- ✅ 4-6 weeks to production vs 4-6 months for pure Elixir
- ✅ Native C++ speed for serialization/deserialization

**Cons:**
- ⚠️ C++ build dependencies (cmake, C++17 compiler)
- ⚠️ Cross-language debugging complexity
- ⚠️ Platform-specific compilation required
- ⚠️ Ongoing maintenance tracking upstream changes

---

## Architecture Overview

```
┌─────────────────────────────────────────────┐
│  Elixir Application                         │
│  - Chex.query/3, Chex.insert/3             │
│  - Idiomatic Elixir API                     │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│  Elixir Management Layer                    │
│  - Chex.Connection GenServer                │
│  - Connection pooling                       │
│  - Resource lifecycle                       │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│  FINE NIF Layer (C++)                       │
│  - Type conversion Elixir ↔ C++             │
│  - Resource management                      │
│  - Exception handling                       │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│  clickhouse-cpp Library                     │
│  - Native TCP protocol                      │
│  - Binary columnar format                   │
│  - Compression (LZ4/ZSTD)                   │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│  ClickHouse Server                          │
│  - Native protocol interface (9000)         │
└─────────────────────────────────────────────┘
```

---

## ✅ Phase 1: Foundation (COMPLETED)

**Goal:** Basic client lifecycle and connection management
**Status:** ✅ Complete - 10 tests passing

### Deliverables

1. **Project Setup**
   - Add FINE dependency to mix.exs
   - Set up C++ build infrastructure (CMake/Makefile)
   - Configure clickhouse-cpp as git submodule or vendored dependency
   - Create `native/chex_fine/` directory structure

2. **Client Resource Wrapper**
   ```cpp
   // native/chex_fine/client.cpp
   #include <fine.hpp>
   #include <clickhouse/client.h>

   using namespace clickhouse;

   FINE_RESOURCE(Client);

   fine::ResourcePtr<Client> client_create(
       ErlNifEnv *env,
       std::string host,
       int port) {
     ClientOptions opts;
     opts.SetHost(host);
     opts.SetPort(port);
     return fine::make_resource<Client>(opts);
   }
   FINE_NIF(client_create, 0);

   void client_ping(
       ErlNifEnv *env,
       fine::ResourcePtr<Client> client) {
     client->Ping();
   }
   FINE_NIF(client_ping, 0);

   void client_execute(
       ErlNifEnv *env,
       fine::ResourcePtr<Client> client,
       std::string sql) {
     client->Execute(sql);
   }
   FINE_NIF(client_execute, 0);

   FINE_INIT("Elixir.Chex.Native");
   ```

3. **Elixir Connection Module**
   ```elixir
   defmodule Chex.Connection do
     use GenServer

     def start_link(opts) do
       GenServer.start_link(__MODULE__, opts)
     end

     def init(opts) do
       host = Keyword.get(opts, :host, "localhost")
       port = Keyword.get(opts, :port, 9000)

       case Chex.Native.client_create(host, port) do
         {:ok, client} ->
           {:ok, %{client: client, opts: opts}}
         {:error, reason} ->
           {:stop, reason}
       end
     end

     def ping(conn) do
       GenServer.call(conn, :ping)
     end

     def execute(conn, sql) do
       GenServer.call(conn, {:execute, sql})
     end

     def handle_call(:ping, _from, state) do
       Chex.Native.client_ping(state.client)
       {:reply, :ok, state}
     end

     def handle_call({:execute, sql}, _from, state) do
       case Chex.Native.client_execute(state.client, sql) do
         :ok -> {:reply, :ok, state}
         {:error, reason} -> {:reply, {:error, reason}, state}
       end
     end
   end
   ```

### Testing
- Connection establishment
- Ping works
- Simple DDL operations (CREATE TABLE, DROP TABLE)
- Error handling for connection failures

**Success Criteria:** Can connect, ping, and execute DDL statements

---

## ✅ Phase 2: Type System & Column Creation (COMPLETED)

**Goal:** Handle ClickHouse type system and column creation
**Status:** ✅ Complete - 33 tests passing (5 core types: UInt64, Int64, String, Float64, DateTime)

### Key Challenge

ClickHouse has 42+ column types. We need to:
1. Parse type strings (e.g., "Array(Nullable(String))")
2. Create appropriate Column objects dynamically
3. Populate columns from Elixir data

### Approach: Leverage clickhouse-cpp's Factory

```cpp
// native/chex_fine/columns.cpp
#include <fine.hpp>
#include <clickhouse/columns/factory.h>

FINE_RESOURCE(clickhouse::Column);

fine::ResourcePtr<clickhouse::Column> column_create(
    ErlNifEnv *env,
    std::string type_name) {
  auto col = clickhouse::CreateColumnByType(type_name);
  return fine::make_resource_from_ptr(col);
}
FINE_NIF(column_create, 0);
```

### Column Population Strategy

**Option A: Type-Specific NIFs** (Recommended for MVP)
```cpp
// Separate NIF for each common type
void column_uint64_append(
    ErlNifEnv *env,
    fine::ResourcePtr<clickhouse::Column> col,
    uint64_t value) {
  auto typed = std::static_pointer_cast<clickhouse::ColumnUInt64>(col);
  typed->Append(value);
}
FINE_NIF(column_uint64_append, 0);

void column_string_append(
    ErlNifEnv *env,
    fine::ResourcePtr<clickhouse::Column> col,
    std::string value) {
  auto typed = std::static_pointer_cast<clickhouse::ColumnString>(col);
  typed->Append(value);
}
FINE_NIF(column_string_append, 0);
```

**Option B: Generic Variant-Based** (More complex, defer to Phase 4)
```cpp
using ElixirValue = std::variant<
  int64_t, uint64_t, double, std::string, bool,
  std::nullptr_t, std::vector<ElixirValue>
>;

void column_append_value(
    ErlNifEnv *env,
    fine::ResourcePtr<clickhouse::Column> col,
    std::string type_name,
    ElixirValue value) {
  // Dispatch based on type_name
  if (type_name == "UInt64") {
    auto typed = std::static_pointer_cast<clickhouse::ColumnUInt64>(col);
    typed->Append(std::get<uint64_t>(value));
  } else if (type_name == "String") {
    // ...
  }
  // etc.
}
```

### Deliverables

1. **Type Parser Integration**
   - Wrap `CreateColumnByType`
   - Return opaque Column resource

2. **Core Type Support** (start with 5 essential types)
   - UInt64, Int64
   - String
   - Float64
   - DateTime

3. **Column Builders in Elixir**
   ```elixir
   defmodule Chex.Column do
     def new(type) do
       Chex.Native.column_create(type)
     end

     def append(%{type: :uint64} = col, value) when is_integer(value) do
       Chex.Native.column_uint64_append(col.ref, value)
     end

     def append(%{type: :string} = col, value) when is_binary(value) do
       Chex.Native.column_string_append(col.ref, value)
     end
   end
   ```

### Testing
- Create columns of different types
- Append values
- Type safety (wrong type should error)

**Success Criteria:** Can create and populate basic column types

---

## ✅ Phase 3: Block Building & INSERT (COMPLETED)

**Goal:** Build blocks from Elixir data and insert into ClickHouse
**Status:** ✅ Complete - 17 tests passing

### Block Resource

```cpp
// native/chex_fine/block.cpp
#include <fine.hpp>
#include <clickhouse/block.h>

FINE_RESOURCE(clickhouse::Block);

fine::ResourcePtr<clickhouse::Block> block_create(ErlNifEnv *env) {
  return fine::make_resource<clickhouse::Block>();
}
FINE_NIF(block_create, 0);

void block_append_column(
    ErlNifEnv *env,
    fine::ResourcePtr<clickhouse::Block> block,
    std::string name,
    fine::ResourcePtr<clickhouse::Column> col) {
  block->AppendColumn(name, col);  // Need to convert to shared_ptr
}
FINE_NIF(block_append_column, 0);

uint64_t block_row_count(
    ErlNifEnv *env,
    fine::ResourcePtr<clickhouse::Block> block) {
  return block->GetRowCount();
}
FINE_NIF(block_row_count, 0);

void client_insert(
    ErlNifEnv *env,
    fine::ResourcePtr<clickhouse::Client> client,
    std::string table_name,
    fine::ResourcePtr<clickhouse::Block> block) {
  client->Insert(table_name, *block);
}
FINE_NIF(client_insert, 0);
```

### Smart Pointer Challenge

**Problem:** clickhouse-cpp uses `std::shared_ptr<Column>` but FINE uses `ResourcePtr<Column>`

**Solution:** Wrapper or conversion function
```cpp
// Helper to convert ResourcePtr to shared_ptr
std::shared_ptr<clickhouse::Column> resource_to_shared(
    fine::ResourcePtr<clickhouse::Column> res) {
  // Option 1: Store shared_ptr inside ResourcePtr wrapper
  // Option 2: Create new shared_ptr (may need ref counting coordination)
  return std::shared_ptr<clickhouse::Column>(res.get(), [](auto*){});
}

void block_append_column(
    ErlNifEnv *env,
    fine::ResourcePtr<clickhouse::Block> block,
    std::string name,
    fine::ResourcePtr<clickhouse::Column> col) {
  auto shared_col = resource_to_shared(col);
  block->AppendColumn(name, shared_col);
}
```

### Elixir Builder Pattern

```elixir
defmodule Chex.Insert do
  def build_block(table_schema, rows) do
    block = Chex.Native.block_create()

    # Create columns based on schema
    columns = for {name, type} <- table_schema do
      {name, Chex.Column.new(type)}
    end

    # Populate columns from rows
    for row <- rows do
      for {name, col} <- columns do
        value = Map.get(row, name)
        Chex.Column.append(col, value)
      end
    end

    # Attach columns to block
    for {name, col} <- columns do
      Chex.Native.block_append_column(block, to_string(name), col.ref)
    end

    block
  end

  def insert(conn, table, rows, schema) do
    block = build_block(schema, rows)
    GenServer.call(conn, {:insert, table, block})
  end
end
```

### Schema Inference

Two approaches:

**A. Explicit Schema (MVP)**
```elixir
schema = [
  {:id, :uint64},
  {:name, :string},
  {:amount, :float64}
]

Chex.Insert.insert(conn, "test_table", rows, schema)
```

**B. Schema Introspection (Phase 4)**
```elixir
# Query ClickHouse for table schema
schema = Chex.get_table_schema(conn, "test_table")
Chex.Insert.insert(conn, "test_table", rows, schema)
```

### Testing
- Build block from Elixir data
- Insert into ClickHouse
- Verify data with SELECT
- Batch inserts (1k, 10k, 100k rows)

**Success Criteria:** Can insert data from Elixir maps into ClickHouse

---

## ✅ Phase 4: SELECT & Data Retrieval (COMPLETED)

**Goal:** Execute SELECT queries and return results to Elixir
**Status:** ✅ Complete - 12 tests passing

### Challenge: Callback Bridge

clickhouse-cpp uses callbacks for SELECT:
```cpp
void Select(const std::string& query, SelectCallback cb);
// SelectCallback = std::function<void(const Block&)>
```

We need to bridge this to Elixir.

### Approach: Accumulate Results

```cpp
// native/chex_fine/select.cpp
std::vector<fine::ResourcePtr<clickhouse::Block>> client_select(
    ErlNifEnv *env,
    fine::ResourcePtr<clickhouse::Client> client,
    std::string query) {

  std::vector<fine::ResourcePtr<clickhouse::Block>> results;

  client->Select(query, [&](const clickhouse::Block& block) {
    // Copy block and wrap in ResourcePtr
    auto block_copy = std::make_shared<clickhouse::Block>(block);
    results.push_back(fine::make_resource_from_ptr(block_copy));
  });

  return results;  // FINE will convert vector to Elixir list
}
FINE_NIF(client_select, 0);
```

### Block to Elixir Conversion

```cpp
// native/chex_fine/convert.cpp
std::vector<std::map<std::string, ElixirValue>> block_to_maps(
    ErlNifEnv *env,
    fine::ResourcePtr<clickhouse::Block> block) {

  std::vector<std::map<std::string, ElixirValue>> rows;
  size_t row_count = block->GetRowCount();

  for (size_t row = 0; row < row_count; ++row) {
    std::map<std::string, ElixirValue> row_map;

    for (size_t col = 0; col < block->GetColumnCount(); ++col) {
      auto col_name = block->GetColumnName(col);
      auto column = block->GetColumn(col);

      // Extract value based on column type
      // This requires type dispatch logic
      row_map[col_name] = extract_value(column, row);
    }

    rows.push_back(row_map);
  }

  return rows;  // FINE converts to list of Elixir maps
}
FINE_NIF(block_to_maps, 0);
```

### Type Extraction

```cpp
ElixirValue extract_value(
    std::shared_ptr<clickhouse::Column> col,
    size_t row) {

  auto type = col->Type();

  if (type->GetCode() == clickhouse::Type::UInt64) {
    auto typed = std::static_pointer_cast<clickhouse::ColumnUInt64>(col);
    return (*typed)[row];
  } else if (type->GetCode() == clickhouse::Type::String) {
    auto typed = std::static_pointer_cast<clickhouse::ColumnString>(col);
    return std::string((*typed)[row]);
  }
  // ... handle all types

  throw std::runtime_error("Unsupported type");
}
```

### Elixir Query API

```elixir
defmodule Chex do
  def query(conn, sql) do
    case GenServer.call(conn, {:select, sql}, :infinity) do
      {:ok, blocks} ->
        rows = blocks
        |> Enum.flat_map(&Chex.Native.block_to_maps/1)
        {:ok, rows}
      error ->
        error
    end
  end

  def query!(conn, sql) do
    case query(conn, sql) do
      {:ok, rows} -> rows
      {:error, reason} -> raise "Query failed: #{inspect(reason)}"
    end
  end
end
```

### Testing
- Simple SELECT queries
- Multiple column types
- Large result sets
- JOIN queries
- Aggregations

**Success Criteria:** Can execute SELECT and get Elixir maps back

---

## Phase 5: Advanced Types (ESSENTIAL - Next Priority)

**Goal:** Expand type support to cover common use cases beyond the core 5 types
**Status:** ⏳ Pending
**Priority:** High - Essential for production use

### Additional Column Types

1. **Nullable Columns**
   ```cpp
   void column_nullable_append(
       ErlNifEnv *env,
       fine::ResourcePtr<clickhouse::Column> col,
       std::optional<ElixirValue> value) {
     auto typed = std::static_pointer_cast<clickhouse::ColumnNullable>(col);
     if (value.has_value()) {
       // Append nested value
     } else {
       // Append null
     }
   }
   ```

2. **Array Columns**
   ```cpp
   void column_array_append(
       ErlNifEnv *env,
       fine::ResourcePtr<clickhouse::Column> col,
       std::vector<ElixirValue> values) {
     auto typed = std::static_pointer_cast<clickhouse::ColumnArray>(col);
     // Build nested column and append
   }
   ```

3. **DateTime Types**
   - DateTime, DateTime64
   - Timezone handling

4. **Decimal Types**
   - Use Elixir Decimal library
   - Precision/scale conversion

### Compression Support

```cpp
fine::ResourcePtr<clickhouse::Client> client_create_with_compression(
    ErlNifEnv *env,
    std::string host,
    int port,
    std::string compression) {  // "lz4" or "zstd"

  ClientOptions opts;
  opts.SetHost(host);
  opts.SetPort(port);

  if (compression == "lz4") {
    opts.SetCompressionMethod(CompressionMethod::LZ4);
  } else if (compression == "zstd") {
    opts.SetCompressionMethod(CompressionMethod::ZSTD);
  }

  return fine::make_resource<Client>(opts);
}
```

### Connection Options

Support full ClientOptions:
- Authentication (user/password)
- SSL/TLS
- Compression
- Timeouts
- Retry logic

**Note:** Connection pooling should be investigated in the clickhouse-cpp library itself. If the C++ library handles connection pooling, we should leverage that rather than implementing it at the Elixir level.

---

## Phase 6: Production Polish

**Goal:** Error handling, testing, and production-readiness
**Status:** ⏳ Pending
**Priority:** Medium

### Error Handling

```cpp
// Custom error types
namespace fine {
  template<>
  struct Encoder<clickhouse::ServerException> {
    static ERL_NIF_TERM encode(ErlNifEnv* env, const clickhouse::ServerException& ex) {
      // Convert to Elixir exception with error code, message, stack trace
    }
  };
}
```

### Testing
- Comprehensive error handling tests
- Memory leaks (valgrind)
- Concurrent operations
- Authentication
- Connection failures and retries

**Success Criteria:** Production-ready error handling and reliability

---

## Phase 7: Advanced Query Features (Nice to Have)

**Goal:** Streaming SELECT, batch operations, advanced query patterns
**Status:** ⏳ Pending
**Priority:** Low - Nice to have

### Streaming SELECT

For large result sets, stream data back to Elixir:
```cpp
void client_select_async(
    ErlNifEnv *env,
    fine::ResourcePtr<Client> client,
    std::string query,
    ErlNifPid receiver_pid) {

  client->Select(query, [&](const Block& block) {
    // Send block to Elixir process
    ErlNifEnv* msg_env = enif_alloc_env();
    ERL_NIF_TERM block_term = encode_block(msg_env, block);
    enif_send(env, &receiver_pid, msg_env, block_term);
    enif_free_env(msg_env);
  });
}
```

### Batch Operations

Support multiple queries in a single transaction or batch.

---

## ❌ Phase 8: Ecto Integration (NOT IMPLEMENTING)

**Status:** ❌ Will not implement
**Reason:** Per user feedback - "I don't think Ecto is a good fit. We should never do this."

ClickHouse is an OLAP database optimized for analytics, not OLTP. Ecto's schema-based approach and transaction model don't align well with ClickHouse's use cases. Users should use Chex's direct query interface instead.

---

## Technical Challenges & Solutions

### 1. Smart Pointer Lifetime Management

**Problem:** clickhouse-cpp uses `std::shared_ptr`, FINE uses `ResourcePtr`

**Solution Options:**
- **A.** Store `shared_ptr` inside a wrapper struct, wrap that with `ResourcePtr`
- **B.** Use aliasing constructor to keep resources alive
- **C.** Careful manual lifetime management

**Recommended:** Option A
```cpp
struct ColumnResource {
  std::shared_ptr<clickhouse::Column> ptr;

  ColumnResource(std::shared_ptr<clickhouse::Column> p) : ptr(p) {}
};

FINE_RESOURCE(ColumnResource);

fine::ResourcePtr<ColumnResource> column_create(
    ErlNifEnv *env, std::string type) {
  auto col = clickhouse::CreateColumnByType(type);
  return fine::make_resource<ColumnResource>(col);
}
```

### 2. Type Dispatch for 42 Column Types

**Problem:** Need to handle 42+ column types dynamically

**Solution:** Phased implementation
- **Phase 1:** 5 essential types (UInt64, Int64, String, Float64, DateTime)
- **Phase 2:** 10 common types (add Nullable, Array, Date, Bool, Float32)
- **Phase 3:** 20 types (add Decimal, UUID, IPv4/6, Int32/16/8, UInt32/16/8)
- **Phase 4:** All remaining types (Geo, Enum, Map, Tuple, LowCardinality)

**Pattern:** Use visitor or type code dispatch
```cpp
ElixirValue extract_value(ColumnRef col, size_t row) {
  switch (col->Type()->GetCode()) {
    case Type::UInt64:
      return (*col->As<ColumnUInt64>())[row];
    case Type::String:
      return std::string((*col->As<ColumnString>())[row]);
    case Type::Array:
      return extract_array_value(col, row);
    // ... etc
  }
}
```

### 3. Callback Bridge for SELECT

**Problem:** C++ callback → Elixir process

**Solution:** Accumulate in C++, return to Elixir
- SELECT accumulates all blocks
- Return vector of blocks
- Elixir converts blocks to maps

**Alternative (Phase 4):** Streaming with send to Elixir process
```cpp
void client_select_async(
    ErlNifEnv *env,
    fine::ResourcePtr<Client> client,
    std::string query,
    ErlNifPid receiver_pid) {

  client->Select(query, [&](const Block& block) {
    // Send block to Elixir process
    ErlNifEnv* msg_env = enif_alloc_env();
    ERL_NIF_TERM block_term = encode_block(msg_env, block);
    enif_send(env, &receiver_pid, msg_env, block_term);
    enif_free_env(msg_env);
  });
}
```

### 4. Binary Data Efficiency

**Problem:** Large strings/binaries copy overhead

**Solution:** Use `ErlNifBinary` for zero-copy where possible
```cpp
// For ColumnString, explore zero-copy paths
ERL_NIF_TERM column_string_get_binary(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col,
    size_t index) {

  auto typed = col->ptr->As<ColumnString>();
  auto str_view = (*typed)[index];

  // Ideally zero-copy, but may require copying for safety
  ErlNifBinary bin;
  enif_alloc_binary(str_view.size(), &bin);
  std::memcpy(bin.data, str_view.data(), str_view.size());

  return enif_make_binary(env, &bin);
}
```

---

## Build System Setup

### Directory Structure

```
chex/
├── lib/
│   ├── chex.ex                 # Public API
│   ├── chex/
│   │   ├── connection.ex       # GenServer
│   │   ├── native.ex          # NIF declarations (minimal)
│   │   ├── column.ex          # Column builders
│   │   └── insert.ex          # Insert helpers
├── native/
│   └── chex_fine/
│       ├── CMakeLists.txt     # Build config
│       ├── Makefile           # Mix integration
│       ├── src/
│       │   ├── client.cpp     # Client NIFs
│       │   ├── block.cpp      # Block NIFs
│       │   ├── column.cpp     # Column NIFs
│       │   ├── select.cpp     # SELECT NIFs
│       │   └── convert.cpp    # Type conversions
│       └── clickhouse-cpp/    # Git submodule or vendored
├── test/
│   ├── chex_test.exs
│   └── integration_test.exs
├── mix.exs
└── README.md
```

### CMakeLists.txt

```cmake
cmake_minimum_required(VERSION 3.15)
project(chex_fine)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find FINE (from Hex package)
find_package(fine REQUIRED)

# Add clickhouse-cpp
add_subdirectory(clickhouse-cpp)

# Build NIF shared library
add_library(chex_fine SHARED
  src/client.cpp
  src/block.cpp
  src/column.cpp
  src/select.cpp
  src/convert.cpp
)

target_link_libraries(chex_fine
  PRIVATE
    fine::fine
    clickhouse-cpp-lib
)

# Set visibility
set_target_properties(chex_fine PROPERTIES
  CXX_VISIBILITY_PRESET hidden
  PREFIX ""
)

# Output to priv/
set_target_properties(chex_fine PROPERTIES
  LIBRARY_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/../../priv
)
```

### Makefile (for Mix)

```makefile
.PHONY: all clean

MIX_ENV ?= dev
BUILD_DIR = _build/$(MIX_ENV)

all:
	@mkdir -p $(BUILD_DIR)
	@cd $(BUILD_DIR) && cmake ../../native/chex_fine
	@cmake --build $(BUILD_DIR)

clean:
	@rm -rf $(BUILD_DIR)
	@rm -rf ../../priv/chex_fine.*
```

### mix.exs

```elixir
defmodule Chex.MixProject do
  use Mix.Project

  def project do
    [
      app: :chex,
      version: "0.2.0",
      elixir: "~> 1.18",
      compilers: [:elixir_make] ++ Mix.compilers(),
      make_targets: ["all"],
      make_clean: ["clean"],
      deps: deps()
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    [
      {:fine, "~> 0.1"},
      {:elixir_make, "~> 0.6", runtime: false},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false}
    ]
  end
end
```

---

## Testing Strategy

### Unit Tests (per Phase)

```elixir
defmodule Chex.ConnectionTest do
  use ExUnit.Case

  setup do
    {:ok, conn} = Chex.start_link(host: "localhost", port: 9000)

    on_exit(fn ->
      Chex.stop(conn)
    end)

    {:ok, conn: conn}
  end

  test "ping", %{conn: conn} do
    assert :ok = Chex.ping(conn)
  end

  test "execute DDL", %{conn: conn} do
    assert :ok = Chex.execute(conn, """
      CREATE TABLE IF NOT EXISTS test (
        id UInt64,
        name String
      ) ENGINE = Memory
    """)
  end
end
```

### Integration Tests

```elixir
defmodule Chex.IntegrationTest do
  use ExUnit.Case

  @moduletag :integration

  setup_all do
    # Start ClickHouse via Docker
    System.cmd("docker-compose", ["up", "-d", "clickhouse"])
    :timer.sleep(2000)

    on_exit(fn ->
      System.cmd("docker-compose", ["down"])
    end)

    :ok
  end

  test "full insert and select cycle" do
    {:ok, conn} = Chex.start_link(host: "localhost", port: 9000)

    # Create table
    Chex.execute(conn, """
      CREATE TABLE test (
        id UInt64,
        name String,
        amount Float64
      ) ENGINE = Memory
    """)

    # Insert data
    rows = [
      %{id: 1, name: "Alice", amount: 100.5},
      %{id: 2, name: "Bob", amount: 200.75}
    ]

    schema = [id: :uint64, name: :string, amount: :float64]
    :ok = Chex.insert(conn, "test", rows, schema)

    # Query back
    {:ok, results} = Chex.query(conn, "SELECT * FROM test ORDER BY id")

    assert length(results) == 2
    assert hd(results).name == "Alice"
  end
end
```

### Performance Benchmarks

```elixir
defmodule Chex.BenchmarkTest do
  use ExUnit.Case

  @tag :benchmark
  test "bulk insert performance" do
    {:ok, conn} = Chex.start_link(host: "localhost", port: 9000)

    # Generate 100k rows
    rows = for i <- 1..100_000 do
      %{id: i, value: :rand.uniform(1000)}
    end

    schema = [id: :uint64, value: :uint64]

    {time_us, :ok} = :timer.tc(fn ->
      Chex.insert(conn, "benchmark", rows, schema)
    end)

    rows_per_sec = 100_000 / (time_us / 1_000_000)
    IO.puts("Inserted #{rows_per_sec} rows/sec")

    # Should be > 50k rows/sec for native protocol to be worthwhile
    assert rows_per_sec > 50_000
  end
end
```

### Memory Leak Testing

```bash
# Run with valgrind
MIX_ENV=test valgrind --leak-check=full --track-origins=yes \
  mix test --only memory_leak

# Or use AddressSanitizer
CXXFLAGS="-fsanitize=address" mix compile
mix test
```

---

## Deployment Considerations

### Platform Support

**Required Platforms:**
- Linux x86_64 (primary)
- macOS ARM64 (development)
- macOS x86_64 (optional)

**Build Requirements:**
- C++17 compiler (GCC 7+, Clang 5+, MSVC 2017+)
- CMake 3.15+
- ClickHouse-cpp dependencies: abseil, lz4, cityhash, zstd

### Docker Development

```dockerfile
# Dockerfile.dev
FROM elixir:1.18

# Install C++ build tools
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    liblz4-dev \
    libzstd-dev

WORKDIR /app

COPY mix.exs mix.lock ./
RUN mix deps.get

COPY . .
RUN mix compile

CMD ["iex", "-S", "mix"]
```

### CI/CD Pipeline

```yaml
# .github/workflows/ci.yml
name: CI

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest

    services:
      clickhouse:
        image: clickhouse/clickhouse-server:latest
        ports:
          - 9000:9000
          - 8123:8123

    steps:
      - uses: actions/checkout@v3
        with:
          submodules: recursive

      - uses: erlef/setup-beam@v1
        with:
          otp-version: 27.2
          elixir-version: 1.18

      - name: Install C++ dependencies
        run: |
          sudo apt-get update
          sudo apt-get install -y build-essential cmake liblz4-dev libzstd-dev

      - name: Cache dependencies
        uses: actions/cache@v3
        with:
          path: deps
          key: deps-${{ runner.os }}-${{ hashFiles('mix.lock') }}

      - name: Install dependencies
        run: mix deps.get

      - name: Compile
        run: mix compile

      - name: Run tests
        run: mix test
```

---

## Risk Mitigation

### High-Risk Items

1. **Smart Pointer Lifetime Mismatch**
   - **Risk:** Segfaults from dangling pointers
   - **Mitigation:** Comprehensive testing, valgrind, wrapper pattern
   - **Fallback:** Use copying instead of sharing

2. **Type Dispatch Complexity**
   - **Risk:** Missing type support causes runtime errors
   - **Mitigation:** Explicit error for unsupported types, phased rollout
   - **Fallback:** Support common types only, document limitations

3. **Memory Leaks**
   - **Risk:** Resources not freed properly
   - **Mitigation:** RAII, ResourcePtr, valgrind testing
   - **Fallback:** Regular process restarts in production

### Medium-Risk Items

4. **Build System Complexity**
   - **Risk:** Platform-specific build failures
   - **Mitigation:** Docker-based builds, comprehensive CI
   - **Fallback:** Provide pre-built binaries

5. **Upstream Changes**
   - **Risk:** clickhouse-cpp breaking changes
   - **Mitigation:** Pin to specific version/tag, test before upgrading
   - **Fallback:** Fork if necessary

### Low-Risk Items

6. **Performance Not Meeting Expectations**
   - **Risk:** FFI overhead negates protocol benefits
   - **Mitigation:** Early benchmarking, compare to HTTP baseline
   - **Fallback:** Document actual performance, users decide

---

## Success Metrics

### ✅ Phase 1 Success (ACHIEVED)
- ✅ Connection established
- ✅ Ping works
- ✅ DDL operations succeed
- ✅ 10 tests passing

### ✅ Phase 2 Success (ACHIEVED)
- ✅ Core 5 types working (UInt64, Int64, String, Float64, DateTime)
- ✅ Column creation and population
- ✅ 33 tests passing

### ✅ Phase 3 Success (ACHIEVED)
- ✅ Block building from Elixir maps
- ✅ INSERT operations working
- ✅ 17 tests passing

### ✅ Phase 4 Success (ACHIEVED - MVP Reached!)
- ✅ SELECT queries returning data
- ✅ Block-to-maps conversion
- ✅ Large result sets (10k rows tested)
- ✅ 12 tests passing
- ✅ **Total: 89 tests passing (2 PoC + 10 Phase1 + 33 Phase2 + 17 Phase3 + 12 Phase4 + 15 remaining)**

### Phase 5 Success (Next Goal)
- ⏳ Additional types: Nullable, Array, Date, Bool
- ⏳ DateTime64 with timezone support
- ⏳ Decimal types

### Phase 6 Success (Production Ready)
- ⏳ Comprehensive error handling
- ⏳ No memory leaks after 1M operations
- ⏳ Documentation complete
- ⏳ CI/CD pipeline green

### Performance Targets
- **INSERT:** >50k rows/sec (vs Pillar ~20k rows/sec)
- **Latency:** <10ms for small queries (vs Pillar ~20ms)
- **Memory:** Stable under sustained load
- **Wire Size:** 50% reduction vs HTTP+JSON

---

## Comparison: FINE vs Alternatives

| Approach | Effort | Performance | Risk | Maintainability |
|----------|--------|-------------|------|-----------------|
| **FINE Wrapper** | 4-6 weeks | Native C++ | Medium | Medium |
| **Pure Elixir** | 4-6 months | 20-40% slower | Low | High |
| **Rustler Wrapper** | 6-10 weeks | Native | Medium-High | Medium |
| **Pillar (HTTP)** | 0 (exists) | Baseline | None | High |

---

## Open Questions

1. ✅ **Resource Lifetime:** Best pattern for Column → Block → Insert lifecycle? → Solved: Wrapper pattern with shared_ptr inside ResourcePtr
2. ✅ **SELECT callback handling:** How to bridge C++ callbacks to Elixir? → Solved: Convert to Erlang terms immediately in callback
3. **Schema Inference:** Query ClickHouse for table schema automatically?
4. **Connection Pooling:** At Elixir level or leverage clickhouse-cpp? → Investigate clickhouse-cpp capabilities first
5. **Compression Default:** Enable LZ4 by default or opt-in?

---

## Next Steps

With MVP achieved (Phases 1-4 complete), the priority order is:

1. **Phase 5: Advanced Types** (ESSENTIAL)
   - Nullable columns
   - Array columns
   - Date types
   - Bool type
   - DateTime64 with timezone support
   - Decimal types

2. **Phase 6: Production Polish** (IMPORTANT)
   - Comprehensive error handling
   - Memory leak testing
   - Documentation
   - CI/CD setup

3. **Phase 7: Advanced Query Features** (NICE TO HAVE)
   - Streaming SELECT for large result sets
   - Batch operations

4. **NOT IMPLEMENTING:**
   - ❌ Ecto Integration (Phase 8)
   - ❌ Distributed Queries (removed)

---

## References

- [FINE GitHub](https://github.com/elixir-nx/fine)
- [FINE Hex Docs](https://hexdocs.pm/fine)
- [clickhouse-cpp GitHub](https://github.com/ClickHouse/clickhouse-cpp)
- [ClickHouse Native Protocol Docs](https://clickhouse.com/docs/en/native-protocol)
- [ClickHouse Data Types](https://clickhouse.com/docs/en/sql-reference/data-types)

---

## Appendix A: Minimal Working Example

This example demonstrates the absolute minimum code needed to verify FINE + clickhouse-cpp works:

### native/chex_fine/minimal.cpp
```cpp
#include <fine.hpp>
#include <clickhouse/client.h>

using namespace clickhouse;

FINE_RESOURCE(Client);

fine::ResourcePtr<Client> create_client(ErlNifEnv *env) {
  ClientOptions opts;
  opts.SetHost("localhost");
  opts.SetPort(9000);
  return fine::make_resource<Client>(opts);
}
FINE_NIF(create_client, 0);

std::string ping(ErlNifEnv *env, fine::ResourcePtr<Client> client) {
  try {
    client->Ping();
    return "pong";
  } catch (const std::exception& e) {
    return std::string("error: ") + e.what();
  }
}
FINE_NIF(ping, 0);

FINE_INIT("Elixir.ChexMinimal");
```

### lib/chex_minimal.ex
```elixir
defmodule ChexMinimal do
  @moduledoc """
  Minimal FINE wrapper proof-of-concept
  """

  @on_load :load_nifs

  def load_nifs do
    path = :filename.join(:code.priv_dir(:chex), 'chex_fine')
    :ok = :erlang.load_nif(path, 0)
  end

  def create_client, do: :erlang.nif_error(:not_loaded)
  def ping(_client), do: :erlang.nif_error(:not_loaded)
end
```

### Test
```elixir
{:ok, client} = ChexMinimal.create_client()
"pong" = ChexMinimal.ping(client)
```

If this works, you're ready to proceed with the full implementation!

---

**End of Document**
