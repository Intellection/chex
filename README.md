# Chex

⚡ **High-performance native ClickHouse client for Elixir**

Chex provides fast access to ClickHouse using the native TCP protocol (port 9000) via C++ NIFs. Native protocol benefits include binary columnar format, efficient compression, and reduced overhead compared to HTTP-based clients.

## Why Chex?

- 🚀 **Native Protocol Performance** - Direct TCP connection using ClickHouse's binary protocol
- 📊 **Columnar-First Design** - API designed for analytics workloads, not OLTP
- 🔧 **Production Ready** - 227 tests covering all ClickHouse types including complex nested structures
- 💪 **Type Complete** - Full support for all ClickHouse types: primitives, dates, decimals, UUIDs, arrays, maps, tuples, nullables, enums, and low cardinality
- 🎯 **Zero-Copy Efficiency** - Bulk operations with minimal overhead
- 🔒 **Memory Safe** - Built with FINE for crash-proof NIFs

## Requirements

- **Elixir**: 1.18+ / Erlang 27+
- **ClickHouse**: Server 20.3+
- **Build**: C++17 compiler, CMake 3.15+, clickhouse-cpp dependencies

## Installation

Add `chex` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:chex, "~> 0.2.0"}
  ]
end
```

Build dependencies will be compiled automatically via `elixir_make`.

## Quick Start

```elixir
# Start a connection
{:ok, conn} = Chex.Connection.start_link(
  host: "localhost",
  port: 9000,
  database: "default"
)

# Create a table
Chex.Connection.execute(conn, """
CREATE TABLE events (
  id UInt64,
  user_id UInt32,
  event_type LowCardinality(String),
  properties Map(String, String),
  tags Array(String),
  timestamp DateTime,
  metadata Nullable(String)
) ENGINE = MergeTree()
ORDER BY (timestamp, user_id)
""")

# Insert data (columnar format - optimal performance!)
columns = %{
  id: [1, 2, 3],
  user_id: [100, 101, 100],
  event_type: ["click", "view", "click"],
  properties: [
    %{"page" => "home", "referrer" => "google"},
    %{"page" => "about"},
    %{"page" => "pricing"}
  ],
  tags: [["web", "desktop"], ["mobile"], ["web"]],
  timestamp: [~U[2024-01-01 10:00:00Z], ~U[2024-01-01 10:01:00Z], ~U[2024-01-01 10:02:00Z]],
  metadata: ["extra", nil, "data"]
}

schema = [
  id: :uint64,
  user_id: :uint32,
  event_type: {:low_cardinality, :string},
  properties: {:map, :string, :string},
  tags: {:array, :string},
  timestamp: :datetime,
  metadata: {:nullable, :string}
]

:ok = Chex.insert(conn, "events", columns, schema)

# Query data
{:ok, results} = Chex.Connection.select(conn, "SELECT * FROM events WHERE user_id = 100")
IO.inspect(results)
# => [
#      %{id: 1, user_id: 100, event_type: "click", ...},
#      %{id: 3, user_id: 100, event_type: "click", ...}
#    ]
```

## Benchmarks

Real-world performance comparison vs Pillar (HTTP-based client) on M3 Pro, tested with 7-column schema.

**Important:** Benchmarks use `Pillar.select/2` which parses JSON responses. Using `Pillar.query/2` (which returns unparsed TSV strings) is not a fair comparison.

### INSERT Performance

| Rows | Chex | Pillar | Speedup | Memory (Chex) | Memory (Pillar) |
|------|------|--------|---------|---------------|-----------------|
| 10k | 13.5 ms | 63.9 ms | **4.7x faster** | 976 B | 45 MB |
| 100k | 184 ms | 626 ms | **3.4x faster** | 976 B | 452 MB |
| 1M | 2,094 ms | 5,545 ms | **2.6x faster** | 976 B | 4.5 GB |

**Chex uses ~4.6 million times less memory** than Pillar for inserts due to columnar format.

### SELECT Performance

| Query Type | Chex | Pillar | Speedup | Memory (Chex) | Memory (Pillar) |
|------------|------|--------|---------|---------------|-----------------|
| Aggregation | 3.6 ms | 4.9 ms | **1.4x faster** | 544 B | 17 KB |
| Filtered (10k rows) | 12 ms | 53 ms | **4.4x faster** | 128 B | 30 MB |
| Full scan (1M rows) | 802 ms | 4,980 ms | **6.2x faster** | 128 B | 3 GB |

**Chex uses ~5.5 million times less memory** than Pillar for large SELECT queries due to streaming columnar format vs materialized row-oriented maps.

### Key Takeaways

- **Native protocol is faster** - Chex's native TCP protocol with binary columnar format outperforms HTTP+JSON
- **Massive memory efficiency** - Millions of times less memory usage due to streaming and columnar format
- **Scales better** - Performance advantage increases with data size (6.2x for 1M rows vs 1.4x for aggregations)

See `bench/README.md` and `BINARY_PASSTHROUGH.md` for detailed analysis and methodology.

## Core Concepts

### Columnar Format (Recommended)

Chex uses a **columnar-first API** that matches ClickHouse's native storage format:

```elixir
# ✅ GOOD: Columnar format - 3 NIF calls for any number of rows
columns = %{
  id: [1, 2, 3, 4, 5],
  name: ["Alice", "Bob", "Charlie", "Dave", "Eve"],
  value: [100.0, 200.0, 300.0, 400.0, 500.0]
}

Chex.insert(conn, "table", columns, schema)
```

Why columnar?
- **100x faster** - M NIF calls instead of N×M (rows × columns)
- **Natural fit** - ClickHouse is a columnar database
- **Analytics-first** - Matches how you work with data (SUM, AVG, GROUP BY operate on columns)
- **Better compression** - Column values compressed together

### Type System

Chex supports **all ClickHouse types** with full roundtrip fidelity:

#### Primitive Types
```elixir
schema = [
  id: :uint64,           # UInt8, UInt16, UInt32, UInt64
  count: :int32,         # Int8, Int16, Int32, Int64
  price: :float64,       # Float32, Float64
  name: :string,         # String
  active: :bool          # Bool (UInt8)
]
```

#### Date and Time
```elixir
schema = [
  created: :date,        # Date (days since epoch)
  updated: :datetime,    # DateTime (seconds since epoch)
  logged: :datetime64    # DateTime64(6) - microsecond precision
]

# Works with Elixir DateTime structs or integers
columns = %{
  created: [~D[2024-01-01], ~D[2024-01-02]],
  updated: [~U[2024-01-01 10:00:00Z], ~U[2024-01-01 11:00:00Z]],
  logged: [~U[2024-01-01 10:00:00.123456Z], 1704103200123456]
}
```

#### Decimals and UUIDs
```elixir
schema = [
  amount: :decimal64,    # Decimal64(9) - fixed-point decimals
  user_id: :uuid         # UUID - 128-bit identifiers
]

columns = %{
  amount: [Decimal.new("99.99"), Decimal.new("149.50")],
  user_id: ["550e8400-e29b-41d4-a716-446655440000", "6ba7b810-9dad-11d1-80b4-00c04fd430c8"]
}
```

#### Nullable Types
```elixir
schema = [
  description: {:nullable, :string},
  count: {:nullable, :uint64}
]

columns = %{
  description: ["text", nil, "more text"],
  count: [100, nil, 200]
}
```

#### Arrays
```elixir
schema = [
  tags: {:array, :string},
  matrix: {:array, {:array, :uint64}},           # Nested arrays
  nullable_list: {:array, {:nullable, :string}}  # Arrays with nulls
]

columns = %{
  tags: [["web", "mobile"], ["desktop"], []],
  matrix: [[[1, 2], [3, 4]], [[5, 6]]],
  nullable_list: [["a", nil, "b"], [nil, "c"]]
}
```

#### Maps and Tuples
```elixir
schema = [
  properties: {:map, :string, :uint64},
  location: {:tuple, [:string, :float64, :float64]},
  metrics: {:map, :string, {:nullable, :uint64}}  # Maps with nullable values
]

columns = %{
  properties: [%{"clicks" => 10, "views" => 100}, %{"shares" => 5}],
  location: [{"NYC", 40.7128, -74.0060}, {"LA", 34.0522, -118.2437}],
  metrics: [%{"count" => 100, "missing" => nil}, %{"total" => nil}]
}
```

#### Enums and LowCardinality
```elixir
schema = [
  status: {:enum8, [{"pending", 1}, {"active", 2}, {"archived", 3}]},
  category: {:low_cardinality, :string},
  tags: {:array, {:low_cardinality, {:nullable, :string}}}  # Complex nesting!
]

columns = %{
  status: ["pending", "active", "pending"],
  category: ["news", "sports", "news"],
  tags: [["tech", nil], ["sports"], ["tech", "startup"]]
}
```

## Usage Guide

### Connection Management

```elixir
# Basic connection
{:ok, conn} = Chex.Connection.start_link(
  host: "localhost",
  port: 9000
)

# With authentication and options
{:ok, conn} = Chex.Connection.start_link(
  host: "clickhouse.example.com",
  port: 9000,
  database: "analytics",
  user: "app_user",
  password: "secret",
  compression: :lz4,
  name: MyApp.ClickHouse
)
```

Connection options:
- `:host` - Server hostname (default: `"localhost"`)
- `:port` - Native TCP port (default: `9000`)
- `:database` - Database name (default: `"default"`)
- `:user` - Username (optional)
- `:password` - Password (optional)
- `:compression` - Compression: `:lz4`, `:none` (default: `:lz4`)
- `:name` - Register connection with a name (optional)

### Executing Queries

#### DDL Operations
```elixir
# Create table
:ok = Chex.Connection.execute(conn, """
CREATE TABLE users (
  id UInt64,
  name String,
  created DateTime
) ENGINE = MergeTree()
ORDER BY id
""")

# Drop table
:ok = Chex.Connection.execute(conn, "DROP TABLE users")

# Alter table
:ok = Chex.Connection.execute(conn, "ALTER TABLE users ADD COLUMN age UInt8")
```

#### SELECT Queries

Chex provides two query formats to suit different use cases:

##### Row-Major Format (Traditional)
Returns results as a list of maps, where each map represents a row:

```elixir
# Simple query
{:ok, rows} = Chex.Connection.select_rows(conn, "SELECT * FROM users")
# => {:ok, [%{id: 1, name: "Alice"}, %{id: 2, name: "Bob"}]}

# With WHERE clause
{:ok, rows} = Chex.Connection.select_rows(conn, "SELECT * FROM users WHERE id > 100")

# Aggregations
{:ok, [result]} = Chex.Connection.select_rows(conn, """
  SELECT
    event_type,
    count() as count,
    uniqExact(user_id) as unique_users
  FROM events
  GROUP BY event_type
  ORDER BY count DESC
""")
```

##### Columnar Format (Efficient for Analytics)
Returns results as a map of column lists, ideal for large result sets and data analysis:

```elixir
# Query returns columnar format
{:ok, cols} = Chex.Connection.select_cols(conn, "SELECT * FROM users")
# => {:ok, %{id: [1, 2, 3], name: ["Alice", "Bob", "Charlie"]}}

# Perfect for analytics workflows
{:ok, data} = Chex.Connection.select_cols(conn, "SELECT user_id, value FROM events")
# => {:ok, %{user_id: [1, 2, 1, 3], value: [10.5, 20.0, 15.5, 30.0]}}

# Easy integration with data processing libraries
%{user_id: user_ids, value: values} = data
total = Enum.sum(values)
```

**Note:** The deprecated `select/2` function still works and delegates to `select_rows/2` for backward compatibility.

### Inserting Data

#### High-Level API (Recommended)
```elixir
# Columnar format - optimal performance
columns = %{
  id: [1, 2, 3],
  name: ["Alice", "Bob", "Charlie"]
}

schema = [id: :uint64, name: :string]

:ok = Chex.insert(conn, "users", columns, schema)
```

#### Low-Level API (Advanced)
```elixir
# Build block manually for maximum control
block = Chex.Native.block_create()

# Create and populate columns
id_col = Chex.Column.new(:uint64)
Chex.Column.append_bulk(id_col, [1, 2, 3])
Chex.Native.block_append_column(block, "id", id_col.ref)

name_col = Chex.Column.new(:string)
Chex.Column.append_bulk(name_col, ["Alice", "Bob", "Charlie"])
Chex.Native.block_append_column(block, "name", name_col.ref)

# Get client and insert
client_ref = GenServer.call(conn, :get_client)
Chex.Native.client_insert(client_ref, "users", block)
```

## Performance Tips

### 1. Use Columnar Format
```elixir
# ❌ BAD: Row-oriented (requires conversion)
rows = [
  %{id: 1, name: "Alice"},
  %{id: 2, name: "Bob"}
]

# ✅ GOOD: Columnar (direct insertion)
columns = %{
  id: [1, 2],
  name: ["Alice", "Bob"]
}
```

### 2. Batch Your Inserts
```elixir
# Insert in batches of 10,000-100,000 rows for optimal throughput
chunk_size = 50_000

data
|> Stream.chunk_every(chunk_size)
|> Enum.each(fn chunk ->
  columns = transpose_to_columnar(chunk)
  Chex.insert(conn, "table", columns, schema)
end)
```

### 3. Use Appropriate Types
```elixir
# ✅ GOOD: LowCardinality for repeated strings
schema = [status: {:low_cardinality, :string}]

# ✅ GOOD: Enum for known values
schema = [priority: {:enum8, [{"low", 1}, {"medium", 2}, {"high", 3}]}]

# ✅ GOOD: Use smallest integer type that fits
schema = [age: :uint8]  # Not :uint64
```

### 4. Enable Compression
```elixir
# LZ4 compression reduces bandwidth by ~70% for typical workloads
{:ok, conn} = Chex.Connection.start_link(
  host: "localhost",
  port: 9000,
  compression: :lz4  # Enabled by default
)
```

## Complex Nesting Examples

Chex supports arbitrarily complex nested types:

```elixir
# Triple-nested arrays with nullables
schema = [matrix: {:array, {:array, {:nullable, :uint64}}}]
columns = %{matrix: [[[1, nil, 3], [nil, 5]], [[10, 20], [], [nil]]]}

# Maps with array values
schema = [data: {:map, :string, {:array, :uint64}}]
columns = %{data: [%{"ids" => [1, 2, 3], "counts" => [10, 20]}]}

# Tuples with complex elements
schema = [record: {:tuple, [:string, {:array, :uint64}, {:nullable, :float64}]}]
columns = %{record: [{"Alice", [1, 2, 3], 99.9}, {"Bob", [4, 5], nil}]}

# Array of low cardinality nullable strings (triple wrapper!)
schema = [tags: {:array, {:low_cardinality, {:nullable, :string}}}]
columns = %{tags: [["tech", nil, "startup"], [nil, "news"]]}
```

All these patterns work with full INSERT→SELECT roundtrip fidelity.

## Architecture

Chex uses a three-layer architecture:

```
┌─────────────────────────────────────┐
│  Elixir Application Layer           │
│  - Chex.insert/4                    │
│  - Chex.Connection GenServer         │
│  - Idiomatic Elixir API             │
└─────────────────────────────────────┘
              ↓
┌─────────────────────────────────────┐
│  FINE NIF Layer (C++)               │
│  - Type conversion Elixir ↔ C++     │
│  - Resource management              │
│  - Exception handling               │
└─────────────────────────────────────┘
              ↓
┌─────────────────────────────────────┐
│  clickhouse-cpp Library             │
│  - Native TCP protocol              │
│  - Binary columnar format           │
│  - LZ4/ZSTD compression             │
└─────────────────────────────────────┘
```

### Why FINE + clickhouse-cpp?

- **Native Protocol** - Binary columnar format with efficient compression
- **Mature Library** - Leverage official ClickHouse C++ client
- **Type Safety** - FINE provides crash-proof NIFs
- **Fast Development** - 4-6 weeks vs 4-6 months for pure Elixir

## Development

### Running ClickHouse Locally

```bash
# Start ClickHouse
docker-compose up -d

# Check it's running
clickhouse-client --query "SELECT version()"
```

### Running Tests

```bash
# Run all tests
mix test

# Run with tracing
mix test --trace

# Run specific test file
mix test test/nesting_integration_test.exs
```

### Test Coverage

- ✅ **227 tests passing** (as of Phase 5E)
- ✅ All primitive types (integers, floats, strings, bools)
- ✅ All temporal types (Date, DateTime, DateTime64)
- ✅ All special types (UUID, Decimal64, Enum8/16, LowCardinality)
- ✅ All complex types (Array, Map, Tuple, Nullable)
- ✅ 14 comprehensive nesting integration tests
- ✅ Full INSERT→SELECT roundtrip validation

## Roadmap

### Completed (Phase 1-5)
- ✅ Native TCP protocol support
- ✅ All ClickHouse primitive types
- ✅ All temporal types (Date, DateTime, DateTime64)
- ✅ UUID and Decimal64 support
- ✅ Nullable types
- ✅ Array types with arbitrary nesting
- ✅ Map and Tuple types
- ✅ Enum8/Enum16 types
- ✅ LowCardinality types
- ✅ Complex type nesting (Array(Map(String, Nullable(T))), etc.)
- ✅ Columnar insert API
- ✅ LZ4 compression

### Planned (Phase 6+)
- ⏳ Explorer DataFrame integration (zero-copy)
- ⏳ SSL/TLS support
- ⏳ Connection pooling
- ⏳ Async query execution
- ⏳ Prepared statements

### Not Planned
- ❌ Ecto integration (ClickHouse is OLAP, not OLTP - not a good fit)
- ❌ HTTP protocol support (use native TCP for better performance)

## Contributing

Contributions are welcome! Areas where we'd love help:

1. **Additional type support** - FixedString, IPv4/IPv6, Geo types
2. **Performance optimization** - Zero-copy paths, SIMD operations
3. **Documentation** - More examples, guides
4. **Testing** - Edge cases, stress tests

Please feel free to submit a Pull Request or open an issue.

## License

MIT License - See LICENSE file for details.

## Acknowledgments

- Built with [FINE](https://github.com/elixir-nx/fine) for crash-proof NIFs
- Powered by [clickhouse-cpp](https://github.com/ClickHouse/clickhouse-cpp) official C++ client
- Inspired by the excellent work of the ClickHouse and Elixir communities

## Resources

- [ClickHouse Documentation](https://clickhouse.com/docs)
- [ClickHouse Data Types](https://clickhouse.com/docs/en/sql-reference/data-types)
- [FINE Documentation](https://hexdocs.pm/fine)
- [Implementation Plan](FINE_PLAN.md) - Detailed architecture and design decisions
