# Natch

⚡ **High-performance native ClickHouse client for Elixir**

Natch provides fast access to ClickHouse using the native TCP protocol (port 9000) via C++ NIFs. Native protocol benefits include binary columnar format, efficient compression, and reduced overhead compared to HTTP-based clients.

## Why Natch?

- **Native Protocol Performance** - Direct TCP connection using ClickHouse's binary protocol
- **Columnar-First Design** - API designed for analytics workloads, not OLTP
- **Type Complete** - Full support for all ClickHouse types: primitives, dates, decimals, UUIDs, arrays, maps, tuples, nullables, enums, and low cardinality
- **SQL Injection Protection** - Type-safe parameterized queries with automatic type inference
- **Zero-Copy Efficiency** - Bulk operations with minimal overhead
- **Memory Safe** - Built with FINE for crash-proof NIFs

## Requirements

- **Elixir**: 1.18+ / Erlang 27+
- **ClickHouse**: Server 20.3+
- **Build**: C++17 compiler, CMake 3.15+, clickhouse-cpp dependencies

## Installation

Add `natch` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:natch, "~> 0.3.0"}
  ]
end
```

**Prebuilt binaries** are available for macOS (x86_64, ARM64) and Linux (x86_64, ARM64) and will be downloaded automatically during installation.

### Building from Source

If prebuilt binaries are not available for your platform, or if you prefer to build from source:

```bash
# Clone the repository
git clone https://github.com/Intellection/natch.git
cd natch

# Initialize the clickhouse-cpp submodule
git submodule update --init --recursive

# Build
mix deps.get
mix compile
```

**Build Requirements:**
- C++17 compiler (GCC 7+, Clang 5+, or MSVC 2017+)
- CMake 3.15+
- OpenSSL development headers
- Git (for submodule)

## Quick Start

### Local ClickHouse

```elixir
# Start a connection
{:ok, conn} = Natch.start_link(
  host: "localhost",
  port: 9000,
  database: "default"
)

# Create a table
Natch.execute(conn, """
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

:ok = Natch.insert_cols(conn, "events", columns, schema)

# Query data
{:ok, results} = Natch.select_rows(conn, "SELECT * FROM events WHERE user_id = 100")
IO.inspect(results)
# => [
#      %{id: 1, user_id: 100, event_type: "click", ...},
#      %{id: 3, user_id: 100, event_type: "click", ...}
#    ]
```

### ClickHouse Cloud (SSL)

ClickHouse Cloud requires SSL/TLS connections on port 9440:

```elixir
{:ok, conn} = Natch.start_link(
  host: "your-instance.clickhouse.cloud",
  port: 9440,
  database: "default",
  user: "default",
  password: "your-password",
  ssl: true  # Enable SSL/TLS
)
```

**Note:** SSL support requires clickhouse-cpp to be built with OpenSSL. If you get a `Natch.OpenSSLError` saying "Library was built with no SSL support", the C++ library needs to be rebuilt with `-DWITH_OPENSSL=ON` CMake flag. This is typically handled automatically by package managers on systems with OpenSSL development libraries installed.

### Timeout Configuration

Configure socket-level timeouts to prevent operations from hanging indefinitely in production:

```elixir
{:ok, conn} = Natch.start_link(
  host: "localhost",
  port: 9000,
  connect_timeout: 5_000,   # Time to establish TCP connection (default: 5000ms)
  recv_timeout: 60_000,     # Time to receive data from server (default: 0 = infinite)
  send_timeout: 60_000      # Time to send data to server (default: 0 = infinite)
)
```

**Important:** The default `recv_timeout` is 0 (no timeout), which allows long-running analytical queries to complete. For production use, consider setting explicit timeouts based on your workload. When a timeout occurs, a `Natch.ConnectionError` is raised.

## Benchmarks

Real-world performance comparison vs Pillar (HTTP-based client) on Apple M3 Pro, tested with 7-column schema.

**Important:** Benchmarks use `Pillar.select/2` which parses JSON responses. Using `Pillar.query/2` (which returns unparsed TSV strings) is not a fair comparison.

### INSERT Performance

| Rows | Natch | Pillar | Speedup |
|------|-------|--------|---------|
| 10k | 12.9 ms | 62.3 ms | **4.8x faster** |
| 100k | 156 ms | 623 ms | **4.0x faster** |
| 1M | 1,338 ms | 5,111 ms | **3.8x faster** |

**Note:** Natch uses optimized columnar generation for best performance. See Performance Tips below.

### SELECT Performance

| Query Type | Natch | Pillar | Speedup |
|------------|-------|--------|---------|
| Aggregation | 3.5 ms | 4.8 ms | **1.4x faster** |
| Filtered (10k rows) | 11.2 ms | 52.8 ms | **4.7x faster** |
| Full scan (1M rows) | 796 ms | 4,908 ms | **6.2x faster** |

### Key Takeaways

- **Native protocol is faster** - Natch's native TCP protocol with binary columnar format outperforms HTTP+JSON
- **Scales better** - Performance advantage increases with data size (6.2x for 1M rows vs 1.4x for aggregations)
- **Low overhead** - Minimal BEAM memory usage due to efficient NIF boundary crossing
- **Optimized for analytics** - Memory-locality optimizations provide 36% faster INSERT performance over naive implementations

See `bench/README.md` and `BINARY_PASSTHROUGH.md` for detailed analysis and methodology.

## Core Concepts

### Columnar Format (Recommended for INSERT)

Natch provides a **symmetrical API** for both SELECT and INSERT operations, with columnar format recommended for maximum INSERT performance:

```elixir
# ✅ RECOMMENDED: Columnar format - M NIF calls (one per column) regardless of row count
columns = %{
  id: [1, 2, 3, 4, 5],
  name: ["Alice", "Bob", "Charlie", "Dave", "Eve"],
  value: [100.0, 200.0, 300.0, 400.0, 500.0]
}

Natch.insert_cols(conn, "table", columns, schema)

# Row format also available for convenience (converts to columnar internally)
rows = [
  %{id: 1, name: "Alice", value: 100.0},
  %{id: 2, name: "Bob", value: 200.0}
]

Natch.insert_rows(conn, "table", rows, schema)
```

Why columnar for INSERT?
- **10-100x faster** - M NIF calls instead of N×M (rows × columns)
- **Natural fit** - Matches ClickHouse's native storage format
- **Analytics-first** - Matches how you work with data (SUM, AVG, GROUP BY operate on columns)
- **Better compression** - Column values compressed together
- **Lower overhead** - No conversion needed (unlike `insert_rows`)

### Type System

Natch supports **all ClickHouse types** with full roundtrip fidelity:

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
{:ok, conn} = Natch.start_link(
  host: "localhost",
  port: 9000
)

# With authentication and options
{:ok, conn} = Natch.start_link(
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
:ok = Natch.execute(conn, """
CREATE TABLE users (
  id UInt64,
  name String,
  created DateTime
) ENGINE = MergeTree()
ORDER BY id
""")

# Drop table
:ok = Natch.execute(conn, "DROP TABLE users")

# Alter table
:ok = Natch.execute(conn, "ALTER TABLE users ADD COLUMN age UInt8")
```

#### SELECT Queries

Natch provides two query formats to suit different use cases:

##### Row-Major Format (Traditional)
Returns results as a list of maps, where each map represents a row:

```elixir
# Simple query
{:ok, rows} = Natch.query(conn, "SELECT * FROM users")
# => {:ok, [%{id: 1, name: "Alice"}, %{id: 2, name: "Bob"}]}

# With WHERE clause
{:ok, rows} = Natch.query(conn, "SELECT * FROM users WHERE id > 100")

# Aggregations
{:ok, [result]} = Natch.query(conn, """
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
{:ok, cols} = Natch.select_cols(conn, "SELECT * FROM users")
# => {:ok, %{id: [1, 2, 3], name: ["Alice", "Bob", "Charlie"]}}

# Perfect for analytics workflows
{:ok, data} = Natch.select_cols(conn, "SELECT user_id, value FROM events")
# => {:ok, %{user_id: [1, 2, 1, 3], value: [10.5, 20.0, 15.5, 30.0]}}

# Easy integration with data processing libraries
%{user_id: user_ids, value: values} = data
total = Enum.sum(values)
```

### Parameterized Queries (SQL Injection Prevention)

Natch provides type-safe parameterized queries that prevent SQL injection by transmitting parameter values separately from the SQL text. Parameters cannot be interpreted as SQL commands, providing strong security guarantees.

#### Basic Usage (Recommended)

The simplest way to use parameterized queries is to pass parameters directly. Types are automatically inferred from your Elixir values:

```elixir
# With keyword list (most common) - types inferred automatically
{:ok, rows} = Natch.select_rows(
  conn,
  "SELECT * FROM users WHERE id = {id}",
  id: 42  # Automatically inferred as UInt64
)
# => {:ok, [%{id: 42, name: "Alice", ...}]}

# Multiple parameters - all types inferred
{:ok, rows} = Natch.select_rows(
  conn,
  "SELECT * FROM events WHERE user_id = {uid} AND status = {status}",
  uid: 123,       # UInt64
  status: "active"  # String
)

# With map
params = %{id: 42, status: "active"}
{:ok, rows} = Natch.select_rows(
  conn,
  "SELECT * FROM users WHERE id = {id} AND status = {status}",
  params
)

# Explicit types when needed (e.g., Int32 instead of Int64)
{:ok, rows} = Natch.select_rows(
  conn,
  "SELECT * FROM metrics WHERE count > {threshold:Int32}",
  threshold: 1000
)
```

**Works with all query functions:**
- `Natch.select_rows/3` - Query with row-major results
- `Natch.select_cols/3` - Query with columnar results
- `Natch.execute/3` - Execute INSERT/UPDATE/DELETE with parameters

#### Type Inference

For `select_rows` and `select_cols`, types are **automatically inferred** from parameter values - no type annotations needed!

```elixir
{:ok, rows} = Natch.select_rows(
  conn,
  """
  SELECT * FROM events
  WHERE user_id = {uid}
    AND created_at > {start}
    AND status = {status}
  """,
  uid: 123,                          # Inferred as UInt64
  start: ~U[2024-01-01 00:00:00Z],  # Inferred as DateTime
  status: "active"                  # Inferred as String
)
```

**Type inference rules:**
- Non-negative integers → `UInt64`
- Negative integers → `Int64`
- Floats → `Float64`
- Strings → `String`
- `DateTime` structs → `DateTime`
- `Date` structs → `Date`

**Note:** For `execute` operations (INSERT/UPDATE/DELETE), explicit types are still required for safety.

**When you need explicit types:**
```elixir
# Force Int32 instead of Int64
{:ok, rows} = Natch.select_rows(conn, "WHERE count = {n:Int32}", n: 100)

# NULL requires explicit type (can't infer from nil)
{:ok, rows} = Natch.select_rows(conn, "WHERE notes = {notes:Nullable(String)}", notes: nil)
```

#### Builder Pattern (Complex Cases)

For complex scenarios requiring explicit type control or dynamic parameter binding, use the Query builder:

```elixir
# Create a query with {name:Type} placeholders
query = Natch.Query.new("SELECT * FROM users WHERE id = {id:UInt64}")
|> Natch.Query.bind(:id, 42)

{:ok, rows} = Natch.select_rows(conn, query)

# Explicit type specification with bind/4
query = Natch.Query.new("SELECT * FROM metrics WHERE count > {min:Int32}")
|> Natch.Query.bind(:min, 1000, :int32)  # Force Int32 instead of Int64

query = Natch.Query.new("SELECT * FROM data WHERE ratio > {threshold:Float32}")
|> Natch.Query.bind(:threshold, 0.5, :float32)  # Single precision float

{:ok, rows} = Natch.select_rows(conn, query)
```

Supported explicit types: `:uint64`, `:uint32`, `:int64`, `:int32`, `:float64`, `:float32`, `:string`, `:datetime`, `:datetime64`, `:date`

#### Examples by Use Case

**SELECT with automatic type inference:**
```elixir
# Clean - no type annotations needed!
{:ok, rows} = Natch.select_rows(
  conn,
  "SELECT * FROM users WHERE id > {min_id} AND status = {status}",
  min_id: 100,
  status: "active"
)
```

**INSERT with explicit types (required for execute):**
```elixir
:ok = Natch.execute(
  conn,
  "INSERT INTO events VALUES ({id:UInt64}, {user_id:UInt32}, {event:String}, {ts:DateTime})",
  id: 1,
  user_id: 100,
  event: "login",
  ts: DateTime.utc_now()
)
```

**UPDATE with explicit types:**
```elixir
:ok = Natch.execute(
  conn,
  "UPDATE users SET status = {status:String} WHERE id = {id:UInt64}",
  status: "inactive",
  id: 42
)
```

**DELETE with explicit types:**
```elixir
:ok = Natch.execute(
  conn,
  "DELETE FROM users WHERE created_at < {cutoff:DateTime}",
  cutoff: ~U[2020-01-01 00:00:00Z]
)
```

**NULL support (requires explicit type):**
```elixir
# nil values work with Nullable(T) columns
:ok = Natch.execute(
  conn,
  "INSERT INTO users VALUES ({id:UInt64}, {name:String}, {notes:Nullable(String)})",
  id: 100,
  name: "Alice",
  notes: nil  # NULL value
)
```

**Columnar output with type inference:**
```elixir
{:ok, cols} = Natch.select_cols(
  conn,
  "SELECT user_id, revenue FROM events WHERE created_at >= {start}",
  start: ~U[2024-01-01 00:00:00Z]
)
# => {:ok, %{user_id: [1, 2, 3], revenue: [100.0, 200.0, 150.0]}}
```

#### SQL Injection Prevention

Parameterized queries are **immune to SQL injection** because parameter values are transmitted separately from SQL text:

```elixir
# ❌ DANGEROUS: String interpolation (vulnerable to SQL injection!)
malicious_input = "'; DROP TABLE users; --"
sql = "SELECT * FROM users WHERE name = '#{malicious_input}'"
Natch.select_rows(conn, sql)  # Would execute DROP TABLE!

# ✅ SAFE: Parameterized query (injection attempts are treated as literal values)
# Type automatically inferred as String
{:ok, rows} = Natch.select_rows(
  conn,
  "SELECT * FROM users WHERE name = {name}",
  name: "'; DROP TABLE users; --"
)
# Returns no results - the malicious string is safely treated as a name to search for
```

#### When to Use Parameterized Queries

**Use parameterized queries when:**
- Query contains user input (form data, API parameters, etc.)
- Building dynamic WHERE clauses
- Inserting user-generated content
- Security is a concern

**String queries are fine for:**
- Static SQL with no variables
- Queries constructed entirely from trusted constants
- DDL operations (CREATE TABLE, etc.)
- Internal/administrative queries

### Inserting Data

Natch provides symmetrical insert APIs matching the SELECT operations:

#### Columnar Format (Recommended for Performance)
```elixir
# insert_cols - optimal performance, matches ClickHouse native format
columns = %{
  id: [1, 2, 3],
  name: ["Alice", "Bob", "Charlie"]
}

schema = [id: :uint64, name: :string]

:ok = Natch.insert_cols(conn, "users", columns, schema)
```

#### Row Format (Convenience)
```elixir
# insert_rows - convenient for small datasets, converts internally
rows = [
  %{id: 1, name: "Alice"},
  %{id: 2, name: "Bob"},
  %{id: 3, name: "Charlie"}
]

schema = [id: :uint64, name: :string]

:ok = Natch.insert_rows(conn, "users", rows, schema)
```

**Performance Note:** `insert_cols` is significantly faster for bulk operations (1000+ rows) as it avoids the O(N×M) conversion overhead. For maximum throughput, collect your data in columnar format from the start.

#### Low-Level API (Advanced)
```elixir
# Build block manually for maximum control
block = Natch.Native.block_create()

# Create and populate columns
id_col = Natch.Column.new(:uint64)
Natch.Column.append_bulk(id_col, [1, 2, 3])
Natch.Native.block_append_column(block, "id", id_col.ref)

name_col = Natch.Column.new(:string)
Natch.Column.append_bulk(name_col, ["Alice", "Bob", "Charlie"])
Natch.Native.block_append_column(block, "name", name_col.ref)

# Get client and insert
client_ref = GenServer.call(conn, :get_client)
Natch.Native.client_insert(client_ref, "users", block)
```

## Performance Tips

### 1. Use Columnar Format with Optimized Generation
```elixir
# ❌ SLOWER: Row format (O(N×M) conversion overhead)
rows = [
  %{id: 1, name: "Alice"},
  %{id: 2, name: "Bob"}
]
Natch.insert_rows(conn, "users", rows, schema)

# ✅ BEST: Columnar format with single-pass generation (optimal memory locality)
initial = %{id: [], name: []}

columns_reversed = Enum.reduce(records, initial, fn record, acc ->
  %{
    id: [record.id | acc.id],
    name: [record.name | acc.name]
  }
end)

columns = Map.new(columns_reversed, fn {key, vals} ->
  {key, :lists.reverse(vals)}
end)

Natch.insert_cols(conn, "users", columns, schema)
```

**Why this pattern?** Sequential prepends create adjacent cons cells in memory, and `:lists.reverse/1` produces contiguous allocations. This optimal memory locality provides 33% better performance than naive columnar generation and beats row-major conversion by 7% for large datasets (1M+ rows).

### 2. Batch Your Inserts
```elixir
# Insert in batches of 10,000-100,000 rows for optimal throughput
chunk_size = 50_000

data
|> Stream.chunk_every(chunk_size)
|> Enum.each(fn chunk ->
  columns = transpose_to_columnar(chunk)
  Natch.insert_cols(conn, "table", columns, schema)
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
{:ok, conn} = Natch.start_link(
  host: "localhost",
  port: 9000,
  compression: :lz4  # Enabled by default
)
```

## Complex Nesting Examples

Natch supports arbitrarily complex nested types:

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

Natch uses a three-layer architecture:

```
┌─────────────────────────────────────┐
│  Elixir Application Layer           │
│  - Natch.insert/4                    │
│  - Natch.Connection GenServer         │
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

- All primitive types (integers, floats, strings, bools)
- All temporal types (Date, DateTime, DateTime64)
- All special types (UUID, Decimal64, Enum8/16, LowCardinality)
- All complex types (Array, Map, Tuple, Nullable)
- Parameterized queries with SQL injection prevention
- Comprehensive nesting integration tests
- Full INSERT→SELECT roundtrip validation

## Roadmap

### Completed (Phase 1-6C)
- Native TCP protocol support
- All ClickHouse primitive types
- All temporal types (Date, DateTime, DateTime64)
- UUID and Decimal64 support
- Nullable types
- Array types with arbitrary nesting
- Map and Tuple types
- Enum8/Enum16 types
- LowCardinality types
- Complex type nesting (Array(Map(String, Nullable(T))), etc.)
- Columnar insert API
- LZ4 compression
- **Parameterized queries** with SQL injection prevention (Phase 6C)

### Planned (Phase 7+)
- Explorer DataFrame integration (zero-copy)
- SSL/TLS support (partial - available via clickhouse-cpp)
- Connection pooling
- Async query execution
- Query streaming for large result sets

### Not Planned
- Ecto integration (ClickHouse is OLAP, not OLTP - not a good fit)
- HTTP protocol support (use native TCP for better performance)

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
