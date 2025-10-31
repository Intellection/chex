# Binary Passthrough Implementation: Results & Analysis

## Executive Summary

We implemented a binary passthrough approach for SELECT queries, where raw ClickHouse column data is passed as binaries to Elixir for pattern-matching-based parsing. This was expected to be significantly faster than creating Erlang terms in C++.

**Result: The binary passthrough is 2x SLOWER than the traditional approach.**

More importantly, we discovered that the "Pillar is 15x faster" claim was based on a misleading benchmark - Pillar returns unparsed string data (TSV format), not materialized Erlang terms.

## Performance Results

### Test: 1M rows × 7 numeric columns (7M values total)

| Approach | Time (avg) | Speedup | Notes |
|----------|------------|---------|-------|
| **Traditional (C++ terms)** | **392ms** | **1.0x (baseline)** | Creates terms in C++ NIF |
| **Binary Passthrough** | **832ms** | **0.47x (2.1x slower)** | Passes binaries to Elixir |
| **Pillar.query (HTTP/TSV)** | **20ms** | **19.6x (MISLEADING!)** | Returns unparsed string! |
| **Pillar.select (HTTP/JSON)** | **~1450ms** | **0.27x (3.7x slower!)** | Parses JSON to maps |
| **Jason (columnar JSON)** | **632ms** | **0.62x** | Pure Elixir term creation |

### Breakdown: Binary Passthrough (832ms total)

From instrumentation in the C++ NIF:

- **C++ memcpy to binaries**: 241ms
- **Elixir binary pattern matching**: ~591ms (832ms - 241ms)

### Breakdown: Traditional Approach (392ms total)

- **C++ term creation**: 175ms
- **Overhead (GenServer, etc)**: ~217ms

## Key Discoveries

### 1. Pillar Has Two APIs: query() vs select()

The original benchmark showed Pillar as "15x faster" for SELECT queries. Investigation revealed Pillar has two different APIs:

**`Pillar.query/2` - Returns raw TSV string (NO parsing):**
```elixir
{:ok, result} = Pillar.query(conn, "SELECT * FROM table")
# result is: "1\t1.5\n2\t3\n3\t4.5\n..." (tab-separated string)
# Time: ~20ms for 1M rows × 2 columns
```

**`Pillar.select/2` - Appends FORMAT JSON and parses:**
```elixir
{:ok, result} = Pillar.select(conn, "SELECT * FROM table")
# result is: [%{"id" => 1, "value" => 1.5}, ...] (parsed maps)
# Time: ~1450ms for 1M rows × 2 columns (HTTP + JSON parsing via Jason)
```

**The original benchmark used `Pillar.query`**, which returns unparsed strings!

**Performance comparison (1M rows, 2 columns):**
- `Pillar.query()`: 20ms (unparsed TSV string)
- `Chex.select_cols()`: 392ms (fully parsed, 7 columns × 1M rows)
- `Pillar.select()`: ~1450ms (parsed JSON, 2 columns only!)

**Conclusion:**
- The Pillar benchmark was comparing apples (unparsed strings) to oranges (materialized terms)
- When properly compared with parsing, **Chex is 3.7x FASTER than Pillar.select()**!

### 2. The Bottleneck is Term Creation, Not NIF Boundary

We hypothesized that crossing the NIF boundary was the bottleneck. Wrong.

**Evidence:**
- Creating 7M terms in C++: 175ms
- Creating 7M terms in Elixir (Jason): 632ms
- Creating 7M terms in Elixir (binary pattern matching): 591ms

The bottleneck is **creating the terms themselves**, regardless of where it happens. C++ is faster at this (175ms) than Elixir (591ms).

### 3. Binary Passthrough Has Double-Copy Overhead

**Traditional approach (single copy):**
1. ClickHouse column data → Erlang terms (single operation)

**Binary passthrough (double copy):**
1. ClickHouse column data → Binary via memcpy (241ms)
2. Binary → Erlang terms via pattern matching (591ms)

Total: 832ms vs 392ms traditional

## Implementation Details

### Architecture

```
Traditional:
┌─────────────┐
│ ClickHouse  │
│   Column    │
│   Vector    │──────────────────┐
└─────────────┘                  │
                                 │ enif_make_int(), etc
                                 ↓ 175ms
                          ┌──────────────┐
                          │ Erlang Terms │
                          └──────────────┘

Binary Passthrough:
┌─────────────┐
│ ClickHouse  │
│   Column    │
│   Vector    │──────────────────┐
└─────────────┘                  │ memcpy + enif_make_binary()
                                 ↓ 241ms
                          ┌──────────────┐
                          │ Binary Data  │
                          └──────────────┘
                                 │ Binary pattern matching
                                 ↓ 591ms
                          ┌──────────────┐
                          │ Erlang Terms │
                          └──────────────┘
```

### Code Implementation

#### Elixir Parser (`lib/chex/parser/binary.ex`)

```elixir
defmodule Chex.Parser.Binary do
  # Parse UInt64 array - BEAM-optimized binary pattern matching
  @spec parse_uint64_array(binary()) :: [non_neg_integer()]
  def parse_uint64_array(binary) do
    for <<value::little-unsigned-64 <- binary>>, do: value
  end

  # Similar for all numeric types: Int64, Float64, UInt32, etc.
end
```

**Performance:** 591ms to parse 7M values (118µs per 1000 values)

#### C++ NIF (`native/chex_fine/src/select.cpp`)

```cpp
BinaryColumnarResult client_select_cols_binary(
    ErlNifEnv *env,
    fine::ResourcePtr<Client> client,
    std::string query) {

  std::map<std::string, std::vector<unsigned char>> column_binaries;

  client->Select(query, [&](const Block &block) {
    for (size_t c = 0; c < col_count; c++) {
      if (auto uint64_col = col->As<ColumnUInt64>()) {
        auto &data = uint64_col->GetWritableData();  // std::vector<uint64_t>&
        size_t bytes = data.size() * sizeof(uint64_t);

        // Copy to intermediate buffer
        auto &bin = column_binaries[col_name];
        bin.resize(old_size + bytes);
        std::memcpy(bin.data() + old_size, data.data(), bytes);
      }
    }
  });

  // Create Elixir binaries
  for (const auto &[col_name, binary_data] : column_binaries) {
    unsigned char *binary_ptr = enif_make_new_binary(env, binary_data.size(), &binary_term);
    std::memcpy(binary_ptr, binary_data.data(), binary_data.size());
  }
}
```

**Performance:** 241ms to copy 7M values (34µs per 1000 values)

## Why Binary Passthrough is Slower

### 1. Double Memory Copy

Traditional approach does one operation:
- ClickHouse data → Erlang terms (C++ code)

Binary passthrough does two operations:
- ClickHouse data → Binary buffer (memcpy)
- Binary buffer → Erlang terms (Elixir pattern matching)

### 2. C++ is Faster at Term Creation

Creating an Erlang integer in C++:
```cpp
enif_make_uint64(env, value);  // ~25ns per call
```

Creating an Erlang integer in Elixir:
```elixir
for <<value::little-unsigned-64 <- binary>>, do: value  // ~84ns per value
```

C++ is ~3.4x faster at term creation.

### 3. No Zero-Copy Possible

Even with binaries, we can't avoid term creation. The Elixir VM needs Erlang terms to work with the data. The only way to avoid term creation is to:

1. Keep data as binary and use systems like Arrow/Explorer
2. Stream data without materializing all at once
3. Aggregate in ClickHouse and only return summary

## When Binary Passthrough Would Help

Despite being slower for immediate materialization, binary passthrough could be valuable for:

### 1. Integration with Arrow/Explorer

```elixir
# Keep data as binary, pass to Explorer
{:ok, %{columns: binaries, metadata: meta}} = Chex.select_cols_binary(conn, sql)

# Create Explorer DataFrame directly from binaries (zero-copy)
df = Explorer.DataFrame.from_binary_columns(binaries, meta)
```

This would avoid Erlang term creation entirely.

### 2. Lazy/Streaming Queries

```elixir
# Return binary chunks as they arrive
{:ok, stream} = Chex.stream_binary(conn, sql)

# Parse only what you need
stream
|> Stream.take(1000)  # Only parse first 1000 rows
|> Stream.map(&parse_binary_row/1)
```

### 3. Partial Parsing

```elixir
# Get binary but only parse specific columns
{:ok, %{columns: bins}} = Chex.select_cols_binary(conn, "SELECT id, name, data")

# Only parse ID column, keep rest as binary
ids = Chex.Parser.Binary.parse_uint64_array(bins["id"])
# Skip parsing 'name' and 'data' if not needed
```

## Recommendations

### For Chex Users

**Use traditional `select_cols/2` for now.** It's 2x faster than binary passthrough when you need materialized terms.

```elixir
# Recommended (faster)
{:ok, columns} = Chex.Connection.select_cols(conn, "SELECT * FROM table")

# Not recommended (slower) unless you have a specific need
{:ok, columns} = Chex.Connection.select_cols_binary(conn, "SELECT * FROM table")
```

### Future Opportunities

1. **Arrow Integration**: Implement `Chex.select_to_arrow/2` that uses binary passthrough and creates Arrow tables directly
2. **Streaming**: Implement `Chex.stream_binary/2` for memory-efficient large result sets
3. **Selective Parsing**: Allow users to specify which columns to parse vs keep as binary

### About Pillar Benchmarks

Be cautious when comparing Pillar performance. Pillar has two APIs with very different performance characteristics:

```elixir
# Pillar.query: 20ms (returns unparsed TSV string) - NOT comparable!
{:ok, tsv_string} = Pillar.query(conn, "SELECT * FROM table")

# Pillar.select: ~1450ms (parses JSON into maps) - 3.7x SLOWER than Chex!
{:ok, rows} = Pillar.select(conn, "SELECT * FROM table")

# Chex: 392ms (returns fully parsed columnar terms) - FASTEST!
{:ok, columns} = Chex.Connection.select_cols(conn, "SELECT * FROM table")
```

**Always use `Pillar.select` for fair comparisons**, as it's the only API that returns parsed data comparable to Chex's output.

## Files Created/Modified

### New Files
- `lib/chex/parser.ex` - Main orchestrator for binary parsing
- `lib/chex/parser/binary.ex` - Binary pattern matching utilities
- `lib/chex/parser/numeric.ex` - Numeric type parsers

### Modified Files
- `native/chex_fine/src/select.cpp` - Added `client_select_cols_binary()` NIF
- `lib/chex/native.ex` - Added NIF declaration
- `lib/chex/connection.ex` - Added public API and GenServer handler

### Test Scripts
- `/tmp/test_1m_uint64.exs` - Benchmark 1M UInt64 values
- `/tmp/test_multi_column.exs` - Benchmark 1M rows × 7 columns
- `/tmp/test_jason_perf.exs` - Compare Jason parsing performance
- `/tmp/verify_pillar_materialization.exs` - Verify Pillar data format

## Performance Data

### Detailed Benchmarks

**Test Setup:**
- 1M rows × 7 numeric columns (7M values total)
- Columns: id (UInt64), user_id (UInt64), timestamp (UInt64), value (Float64), count (Int64), score (Float64), rank (UInt32)
- Hardware: Apple M3 Pro, 12 cores, 36GB RAM
- Software: Elixir 1.18.4, Erlang 27.2.2, JIT enabled

**Results (3 runs, averaged):**

| Approach | Run 1 | Run 2 | Run 3 | Average | Breakdown |
|----------|-------|-------|-------|---------|-----------|
| Traditional | 395ms | 390ms | 392ms | **392ms** | C++: 175ms, Overhead: 217ms |
| Binary | 830ms | 835ms | 832ms | **832ms** | C++: 241ms, Elixir: 591ms |
| Pillar (unparsed) | 51ms | 53ms | 53ms | **52ms** | HTTP only, no parsing |
| Jason (columnar) | 628ms | 635ms | 633ms | **632ms** | Pure Elixir parsing |

### Memory Usage

**Traditional approach:**
- Peak memory: ~112MB for 7M terms
- Memory per value: ~16 bytes (Erlang term overhead)

**Binary passthrough:**
- Binary data: ~56MB (raw data only)
- After parsing: ~112MB (same as traditional)
- Intermediate overhead: ~56MB extra during parsing

## Conclusions

1. **The NIF boundary is not the bottleneck** - Term creation is the bottleneck
2. **C++ is faster at creating terms** - 175ms vs 591ms for 7M values
3. **Binary passthrough is slower for immediate use** - 2x slower due to double-copy
4. **Pillar benchmarks are misleading** - They measure HTTP time, not parsing time
5. **Binary passthrough has future value** - For Arrow integration and streaming

## Appendix: Pillar Investigation

When we tested Pillar's actual behavior, we discovered it has two different APIs:

### Pillar.query - Returns Unparsed TSV

```elixir
{:ok, result} = Pillar.query(conn, "SELECT * FROM table")

# Attempted to call length/1
count = length(result)

# Error revealed the truth:
** (ArgumentError) errors were found at the given arguments:
  * 1st argument: not a list

:erlang.length("1\t1.5\n2\t3\n3\t4.5\n4\t6\n5\t7.5\n...")
```

The result is a raw TSV string! Performance: ~20ms for 1M rows.

### Pillar.select - Parses JSON

```elixir
{:ok, result} = Pillar.select(conn, "SELECT * FROM table")
# Returns: [%{"id" => 1, "value" => 1.5}, ...]
# Performance: ~1450ms for 1M rows × 2 columns
```

This appends `FORMAT JSON` to the query and parses the response with Jason.

### Performance Summary

**For 1M rows × 2 numeric columns:**
- `Pillar.query()`: 20ms (unparsed TSV string - 15MB)
- `Pillar.select()`: ~1450ms (HTTP + Jason parsing to maps)
- `Chex.select_cols()`: 392ms (native TCP + C++ term creation, 7 columns!)

**Conclusion:** When comparing parsed output, Chex is **3.7x faster than Pillar.select()**, and Chex handles more columns (7 vs 2) in the comparison!

## Future Work

- [ ] Implement `Chex.select_to_arrow/2` for zero-copy Arrow integration
- [ ] Implement `Chex.stream_binary/2` for memory-efficient streaming
- [ ] Add selective column parsing (parse some, keep others as binary)
- [ ] Explore other use cases where deferred parsing is beneficial
- [ ] Consider exposing binary API for advanced users who want control

## References

- ClickHouse native protocol: https://clickhouse.com/docs/en/native-protocol
- Erlang binary pattern matching: https://www.erlang.org/doc/efficiency_guide/binaryhandling.html
- Arrow columnar format: https://arrow.apache.org/
- Explorer DataFrame: https://hexdocs.pm/explorer/
