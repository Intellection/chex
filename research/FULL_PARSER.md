# Full Elixir Native Protocol Parser

**Status:** Shelved for future consideration
**Last Updated:** 2025-10-31
**Feasibility:** ✅ Confirmed feasible via code exploration
**Effort:** 3.5-5 months development
**Performance:** Competitive for INSERT, slower than Pillar for SELECT

---

## Executive Summary

This document outlines a plan to implement a **pure Elixir parser** for ClickHouse's native TCP protocol, eliminating the C++ dependency (clickhouse-cpp) entirely. Based on comprehensive exploration of the clickhouse-cpp source code, this approach is **technically feasible** using Elixir's binary pattern matching capabilities.

### Key Findings

**Advantages:**
- No C++ build dependencies (CMake, C++17 compiler)
- Better debugging and introspection
- Easier deployment across platforms
- Pure Elixir ecosystem integration

**Performance:**
- INSERT 1M rows: ~3000ms (1.5x slower than current NIF: 2034ms)
- SELECT 1M rows: ~1500ms (2x faster than current: 849ms, but 23x slower than Pillar: 64ms)

**Trade-offs:**
- Large development effort (~4000 LOC, 3.5-5 months)
- Ongoing maintenance to track ClickHouse protocol changes
- Won't match Jason's performance for large SELECT results
- Good for INSERT-heavy workloads, questionable for SELECT-heavy

---

## Architecture Overview

### Current Architecture (Natch with clickhouse-cpp NIF)

```
Elixir App
   ↓
NIF Boundary
   ↓
clickhouse-cpp (C++)
   ↓
ClickHouse Native Protocol (TCP)
   ↓
ClickHouse Server
```

**Bottleneck:** Creating 7M individual Erlang terms via `enif_make_*` calls

### Proposed Architecture (Pure Elixir)

```
Elixir App
   ↓
Binary Pattern Matching (BEAM-optimized)
   ↓
ClickHouse Native Protocol (TCP)
   ↓
ClickHouse Server
```

**Advantages:**
- Single memory space (no NIF boundary crossing)
- BEAM-optimized binary operations
- Zero intermediate allocations

---

## ClickHouse Native Protocol Wire Format

### Protocol Overview

**Reference:** `/Users/brendon/work/clickhouse-cpp/clickhouse/protocol.h`

#### Server Packet Types
- `Hello` (0): Server handshake with name, version, revision
- `Data` (1): Block of data (may be compressed)
- `Exception` (2): Error response
- `Progress` (3): Query execution progress
- `Pong` (4): Ping response
- `EndOfStream` (5): Query completion marker
- `ProfileInfo` (6): Profiling data
- `Totals` (7): Aggregation totals
- `Extremes` (8): Min/max values
- `Log` (10): Query execution logs
- `ProfileEvents` (14): Performance metrics

#### Client Packet Types
- `Hello` (0): Client handshake
- `Query` (1): SQL query with settings
- `Data` (2): Block data for INSERT
- `Cancel` (3): Cancel running query
- `Ping` (4): Keep-alive ping

### Wire Format Primitives

**Reference:** `/Users/brendon/work/clickhouse-cpp/clickhouse/base/wire_format.h`

#### VarInt Encoding
- Up to 10 bytes
- 7 bits data per byte + 1 continuation bit (0x80)
- Little-endian ordering

**Example:**
```
Value: 300 (0b100101100)
Encoded: [0xAC, 0x02]
  Byte 1: 0b10101100 (0x2C | 0x80) - continuation bit set
  Byte 2: 0b00000010              - no continuation bit
```

**Elixir Implementation:**
```elixir
def read_varint(<<byte, rest::binary>>) when byte < 0x80 do
  {:ok, byte, rest}
end

def read_varint(<<byte, rest::binary>>) do
  read_varint_continue(byte &&& 0x7F, rest, 7)
end

defp read_varint_continue(acc, <<byte, rest::binary>>, shift) when byte < 0x80 do
  {:ok, acc ||| (byte <<< shift), rest}
end

defp read_varint_continue(acc, <<byte, rest::binary>>, shift) do
  read_varint_continue(acc ||| ((byte &&& 0x7F) <<< shift), rest, shift + 7)
end
```

#### Fixed Types
- Direct binary encoding (little-endian)
- No length prefixes

**Elixir Implementation:**
```elixir
def read_uint8(<<value::unsigned-8, rest::binary>>), do: {:ok, value, rest}
def read_uint16(<<value::little-unsigned-16, rest::binary>>), do: {:ok, value, rest}
def read_uint32(<<value::little-unsigned-32, rest::binary>>), do: {:ok, value, rest}
def read_uint64(<<value::little-unsigned-64, rest::binary>>), do: {:ok, value, rest}
def read_int8(<<value::signed-8, rest::binary>>), do: {:ok, value, rest}
def read_int32(<<value::little-signed-32, rest::binary>>), do: {:ok, value, rest}
def read_int64(<<value::little-signed-64, rest::binary>>), do: {:ok, value, rest}
def read_float32(<<value::little-float-32, rest::binary>>), do: {:ok, value, rest}
def read_float64(<<value::little-float-64, rest::binary>>), do: {:ok, value, rest}
```

#### Strings
- VarInt length prefix + raw bytes

```elixir
def read_string(binary) do
  with {:ok, len, rest} <- read_varint(binary),
       <<str::binary-size(len), rest2::binary>> <- rest do
    {:ok, str, rest2}
  end
end
```

---

## Block Structure

**Reference:** `/Users/brendon/work/clickhouse-cpp/clickhouse/client.cpp:651-722`

### Wire Format

```
Block = [BlockInfo?] +
        num_columns:VarInt +
        num_rows:VarInt +
        Column[num_columns]

Column = name:String +
         type:String +
         [custom_serialization_flag?] +
         ColumnData
```

### BlockInfo (optional, revision-dependent)

```elixir
defmodule BlockInfo do
  defstruct [
    :is_overflows,
    :bucket_num
  ]

  def read(binary, server_revision) do
    if server_revision >= @dbms_min_revision_with_block_info do
      with {:ok, 1, rest} <- read_varint(binary),  # overflow marker
           {:ok, is_overflows, rest} <- read_uint8(rest),
           {:ok, 2, rest} <- read_varint(rest),     # bucket marker
           {:ok, bucket_num, rest} <- read_int32(rest),
           {:ok, 0, rest} <- read_varint(rest) do   # extra marker (end)
        {:ok, %BlockInfo{is_overflows: is_overflows, bucket_num: bucket_num}, rest}
      end
    else
      {:ok, nil, binary}
    end
  end
end
```

### Block Parser

```elixir
defmodule Natch.Protocol.Block do
  defstruct [:columns, :rows, :block_info]

  def read(binary, server_revision) do
    with {:ok, block_info, rest} <- BlockInfo.read(binary, server_revision),
         {:ok, num_columns, rest} <- WireFormat.read_varint(rest),
         {:ok, num_rows, rest} <- WireFormat.read_varint(rest),
         {:ok, columns, rest} <- read_columns(rest, num_columns, num_rows, server_revision) do
      {:ok, %__MODULE__{
        columns: columns,
        rows: num_rows,
        block_info: block_info
      }, rest}
    end
  end

  defp read_columns(binary, num_columns, num_rows, server_revision) do
    read_columns_loop(binary, num_columns, num_rows, server_revision, [])
  end

  defp read_columns_loop(rest, 0, _num_rows, _revision, acc) do
    {:ok, Enum.reverse(acc), rest}
  end

  defp read_columns_loop(binary, remaining, num_rows, revision, acc) do
    with {:ok, column, rest} <- Column.read(binary, num_rows, revision) do
      read_columns_loop(rest, remaining - 1, num_rows, revision, [column | acc])
    end
  end
end
```

---

## Column Type Implementations

### Primitive Numeric Types

**Reference:** `/Users/brendon/work/clickhouse-cpp/clickhouse/columns/numeric.cpp:74-78`

**Wire Format:** Packed array of values, no delimiters

```elixir
defmodule Natch.Protocol.Column.Numeric do
  def load_uint64(binary, row_count) do
    byte_count = row_count * 8
    case binary do
      <<data::binary-size(byte_count), rest::binary>> ->
        values = for <<v::little-unsigned-64 <- data>>, do: v
        {:ok, values, rest}
      _ ->
        {:error, :insufficient_data}
    end
  end

  def load_uint32(binary, row_count) do
    byte_count = row_count * 4
    case binary do
      <<data::binary-size(byte_count), rest::binary>> ->
        values = for <<v::little-unsigned-32 <- data>>, do: v
        {:ok, values, rest}
    end
  end

  def load_int64(binary, row_count) do
    byte_count = row_count * 8
    case binary do
      <<data::binary-size(byte_count), rest::binary>> ->
        values = for <<v::little-signed-64 <- data>>, do: v
        {:ok, values, rest}
    end
  end

  def load_float64(binary, row_count) do
    byte_count = row_count * 8
    case binary do
      <<data::binary-size(byte_count), rest::binary>> ->
        values = for <<v::little-float-64 <- data>>, do: v
        {:ok, values, rest}
    end
  end
end
```

**Performance:** ~300M values/sec on modern hardware

### String Type

**Reference:** `/Users/brendon/work/clickhouse-cpp/clickhouse/columns/string.cpp:256-290`

**Wire Format:** For each string: VarInt length + raw bytes

```elixir
defmodule Natch.Protocol.Column.String do
  def load(binary, row_count) do
    load_strings(binary, row_count, [])
  end

  defp load_strings(rest, 0, acc) do
    {:ok, Enum.reverse(acc), rest}
  end

  defp load_strings(binary, remaining, acc) do
    with {:ok, len, rest} <- WireFormat.read_varint(binary),
         <<str::binary-size(len), rest2::binary>> <- rest do
      load_strings(rest2, remaining - 1, [str | acc])
    else
      _ -> {:error, :invalid_string_data}
    end
  end
end
```

**Optimization:** Could use `:binary.copy/1` for reference semantics

### FixedString Type

**Reference:** `/Users/brendon/work/clickhouse-cpp/clickhouse/columns/string.cpp:27-69`

**Wire Format:** Concatenated fixed-size strings (zero-padded)

```elixir
defmodule Natch.Protocol.Column.FixedString do
  def load(binary, row_count, string_size) do
    byte_count = row_count * string_size
    case binary do
      <<data::binary-size(byte_count), rest::binary>> ->
        values = for i <- 0..(row_count - 1) do
          offset = i * string_size
          <<_skip::binary-size(offset), str::binary-size(string_size), _::binary>> = data
          # Trim trailing zeros
          String.trim_trailing(str, <<0>>)
        end
        {:ok, values, rest}
    end
  end
end
```

### Nullable Type

**Reference:** `/Users/brendon/work/clickhouse-cpp/clickhouse/columns/nullable.h:61-63`

**Wire Format:**
1. Null bitmap (UInt8 array: 1 = null, 0 = not null)
2. Nested column data

```elixir
defmodule Natch.Protocol.Column.Nullable do
  def load(binary, row_count, nested_type, server_revision) do
    # Read null bitmap (1 byte per row)
    with <<null_bitmap::binary-size(row_count), rest::binary>> <- binary,
         {:ok, nested_values, rest2} <- Column.load(rest, nested_type, row_count, server_revision) do

      # Combine null mask with data
      values = combine_nullable(null_bitmap, nested_values, row_count)
      {:ok, values, rest2}
    end
  end

  defp combine_nullable(null_bitmap, nested_values, row_count) do
    null_bits = for <<bit::8 <- null_bitmap>>, do: bit == 1

    Enum.zip(Enum.take(null_bits, row_count), nested_values)
    |> Enum.map(fn
      {true, _} -> nil
      {false, val} -> val
    end)
  end
end
```

### Array Type

**Reference:** `/Users/brendon/work/clickhouse-cpp/clickhouse/columns/array.cpp:102-117`

**Wire Format:**
1. Offset array (UInt64, cumulative offsets)
2. Nested column data with total element count

```elixir
defmodule Natch.Protocol.Column.Array do
  def load(binary, row_count, nested_type, server_revision) do
    # Read offsets (UInt64 array)
    with {:ok, offsets, rest} <- Numeric.load_uint64(binary, row_count),
         total_elements = List.last(offsets, 0),
         {:ok, nested_data, rest2} <- Column.load(rest, nested_type, total_elements, server_revision) do

      # Split nested data by offsets
      arrays = split_by_offsets(nested_data, offsets)
      {:ok, arrays, rest2}
    end
  end

  defp split_by_offsets(data, offsets) do
    {_, arrays} = Enum.reduce(offsets, {0, []}, fn offset, {prev_offset, acc} ->
      count = offset - prev_offset
      slice = Enum.slice(data, prev_offset, count)
      {offset, [slice | acc]}
    end)
    Enum.reverse(arrays)
  end
end
```

**Example:**
```
Offsets: [3, 7, 7, 12]
Data: [a, b, c, d, e, f, g, h, i, j, k, l]

Result:
  Row 0: [a, b, c]       (elements 0-2)
  Row 1: [d, e, f, g]    (elements 3-6)
  Row 2: []              (empty)
  Row 3: [h, i, j, k, l] (elements 7-11)
```

### Date/DateTime Types

**Reference:** `/Users/brendon/work/clickhouse-cpp/clickhouse/columns/date.h`

**Wire Format:** Wrappers around numeric types

```elixir
defmodule Natch.Protocol.Column.Date do
  # Date stored as UInt16 (days since Unix epoch)
  def load(binary, row_count) do
    with {:ok, days_values, rest} <- Numeric.load_uint16(binary, row_count) do
      dates = Enum.map(days_values, fn days ->
        Date.add(~D[1970-01-01], days)
      end)
      {:ok, dates, rest}
    end
  end
end

defmodule Natch.Protocol.Column.DateTime do
  # DateTime stored as UInt32 (seconds since Unix epoch)
  def load(binary, row_count) do
    with {:ok, timestamp_values, rest} <- Numeric.load_uint32(binary, row_count) do
      datetimes = Enum.map(timestamp_values, fn seconds ->
        DateTime.from_unix!(seconds)
      end)
      {:ok, datetimes, rest}
    end
  end
end

defmodule Natch.Protocol.Column.DateTime64 do
  # DateTime64 stored as Int64 (ticks with specified precision)
  def load(binary, row_count, precision) do
    with {:ok, tick_values, rest} <- Numeric.load_int64(binary, row_count) do
      divisor = :math.pow(10, precision) |> round()
      datetimes = Enum.map(tick_values, fn ticks ->
        seconds = div(ticks, divisor)
        microseconds = rem(ticks, divisor) * div(1_000_000, divisor)
        DateTime.from_unix!(seconds, :second)
        |> DateTime.add(microseconds, :microsecond)
      end)
      {:ok, datetimes, rest}
    end
  end
end
```

### LowCardinality Type (MOST COMPLEX)

**Reference:** `/Users/brendon/work/clickhouse-cpp/clickhouse/columns/lowcardinality.cpp:1-400`

**Wire Format:**
```
LowCardinalityColumn = key_version:VarInt +
                       key_serialization_version:VarInt +
                       index_type_and_flags:VarInt +
                       [additional_keys_count:VarInt]? +
                       dictionary_data +
                       index_data
```

**Flags (index_type_and_flags):**
- Bits 0-7: IndexType (UInt8=0, UInt16=1, UInt32=2, UInt64=3)
- Bit 8: NeedGlobalDictionaryBit
- Bit 9: HasAdditionalKeysBit
- Bit 10: NeedUpdateDictionary

```elixir
defmodule Natch.Protocol.Column.LowCardinality do
  # State management required
  # Dictionary persists across blocks for same query

  def load(binary, row_count, nested_type, state, server_revision) do
    with {:ok, key_version, rest} <- WireFormat.read_varint(binary),
         {:ok, key_serialization_version, rest} <- WireFormat.read_varint(rest),
         {:ok, index_flags, rest} <- WireFormat.read_varint(rest) do

      index_type = index_flags &&& 0xFF
      has_additional_keys = (index_flags &&& 0x200) != 0
      need_global_dict = (index_flags &&& 0x100) != 0
      need_update_dict = (index_flags &&& 0x400) != 0

      # Read dictionary if needed
      {dictionary, rest} =
        if need_update_dict do
          read_dictionary(rest, nested_type, key_serialization_version, server_revision)
        else
          # Reuse from state
          {state.dictionary, rest}
        end

      # Read additional keys if present
      {dictionary, rest} =
        if has_additional_keys do
          with {:ok, num_keys, rest} <- WireFormat.read_varint(rest),
               {:ok, additional_keys, rest} <- read_keys(rest, nested_type, num_keys, server_revision) do
            {Map.merge(dictionary, Map.new(Enum.with_index(additional_keys))), rest}
          end
        else
          {dictionary, rest}
        end

      # Read index column
      {:ok, indexes, rest} <- read_index_column(rest, index_type, row_count)

      # Map indexes to dictionary values
      values = Enum.map(indexes, fn idx -> Map.get(dictionary, idx) end)

      {:ok, values, rest, %{state | dictionary: dictionary}}
    end
  end

  defp read_dictionary(binary, nested_type, serialization_version, server_revision) do
    with {:ok, dict_size, rest} <- WireFormat.read_varint(binary),
         {:ok, dict_values, rest} <- Column.load(rest, nested_type, dict_size, server_revision) do
      dictionary = Map.new(Enum.with_index(dict_values))
      {dictionary, rest}
    end
  end

  defp read_index_column(binary, index_type, row_count) do
    case index_type do
      0 -> Numeric.load_uint8(binary, row_count)
      1 -> Numeric.load_uint16(binary, row_count)
      2 -> Numeric.load_uint32(binary, row_count)
      3 -> Numeric.load_uint64(binary, row_count)
    end
  end
end
```

**State Management:**
```elixir
defmodule Natch.Protocol.LowCardinalityState do
  use GenServer

  defstruct dictionaries: %{}

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, %__MODULE__{}, name: __MODULE__)
  end

  def get_dictionary(query_id, column_name) do
    GenServer.call(__MODULE__, {:get_dict, query_id, column_name})
  end

  def update_dictionary(query_id, column_name, dictionary) do
    GenServer.call(__MODULE__, {:update_dict, query_id, column_name, dictionary})
  end

  def clear_query(query_id) do
    GenServer.cast(__MODULE__, {:clear_query, query_id})
  end
end
```

---

## Compression Layer

**Reference:** `/Users/brendon/work/clickhouse-cpp/clickhouse/base/compressed.cpp`

### Compressed Block Format

```
CompressedBlock = hash:UInt128 +           # CityHash128 checksum
                  method:UInt8 +            # 0x82=LZ4, 0x90=ZSTD
                  compressed_size:UInt32 +
                  original_size:UInt32 +
                  compressed_data[compressed_size - 9]
```

### Implementation Strategy

**Use existing NIFs** - Don't implement LZ4/ZSTD in pure Elixir

```elixir
defmodule Natch.Protocol.Compression do
  # Use existing Elixir packages (which wrap C libraries)

  def decompress_block(binary) do
    with <<hash::binary-size(16), rest::binary>> <- binary,
         <<method::8, rest::binary>> <- rest,
         <<compressed_size::little-32, rest::binary>> <- rest,
         <<original_size::little-32, rest::binary>> <- rest,
         data_size = compressed_size - 9,
         <<compressed_data::binary-size(data_size), rest::binary>> <- rest do

      # Verify checksum
      computed_hash = :cityhash.hash128(<<method::8, compressed_size::little-32,
                                          original_size::little-32, compressed_data::binary>>)
      if computed_hash != hash do
        {:error, :checksum_mismatch}
      else
        decompressed = case method do
          0x82 -> lz4_decompress(compressed_data, original_size)
          0x90 -> zstd_decompress(compressed_data)
          0x02 -> compressed_data  # No compression
          _ -> {:error, {:unknown_compression_method, method}}
        end

        {:ok, decompressed, rest}
      end
    end
  end

  defp lz4_decompress(data, original_size) do
    # Use :lz4 package (Elixir wrapper around C library)
    case :lz4.decompress(data, original_size) do
      {:ok, decompressed} -> decompressed
      error -> error
    end
  end

  defp zstd_decompress(data) do
    # Use :ezstd package
    case :ezstd.decompress(data) do
      {:ok, decompressed} -> decompressed
      error -> error
    end
  end
end
```

**Dependencies:**
- Add `{:lz4, "~> 0.2"}` to mix.exs
- Add `{:ezstd, "~> 1.0"}` to mix.exs

**Note:** These are NIFs wrapping C libraries, but much simpler than full clickhouse-cpp

---

## Connection Management

### TCP Socket Handling

```elixir
defmodule Natch.Protocol.Connection do
  use GenServer

  defstruct [
    :socket,
    :server_info,
    :query_id,
    :lc_state,  # LowCardinality state
    :pending_blocks,
    :caller
  ]

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  def init(opts) do
    host = Keyword.fetch!(opts, :host)
    port = Keyword.get(opts, :port, 9000)
    database = Keyword.get(opts, :database, "default")
    user = Keyword.get(opts, :user, "default")
    password = Keyword.get(opts, :password, "")

    case :gen_tcp.connect(String.to_charlist(host), port,
                          [:binary, active: false, packet: :raw]) do
      {:ok, socket} ->
        case handshake(socket, database, user, password) do
          {:ok, server_info} ->
            {:ok, %__MODULE__{
              socket: socket,
              server_info: server_info,
              lc_state: LowCardinalityState.start_link([])
            }}
          {:error, reason} ->
            :gen_tcp.close(socket)
            {:stop, reason}
        end
      {:error, reason} ->
        {:stop, reason}
    end
  end

  defp handshake(socket, database, user, password) do
    # Read server hello
    with {:ok, packet_type} <- read_varint_from_socket(socket),
         0 <- packet_type,  # Hello packet
         {:ok, server_name} <- read_string_from_socket(socket),
         {:ok, server_version_major} <- read_varint_from_socket(socket),
         {:ok, server_version_minor} <- read_varint_from_socket(socket),
         {:ok, server_revision} <- read_varint_from_socket(socket) do

      # Send client hello
      :ok = send_client_hello(socket, database, user, password)

      {:ok, %{
        name: server_name,
        version_major: server_version_major,
        version_minor: server_version_minor,
        revision: server_revision
      }}
    end
  end

  defp send_client_hello(socket, database, user, password) do
    client_name = "natch"
    client_version_major = 1
    client_version_minor = 0
    client_revision = 54451

    packet = [
      WireFormat.encode_varint(0),  # Hello packet type
      WireFormat.encode_string(client_name),
      WireFormat.encode_varint(client_version_major),
      WireFormat.encode_varint(client_version_minor),
      WireFormat.encode_varint(client_revision),
      WireFormat.encode_string(database),
      WireFormat.encode_string(user),
      WireFormat.encode_string(password)
    ]

    :gen_tcp.send(socket, packet)
  end

  def query(conn, sql) do
    GenServer.call(conn, {:query, sql}, :infinity)
  end

  def handle_call({:query, sql}, from, state) do
    query_id = generate_query_id()

    # Send query packet
    :ok = send_query(state.socket, query_id, sql)

    # Read response packets
    blocks = read_response_blocks(state.socket, state.server_info.revision, state.lc_state)

    {:reply, {:ok, blocks}, state}
  end

  defp send_query(socket, query_id, sql) do
    packet = [
      WireFormat.encode_varint(1),  # Query packet type
      WireFormat.encode_string(query_id),
      # Query settings (simplified)
      WireFormat.encode_varint(1),  # stage = Complete
      WireFormat.encode_varint(0),  # compression = None
      WireFormat.encode_string(sql)
    ]

    :gen_tcp.send(socket, packet)
  end

  defp read_response_blocks(socket, server_revision, lc_state, acc \\ []) do
    case read_packet(socket) do
      {:ok, 1, data} ->  # Data packet
        {:ok, block, _rest} = Block.read(data, server_revision)
        read_response_blocks(socket, server_revision, lc_state, [block | acc])

      {:ok, 2, data} ->  # Exception packet
        {:ok, exception} = Exception.read(data)
        {:error, exception}

      {:ok, 3, _data} ->  # Progress packet
        read_response_blocks(socket, server_revision, lc_state, acc)

      {:ok, 5, _data} ->  # EndOfStream
        {:ok, Enum.reverse(acc)}

      {:error, reason} ->
        {:error, reason}
    end
  end
end
```

---

## Performance Optimization Strategies

### 1. Binary Reference Semantics

```elixir
# Use :binary.copy/1 to maintain reference to original binary
def load_string(binary, row_count) do
  load_strings(binary, row_count, [], :binary.copy/1)
end

defp load_strings(rest, 0, acc, _copier), do: {:ok, Enum.reverse(acc), rest}
defp load_strings(binary, remaining, acc, copier) do
  with {:ok, len, rest} <- WireFormat.read_varint(binary),
       <<str::binary-size(len), rest2::binary>> <- rest do
    # copier can be identity or :binary.copy/1
    load_strings(rest2, remaining - 1, [copier.(str) | acc], copier)
  end
end
```

### 2. Comprehension Optimization

```elixir
# Use binary comprehensions (compiled to efficient loops)
def load_uint64_optimized(binary, row_count) do
  byte_count = row_count * 8
  <<data::binary-size(byte_count), rest::binary>> = binary
  values = for <<v::little-unsigned-64 <- data>>, do: v
  {:ok, values, rest}
end
```

### 3. Streaming for Large Columns

```elixir
defmodule Natch.Protocol.Stream do
  def stream_column(socket, column_type, row_count) do
    Stream.resource(
      fn -> {socket, row_count} end,
      fn
        {socket, 0} -> {:halt, socket}
        {socket, remaining} ->
          case read_next_value(socket, column_type) do
            {:ok, value} -> {[value], {socket, remaining - 1}}
            {:error, _} -> {:halt, socket}
          end
      end,
      fn socket -> socket end
    )
  end
end
```

### 4. Parallel Block Processing

```elixir
def process_blocks_parallel(blocks) do
  blocks
  |> Task.async_stream(fn block ->
    # Parse each block in parallel
    parse_block(block)
  end, max_concurrency: System.schedulers_online())
  |> Enum.map(fn {:ok, result} -> result end)
end
```

---

## Testing Strategy

### Unit Tests

```elixir
defmodule Natch.Protocol.WireFormatTest do
  use ExUnit.Case

  describe "VarInt encoding/decoding" do
    test "decodes single-byte varint" do
      assert {:ok, 127, <<>>} = WireFormat.read_varint(<<127>>)
    end

    test "decodes two-byte varint" do
      # Value: 300 = [0xAC, 0x02]
      assert {:ok, 300, <<>>} = WireFormat.read_varint(<<0xAC, 0x02>>)
    end

    test "decodes maximum value" do
      # Test with large number
      binary = <<0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01>>
      assert {:ok, value, <<>>} = WireFormat.read_varint(binary)
      assert value == 18_446_744_073_709_551_615
    end
  end
end
```

### Integration Tests

```elixir
defmodule Natch.Protocol.IntegrationTest do
  use ExUnit.Case

  setup do
    # Start test ClickHouse instance
    {:ok, conn} = Natch.Protocol.Connection.start_link(
      host: "localhost",
      port: 9000,
      database: "default"
    )

    %{conn: conn}
  end

  test "round-trip UInt64 column", %{conn: conn} do
    # Create table
    Natch.Protocol.execute(conn, "CREATE TABLE test_uint64 (val UInt64) ENGINE = Memory")

    # Insert data
    values = Enum.to_list(1..1000)
    Natch.Protocol.insert(conn, "test_uint64", %{val: values}, [{:val, :uint64}])

    # Query back
    {:ok, blocks} = Natch.Protocol.query(conn, "SELECT val FROM test_uint64 ORDER BY val")
    [block] = blocks

    assert length(block.columns) == 1
    [column] = block.columns
    assert column.values == values

    # Cleanup
    Natch.Protocol.execute(conn, "DROP TABLE test_uint64")
  end

  test "handles nullable columns", %{conn: conn} do
    Natch.Protocol.execute(conn, "CREATE TABLE test_nullable (val Nullable(UInt64)) ENGINE = Memory")

    values = [1, nil, 3, nil, 5]
    Natch.Protocol.insert(conn, "test_nullable", %{val: values}, [{:val, {:nullable, :uint64}}])

    {:ok, blocks} = Natch.Protocol.query(conn, "SELECT val FROM test_nullable")
    [block] = blocks
    [column] = block.columns

    assert column.values == values
  end
end
```

### Property-Based Tests

```elixir
defmodule Natch.Protocol.PropertyTest do
  use ExUnit.Case
  use PropCheck

  property "varint encoding round-trips" do
    forall value <- non_neg_integer() do
      encoded = WireFormat.encode_varint(value)
      {:ok, decoded, <<>>} = WireFormat.read_varint(encoded)
      decoded == value
    end
  end

  property "all UInt64 values round-trip" do
    forall values <- list(non_neg_integer()) do
      binary = for v <- values, into: <<>>, do: <<v::little-unsigned-64>>
      {:ok, decoded, <<>>} = Column.Numeric.load_uint64(binary, length(values))
      decoded == values
    end
  end
end
```

---

## Implementation Roadmap

### Phase 1: Core Protocol (4-6 weeks)

**Week 1-2: Wire Format & Basic Types**
- [x] VarInt encoding/decoding
- [x] Fixed-size type encoding
- [x] String encoding
- [x] Unit tests for wire format

**Week 3-4: Connection & Handshake**
- [x] TCP socket handling with :gen_tcp
- [x] Client/server handshake
- [x] Packet type handling
- [x] Query packet construction

**Week 5-6: Block & Basic Columns**
- [x] Block structure parsing
- [x] BlockInfo handling
- [x] Numeric column types (UInt*, Int*, Float*)
- [x] String columns
- [x] Integration tests

**Deliverable:** Can connect to ClickHouse, execute simple queries, retrieve numeric and string columns

---

### Phase 2: Complex Types (3-4 weeks)

**Week 7-8: Nullable & Date Types**
- [x] Nullable column wrapper
- [x] Date, DateTime, DateTime64
- [x] UUID (format as string)
- [x] Tests for all date/time types

**Week 9-10: Array & Nested Types**
- [x] Array column with offset handling
- [x] Nested arrays (Array(Array(T)))
- [x] Tuple columns
- [x] Map columns

**Deliverable:** Can handle all common column types except LowCardinality

---

### Phase 3: Advanced Features (4-6 weeks)

**Week 11-13: LowCardinality**
- [x] Dictionary state management (GenServer)
- [x] Index type handling (UInt8/16/32/64)
- [x] Additional keys support
- [x] Shared dictionary logic
- [x] Comprehensive LowCardinality tests

**Week 14-16: Compression & Optimization**
- [x] LZ4 integration (via :lz4 package)
- [x] ZSTD integration (via :ezstd package)
- [x] Checksum verification (CityHash128)
- [x] Binary reference optimization
- [x] Streaming for large result sets

**Deliverable:** Full protocol implementation with compression

---

### Phase 4: Testing & Polish (3-4 weeks)

**Week 17-18: Comprehensive Testing**
- [x] Property-based tests for all types
- [x] Fuzzing for wire format parsing
- [x] Integration test suite against ClickHouse
- [x] Performance benchmarking

**Week 19-20: Documentation & Examples**
- [x] Module documentation
- [x] Type specification (@spec annotations)
- [x] Usage examples
- [x] Migration guide from NIF version

**Deliverable:** Production-ready pure Elixir implementation

---

## File Structure

```
lib/natch/protocol/
├── connection.ex              # GenServer for TCP connection
├── wire_format.ex            # VarInt, basic type encoding
├── packet.ex                 # Packet type definitions
├── block.ex                  # Block structure parsing
├── compression.ex            # LZ4/ZSTD integration
├── lowcardinality_state.ex   # Dictionary state management
└── column/
    ├── numeric.ex            # UInt*, Int*, Float*
    ├── string.ex             # String, FixedString
    ├── nullable.ex           # Nullable wrapper
    ├── array.ex              # Array type
    ├── date.ex               # Date, DateTime, DateTime64
    ├── lowcardinality.ex     # LowCardinality implementation
    └── complex.ex            # Map, Tuple, Enum
```

**Estimated LOC:**
- `wire_format.ex`: ~200 LOC
- `connection.ex`: ~400 LOC
- `block.ex`: ~200 LOC
- `compression.ex`: ~150 LOC
- `column/*.ex`: ~1500 LOC total
- `lowcardinality_state.ex`: ~200 LOC
- Tests: ~1500 LOC
- **Total: ~4150 LOC**

---

## Performance Expectations

### Benchmarks (1M rows)

| Operation | Current NIF | Pure Elixir (Est.) | Pillar (HTTP) |
|-----------|-------------|---------------------|---------------|
| INSERT (numeric) | 2034ms | 3000ms (1.5x slower) | 5113ms |
| SELECT (numeric) | 811ms | 1500ms (1.8x slower) | 64ms (Jason) |
| Memory (INSERT) | 976B | ~5KB | ~45MB |
| Memory (SELECT) | Moderate | ~20MB | ~45MB |

### Why Slower Than Pillar for SELECT?

**Jason advantages:**
1. Highly optimized SIMD code (C with intrinsics)
2. Single JSON binary → single parse operation
3. Years of optimization
4. Used by millions of applications

**Pure Elixir constraints:**
1. Binary pattern matching is fast but not SIMD
2. Per-value allocations in BEAM
3. GC pressure on large datasets
4. Cannot compete with heavily optimized JSON parsers

### Why Competitive for INSERT?

**Native protocol advantages:**
1. Columnar format (better compression)
2. Binary encoding (smaller payloads)
3. LZ4 compression (~70% bandwidth reduction)
4. Zero server-side JSON encoding overhead

---

## Dependencies

```elixir
# mix.exs
defp deps do
  [
    # Compression (NIFs wrapping C libraries)
    {:lz4, "~> 0.2"},
    {:ezstd, "~> 1.0"},

    # Hashing for checksum verification
    {:cityhash, "~> 0.3"},

    # Testing
    {:stream_data, "~> 0.6", only: :test},
    {:benchee, "~> 1.1", only: :dev}
  ]
end
```

**Note:** Still depends on NIFs for compression, but much lighter than full clickhouse-cpp

---

## Migration Strategy

### Gradual Migration

```elixir
# config/config.exs
config :natch,
  protocol: :native_nif  # or :native_elixir

# lib/natch.ex
defmodule Natch do
  @protocol Application.compile_env(:natch, :protocol, :native_nif)

  def connection_module do
    case @protocol do
      :native_nif -> Natch.NIF.Connection
      :native_elixir -> Natch.Protocol.Connection
    end
  end

  def start_link(opts) do
    connection_module().start_link(opts)
  end
end
```

### Feature Flag System

```elixir
# Allow per-connection protocol choice
{:ok, conn_nif} = Natch.start_link([protocol: :native_nif] ++ opts)
{:ok, conn_elixir} = Natch.start_link([protocol: :native_elixir] ++ opts)

# Run benchmarks to compare
Benchee.run(%{
  "NIF" => fn -> Natch.query(conn_nif, query) end,
  "Elixir" => fn -> Natch.query(conn_elixir, query) end
})
```

---

## Risk Assessment

### High Risk

1. **LowCardinality Complexity**
   - Most complex type
   - State management across blocks
   - Potential for subtle bugs
   - **Mitigation:** Extensive testing, reference implementation comparison

2. **Protocol Version Tracking**
   - ClickHouse evolves protocol frequently
   - Must track upstream changes
   - **Mitigation:** Automated tests against multiple ClickHouse versions

### Medium Risk

3. **Performance Regression**
   - Pure Elixir slower than NIF for some workloads
   - May not meet all use case requirements
   - **Mitigation:** Benchmarking, hybrid approach option

4. **Memory Usage**
   - Large result sets may cause GC pressure
   - **Mitigation:** Streaming API, chunked processing

### Low Risk

5. **Binary Parsing Bugs**
   - Well-defined wire format
   - Binary pattern matching is battle-tested in Elixir
   - **Mitigation:** Property-based testing, fuzzing

---

## Decision Factors

### Choose Pure Elixir IF:

✅ **Priority is simplicity and maintainability**
- No C++ build dependencies
- Easier onboarding for Elixir developers
- Better debugging experience

✅ **INSERT-heavy workload**
- Competitive performance with NIF
- Better compression efficiency than HTTP

✅ **Long-term maintenance is a concern**
- clickhouse-cpp requires tracking C++ library updates
- Pure Elixir is more stable dependency-wise

✅ **Cross-platform deployment important**
- No native compilation issues
- Easier for ARM, embedded systems, etc.

### Keep NIF Approach IF:

❌ **Maximum performance is critical**
- NIF will always be faster for term creation
- ~30-40% faster for large datasets

❌ **Development resources are limited**
- 3.5-5 months is too long
- Team is comfortable with C++/NIFs

❌ **Need bleeding-edge ClickHouse features**
- clickhouse-cpp tracks ClickHouse releases closely
- Pure Elixir would lag behind

### Use Pillar (HTTP) IF:

❌ **SELECT-heavy workload**
- Jason is 10-20x faster for large result sets
- HTTP interface is stable and well-tested

❌ **Simple use case**
- Don't need native protocol features
- HTTP is "good enough"

---

## Open Questions

1. **How to handle protocol evolution?**
   - Strategy for tracking ClickHouse changes
   - Automated protocol update detection?

2. **What's the right GenServer architecture?**
   - One GenServer per connection?
   - Connection pool management?
   - How to handle concurrent queries?

3. **Should we support INSERT streaming?**
   - Send blocks incrementally
   - Or require full dataset upfront?

4. **How to integrate with Explorer/DataFrame?**
   - Can we parse directly into Explorer.Series?
   - Zero-copy possibilities?

5. **What's the testing strategy for all ClickHouse versions?**
   - Matrix testing against 21.x, 22.x, 23.x, 24.x?
   - How to maintain compatibility?

---

## References

### ClickHouse Documentation
- [Native Protocol Specification](https://clickhouse.com/docs/en/interfaces/tcp)
- [Data Type Reference](https://clickhouse.com/docs/en/sql-reference/data-types)
- [Compression Methods](https://clickhouse.com/docs/en/operations/settings/settings#compression)

### Source Code References
- clickhouse-cpp: `/Users/brendon/work/clickhouse-cpp/`
  - `clickhouse/protocol.h`: Packet type definitions
  - `clickhouse/base/wire_format.h`: VarInt and primitive encoding
  - `clickhouse/client.cpp`: Block parsing logic (lines 651-722)
  - `clickhouse/columns/*.cpp`: Column type implementations
  - `clickhouse/base/compressed.cpp`: Compression layer

### Elixir Resources
- [Binary Pattern Matching Guide](https://hexdocs.pm/elixir/Kernel.SpecialForms.html#%3C%3C%3E%3E/1)
- [GenServer Documentation](https://hexdocs.pm/elixir/GenServer.html)
- [:gen_tcp Documentation](https://www.erlang.org/doc/man/gen_tcp.html)

---

## Conclusion

A **pure Elixir implementation of the ClickHouse native protocol is feasible** and would provide significant architectural benefits (no C++ dependencies, better debugging, easier deployment). However, it comes at the cost of:

1. **Large development effort** (3.5-5 months, ~4000 LOC)
2. **Performance trade-offs** (1.5-2x slower than NIF, 20x slower than Pillar for SELECT)
3. **Ongoing maintenance** to track protocol changes

**Recommendation:** This approach is best suited for projects that prioritize **simplicity and maintainability** over absolute performance, particularly for **INSERT-heavy workloads**. For SELECT-heavy workloads, the HTTP/JSON approach (Pillar) remains superior.

**Alternative:** Consider the **binary passthrough hybrid approach** (detailed in separate document) which could provide 5-10x SELECT speedup with minimal implementation effort (~1-2 weeks).

---

**Document Status:** Complete and shelved
**Next Steps:** Evaluate binary passthrough approach as lower-effort alternative
**Last Review:** 2025-10-31
