defmodule Natch.Column do
  @moduledoc """
  Column builder for ClickHouse native types.

  Provides an Elixir API for creating and populating ClickHouse columns.
  """

  alias Natch.Native

  @type column :: %__MODULE__{
          ref: reference(),
          type: atom() | tuple(),
          clickhouse_type: String.t()
        }

  defstruct [:ref, :type, :clickhouse_type]

  @doc """
  Creates a new column of the specified type.

  ## Supported Types

  **Integers:**
  - `:uint64` - UInt64
  - `:uint32` - UInt32
  - `:uint16` - UInt16
  - `:int64` - Int64
  - `:int32` - Int32
  - `:int16` - Int16
  - `:int8` - Int8

  **Floats:**
  - `:float64` - Float64
  - `:float32` - Float32

  **Strings:**
  - `:string` - String

  **Dates/Times:**
  - `:datetime` - DateTime (Unix timestamp in seconds)
  - `:datetime64` - DateTime64(6) (Unix timestamp in microseconds)
  - `:date` - Date (days since epoch)

  **Boolean:**
  - `:bool` - Bool (stored as UInt8)

  **UUID:**
  - `:uuid` - UUID (128-bit universally unique identifier)

  **Decimal:**
  - `:decimal` - Decimal64(9) (fixed-point decimal with 9 decimal places)

  **Arrays:**
  - `{:array, inner_type}` - Array(T) for any supported type T
  - Supports nesting: `{:array, {:array, :uint64}}` → Array(Array(UInt64))

  ## Examples

      iex> Natch.Column.new(:uint64)
      %Natch.Column{type: :uint64, clickhouse_type: "UInt64", ref: #Reference<...>}

      iex> Natch.Column.new(:string)
      %Natch.Column{type: :string, clickhouse_type: "String", ref: #Reference<...>}

      iex> Natch.Column.new({:array, :uint64})
      %Natch.Column{type: {:array, :uint64}, clickhouse_type: "Array(UInt64)", ref: #Reference<...>}
  """
  @spec new(atom() | tuple()) :: column()
  def new(type) do
    clickhouse_type = elixir_type_to_clickhouse(type)
    ref = Native.column_create(clickhouse_type)

    %__MODULE__{
      ref: ref,
      type: type,
      clickhouse_type: clickhouse_type
    }
  end

  @doc """
  Appends multiple values to the column in bulk (single NIF call).

  This is the primary, high-performance API. Values must be a list matching
  the column type.

  ## Examples

      col = Natch.Column.new(:uint64)
      :ok = Natch.Column.append_bulk(col, [1, 2, 3, 4, 5])

      col = Natch.Column.new(:string)
      :ok = Natch.Column.append_bulk(col, ["hello", "world"])

      col = Natch.Column.new(:datetime)
      :ok = Natch.Column.append_bulk(col, [~U[2024-01-01 10:00:00Z], ~U[2024-01-01 11:00:00Z]])
  """
  @spec append_bulk(column(), [term()]) :: :ok
  def append_bulk(%__MODULE__{type: :uint64, ref: ref}, values) when is_list(values) do
    # Validate all values
    unless Enum.all?(values, &(is_integer(&1) and &1 >= 0)) do
      raise ArgumentError, "All values must be non-negative integers for UInt64 column"
    end

    Native.column_uint64_append_bulk(ref, values)
  end

  def append_bulk(%__MODULE__{type: :int64, ref: ref}, values) when is_list(values) do
    unless Enum.all?(values, &is_integer/1) do
      raise ArgumentError, "All values must be integers for Int64 column"
    end

    Native.column_int64_append_bulk(ref, values)
  end

  def append_bulk(%__MODULE__{type: :string, ref: ref}, values) when is_list(values) do
    unless Enum.all?(values, &is_binary/1) do
      raise ArgumentError, "All values must be strings for String column"
    end

    Native.column_string_append_bulk(ref, values)
  end

  def append_bulk(%__MODULE__{type: :float64, ref: ref}, values) when is_list(values) do
    unless Enum.all?(values, &(is_float(&1) or is_integer(&1))) do
      raise ArgumentError, "All values must be numbers for Float64 column"
    end

    # Convert integers to floats
    float_values =
      Enum.map(values, fn
        val when is_float(val) -> val
        val when is_integer(val) -> val * 1.0
      end)

    Native.column_float64_append_bulk(ref, float_values)
  end

  def append_bulk(%__MODULE__{type: :datetime, ref: ref}, values) when is_list(values) do
    # Convert all to Unix timestamps
    timestamps =
      Enum.map(values, fn
        %DateTime{} = dt -> DateTime.to_unix(dt)
        timestamp when is_integer(timestamp) -> timestamp
        other -> raise ArgumentError, "Invalid datetime value: #{inspect(other)}"
      end)

    Native.column_datetime_append_bulk(ref, timestamps)
  end

  def append_bulk(%__MODULE__{type: :datetime64, ref: ref}, values) when is_list(values) do
    # Convert all to Unix timestamps with microsecond precision
    ticks =
      Enum.map(values, fn
        %DateTime{} = dt -> DateTime.to_unix(dt, :microsecond)
        ticks when is_integer(ticks) -> ticks
        other -> raise ArgumentError, "Invalid datetime64 value: #{inspect(other)}"
      end)

    Native.column_datetime64_append_bulk(ref, ticks)
  end

  def append_bulk(%__MODULE__{type: :date, ref: ref}, values) when is_list(values) do
    # Convert all to days since epoch
    days =
      Enum.map(values, fn
        %Date{} = date ->
          Date.diff(date, ~D[1970-01-01])

        days when is_integer(days) ->
          days

        other ->
          raise ArgumentError, "Invalid date value: #{inspect(other)}"
      end)

    Native.column_date_append_bulk(ref, days)
  end

  def append_bulk(%__MODULE__{type: :bool, ref: ref}, values) when is_list(values) do
    unless Enum.all?(values, &is_boolean/1) do
      raise ArgumentError, "All values must be booleans for Bool column"
    end

    # Convert booleans to 0/1
    int_values =
      Enum.map(values, fn
        true -> 1
        false -> 0
      end)

    Native.column_uint8_append_bulk(ref, int_values)
  end

  def append_bulk(%__MODULE__{type: :uint32, ref: ref}, values) when is_list(values) do
    unless Enum.all?(values, &(is_integer(&1) and &1 >= 0 and &1 <= 4_294_967_295)) do
      raise ArgumentError,
            "All values must be non-negative integers 0..4294967295 for UInt32 column"
    end

    Native.column_uint32_append_bulk(ref, values)
  end

  def append_bulk(%__MODULE__{type: :uint16, ref: ref}, values) when is_list(values) do
    unless Enum.all?(values, &(is_integer(&1) and &1 >= 0 and &1 <= 65_535)) do
      raise ArgumentError, "All values must be non-negative integers 0..65535 for UInt16 column"
    end

    Native.column_uint16_append_bulk(ref, values)
  end

  def append_bulk(%__MODULE__{type: :int32, ref: ref}, values) when is_list(values) do
    unless Enum.all?(values, &(is_integer(&1) and &1 >= -2_147_483_648 and &1 <= 2_147_483_647)) do
      raise ArgumentError,
            "All values must be integers -2147483648..2147483647 for Int32 column"
    end

    Native.column_int32_append_bulk(ref, values)
  end

  def append_bulk(%__MODULE__{type: :int16, ref: ref}, values) when is_list(values) do
    unless Enum.all?(values, &(is_integer(&1) and &1 >= -32_768 and &1 <= 32_767)) do
      raise ArgumentError, "All values must be integers -32768..32767 for Int16 column"
    end

    Native.column_int16_append_bulk(ref, values)
  end

  def append_bulk(%__MODULE__{type: :int8, ref: ref}, values) when is_list(values) do
    unless Enum.all?(values, &(is_integer(&1) and &1 >= -128 and &1 <= 127)) do
      raise ArgumentError, "All values must be integers -128..127 for Int8 column"
    end

    Native.column_int8_append_bulk(ref, values)
  end

  def append_bulk(%__MODULE__{type: :float32, ref: ref}, values) when is_list(values) do
    unless Enum.all?(values, &(is_float(&1) or is_integer(&1))) do
      raise ArgumentError, "All values must be numbers for Float32 column"
    end

    # Convert integers to floats
    float_values =
      Enum.map(values, fn
        val when is_float(val) -> val
        val when is_integer(val) -> val * 1.0
      end)

    Native.column_float32_append_bulk(ref, float_values)
  end

  def append_bulk(%__MODULE__{type: :uuid, ref: ref}, values) when is_list(values) do
    # Parse UUID strings to extract high/low 64-bit pairs
    uuid_pairs =
      Enum.map(values, fn
        <<_::128>> = uuid_bin ->
          # 16-byte binary UUID
          <<high::64, low::64>> = uuid_bin
          {high, low}

        uuid_str when is_binary(uuid_str) ->
          parse_uuid(uuid_str)

        other ->
          raise ArgumentError, "Invalid UUID value: #{inspect(other)}"
      end)

    # Split into separate lists of highs and lows
    {highs, lows} = Enum.unzip(uuid_pairs)

    Native.column_uuid_append_bulk(ref, highs, lows)
  end

  def append_bulk(%__MODULE__{type: :decimal, ref: ref}, values) when is_list(values) do
    # Convert Decimal structs to scaled int64 values
    # Decimal64(9) means scale=9, so multiply by 10^9
    scale = 9
    multiplier = :math.pow(10, scale) |> trunc()

    scaled_values =
      Enum.map(values, fn
        %Decimal{} = decimal ->
          # Convert Decimal to float, multiply by 10^scale, convert to int64
          decimal
          |> Decimal.mult(Decimal.new(multiplier))
          |> Decimal.to_integer()

        value when is_integer(value) ->
          # Already scaled integer
          value

        value when is_float(value) ->
          # Scale the float value
          trunc(value * multiplier)

        other ->
          raise ArgumentError, "Invalid decimal value: #{inspect(other)}"
      end)

    Native.column_decimal_append_bulk(ref, scaled_values)
  end

  # Nullable type handlers
  def append_bulk(%__MODULE__{type: :nullable_uint64, ref: ref}, values) when is_list(values) do
    {actual_values, nulls} = split_nullable_values(values, 0)
    Native.column_nullable_uint64_append_bulk(ref, actual_values, nulls)
  end

  def append_bulk(%__MODULE__{type: :nullable_int64, ref: ref}, values) when is_list(values) do
    {actual_values, nulls} = split_nullable_values(values, 0)
    Native.column_nullable_int64_append_bulk(ref, actual_values, nulls)
  end

  def append_bulk(%__MODULE__{type: :nullable_string, ref: ref}, values) when is_list(values) do
    {actual_values, nulls} = split_nullable_values(values, "")
    Native.column_nullable_string_append_bulk(ref, actual_values, nulls)
  end

  def append_bulk(%__MODULE__{type: :nullable_float64, ref: ref}, values) when is_list(values) do
    {actual_values, nulls} = split_nullable_values(values, 0.0)
    Native.column_nullable_float64_append_bulk(ref, actual_values, nulls)
  end

  # Generic Nullable(T) for any inner type
  def append_bulk(%__MODULE__{type: {:nullable, inner_type}, ref: ref}, values)
      when is_list(values) do
    # Determine default value and NIF based on inner type
    case inner_type do
      :uint64 ->
        {actual_values, nulls} = split_nullable_values(values, 0)
        Native.column_nullable_uint64_append_bulk(ref, actual_values, nulls)

      :int64 ->
        {actual_values, nulls} = split_nullable_values(values, 0)
        Native.column_nullable_int64_append_bulk(ref, actual_values, nulls)

      :string ->
        {actual_values, nulls} = split_nullable_values(values, "")
        Native.column_nullable_string_append_bulk(ref, actual_values, nulls)

      :float64 ->
        {actual_values, nulls} = split_nullable_values(values, 0.0)
        Native.column_nullable_float64_append_bulk(ref, actual_values, nulls)

      other ->
        raise ArgumentError,
              "Nullable is only supported for UInt64, Int64, String, Float64. Got: #{inspect(other)}"
    end
  end

  # Array type - always use generic path
  # The generic path works for ALL array types and is already very fast (~5-10 µs)
  def append_bulk(%__MODULE__{type: {:array, _inner_type}, ref: _ref} = col, arrays)
      when is_list(arrays) do
    # Validate all values are lists
    unless Enum.all?(arrays, &is_list/1) do
      raise ArgumentError, "All values must be lists for Array column, got: #{inspect(arrays)}"
    end

    # Always use generic path for arrays
    # Why not fast path? CreateColumnByType creates generic Column base class,
    # not typed ColumnArrayT<T> that fast path NIFs require. Generic path is
    # already very fast and works universally for all types including nested arrays.
    append_array_generic(col, arrays)
  end

  # LowCardinality type - transparent optimization
  # Dictionary encoding is handled automatically by ClickHouse
  def append_bulk(%__MODULE__{type: {:low_cardinality, inner_type}, ref: lc_ref}, values)
      when is_list(values) do
    # Build a temporary column with the inner type
    temp_col = new(inner_type)
    append_bulk(temp_col, values)

    # Pass to LowCardinality NIF which handles dictionary building
    Native.column_lowcardinality_append_from_column(lc_ref, temp_col.ref)
  end

  # Enum8 type - stored as Int8 with named values
  def append_bulk(%__MODULE__{type: {:enum8, items}, ref: ref}, values) when is_list(values) do
    # Convert string names to integers if needed
    int_values =
      if values == [] do
        # Empty list, no conversion needed
        []
      else
        case hd(values) do
          val when is_integer(val) ->
            # Already integers, validate range
            unless Enum.all?(values, &(is_integer(&1) and &1 >= -128 and &1 <= 127)) do
              raise ArgumentError, "All values must be integers -128..127 for Enum8 column"
            end

            values

          val when is_binary(val) ->
            # String names, look up values from items definition
            enum_map = Map.new(items)

            Enum.map(values, fn name ->
              case Map.fetch(enum_map, name) do
                {:ok, val} -> val
                :error -> raise ArgumentError, "Unknown enum name: #{name}"
              end
            end)
        end
      end

    Native.column_int8_append_bulk(ref, int_values)
  end

  # Enum16 type - stored as Int16 with named values
  def append_bulk(%__MODULE__{type: {:enum16, items}, ref: ref}, values) when is_list(values) do
    # Convert string names to integers if needed
    int_values =
      if values == [] do
        # Empty list, no conversion needed
        []
      else
        case hd(values) do
          val when is_integer(val) ->
            # Already integers, validate range
            unless Enum.all?(values, &(is_integer(&1) and &1 >= -32_768 and &1 <= 32_767)) do
              raise ArgumentError, "All values must be integers -32768..32767 for Enum16 column"
            end

            values

          val when is_binary(val) ->
            # String names, look up values from items definition
            enum_map = Map.new(items)

            Enum.map(values, fn name ->
              case Map.fetch(enum_map, name) do
                {:ok, val} -> val
                :error -> raise ArgumentError, "Unknown enum name: #{name}"
              end
            end)
        end
      end

    Native.column_int16_append_bulk(ref, int_values)
  end

  def append_bulk(%__MODULE__{type: type}, values) when is_list(values) do
    raise ArgumentError,
          "Invalid values #{inspect(values)} for column type #{type}"
  end

  def append_bulk(%__MODULE__{}, values) do
    raise ArgumentError, "append_bulk/2 requires a list of values, got: #{inspect(values)}"
  end

  @doc """
  Appends tuple values using columnar API (high performance).

  For Tuple columns, accepts pre-separated column data for maximum performance.
  This is the fastest way to insert tuple data as it avoids transposing rows into columns.

  ## Parameters
  - `column` - A tuple column created with `new({:tuple, [type1, type2, ...]})`
  - `column_lists` - A list of lists, one per tuple element, all with the same length

  ## Example
      # Create Tuple(String, UInt64, Date) column
      col = Column.new({:tuple, [:string, :uint64, :date]})

      # Fastest: Pre-separated columnar data
      names = ["Alice", "Bob", "Charlie"]
      scores = [100, 200, 300]
      dates = [~D[2024-01-01], ~D[2024-01-02], ~D[2024-01-03]]

      Column.append_tuple_columns(col, [names, scores, dates])
  """
  def append_tuple_columns(
        %__MODULE__{type: {:tuple, element_types}, ref: tuple_ref},
        column_lists
      )
      when is_list(column_lists) do
    unless length(column_lists) == length(element_types) do
      raise ArgumentError,
            "Column count mismatch: expected #{length(element_types)} columns, got #{length(column_lists)}"
    end

    # Validate all columns have the same length
    unless Enum.all?(column_lists, &is_list/1) do
      raise ArgumentError, "All column_lists must be lists"
    end

    column_lengths = Enum.map(column_lists, &length/1)

    unless Enum.all?(column_lengths, &(&1 == hd(column_lengths))) do
      raise ArgumentError,
            "All columns must have the same length, got: #{inspect(column_lengths)}"
    end

    # Build columns for each tuple element
    nested_cols =
      Enum.zip(element_types, column_lists)
      |> Enum.map(fn {type, values} ->
        col = new(type)
        append_bulk(col, values)
        col
      end)

    # Pass column refs to NIF
    Native.column_tuple_append_from_columns(tuple_ref, Enum.map(nested_cols, & &1.ref))
  end

  def append_tuple_columns(%__MODULE__{type: type}, _) do
    raise ArgumentError,
          "append_tuple_columns/2 only works with tuple columns, got: #{inspect(type)}"
  end

  @doc """
  Appends map values using columnar API (high performance).

  For Map columns, accepts pre-separated key/value arrays for maximum performance.
  This is the fastest way to insert map data as it avoids row-by-row processing.

  Maps in ClickHouse are stored as Array(Tuple(K, V)), so this function converts
  the columnar format into that structure efficiently.

  ## Parameters
  - `column` - A map column created with `new({:map, key_type, value_type})`
  - `keys_arrays` - A list of key lists, one per map/row
  - `values_arrays` - A list of value lists, one per map/row, matching keys_arrays length

  ## Example
      # Create Map(String, UInt64) column
      col = Column.new({:map, :string, :uint64})

      # Fastest: Pre-separated key/value arrays
      keys_arrays = [["k1", "k2"], ["k3"], ["k4", "k5", "k6"]]
      values_arrays = [[1, 2], [3], [4, 5, 6]]

      Column.append_map_arrays(col, keys_arrays, values_arrays)
  """
  def append_map_arrays(
        %__MODULE__{type: {:map, key_type, value_type}, ref: map_ref},
        keys_arrays,
        values_arrays
      )
      when is_list(keys_arrays) and is_list(values_arrays) do
    unless length(keys_arrays) == length(values_arrays) do
      raise ArgumentError,
            "Keys and values arrays must have the same length, got: #{length(keys_arrays)} keys vs #{length(values_arrays)} values"
    end

    # Validate each key array matches its value array in length
    Enum.zip(keys_arrays, values_arrays)
    |> Enum.each(fn {keys, values} ->
      unless length(keys) == length(values) do
        raise ArgumentError,
              "Each keys array must match its values array length, got: #{length(keys)} keys vs #{length(values)} values"
      end
    end)

    # Build Array(Tuple(K,V)) column
    tuple_type = {:tuple, [key_type, value_type]}
    array_tuple_col = new({:array, tuple_type})

    # For each map (row), we need to build the tuples
    # Concatenate all keys and values across all maps (only one level)
    # Use Enum.concat instead of List.flatten to preserve nested structures
    all_keys_per_map = keys_arrays
    all_values_per_map = values_arrays

    # Build nested tuple column with all keys and values
    # Enum.concat only concatenates one level, preserving nested arrays
    all_keys = Enum.concat(all_keys_per_map)
    all_values = Enum.concat(all_values_per_map)

    nested_tuple_col = new(tuple_type)
    append_tuple_columns(nested_tuple_col, [all_keys, all_values])

    # Calculate offsets for the array (cumulative counts of tuples)
    offsets =
      Enum.scan(keys_arrays, 0, fn keys, acc ->
        acc + length(keys)
      end)

    # Build the Array(Tuple(K,V)) using the array NIF
    Native.column_array_append_from_column(array_tuple_col.ref, nested_tuple_col.ref, offsets)

    # Now append the Array(Tuple(K,V)) to the Map using the map NIF
    Native.column_map_append_from_array(map_ref, array_tuple_col.ref)
  end

  def append_map_arrays(%__MODULE__{type: type}, _, _) do
    raise ArgumentError,
          "append_map_arrays/3 only works with map columns, got: #{inspect(type)}"
  end

  @doc """
  Returns the number of elements in the column.

  ## Examples

      col = Natch.Column.new(:uint64)
      Natch.Column.append(col, 1)
      Natch.Column.append(col, 2)
      Natch.Column.size(col)
      # => 2
  """
  @spec size(column()) :: non_neg_integer()
  def size(%__MODULE__{ref: ref}) do
    Native.column_size(ref)
  end

  # Private functions

  defp elixir_type_to_clickhouse(:uint64), do: "UInt64"
  defp elixir_type_to_clickhouse(:uint32), do: "UInt32"
  defp elixir_type_to_clickhouse(:uint16), do: "UInt16"
  defp elixir_type_to_clickhouse(:int64), do: "Int64"
  defp elixir_type_to_clickhouse(:int32), do: "Int32"
  defp elixir_type_to_clickhouse(:int16), do: "Int16"
  defp elixir_type_to_clickhouse(:int8), do: "Int8"
  defp elixir_type_to_clickhouse(:float64), do: "Float64"
  defp elixir_type_to_clickhouse(:float32), do: "Float32"
  defp elixir_type_to_clickhouse(:string), do: "String"
  defp elixir_type_to_clickhouse(:datetime), do: "DateTime"
  defp elixir_type_to_clickhouse(:datetime64), do: "DateTime64(6)"
  defp elixir_type_to_clickhouse(:date), do: "Date"
  defp elixir_type_to_clickhouse(:bool), do: "Bool"
  defp elixir_type_to_clickhouse(:uuid), do: "UUID"
  defp elixir_type_to_clickhouse(:decimal), do: "Decimal64(9)"
  # Nullable types
  defp elixir_type_to_clickhouse(:nullable_uint64), do: "Nullable(UInt64)"
  defp elixir_type_to_clickhouse(:nullable_int64), do: "Nullable(Int64)"
  defp elixir_type_to_clickhouse(:nullable_string), do: "Nullable(String)"
  defp elixir_type_to_clickhouse(:nullable_float64), do: "Nullable(Float64)"

  defp elixir_type_to_clickhouse({:array, inner_type}) do
    "Array(#{elixir_type_to_clickhouse(inner_type)})"
  end

  defp elixir_type_to_clickhouse({:nullable, inner_type}) do
    "Nullable(#{elixir_type_to_clickhouse(inner_type)})"
  end

  defp elixir_type_to_clickhouse({:tuple, element_types}) when is_list(element_types) do
    types_str = Enum.map_join(element_types, ", ", &elixir_type_to_clickhouse/1)
    "Tuple(#{types_str})"
  end

  defp elixir_type_to_clickhouse({:map, key_type, value_type}) do
    "Map(#{elixir_type_to_clickhouse(key_type)}, #{elixir_type_to_clickhouse(value_type)})"
  end

  defp elixir_type_to_clickhouse({:low_cardinality, inner_type}) do
    "LowCardinality(#{elixir_type_to_clickhouse(inner_type)})"
  end

  defp elixir_type_to_clickhouse({:enum8, items}) when is_list(items) do
    # Format: Enum8('name1' = 1, 'name2' = 2)
    items_str = Enum.map_join(items, ", ", fn {name, val} -> "'#{name}' = #{val}" end)
    "Enum8(#{items_str})"
  end

  defp elixir_type_to_clickhouse({:enum16, items}) when is_list(items) do
    # Format: Enum16('name1' = 1, 'name2' = 2)
    items_str = Enum.map_join(items, ", ", fn {name, val} -> "'#{name}' = #{val}" end)
    "Enum16(#{items_str})"
  end

  defp elixir_type_to_clickhouse(type) do
    raise ArgumentError, "Unsupported column type: #{inspect(type)}"
  end

  # Helper function to split nullable values into actual values and null flags
  # Returns {[values], [nulls]} where null flags are UInt8 (0 = not null, 1 = null)
  defp split_nullable_values(values, default_value) do
    Enum.map_reduce(values, [], fn
      nil, nulls -> {default_value, [1 | nulls]}
      value, nulls -> {value, [0 | nulls]}
    end)
    |> then(fn {vals, nulls} -> {vals, Enum.reverse(nulls)} end)
  end

  # Parse UUID string like "550e8400-e29b-41d4-a716-446655440000" to {high, low} uint64 pair
  defp parse_uuid(uuid_str) when is_binary(uuid_str) do
    # Remove hyphens and validate format
    hex_str = String.replace(uuid_str, "-", "")

    unless String.length(hex_str) == 32 and String.match?(hex_str, ~r/^[0-9a-fA-F]{32}$/) do
      raise ArgumentError, "Invalid UUID format: #{uuid_str}"
    end

    # Convert to binary (16 bytes)
    uuid_bin = Base.decode16!(hex_str, case: :mixed)

    # Split into high and low 64-bit integers
    <<high::64, low::64>> = uuid_bin
    {high, low}
  end

  # Generic path for Array columns - works for ANY inner type
  # Builds nested column, then passes it to C++ via column_array_append_from_column
  defp append_array_generic(%__MODULE__{type: {:array, inner_type}, ref: array_ref}, arrays) do
    # Build nested column with all array elements
    nested_col = new(inner_type)

    # Accumulate offsets as we append arrays
    offsets =
      Enum.reduce(arrays, {[], 0}, fn array_values, {offsets_acc, offset} ->
        # Recursively append values to nested column
        # This works for nested arrays too! append_bulk calls itself recursively.
        append_bulk(nested_col, array_values)
        new_offset = offset + length(array_values)
        {[new_offset | offsets_acc], new_offset}
      end)
      |> elem(0)
      |> Enum.reverse()

    # Pass pre-built nested column to generic NIF
    Native.column_array_append_from_column(array_ref, nested_col.ref, offsets)
  end
end
