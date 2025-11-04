defmodule Natch.Block do
  @moduledoc """
  Pure functions for building ClickHouse blocks from columnar data.

  This module provides the core block-building logic without any GenServer
  coordination. Blocks are the fundamental unit for inserting data into
  ClickHouse via the native TCP protocol.

  ## Columnar Format

  Natch uses a columnar format that matches ClickHouse's native storage:

      columns = %{
        id: [1, 2, 3],
        name: ["Alice", "Bob", "Charlie"],
        amount: [100.0, 200.0, 300.0]
      }

      schema = [id: :uint64, name: :string, amount: :float64]

      block = Natch.Block.build_block(columns, schema)

  This format is:
  - **10-1000x faster** than row-oriented (1 NIF call per column vs N×M calls)
  - **Matches ClickHouse native format** (no transposition needed)
  - **Natural for analytics** (operate on columns, not rows)

  If you have row-oriented data, use `Natch.Conversion.rows_to_columns/2`.
  """

  alias Natch.{Column, Native}

  @doc """
  Builds a Block from columnar data and schema.

  This is a pure function that constructs a Block resource from columnar data.
  Type validation happens in `Natch.Column.append_bulk/2` as each column is built.
  The block can then be inserted via `Natch.insert/4` or used for testing.

  ## Parameters

  - `columns` - Map of column_name => [values]
  - `schema` - Keyword list mapping column names to types

  ## Schema Types

  - `:uint64` - Unsigned 64-bit integer
  - `:int64` - Signed 64-bit integer
  - `:string` - String
  - `:float64` - Float
  - `:datetime` - DateTime (pass Elixir DateTime, converts to Unix timestamp)

  ## Examples

      schema = [id: :uint64, name: :string]
      columns = %{id: [1, 2, 3], name: ["Alice", "Bob", "Charlie"]}
      block = Natch.Block.build_block(columns, schema)

      # Block is a reference that can be passed to Native.client_insert
  """
  @spec build_block(map(), keyword()) :: reference()
  def build_block(columns, schema) when is_map(columns) and is_list(schema) do
    # Create empty block
    block = Native.block_create()

    # Build columns using bulk operations
    column_refs = build_columns_bulk(columns, schema)

    # Append each column to the block
    for {name, column_ref} <- column_refs do
      Native.block_append_column(block, to_string(name), column_ref)
    end

    block
  rescue
    e -> Natch.Error.handle_nif_error(e)
  end

  @doc """
  Builds columns from columnar data using bulk append operations.

  Returns a keyword list of {column_name, column_ref} pairs.

  This is the high-performance path: 1 NIF call per column instead of N calls per value.

  ## Examples

      columns = %{id: [1, 2, 3], name: ["Alice", "Bob", "Charlie"]}
      schema = [id: :uint64, name: :string]
      column_refs = Natch.Block.build_columns_bulk(columns, schema)
      # => [id: #Reference<...>, name: #Reference<...>]

  ## Complex Types

  For Tuple columns, provide list of tuples:
      columns = %{data: [{"Alice", 100}, {"Bob", 200}]}
      schema = [data: {:tuple, [:string, :uint64]}]

  For Map columns, provide list of maps:
      columns = %{metrics: [%{"k1" => 1, "k2" => 2}, %{"k3" => 3}]}
      schema = [metrics: {:map, :string, :uint64}]
  """
  @spec build_columns_bulk(map(), keyword()) :: keyword()
  def build_columns_bulk(columns, schema) when is_map(columns) and is_list(schema) do
    for {name, type} <- schema do
      # Get column values - support both atom and string keys
      values = Map.get(columns, name) || Map.get(columns, to_string(name))

      cond do
        values == nil ->
          raise ArgumentError,
                "Missing column #{inspect(name)} in columns #{inspect(Map.keys(columns))}"

        not is_list(values) ->
          raise ArgumentError,
                "Column #{inspect(name)} must be a list, got: #{inspect(values)}"

        true ->
          # Create column and append values using appropriate method
          column = Column.new(type)
          append_column_values(column, type, values)
          {name, column.ref}
      end
    end
  end

  # Append values to column based on type
  defp append_column_values(column, {:tuple, _element_types}, values) do
    # Convert list of tuples to columnar format
    # Input: [{"Alice", 100}, {"Bob", 200}]
    # Output: [["Alice", "Bob"], [100, 200]]
    if values == [] do
      # Empty list, nothing to append
      :ok
    else
      tuple_size = tuple_size(hd(values))
      column_lists = transpose_tuples(values, tuple_size)
      Column.append_tuple_columns(column, column_lists)
    end
  end

  defp append_column_values(column, {:map, _key_type, _value_type}, values) do
    # Convert list of maps to keys_arrays/values_arrays format
    # Input: [%{"k1" => 1, "k2" => 2}, %{"k3" => 3}]
    # Output: keys_arrays = [["k1", "k2"], ["k3"]], values_arrays = [[1, 2], [3]]
    {keys_arrays, values_arrays} = transpose_maps(values)
    Column.append_map_arrays(column, keys_arrays, values_arrays)
  end

  defp append_column_values(column, _type, values) do
    # Standard append_bulk for all other types
    Column.append_bulk(column, values)
  end

  # Transpose list of tuples into list of column lists
  # [{"a", 1}, {"b", 2}] -> [["a", "b"], [1, 2]]
  defp transpose_tuples(tuples, size) do
    for i <- 0..(size - 1) do
      Enum.map(tuples, fn tuple -> elem(tuple, i) end)
    end
  end

  # Transpose list of maps into keys_arrays and values_arrays
  # [%{"k1" => 1, "k2" => 2}, %{"k3" => 3}] -> {[["k1", "k2"], ["k3"]], [[1, 2], [3]]}
  defp transpose_maps(maps) do
    Enum.map(maps, fn map ->
      # Convert each map to {keys, values} pair
      {Map.keys(map), Map.values(map)}
    end)
    |> Enum.unzip()
  end
end
