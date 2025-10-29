defmodule Chex.Insert do
  @moduledoc """
  Insert operations for ClickHouse via native TCP protocol.

  Provides high-level API for building blocks from Elixir data and inserting
  into ClickHouse tables.
  """

  alias Chex.{Column, Native}

  @doc """
  Inserts rows into a table.

  ## Parameters

  - `conn` - Connection GenServer
  - `table` - Table name
  - `rows` - List of maps (each map is a row)
  - `schema` - Keyword list mapping column names to types

  ## Schema Types

  - `:uint64` - Unsigned 64-bit integer
  - `:int64` - Signed 64-bit integer
  - `:string` - String
  - `:float64` - Float
  - `:datetime` - DateTime

  ## Examples

      schema = [
        id: :uint64,
        name: :string,
        amount: :float64,
        created_at: :datetime
      ]

      rows = [
        %{id: 1, name: "Alice", amount: 100.5, created_at: ~U[2024-10-29 10:00:00Z]},
        %{id: 2, name: "Bob", amount: 200.75, created_at: ~U[2024-10-29 11:00:00Z]}
      ]

      Chex.Insert.insert(conn, "users", rows, schema)
  """
  @spec insert(GenServer.server(), String.t(), [map()], keyword()) :: :ok | {:error, term()}
  def insert(conn, table, rows, schema) when is_list(rows) and is_list(schema) do
    GenServer.call(conn, {:insert, table, rows, schema}, :infinity)
  end

  @doc """
  Builds a Block from rows and schema.

  This is a lower-level function that creates a Block resource without
  inserting it. Useful for testing or custom insertion logic.

  ## Examples

      schema = [id: :uint64, name: :string]
      rows = [%{id: 1, name: "Alice"}]
      block = Chex.Insert.build_block(rows, schema)
  """
  @spec build_block([map()], keyword()) :: reference()
  def build_block(rows, schema) when is_list(rows) and is_list(schema) do
    # Create empty block
    block = Native.block_create()

    # Build columns
    columns = build_columns(rows, schema)

    # Append each column to the block
    for {name, column} <- columns do
      Native.block_append_column(block, to_string(name), column.ref)
    end

    block
  end

  @doc """
  Builds columns from rows and schema.

  Returns a keyword list of {column_name, Column} pairs.
  """
  @spec build_columns([map()], keyword()) :: keyword()
  def build_columns(rows, schema) when is_list(rows) and is_list(schema) do
    # Create empty columns for each schema entry
    columns =
      for {name, type} <- schema do
        {name, Column.new(type)}
      end

    # Populate columns row by row
    for row <- rows do
      for {name, column} <- columns do
        value = Map.get(row, name) || Map.get(row, to_string(name))

        if value == nil do
          raise ArgumentError,
                "Missing value for column #{inspect(name)} in row #{inspect(row)}"
        end

        Column.append(column, value)
      end
    end

    columns
  end

  @doc """
  Validates that all rows have the required columns from schema.

  Returns :ok or {:error, reason}.
  """
  @spec validate_rows([map()], keyword()) :: :ok | {:error, String.t()}
  def validate_rows(rows, schema) do
    schema_keys = Keyword.keys(schema)

    Enum.reduce_while(rows, :ok, fn row, _acc ->
      row_keys = Map.keys(row) |> Enum.map(&normalize_key/1)
      schema_keys_normalized = Enum.map(schema_keys, &normalize_key/1)

      missing = schema_keys_normalized -- row_keys

      if missing == [] do
        {:cont, :ok}
      else
        {:halt,
         {:error,
          "Row #{inspect(row)} is missing columns: #{inspect(Enum.map(missing, &to_string/1))}"}}
      end
    end)
  end

  # Private functions

  defp normalize_key(key) when is_atom(key), do: key
  defp normalize_key(key) when is_binary(key), do: String.to_atom(key)
end
