defmodule Natch.Conversion do
  @moduledoc """
  Conversion utilities between row-oriented and column-oriented data formats.

  Provides helpers for users with row-based data sources who need to convert
  to ClickHouse's native columnar format.

  ## Validation

  Type and length validation happens automatically in `Natch.Column.append_bulk/2`
  and the underlying FINE NIFs when you build blocks, providing type safety with
  optimal performance.
  """

  @type schema :: [{atom(), atom()}]

  @doc """
  Converts row-oriented data (list of maps) to column-oriented format (map of lists).

  ## Examples

      iex> rows = [
      ...>   %{id: 1, name: "Alice", age: 30},
      ...>   %{id: 2, name: "Bob", age: 25}
      ...> ]
      iex> schema = [id: :uint64, name: :string, age: :uint64]
      iex> Natch.Conversion.rows_to_columns(rows, schema)
      %{
        id: [1, 2],
        name: ["Alice", "Bob"],
        age: [30, 25]
      }
  """
  @spec rows_to_columns([map()], schema()) :: map()
  def rows_to_columns(rows, schema) when is_list(rows) and is_list(schema) do
    for {name, _type} <- schema, into: %{} do
      values =
        Enum.map(rows, fn row ->
          # Support both atom and string keys
          Map.get(row, name) || Map.get(row, to_string(name)) ||
            raise ArgumentError, "Missing column #{inspect(name)} in row #{inspect(row)}"
        end)

      {name, values}
    end
  end

  @doc """
  Converts column-oriented data (map of lists) to row-oriented format (list of maps).

  Useful for testing or when you need row-based output.

  ## Examples

      iex> columns = %{
      ...>   id: [1, 2],
      ...>   name: ["Alice", "Bob"],
      ...>   age: [30, 25]
      ...> }
      iex> schema = [id: :uint64, name: :string, age: :uint64]
      iex> Natch.Conversion.columns_to_rows(columns, schema)
      [
        %{id: 1, name: "Alice", age: 30},
        %{id: 2, name: "Bob", age: 25}
      ]
  """
  @spec columns_to_rows(map(), schema()) :: [map()]
  def columns_to_rows(columns, schema) when is_map(columns) and is_list(schema) do
    # Get row count from first column
    column_names = Keyword.keys(schema)
    first_column_name = hd(column_names)
    row_count = length(Map.fetch!(columns, first_column_name))

    # Build rows
    if row_count == 0 do
      []
    else
      for row_idx <- 0..(row_count - 1) do
        for {name, _type} <- schema, into: %{} do
          column_values = Map.fetch!(columns, name)
          {name, Enum.at(column_values, row_idx)}
        end
      end
    end
  end
end
