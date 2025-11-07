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
    column_names = Keyword.keys(schema)

    # Initialize empty lists for each column
    initial_acc = Map.new(column_names, fn name -> {name, []} end)

    # Single traversal: accumulate all columns simultaneously
    columns =
      Enum.reduce(rows, initial_acc, fn row, acc ->
        Enum.reduce(column_names, acc, fn name, col_acc ->
          # Support both atom and string keys
          value =
            Map.get(row, name) || Map.get(row, to_string(name)) ||
              raise ArgumentError, "Missing column #{inspect(name)} in row #{inspect(row)}"

          Map.update!(col_acc, name, fn list -> [value | list] end)
        end)
      end)

    # Reverse all columns (we built them backwards for efficiency)
    Map.new(columns, fn {name, values} -> {name, Enum.reverse(values)} end)
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
    column_names = Keyword.keys(schema)

    # Get all column lists upfront
    column_lists = Enum.map(column_names, fn name -> Map.fetch!(columns, name) end)

    # Handle empty case
    if Enum.all?(column_lists, &(&1 == [])) do
      []
    else
      # Zip all columns together and convert to maps - O(M) complexity
      column_lists
      |> Enum.zip()
      |> Enum.map(fn row_tuple ->
        row_tuple
        |> Tuple.to_list()
        |> Enum.zip(column_names)
        |> Map.new(fn {value, name} -> {name, value} end)
      end)
    end
  end
end
