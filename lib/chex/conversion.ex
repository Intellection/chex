defmodule Chex.Conversion do
  @moduledoc """
  Conversion utilities between row-oriented and column-oriented data formats.

  Provides helpers for users with row-based data sources who need to convert
  to ClickHouse's native columnar format.
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
      iex> Chex.Conversion.rows_to_columns(rows, schema)
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
      iex> Chex.Conversion.columns_to_rows(columns, schema)
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

  @doc """
  Validates that all columns have the same length.

  Returns `:ok` if valid, `{:error, reason}` otherwise.

  ## Examples

      iex> columns = %{id: [1, 2, 3], name: ["Alice", "Bob", "Charlie"]}
      iex> schema = [id: :uint64, name: :string]
      iex> Chex.Conversion.validate_column_lengths(columns, schema)
      :ok

      iex> columns = %{id: [1, 2, 3], name: ["Alice", "Bob"]}
      iex> schema = [id: :uint64, name: :string]
      iex> Chex.Conversion.validate_column_lengths(columns, schema)
      {:error, "Column length mismatch: id has 3 rows, name has 2 rows"}
  """
  @spec validate_column_lengths(map(), schema()) :: :ok | {:error, String.t()}
  def validate_column_lengths(columns, schema) when is_map(columns) and is_list(schema) do
    lengths =
      for {name, _type} <- schema do
        # Support both atom and string keys
        column_values = Map.get(columns, name) || Map.get(columns, to_string(name))

        cond do
          column_values == nil ->
            throw({:error, "Missing column #{inspect(name)}"})

          not is_list(column_values) ->
            throw({:error, "Column #{inspect(name)} is not a list"})

          true ->
            {name, length(column_values)}
        end
      end

    case Enum.uniq_by(lengths, fn {_name, len} -> len end) do
      [{_name, _len}] ->
        :ok

      _multiple_lengths ->
        length_desc =
          lengths
          |> Enum.map(fn {name, len} -> "#{name} has #{len} rows" end)
          |> Enum.join(", ")

        {:error, "Column length mismatch: #{length_desc}"}
    end
  catch
    {:error, reason} -> {:error, reason}
  end

  @doc """
  Validates that column values match their declared types.

  Returns `:ok` if valid, `{:error, reason}` otherwise.

  ## Examples

      iex> columns = %{id: [1, 2, 3], name: ["Alice", "Bob", "Charlie"]}
      iex> schema = [id: :uint64, name: :string]
      iex> Chex.Conversion.validate_column_types(columns, schema)
      :ok

      iex> columns = %{id: ["not", "numbers"], name: ["Alice", "Bob"]}
      iex> schema = [id: :uint64, name: :string]
      iex> Chex.Conversion.validate_column_types(columns, schema)
      {:error, "Column id: expected UInt64 but got invalid value: \\"not\\""}
  """
  @spec validate_column_types(map(), schema()) :: :ok | {:error, String.t()}
  def validate_column_types(columns, schema) when is_map(columns) and is_list(schema) do
    Enum.reduce_while(schema, :ok, fn {name, type}, _acc ->
      # Support both atom and string keys
      column_values = Map.get(columns, name) || Map.get(columns, to_string(name)) || []

      case validate_values_for_type(column_values, type) do
        :ok ->
          {:cont, :ok}

        {:error, value} ->
          type_name = type_to_name(type)

          {:halt,
           {:error,
            "Column #{name}: expected #{type_name} but got invalid value: #{inspect(value)}"}}
      end
    end)
  end

  # Private helpers

  defp validate_values_for_type(values, :uint64) do
    case Enum.find(values, fn val -> not (is_integer(val) and val >= 0) end) do
      nil -> :ok
      invalid -> {:error, invalid}
    end
  end

  defp validate_values_for_type(values, :int64) do
    case Enum.find(values, fn val -> not is_integer(val) end) do
      nil -> :ok
      invalid -> {:error, invalid}
    end
  end

  defp validate_values_for_type(values, :string) do
    case Enum.find(values, fn val -> not is_binary(val) end) do
      nil -> :ok
      invalid -> {:error, invalid}
    end
  end

  defp validate_values_for_type(values, :float64) do
    case Enum.find(values, fn val -> not (is_float(val) or is_integer(val)) end) do
      nil -> :ok
      invalid -> {:error, invalid}
    end
  end

  defp validate_values_for_type(values, :datetime) do
    case Enum.find(values, fn
           %DateTime{} -> false
           val when is_integer(val) -> false
           _ -> true
         end) do
      nil -> :ok
      invalid -> {:error, invalid}
    end
  end

  defp validate_values_for_type(_values, type) do
    {:error, "Unsupported type: #{inspect(type)}"}
  end

  defp type_to_name(:uint64), do: "UInt64"
  defp type_to_name(:int64), do: "Int64"
  defp type_to_name(:string), do: "String"
  defp type_to_name(:float64), do: "Float64"
  defp type_to_name(:datetime), do: "DateTime"
  defp type_to_name(type), do: inspect(type)
end
