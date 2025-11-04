defmodule Natch.ConversionTest do
  use ExUnit.Case, async: true

  alias Natch.Conversion

  describe "rows_to_columns/2" do
    test "converts single row" do
      rows = [%{id: 1, name: "Alice"}]
      schema = [id: :uint64, name: :string]

      assert Conversion.rows_to_columns(rows, schema) == %{
               id: [1],
               name: ["Alice"]
             }
    end

    test "converts multiple rows" do
      rows = [
        %{id: 1, name: "Alice", age: 30},
        %{id: 2, name: "Bob", age: 25},
        %{id: 3, name: "Charlie", age: 35}
      ]

      schema = [id: :uint64, name: :string, age: :uint64]

      assert Conversion.rows_to_columns(rows, schema) == %{
               id: [1, 2, 3],
               name: ["Alice", "Bob", "Charlie"],
               age: [30, 25, 35]
             }
    end

    test "handles empty list" do
      rows = []
      schema = [id: :uint64, name: :string]

      assert Conversion.rows_to_columns(rows, schema) == %{
               id: [],
               name: []
             }
    end

    test "supports string keys in rows" do
      rows = [
        %{"id" => 1, "name" => "Alice"},
        %{"id" => 2, "name" => "Bob"}
      ]

      schema = [id: :uint64, name: :string]

      assert Conversion.rows_to_columns(rows, schema) == %{
               id: [1, 2],
               name: ["Alice", "Bob"]
             }
    end

    test "raises on missing column" do
      rows = [%{id: 1}]
      schema = [id: :uint64, name: :string]

      assert_raise ArgumentError, ~r/Missing column :name/, fn ->
        Conversion.rows_to_columns(rows, schema)
      end
    end
  end

  describe "columns_to_rows/2" do
    test "converts single row" do
      columns = %{id: [1], name: ["Alice"]}
      schema = [id: :uint64, name: :string]

      assert Conversion.columns_to_rows(columns, schema) == [
               %{id: 1, name: "Alice"}
             ]
    end

    test "converts multiple rows" do
      columns = %{
        id: [1, 2, 3],
        name: ["Alice", "Bob", "Charlie"],
        age: [30, 25, 35]
      }

      schema = [id: :uint64, name: :string, age: :uint64]

      assert Conversion.columns_to_rows(columns, schema) == [
               %{id: 1, name: "Alice", age: 30},
               %{id: 2, name: "Bob", age: 25},
               %{id: 3, name: "Charlie", age: 35}
             ]
    end

    test "handles empty columns" do
      columns = %{id: [], name: []}
      schema = [id: :uint64, name: :string]

      assert Conversion.columns_to_rows(columns, schema) == []
    end

    test "preserves column order from schema" do
      columns = %{
        age: [30, 25],
        name: ["Alice", "Bob"],
        id: [1, 2]
      }

      schema = [id: :uint64, name: :string, age: :uint64]

      result = Conversion.columns_to_rows(columns, schema)

      assert result == [
               %{id: 1, name: "Alice", age: 30},
               %{id: 2, name: "Bob", age: 25}
             ]
    end
  end

  # validate_column_lengths/2 and validate_column_types/2 tests removed
  # These functions are deprecated - validation now happens in Column.append_bulk/2 and FINE NIFs

  describe "roundtrip conversions" do
    test "rows -> columns -> rows preserves data" do
      original_rows = [
        %{id: 1, name: "Alice", amount: 100.5},
        %{id: 2, name: "Bob", amount: 200.75},
        %{id: 3, name: "Charlie", amount: 300.25}
      ]

      schema = [id: :uint64, name: :string, amount: :float64]

      columns = Conversion.rows_to_columns(original_rows, schema)
      result_rows = Conversion.columns_to_rows(columns, schema)

      assert result_rows == original_rows
    end

    test "columns -> rows -> columns preserves data" do
      original_columns = %{
        id: [1, 2, 3],
        name: ["Alice", "Bob", "Charlie"],
        amount: [100.5, 200.75, 300.25]
      }

      schema = [id: :uint64, name: :string, amount: :float64]

      rows = Conversion.columns_to_rows(original_columns, schema)
      result_columns = Conversion.rows_to_columns(rows, schema)

      assert result_columns == original_columns
    end
  end
end
