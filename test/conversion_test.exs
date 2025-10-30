defmodule Chex.ConversionTest do
  use ExUnit.Case, async: true

  alias Chex.Conversion

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

  describe "validate_column_lengths/2" do
    test "returns :ok for matching lengths" do
      columns = %{
        id: [1, 2, 3],
        name: ["Alice", "Bob", "Charlie"]
      }

      schema = [id: :uint64, name: :string]

      assert Conversion.validate_column_lengths(columns, schema) == :ok
    end

    test "returns :ok for empty columns" do
      columns = %{id: [], name: []}
      schema = [id: :uint64, name: :string]

      assert Conversion.validate_column_lengths(columns, schema) == :ok
    end

    test "returns error for mismatched lengths" do
      columns = %{
        id: [1, 2, 3],
        name: ["Alice", "Bob"]
      }

      schema = [id: :uint64, name: :string]

      assert {:error, message} = Conversion.validate_column_lengths(columns, schema)
      assert message =~ "Column length mismatch"
      assert message =~ "id has 3 rows"
      assert message =~ "name has 2 rows"
    end

    test "returns error for missing column" do
      columns = %{id: [1, 2, 3]}
      schema = [id: :uint64, name: :string]

      assert {:error, message} = Conversion.validate_column_lengths(columns, schema)
      assert message =~ "Missing column :name"
    end

    test "returns error for non-list column" do
      columns = %{id: [1, 2, 3], name: "not a list"}
      schema = [id: :uint64, name: :string]

      assert {:error, message} = Conversion.validate_column_lengths(columns, schema)
      assert message =~ "Column :name is not a list"
    end

    test "supports string keys" do
      columns = %{"id" => [1, 2], "name" => ["Alice", "Bob"]}
      schema = [id: :uint64, name: :string]

      assert Conversion.validate_column_lengths(columns, schema) == :ok
    end
  end

  describe "validate_column_types/2" do
    test "returns :ok for valid uint64 values" do
      columns = %{id: [0, 1, 100, 18_446_744_073_709_551_615]}
      schema = [id: :uint64]

      assert Conversion.validate_column_types(columns, schema) == :ok
    end

    test "returns error for negative uint64" do
      columns = %{id: [1, -1, 3]}
      schema = [id: :uint64]

      assert {:error, message} = Conversion.validate_column_types(columns, schema)
      assert message =~ "Column id"
      assert message =~ "expected UInt64"
      assert message =~ "-1"
    end

    test "returns error for non-integer uint64" do
      columns = %{id: [1, "string", 3]}
      schema = [id: :uint64]

      assert {:error, message} = Conversion.validate_column_types(columns, schema)
      assert message =~ "Column id"
      assert message =~ "expected UInt64"
    end

    test "returns :ok for valid int64 values" do
      columns = %{value: [-9_223_372_036_854_775_808, 0, 9_223_372_036_854_775_807]}
      schema = [value: :int64]

      assert Conversion.validate_column_types(columns, schema) == :ok
    end

    test "returns error for non-integer int64" do
      columns = %{value: [1, 2.5, 3]}
      schema = [value: :int64]

      assert {:error, message} = Conversion.validate_column_types(columns, schema)
      assert message =~ "Column value"
      assert message =~ "expected Int64"
      assert message =~ "2.5"
    end

    test "returns :ok for valid string values" do
      columns = %{name: ["Alice", "Bob", "", "Hello 世界 🌍"]}
      schema = [name: :string]

      assert Conversion.validate_column_types(columns, schema) == :ok
    end

    test "returns error for non-string values" do
      columns = %{name: ["Alice", 123, "Bob"]}
      schema = [name: :string]

      assert {:error, message} = Conversion.validate_column_types(columns, schema)
      assert message =~ "Column name"
      assert message =~ "expected String"
      assert message =~ "123"
    end

    test "returns :ok for valid float64 values" do
      columns = %{amount: [1.5, 2.0, -3.14, 0.0, 42]}
      schema = [amount: :float64]

      assert Conversion.validate_column_types(columns, schema) == :ok
    end

    test "returns error for non-numeric float64" do
      columns = %{amount: [1.5, "not a number", 3.14]}
      schema = [amount: :float64]

      assert {:error, message} = Conversion.validate_column_types(columns, schema)
      assert message =~ "Column amount"
      assert message =~ "expected Float64"
    end

    test "returns :ok for valid datetime values" do
      columns = %{
        created_at: [
          ~U[2024-10-29 10:00:00Z],
          1_730_220_600,
          ~U[1970-01-01 00:00:00Z]
        ]
      }

      schema = [created_at: :datetime]

      assert Conversion.validate_column_types(columns, schema) == :ok
    end

    test "returns error for invalid datetime values" do
      columns = %{created_at: [~U[2024-10-29 10:00:00Z], "not a datetime"]}
      schema = [created_at: :datetime]

      assert {:error, message} = Conversion.validate_column_types(columns, schema)
      assert message =~ "Column created_at"
      assert message =~ "expected DateTime"
    end

    test "validates multiple columns" do
      columns = %{
        id: [1, 2],
        name: ["Alice", "Bob"],
        amount: [100.5, 200.75]
      }

      schema = [id: :uint64, name: :string, amount: :float64]

      assert Conversion.validate_column_types(columns, schema) == :ok
    end

    test "stops at first error" do
      columns = %{
        id: [1, "invalid"],
        name: ["Alice", 123]
      }

      schema = [id: :uint64, name: :string]

      # Should error on id before checking name
      assert {:error, message} = Conversion.validate_column_types(columns, schema)
      assert message =~ "Column id"
    end

    test "supports string keys" do
      columns = %{"id" => [1, 2], "name" => ["Alice", "Bob"]}
      schema = [id: :uint64, name: :string]

      assert Conversion.validate_column_types(columns, schema) == :ok
    end
  end

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
