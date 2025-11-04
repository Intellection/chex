defmodule Natch.NestingIntegrationTest do
  use ExUnit.Case, async: true

  alias Natch.Connection

  setup do
    # Generate unique table name for this test
    table = "test_#{System.unique_integer([:positive, :monotonic])}_#{:rand.uniform(999_999)}"

    # Start connection
    {:ok, conn} = Connection.start_link(host: "localhost", port: 9000)

    on_exit(fn ->
      # Clean up test table if it exists
      if Process.alive?(conn) do
        try do
          Connection.execute(conn, "DROP TABLE IF EXISTS #{table}")
        catch
          :exit, _ -> :ok
        end

        # Use Process.exit to avoid race conditions
        Process.exit(conn, :normal)
      end
    end)

    {:ok, conn: conn, table: table}
  end

  describe "Array(Nullable(T)) roundtrip" do
    test "Array(Nullable(String)) with mixed nulls", %{conn: conn, table: table} do
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        tags Array(Nullable(String))
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, tags: {:array, {:nullable, :string}}]

      columns = %{
        id: [1, 2, 3],
        tags: [
          ["hello", nil, "world"],
          [nil, "test"],
          ["foo", "bar", nil]
        ]
      }

      assert :ok = Natch.insert(conn, table, columns, schema)

      # SELECT and verify
      assert {:ok, result} = Connection.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert length(result) == 3

      assert result == [
               %{id: 1, tags: ["hello", nil, "world"]},
               %{id: 2, tags: [nil, "test"]},
               %{id: 3, tags: ["foo", "bar", nil]}
             ]
    end

    test "Array(Nullable(UInt64)) with all nulls", %{conn: conn, table: table} do
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        values Array(Nullable(UInt64))
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, values: {:array, {:nullable, :uint64}}]

      columns = %{
        id: [1, 2],
        values: [
          [100, nil, 200],
          [nil, nil]
        ]
      }

      assert :ok = Natch.insert(conn, table, columns, schema)

      assert {:ok, result} = Connection.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert length(result) == 2

      assert result == [
               %{id: 1, values: [100, nil, 200]},
               %{id: 2, values: [nil, nil]}
             ]
    end
  end

  describe "LowCardinality(Nullable(String)) roundtrip" do
    test "with interspersed nulls and duplicates", %{conn: conn, table: table} do
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        status LowCardinality(Nullable(String))
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, status: {:low_cardinality, {:nullable, :string}}]

      columns = %{
        id: [1, 2, 3, 4, 5],
        status: ["active", nil, "inactive", "active", nil]
      }

      assert :ok = Natch.insert(conn, table, columns, schema)

      assert {:ok, result} = Connection.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert length(result) == 5

      assert result == [
               %{id: 1, status: "active"},
               %{id: 2, status: nil},
               %{id: 3, status: "inactive"},
               %{id: 4, status: "active"},
               %{id: 5, status: nil}
             ]
    end
  end

  describe "Tuple with Nullable elements roundtrip" do
    test "Tuple(Nullable(String), UInt64)", %{conn: conn, table: table} do
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        data Tuple(Nullable(String), UInt64)
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, data: {:tuple, [{:nullable, :string}, :uint64]}]

      columns = %{
        id: [1, 2, 3],
        data: [
          {nil, 100},
          {"test", 200},
          {nil, 300}
        ]
      }

      assert :ok = Natch.insert(conn, table, columns, schema)

      assert {:ok, result} = Connection.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert length(result) == 3

      assert result == [
               %{id: 1, data: {nil, 100}},
               %{id: 2, data: {"test", 200}},
               %{id: 3, data: {nil, 300}}
             ]
    end
  end

  describe "Map with Nullable values roundtrip" do
    test "Map(String, Nullable(UInt64))", %{conn: conn, table: table} do
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        metrics Map(String, Nullable(UInt64))
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, metrics: {:map, :string, {:nullable, :uint64}}]

      columns = %{
        id: [1, 2],
        metrics: [
          %{"count" => 100, "missing" => nil},
          %{"total" => nil}
        ]
      }

      assert :ok = Natch.insert(conn, table, columns, schema)

      assert {:ok, result} = Connection.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert length(result) == 2

      assert result == [
               %{id: 1, metrics: %{"count" => 100, "missing" => nil}},
               %{id: 2, metrics: %{"total" => nil}}
             ]
    end
  end

  describe "Array(LowCardinality(String)) roundtrip" do
    test "with duplicated values", %{conn: conn, table: table} do
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        tags Array(LowCardinality(String))
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, tags: {:array, {:low_cardinality, :string}}]

      columns = %{
        id: [1, 2, 3],
        tags: [
          ["apple", "banana", "apple"],
          ["cherry", "banana"],
          ["apple", "cherry"]
        ]
      }

      assert :ok = Natch.insert(conn, table, columns, schema)

      assert {:ok, result} = Connection.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert length(result) == 3

      assert result == [
               %{id: 1, tags: ["apple", "banana", "apple"]},
               %{id: 2, tags: ["cherry", "banana"]},
               %{id: 3, tags: ["apple", "cherry"]}
             ]
    end
  end

  describe "Array(LowCardinality(Nullable(String))) roundtrip - triple wrapper!" do
    test "dictionary encoding with nulls", %{conn: conn, table: table} do
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        tags Array(LowCardinality(Nullable(String)))
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, tags: {:array, {:low_cardinality, {:nullable, :string}}}]

      columns = %{
        id: [1, 2],
        tags: [
          ["apple", nil, "banana"],
          [nil, "apple", "cherry"]
        ]
      }

      assert :ok = Natch.insert(conn, table, columns, schema)

      assert {:ok, result} = Connection.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert length(result) == 2

      assert result == [
               %{id: 1, tags: ["apple", nil, "banana"]},
               %{id: 2, tags: [nil, "apple", "cherry"]}
             ]
    end
  end

  describe "Map(String, Array(UInt64)) roundtrip" do
    test "arrays as map values", %{conn: conn, table: table} do
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        data Map(String, Array(UInt64))
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, data: {:map, :string, {:array, :uint64}}]

      columns = %{
        id: [1, 2],
        data: [
          %{"ids" => [1, 2, 3], "counts" => [10, 20]},
          %{"values" => [100, 200, 300]}
        ]
      }

      assert :ok = Natch.insert(conn, table, columns, schema)

      assert {:ok, result} = Connection.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert length(result) == 2

      assert result == [
               %{id: 1, data: %{"ids" => [1, 2, 3], "counts" => [10, 20]}},
               %{id: 2, data: %{"values" => [100, 200, 300]}}
             ]
    end
  end

  describe "Tuple(String, Array(UInt64)) roundtrip" do
    test "array as tuple element", %{conn: conn, table: table} do
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        data Tuple(String, Array(UInt64))
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, data: {:tuple, [:string, {:array, :uint64}]}]

      columns = %{
        id: [1, 2],
        data: [
          {"Alice", [100, 200, 300]},
          {"Bob", [50]}
        ]
      }

      assert :ok = Natch.insert(conn, table, columns, schema)

      assert {:ok, result} = Connection.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert length(result) == 2

      assert result == [
               %{id: 1, data: {"Alice", [100, 200, 300]}},
               %{id: 2, data: {"Bob", [50]}}
             ]
    end
  end

  describe "Array(Array(Nullable(UInt64))) roundtrip - triple nesting" do
    test "with nulls at innermost level", %{conn: conn, table: table} do
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        matrix Array(Array(Nullable(UInt64)))
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, matrix: {:array, {:array, {:nullable, :uint64}}}]

      columns = %{
        id: [1, 2],
        matrix: [
          [[1, nil, 3], [nil, 5]],
          [[10, 20], [], [nil]]
        ]
      }

      assert :ok = Natch.insert(conn, table, columns, schema)

      assert {:ok, result} = Connection.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert length(result) == 2

      assert result == [
               %{id: 1, matrix: [[1, nil, 3], [nil, 5]]},
               %{id: 2, matrix: [[10, 20], [], [nil]]}
             ]
    end
  end

  describe "Array(Array(Array(UInt64))) roundtrip - triple nesting" do
    test "deep nesting stress test", %{conn: conn, table: table} do
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        data Array(Array(Array(UInt64)))
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, data: {:array, {:array, {:array, :uint64}}}]

      columns = %{
        id: [1, 2],
        data: [
          [[[1, 2], [3]], [[4]]],
          [[[5, 6, 7]], [[8]]]
        ]
      }

      assert :ok = Natch.insert(conn, table, columns, schema)

      assert {:ok, result} = Connection.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert length(result) == 2

      assert result == [
               %{id: 1, data: [[[1, 2], [3]], [[4]]]},
               %{id: 2, data: [[[5, 6, 7]], [[8]]]}
             ]
    end
  end

  describe "Array(Enum8) roundtrip" do
    test "enums in arrays", %{conn: conn, table: table} do
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        sizes Array(Enum8('small' = 1, 'medium' = 2, 'large' = 3))
      ) ENGINE = Memory
      """)

      schema = [
        id: :uint64,
        sizes: {:array, {:enum8, [{"small", 1}, {"medium", 2}, {"large", 3}]}}
      ]

      columns = %{
        id: [1, 2],
        sizes: [
          ["small", "large"],
          ["medium", "small", "large"]
        ]
      }

      assert :ok = Natch.insert(conn, table, columns, schema)

      assert {:ok, result} = Connection.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert length(result) == 2

      assert result == [
               %{id: 1, sizes: ["small", "large"]},
               %{id: 2, sizes: ["medium", "small", "large"]}
             ]
    end
  end

  describe "Tuple with Enum8 element roundtrip" do
    test "Tuple(Enum8, UInt64)", %{conn: conn, table: table} do
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        data Tuple(Enum8('low' = 1, 'high' = 2), UInt64)
      ) ENGINE = Memory
      """)

      schema = [
        id: :uint64,
        data: {:tuple, [{:enum8, [{"low", 1}, {"high", 2}]}, :uint64]}
      ]

      columns = %{
        id: [1, 2, 3],
        data: [
          {"low", 100},
          {"high", 200},
          {"low", 150}
        ]
      }

      assert :ok = Natch.insert(conn, table, columns, schema)

      assert {:ok, result} = Connection.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert length(result) == 3

      assert result == [
               %{id: 1, data: {"low", 100}},
               %{id: 2, data: {"high", 200}},
               %{id: 3, data: {"low", 150}}
             ]
    end
  end

  describe "Map(String, Enum16) roundtrip" do
    test "enum as map value", %{conn: conn, table: table} do
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        ranks Map(String, Enum16('bronze' = 100, 'silver' = 200, 'gold' = 300))
      ) ENGINE = Memory
      """)

      schema = [
        id: :uint64,
        ranks: {:map, :string, {:enum16, [{"bronze", 100}, {"silver", 200}, {"gold", 300}]}}
      ]

      columns = %{
        id: [1, 2],
        ranks: [
          %{"player1" => "gold", "player2" => "silver"},
          %{"player3" => "bronze"}
        ]
      }

      assert :ok = Natch.insert(conn, table, columns, schema)

      assert {:ok, result} = Connection.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert length(result) == 2

      assert result == [
               %{id: 1, ranks: %{"player1" => "gold", "player2" => "silver"}},
               %{id: 2, ranks: %{"player3" => "bronze"}}
             ]
    end
  end
end
