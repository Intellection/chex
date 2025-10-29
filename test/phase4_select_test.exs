defmodule Chex.Phase4SelectTest do
  use ExUnit.Case, async: false

  @moduletag :phase4

  alias Chex.{Connection, Insert}

  setup do
    # Start connection
    {:ok, conn} = Connection.start_link(host: "localhost", port: 9000)

    # Clean up any existing test table
    try do
      Connection.execute(conn, "DROP TABLE IF EXISTS chex_test_phase4")
    rescue
      _ -> :ok
    end

    on_exit(fn ->
      if Process.alive?(conn) do
        try do
          Connection.execute(conn, "DROP TABLE IF EXISTS chex_test_phase4")
        rescue
          _ -> :ok
        end

        GenServer.stop(conn)
      end
    end)

    {:ok, conn: conn}
  end

  describe "SELECT operations" do
    test "can select from empty table", %{conn: conn} do
      # Create table
      Connection.execute(conn, """
      CREATE TABLE chex_test_phase4 (
        id UInt64,
        name String
      ) ENGINE = Memory
      """)

      # Query empty table
      assert {:ok, []} = Connection.select(conn, "SELECT * FROM chex_test_phase4")
    end

    test "can select single row", %{conn: conn} do
      # Create and populate table
      Connection.execute(conn, """
      CREATE TABLE chex_test_phase4 (
        id UInt64,
        name String
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, name: :string]
      rows = [%{id: 1, name: "Alice"}]
      Insert.insert(conn, "chex_test_phase4", rows, schema)

      # Query
      assert {:ok, result} = Connection.select(conn, "SELECT id, name FROM chex_test_phase4")
      assert length(result) == 1
      assert [%{id: 1, name: "Alice"}] = result
    end

    test "can select multiple rows", %{conn: conn} do
      # Create and populate table
      Connection.execute(conn, """
      CREATE TABLE chex_test_phase4 (
        id UInt64,
        name String
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, name: :string]

      rows = [
        %{id: 1, name: "Alice"},
        %{id: 2, name: "Bob"},
        %{id: 3, name: "Charlie"}
      ]

      Insert.insert(conn, "chex_test_phase4", rows, schema)

      # Query
      assert {:ok, result} = Connection.select(conn, "SELECT id, name FROM chex_test_phase4")
      assert length(result) == 3

      assert Enum.any?(result, fn r -> r.id == 1 && r.name == "Alice" end)
      assert Enum.any?(result, fn r -> r.id == 2 && r.name == "Bob" end)
      assert Enum.any?(result, fn r -> r.id == 3 && r.name == "Charlie" end)
    end

    test "can select with WHERE clause", %{conn: conn} do
      # Create and populate table
      Connection.execute(conn, """
      CREATE TABLE chex_test_phase4 (
        id UInt64,
        name String
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, name: :string]

      rows = [
        %{id: 1, name: "Alice"},
        %{id: 2, name: "Bob"},
        %{id: 3, name: "Charlie"}
      ]

      Insert.insert(conn, "chex_test_phase4", rows, schema)

      # Query with WHERE
      assert {:ok, result} =
               Connection.select(conn, "SELECT * FROM chex_test_phase4 WHERE id = 2")

      assert length(result) == 1
      assert [%{id: 2, name: "Bob"}] = result
    end

    test "can select all supported types", %{conn: conn} do
      # Create table
      Connection.execute(conn, """
      CREATE TABLE chex_test_phase4 (
        id UInt64,
        value Int64,
        name String,
        amount Float64,
        created_at DateTime
      ) ENGINE = Memory
      """)

      # Insert data
      schema = [
        id: :uint64,
        value: :int64,
        name: :string,
        amount: :float64,
        created_at: :datetime
      ]

      rows = [
        %{
          id: 1,
          value: -42,
          name: "Test",
          amount: 99.99,
          created_at: ~U[2024-10-29 10:00:00Z]
        }
      ]

      Insert.insert(conn, "chex_test_phase4", rows, schema)

      # Query
      assert {:ok, [result]} =
               Connection.select(
                 conn,
                 "SELECT id, value, name, amount, created_at FROM chex_test_phase4"
               )

      assert result.id == 1
      assert result.value == -42
      assert result.name == "Test"
      assert_in_delta result.amount, 99.99, 0.01

      # DateTime comes back as Unix timestamp
      expected_ts = DateTime.to_unix(~U[2024-10-29 10:00:00Z])
      assert result.created_at == expected_ts
    end

    test "can select with ORDER BY", %{conn: conn} do
      # Create and populate table
      Connection.execute(conn, """
      CREATE TABLE chex_test_phase4 (
        id UInt64,
        name String
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, name: :string]

      rows = [
        %{id: 3, name: "Charlie"},
        %{id: 1, name: "Alice"},
        %{id: 2, name: "Bob"}
      ]

      Insert.insert(conn, "chex_test_phase4", rows, schema)

      # Query with ORDER BY
      assert {:ok, result} =
               Connection.select(conn, "SELECT * FROM chex_test_phase4 ORDER BY id ASC")

      assert length(result) == 3
      assert Enum.at(result, 0).id == 1
      assert Enum.at(result, 1).id == 2
      assert Enum.at(result, 2).id == 3
    end

    test "can select with LIMIT", %{conn: conn} do
      # Create and populate table
      Connection.execute(conn, """
      CREATE TABLE chex_test_phase4 (
        id UInt64,
        name String
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, name: :string]

      rows = [
        %{id: 1, name: "Alice"},
        %{id: 2, name: "Bob"},
        %{id: 3, name: "Charlie"}
      ]

      Insert.insert(conn, "chex_test_phase4", rows, schema)

      # Query with LIMIT
      assert {:ok, result} = Connection.select(conn, "SELECT * FROM chex_test_phase4 LIMIT 2")
      assert length(result) == 2
    end

    test "can select specific columns", %{conn: conn} do
      # Create and populate table
      Connection.execute(conn, """
      CREATE TABLE chex_test_phase4 (
        id UInt64,
        name String,
        amount Float64
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, name: :string, amount: :float64]
      rows = [%{id: 1, name: "Alice", amount: 100.5}]
      Insert.insert(conn, "chex_test_phase4", rows, schema)

      # Query specific columns
      assert {:ok, [result]} = Connection.select(conn, "SELECT name FROM chex_test_phase4")
      assert result.name == "Alice"
      refute Map.has_key?(result, :id)
      refute Map.has_key?(result, :amount)
    end

    test "can select with aggregate functions", %{conn: conn} do
      # Create and populate table
      Connection.execute(conn, """
      CREATE TABLE chex_test_phase4 (
        id UInt64,
        amount Float64
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, amount: :float64]

      rows = [
        %{id: 1, amount: 100.0},
        %{id: 2, amount: 200.0},
        %{id: 3, amount: 300.0}
      ]

      Insert.insert(conn, "chex_test_phase4", rows, schema)

      # Query with COUNT
      assert {:ok, [result]} =
               Connection.select(conn, "SELECT count() as cnt FROM chex_test_phase4")

      assert result.cnt == 3

      # Query with SUM
      assert {:ok, [result]} =
               Connection.select(conn, "SELECT sum(amount) as total FROM chex_test_phase4")

      assert_in_delta result.total, 600.0, 0.01
    end

    test "can handle large result sets", %{conn: conn} do
      # Create table
      Connection.execute(conn, """
      CREATE TABLE chex_test_phase4 (
        id UInt64,
        value UInt64
      ) ENGINE = Memory
      """)

      # Insert 10k rows
      rows =
        for i <- 1..10_000 do
          %{id: i, value: i * 2}
        end

      schema = [id: :uint64, value: :uint64]
      Insert.insert(conn, "chex_test_phase4", rows, schema)

      # Query all
      assert {:ok, result} = Connection.select(conn, "SELECT * FROM chex_test_phase4")
      assert length(result) == 10_000

      # Verify a few rows
      assert Enum.any?(result, fn r -> r.id == 1 && r.value == 2 end)
      assert Enum.any?(result, fn r -> r.id == 5000 && r.value == 10_000 end)
      assert Enum.any?(result, fn r -> r.id == 10_000 && r.value == 20_000 end)
    end

    test "returns error for invalid query", %{conn: conn} do
      result = Connection.select(conn, "SELECT * FROM nonexistent_table")
      assert {:error, _reason} = result
    end
  end

  describe "Complete insert/query cycle" do
    test "can insert and query back all types", %{conn: conn} do
      # Create table
      Connection.execute(conn, """
      CREATE TABLE chex_test_phase4 (
        id UInt64,
        value Int64,
        name String,
        amount Float64,
        created_at DateTime
      ) ENGINE = Memory
      """)

      # Insert data
      schema = [
        id: :uint64,
        value: :int64,
        name: :string,
        amount: :float64,
        created_at: :datetime
      ]

      insert_rows = [
        %{
          id: 1,
          value: -42,
          name: "First",
          amount: 99.99,
          created_at: ~U[2024-10-29 10:00:00Z]
        },
        %{
          id: 2,
          value: 123,
          name: "Second",
          amount: 456.78,
          created_at: ~U[2024-10-29 11:00:00Z]
        }
      ]

      assert :ok = Insert.insert(conn, "chex_test_phase4", insert_rows, schema)

      # Query back
      assert {:ok, select_rows} =
               Connection.select(conn, "SELECT * FROM chex_test_phase4 ORDER BY id")

      assert length(select_rows) == 2

      # Verify first row
      row1 = Enum.at(select_rows, 0)
      assert row1.id == 1
      assert row1.value == -42
      assert row1.name == "First"
      assert_in_delta row1.amount, 99.99, 0.01
      assert row1.created_at == DateTime.to_unix(~U[2024-10-29 10:00:00Z])

      # Verify second row
      row2 = Enum.at(select_rows, 1)
      assert row2.id == 2
      assert row2.value == 123
      assert row2.name == "Second"
      assert_in_delta row2.amount, 456.78, 0.01
      assert row2.created_at == DateTime.to_unix(~U[2024-10-29 11:00:00Z])
    end
  end
end
