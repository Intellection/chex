defmodule Natch.QueryTest do
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

  describe "SELECT operations" do
    test "can select from empty table", %{conn: conn, table: table} do
      # Create table
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        name String
      ) ENGINE = Memory
      """)

      # Query empty table
      assert {:ok, []} = Connection.select_rows(conn, "SELECT * FROM #{table}")
    end

    test "can select single row", %{conn: conn, table: table} do
      # Create and populate table
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        name String
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, name: :string]
      columns = %{id: [1], name: ["Alice"]}
      Natch.insert(conn, "#{table}", columns, schema)

      # Query
      assert {:ok, result} = Connection.select_rows(conn, "SELECT id, name FROM #{table}")
      assert length(result) == 1
      assert [%{id: 1, name: "Alice"}] = result
    end

    test "can select multiple rows", %{conn: conn, table: table} do
      # Create and populate table
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        name String
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, name: :string]

      columns = %{
        id: [1, 2, 3],
        name: ["Alice", "Bob", "Charlie"]
      }

      Natch.insert(conn, "#{table}", columns, schema)

      # Query
      assert {:ok, result} = Connection.select_rows(conn, "SELECT id, name FROM #{table}")
      assert length(result) == 3

      assert Enum.any?(result, fn r -> r.id == 1 && r.name == "Alice" end)
      assert Enum.any?(result, fn r -> r.id == 2 && r.name == "Bob" end)
      assert Enum.any?(result, fn r -> r.id == 3 && r.name == "Charlie" end)
    end

    test "can select with WHERE clause", %{conn: conn, table: table} do
      # Create and populate table
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        name String
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, name: :string]

      columns = %{
        id: [1, 2, 3],
        name: ["Alice", "Bob", "Charlie"]
      }

      Natch.insert(conn, "#{table}", columns, schema)

      # Query with WHERE
      assert {:ok, result} =
               Connection.select_rows(conn, "SELECT * FROM #{table} WHERE id = 2")

      assert length(result) == 1
      assert [%{id: 2, name: "Bob"}] = result
    end

    test "can select all supported types", %{conn: conn, table: table} do
      # Create table
      Connection.execute(conn, """
      CREATE TABLE #{table} (
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

      columns = %{
        id: [1],
        value: [-42],
        name: ["Test"],
        amount: [99.99],
        created_at: [~U[2024-10-29 10:00:00Z]]
      }

      Natch.insert(conn, "#{table}", columns, schema)

      # Query
      assert {:ok, [result]} =
               Connection.select_rows(
                 conn,
                 "SELECT id, value, name, amount, created_at FROM #{table}"
               )

      assert result.id == 1
      assert result.value == -42
      assert result.name == "Test"
      assert_in_delta result.amount, 99.99, 0.01

      # DateTime comes back as Unix timestamp
      expected_ts = DateTime.to_unix(~U[2024-10-29 10:00:00Z])
      assert result.created_at == expected_ts
    end

    test "can select with ORDER BY", %{conn: conn, table: table} do
      # Create and populate table
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        name String
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, name: :string]

      columns = %{
        id: [3, 1, 2],
        name: ["Charlie", "Alice", "Bob"]
      }

      Natch.insert(conn, "#{table}", columns, schema)

      # Query with ORDER BY
      assert {:ok, result} =
               Connection.select_rows(conn, "SELECT * FROM #{table} ORDER BY id ASC")

      assert length(result) == 3
      assert Enum.at(result, 0).id == 1
      assert Enum.at(result, 1).id == 2
      assert Enum.at(result, 2).id == 3
    end

    test "can select with LIMIT", %{conn: conn, table: table} do
      # Create and populate table
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        name String
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, name: :string]

      columns = %{
        id: [1, 2, 3],
        name: ["Alice", "Bob", "Charlie"]
      }

      Natch.insert(conn, "#{table}", columns, schema)

      # Query with LIMIT
      assert {:ok, result} = Connection.select_rows(conn, "SELECT * FROM #{table} LIMIT 2")
      assert length(result) == 2
    end

    test "can select specific columns", %{conn: conn, table: table} do
      # Create and populate table
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        name String,
        amount Float64
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, name: :string, amount: :float64]
      columns = %{id: [1], name: ["Alice"], amount: [100.5]}
      Natch.insert(conn, "#{table}", columns, schema)

      # Query specific columns
      assert {:ok, [result]} = Connection.select_rows(conn, "SELECT name FROM #{table}")
      assert result.name == "Alice"
      refute Map.has_key?(result, :id)
      refute Map.has_key?(result, :amount)
    end

    test "can select with aggregate functions", %{conn: conn, table: table} do
      # Create and populate table
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        amount Float64
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, amount: :float64]

      columns = %{
        id: [1, 2, 3],
        amount: [100.0, 200.0, 300.0]
      }

      Natch.insert(conn, "#{table}", columns, schema)

      # Query with COUNT
      assert {:ok, [result]} =
               Connection.select_rows(conn, "SELECT count() as cnt FROM #{table}")

      assert result.cnt == 3

      # Query with SUM
      assert {:ok, [result]} =
               Connection.select_rows(conn, "SELECT sum(amount) as total FROM #{table}")

      assert_in_delta result.total, 600.0, 0.01
    end

    test "can handle large result sets", %{conn: conn, table: table} do
      # Create table
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        value UInt64
      ) ENGINE = Memory
      """)

      # Insert 10k rows
      columns = %{
        id: Enum.to_list(1..10_000),
        value: Enum.map(1..10_000, &(&1 * 2))
      }

      schema = [id: :uint64, value: :uint64]
      Natch.insert(conn, "#{table}", columns, schema)

      # Query all
      assert {:ok, result} = Connection.select_rows(conn, "SELECT * FROM #{table}")
      assert length(result) == 10_000

      # Verify a few rows
      assert Enum.any?(result, fn r -> r.id == 1 && r.value == 2 end)
      assert Enum.any?(result, fn r -> r.id == 5000 && r.value == 10_000 end)
      assert Enum.any?(result, fn r -> r.id == 10_000 && r.value == 20_000 end)
    end

    test "returns error for invalid query", %{conn: conn, table: _table} do
      result = Connection.select_rows(conn, "SELECT * FROM nonexistent_table")
      assert {:error, _reason} = result
    end
  end

  describe "Complete insert/query cycle" do
    test "can insert and query back all types", %{conn: conn, table: table} do
      # Create table
      Connection.execute(conn, """
      CREATE TABLE #{table} (
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

      columns = %{
        id: [1, 2],
        value: [-42, 123],
        name: ["First", "Second"],
        amount: [99.99, 456.78],
        created_at: [~U[2024-10-29 10:00:00Z], ~U[2024-10-29 11:00:00Z]]
      }

      assert :ok = Natch.insert(conn, "#{table}", columns, schema)

      # Query back
      assert {:ok, select_rows} =
               Connection.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")

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

  describe "New column types integration" do
    test "can insert and query Bool values", %{conn: conn, table: table} do
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        is_active Bool
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, is_active: :bool]
      columns = %{id: [1, 2, 3], is_active: [true, false, true]}
      assert :ok = Natch.insert(conn, table, columns, schema)

      {:ok, result} = Connection.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert length(result) == 3
      assert [%{id: 1, is_active: 1}, %{id: 2, is_active: 0}, %{id: 3, is_active: 1}] = result
    end

    test "can insert and query Date values", %{conn: conn, table: table} do
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        event_date Date
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, event_date: :date]
      columns = %{id: [1, 2], event_date: [~D[2024-01-15], ~D[2024-12-31]]}
      assert :ok = Natch.insert(conn, table, columns, schema)

      {:ok, result} = Connection.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert length(result) == 2

      # Date is returned as days since epoch (uint16)
      assert result |> Enum.at(0) |> Map.get(:event_date) ==
               Date.diff(~D[2024-01-15], ~D[1970-01-01])

      assert result |> Enum.at(1) |> Map.get(:event_date) ==
               Date.diff(~D[2024-12-31], ~D[1970-01-01])
    end

    test "can insert and query Float32 values", %{conn: conn, table: table} do
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        price Float32
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, price: :float32]
      columns = %{id: [1, 2], price: [19.99, -5.5]}
      assert :ok = Natch.insert(conn, table, columns, schema)

      {:ok, result} = Connection.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert length(result) == 2
      assert_in_delta Enum.at(result, 0).price, 19.99, 0.01
      assert_in_delta Enum.at(result, 1).price, -5.5, 0.01
    end

    test "can insert and query UInt32 values", %{conn: conn, table: table} do
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        count UInt32
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, count: :uint32]
      columns = %{id: [1, 2, 3], count: [0, 1000, 4_294_967_295]}
      assert :ok = Natch.insert(conn, table, columns, schema)

      {:ok, result} = Connection.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert [%{count: 0}, %{count: 1000}, %{count: 4_294_967_295}] = result
    end

    test "can insert and query UInt16 values", %{conn: conn, table: table} do
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        port UInt16
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, port: :uint16]
      columns = %{id: [1, 2, 3], port: [0, 8080, 65_535]}
      assert :ok = Natch.insert(conn, table, columns, schema)

      {:ok, result} = Connection.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert [%{port: 0}, %{port: 8080}, %{port: 65_535}] = result
    end

    test "can insert and query Int32 values", %{conn: conn, table: table} do
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        temperature Int32
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, temperature: :int32]
      columns = %{id: [1, 2, 3], temperature: [-2_147_483_648, 0, 2_147_483_647]}
      assert :ok = Natch.insert(conn, table, columns, schema)

      {:ok, result} = Connection.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")

      assert [%{temperature: -2_147_483_648}, %{temperature: 0}, %{temperature: 2_147_483_647}] =
               result
    end

    test "can insert and query Int16 values", %{conn: conn, table: table} do
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        offset Int16
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, offset: :int16]
      columns = %{id: [1, 2, 3], offset: [-32_768, 0, 32_767]}
      assert :ok = Natch.insert(conn, table, columns, schema)

      {:ok, result} = Connection.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert [%{offset: -32_768}, %{offset: 0}, %{offset: 32_767}] = result
    end

    test "can insert and query Int8 values", %{conn: conn, table: table} do
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        delta Int8
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, delta: :int8]
      columns = %{id: [1, 2, 3], delta: [-128, 0, 127]}
      assert :ok = Natch.insert(conn, table, columns, schema)

      {:ok, result} = Connection.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert [%{delta: -128}, %{delta: 0}, %{delta: 127}] = result
    end

    test "can insert and query mixed new types", %{conn: conn, table: table} do
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        is_enabled Bool,
        launch_date Date,
        price Float32,
        count UInt32,
        port UInt16,
        temp Int32,
        offset Int16,
        delta Int8
      ) ENGINE = Memory
      """)

      schema = [
        id: :uint64,
        is_enabled: :bool,
        launch_date: :date,
        price: :float32,
        count: :uint32,
        port: :uint16,
        temp: :int32,
        offset: :int16,
        delta: :int8
      ]

      columns = %{
        id: [1, 2],
        is_enabled: [true, false],
        launch_date: [~D[2024-01-01], ~D[2024-12-31]],
        price: [99.99, 19.99],
        count: [1000, 2000],
        port: [8080, 3000],
        temp: [20, -5],
        offset: [100, -50],
        delta: [10, -10]
      }

      assert :ok = Natch.insert(conn, table, columns, schema)

      {:ok, result} = Connection.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert length(result) == 2

      # Verify first row
      row1 = Enum.at(result, 0)
      assert row1.id == 1
      assert row1.is_enabled == 1
      assert row1.launch_date == Date.diff(~D[2024-01-01], ~D[1970-01-01])
      assert_in_delta row1.price, 99.99, 0.01
      assert row1.count == 1000
      assert row1.port == 8080
      assert row1.temp == 20
      assert row1.offset == 100
      assert row1.delta == 10
    end

    test "can insert and query UUID values", %{conn: conn, table: table} do
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        user_id UUID
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, user_id: :uuid]

      columns = %{
        id: [1, 2, 3],
        user_id: [
          "550e8400-e29b-41d4-a716-446655440000",
          "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
          "6ba7b811-9dad-11d1-80b4-00c04fd430c8"
        ]
      }

      assert :ok = Natch.insert(conn, table, columns, schema)

      {:ok, result} = Connection.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert length(result) == 3

      # Verify UUIDs are returned as strings
      assert result |> Enum.at(0) |> Map.get(:user_id) == "550e8400-e29b-41d4-a716-446655440000"
      assert result |> Enum.at(1) |> Map.get(:user_id) == "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
      assert result |> Enum.at(2) |> Map.get(:user_id) == "6ba7b811-9dad-11d1-80b4-00c04fd430c8"
    end

    test "can insert and query DateTime64 values", %{conn: conn, table: table} do
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        timestamp DateTime64(6)
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, timestamp: :datetime64]

      dt1 = ~U[2024-01-01 10:00:00.123456Z]
      dt2 = ~U[2024-01-02 15:30:45.987654Z]
      dt3 = ~U[2024-01-03 20:15:30.111222Z]

      columns = %{
        id: [1, 2, 3],
        timestamp: [dt1, dt2, dt3]
      }

      assert :ok = Natch.insert(conn, table, columns, schema)

      {:ok, result} = Connection.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert length(result) == 3

      # Verify timestamps are returned as microsecond integers
      assert result |> Enum.at(0) |> Map.get(:timestamp) == DateTime.to_unix(dt1, :microsecond)
      assert result |> Enum.at(1) |> Map.get(:timestamp) == DateTime.to_unix(dt2, :microsecond)
      assert result |> Enum.at(2) |> Map.get(:timestamp) == DateTime.to_unix(dt3, :microsecond)
    end

    test "can insert and query Decimal values", %{conn: conn, table: table} do
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        price Decimal64(9)
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, price: :decimal]

      dec1 = Decimal.new("123.456789")
      dec2 = Decimal.new("987.654321")
      dec3 = Decimal.new("-456.789012")

      columns = %{
        id: [1, 2, 3],
        price: [dec1, dec2, dec3]
      }

      assert :ok = Natch.insert(conn, table, columns, schema)

      {:ok, result} = Connection.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert length(result) == 3

      # Verify decimals are returned as scaled int64 values
      # Scale is 9, so multiply by 10^9
      assert result |> Enum.at(0) |> Map.get(:price) == 123_456_789_000
      assert result |> Enum.at(1) |> Map.get(:price) == 987_654_321_000
      assert result |> Enum.at(2) |> Map.get(:price) == -456_789_012_000
    end

    test "can insert and query Nullable values", %{conn: conn, table: table} do
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        name Nullable(String),
        score Nullable(UInt64)
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, name: :nullable_string, score: :nullable_uint64]

      columns = %{
        id: [1, 2, 3, 4],
        name: ["Alice", nil, "Charlie", "David"],
        score: [100, 200, nil, 400]
      }

      assert :ok = Natch.insert(conn, table, columns, schema)

      {:ok, result} = Connection.select_rows(conn, "SELECT * FROM #{table} ORDER BY id")
      assert length(result) == 4

      # Verify nullable values are returned correctly
      assert result |> Enum.at(0) |> Map.get(:name) == "Alice"
      assert result |> Enum.at(1) |> Map.get(:name) == nil
      assert result |> Enum.at(2) |> Map.get(:name) == "Charlie"

      assert result |> Enum.at(0) |> Map.get(:score) == 100
      assert result |> Enum.at(1) |> Map.get(:score) == 200
      assert result |> Enum.at(2) |> Map.get(:score) == nil
    end
  end
end
