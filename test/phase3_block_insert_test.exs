defmodule Chex.Phase3BlockInsertTest do
  use ExUnit.Case, async: false

  @moduletag :phase3

  alias Chex.{Insert, Native}

  setup do
    # Start connection
    {:ok, conn} = Chex.Connection.start_link(host: "localhost", port: 9000)

    # Clean up any existing test table
    try do
      Chex.Connection.execute(conn, "DROP TABLE IF EXISTS chex_test_phase3")
    rescue
      _ -> :ok
    end

    on_exit(fn ->
      if Process.alive?(conn) do
        try do
          Chex.Connection.execute(conn, "DROP TABLE IF EXISTS chex_test_phase3")
        rescue
          _ -> :ok
        end

        GenServer.stop(conn)
      end
    end)

    {:ok, conn: conn}
  end

  describe "Block operations" do
    test "can create empty block" do
      block = Native.block_create()
      assert is_reference(block)
      assert Native.block_row_count(block) == 0
      assert Native.block_column_count(block) == 0
    end

    test "can append column to block" do
      block = Native.block_create()
      col = Chex.Column.new(:uint64)
      Chex.Column.append(col, 1)
      Chex.Column.append(col, 2)

      Native.block_append_column(block, "id", col.ref)

      assert Native.block_row_count(block) == 2
      assert Native.block_column_count(block) == 1
    end

    test "can append multiple columns to block" do
      block = Native.block_create()

      col1 = Chex.Column.new(:uint64)
      Chex.Column.append(col1, 1)
      Chex.Column.append(col1, 2)

      col2 = Chex.Column.new(:string)
      Chex.Column.append(col2, "first")
      Chex.Column.append(col2, "second")

      Native.block_append_column(block, "id", col1.ref)
      Native.block_append_column(block, "name", col2.ref)

      assert Native.block_row_count(block) == 2
      assert Native.block_column_count(block) == 2
    end
  end

  describe "Building blocks from rows" do
    test "can build block from single row" do
      schema = [id: :uint64, name: :string]
      rows = [%{id: 1, name: "Alice"}]

      block = Insert.build_block(rows, schema)

      assert Native.block_row_count(block) == 1
      assert Native.block_column_count(block) == 2
    end

    test "can build block from multiple rows" do
      schema = [id: :uint64, name: :string, amount: :float64]

      rows = [
        %{id: 1, name: "Alice", amount: 100.5},
        %{id: 2, name: "Bob", amount: 200.75},
        %{id: 3, name: "Charlie", amount: 300.25}
      ]

      block = Insert.build_block(rows, schema)

      assert Native.block_row_count(block) == 3
      assert Native.block_column_count(block) == 3
    end

    test "can build block with all supported types" do
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

      block = Insert.build_block(rows, schema)

      assert Native.block_row_count(block) == 1
      assert Native.block_column_count(block) == 5
    end

    test "raises on missing column in row" do
      schema = [id: :uint64, name: :string]
      rows = [%{id: 1}]

      # Missing 'name' column
      assert_raise ArgumentError, ~r/Missing value for column :name/, fn ->
        Insert.build_block(rows, schema)
      end
    end
  end

  describe "INSERT operations" do
    test "can insert single row", %{conn: conn} do
      # Create table
      Chex.Connection.execute(conn, """
      CREATE TABLE chex_test_phase3 (
        id UInt64,
        name String
      ) ENGINE = Memory
      """)

      # Insert data
      schema = [id: :uint64, name: :string]
      rows = [%{id: 1, name: "Alice"}]

      assert :ok = Insert.insert(conn, "chex_test_phase3", rows, schema)
    end

    test "can insert multiple rows", %{conn: conn} do
      # Create table
      Chex.Connection.execute(conn, """
      CREATE TABLE chex_test_phase3 (
        id UInt64,
        name String,
        amount Float64
      ) ENGINE = Memory
      """)

      # Insert data
      schema = [id: :uint64, name: :string, amount: :float64]

      rows = [
        %{id: 1, name: "Alice", amount: 100.5},
        %{id: 2, name: "Bob", amount: 200.75},
        %{id: 3, name: "Charlie", amount: 300.25}
      ]

      assert :ok = Insert.insert(conn, "chex_test_phase3", rows, schema)
    end

    test "can insert with all supported types", %{conn: conn} do
      # Create table
      Chex.Connection.execute(conn, """
      CREATE TABLE chex_test_phase3 (
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
        },
        %{
          id: 2,
          value: 123,
          name: "Another",
          amount: 456.78,
          created_at: ~U[2024-10-29 11:00:00Z]
        }
      ]

      assert :ok = Insert.insert(conn, "chex_test_phase3", rows, schema)
    end

    test "can insert large batch", %{conn: conn} do
      # Create table
      Chex.Connection.execute(conn, """
      CREATE TABLE chex_test_phase3 (
        id UInt64,
        value UInt64
      ) ENGINE = Memory
      """)

      # Generate 10k rows
      rows =
        for i <- 1..10_000 do
          %{id: i, value: i * 2}
        end

      schema = [id: :uint64, value: :uint64]

      assert :ok = Insert.insert(conn, "chex_test_phase3", rows, schema)
    end

    test "can insert with string keys in rows", %{conn: conn} do
      # Create table
      Chex.Connection.execute(conn, """
      CREATE TABLE chex_test_phase3 (
        id UInt64,
        name String
      ) ENGINE = Memory
      """)

      # Use string keys instead of atoms
      schema = [id: :uint64, name: :string]
      rows = [%{"id" => 1, "name" => "Alice"}]

      assert :ok = Insert.insert(conn, "chex_test_phase3", rows, schema)
    end

    test "returns error for invalid table", %{conn: conn} do
      schema = [id: :uint64]
      rows = [%{id: 1}]

      result = Insert.insert(conn, "nonexistent_table", rows, schema)
      assert {:error, _reason} = result
    end
  end

  describe "Row validation" do
    test "validate_rows succeeds with valid data" do
      schema = [id: :uint64, name: :string]
      rows = [%{id: 1, name: "Alice"}, %{id: 2, name: "Bob"}]

      assert :ok = Insert.validate_rows(rows, schema)
    end

    test "validate_rows fails with missing column" do
      schema = [id: :uint64, name: :string]
      rows = [%{id: 1}]

      assert {:error, reason} = Insert.validate_rows(rows, schema)
      assert reason =~ "missing columns"
    end

    test "validate_rows works with string keys" do
      schema = [id: :uint64, name: :string]
      rows = [%{"id" => 1, "name" => "Alice"}]

      assert :ok = Insert.validate_rows(rows, schema)
    end
  end

  describe "Multiple sequential inserts" do
    test "can insert multiple batches", %{conn: conn} do
      # Create table
      Chex.Connection.execute(conn, """
      CREATE TABLE chex_test_phase3 (
        id UInt64,
        batch UInt64
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, batch: :uint64]

      # First batch
      rows1 = [%{id: 1, batch: 1}, %{id: 2, batch: 1}]
      assert :ok = Insert.insert(conn, "chex_test_phase3", rows1, schema)

      # Second batch
      rows2 = [%{id: 3, batch: 2}, %{id: 4, batch: 2}]
      assert :ok = Insert.insert(conn, "chex_test_phase3", rows2, schema)

      # Third batch
      rows3 = [%{id: 5, batch: 3}]
      assert :ok = Insert.insert(conn, "chex_test_phase3", rows3, schema)
    end
  end
end
