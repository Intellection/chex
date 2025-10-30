defmodule Chex.BlockTest do
  use ExUnit.Case, async: true

  alias Chex.{Block, Native}

  setup do
    # Generate unique table name for this test
    table = "test_#{System.unique_integer([:positive, :monotonic])}_#{:rand.uniform(999_999)}"

    # Start connection
    {:ok, conn} = Chex.Connection.start_link(host: "localhost", port: 9000)

    on_exit(fn ->
      # Clean up test table if it exists
      if Process.alive?(conn) do
        try do
          Chex.Connection.execute(conn, "DROP TABLE IF EXISTS #{table}")
        rescue
          _ -> :ok
        end

        GenServer.stop(conn)
      end
    end)

    {:ok, conn: conn, table: table}
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
      Chex.Column.append_bulk(col, [1, 2])

      Native.block_append_column(block, "id", col.ref)

      assert Native.block_row_count(block) == 2
      assert Native.block_column_count(block) == 1
    end

    test "can append multiple columns to block" do
      block = Native.block_create()

      col1 = Chex.Column.new(:uint64)
      Chex.Column.append_bulk(col1, [1, 2])

      col2 = Chex.Column.new(:string)
      Chex.Column.append_bulk(col2, ["first", "second"])

      Native.block_append_column(block, "id", col1.ref)
      Native.block_append_column(block, "name", col2.ref)

      assert Native.block_row_count(block) == 2
      assert Native.block_column_count(block) == 2
    end
  end

  describe "Building blocks from columns" do
    test "can build block from single row" do
      schema = [id: :uint64, name: :string]
      columns = %{id: [1], name: ["Alice"]}

      block = Block.build_block(columns, schema)

      assert Native.block_row_count(block) == 1
      assert Native.block_column_count(block) == 2
    end

    test "can build block from multiple rows" do
      schema = [id: :uint64, name: :string, amount: :float64]

      columns = %{
        id: [1, 2, 3],
        name: ["Alice", "Bob", "Charlie"],
        amount: [100.5, 200.75, 300.25]
      }

      block = Block.build_block(columns, schema)

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

      columns = %{
        id: [1],
        value: [-42],
        name: ["Test"],
        amount: [99.99],
        created_at: [~U[2024-10-29 10:00:00Z]]
      }

      block = Block.build_block(columns, schema)

      assert Native.block_row_count(block) == 1
      assert Native.block_column_count(block) == 5
    end

    test "raises on missing column" do
      schema = [id: :uint64, name: :string]
      columns = %{id: [1]}

      # Missing 'name' column
      assert_raise ArgumentError, ~r/Missing column :name/, fn ->
        Block.build_block(columns, schema)
      end
    end
  end

  describe "INSERT operations" do
    test "can insert single row", %{conn: conn, table: table} do
      # Create table
      Chex.Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        name String
      ) ENGINE = Memory
      """)

      # Insert data
      schema = [id: :uint64, name: :string]
      columns = %{id: [1], name: ["Alice"]}

      assert :ok = Chex.insert(conn, "#{table}", columns, schema)
    end

    test "can insert multiple rows", %{conn: conn, table: table} do
      # Create table
      Chex.Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        name String,
        amount Float64
      ) ENGINE = Memory
      """)

      # Insert data
      schema = [id: :uint64, name: :string, amount: :float64]

      columns = %{
        id: [1, 2, 3],
        name: ["Alice", "Bob", "Charlie"],
        amount: [100.5, 200.75, 300.25]
      }

      assert :ok = Chex.insert(conn, "#{table}", columns, schema)
    end

    test "can insert with all supported types", %{conn: conn, table: table} do
      # Create table
      Chex.Connection.execute(conn, """
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
        name: ["Test", "Another"],
        amount: [99.99, 456.78],
        created_at: [~U[2024-10-29 10:00:00Z], ~U[2024-10-29 11:00:00Z]]
      }

      assert :ok = Chex.insert(conn, "#{table}", columns, schema)
    end

    test "can insert large batch", %{conn: conn, table: table} do
      # Create table
      Chex.Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        value UInt64
      ) ENGINE = Memory
      """)

      # Generate 10k rows in columnar format
      columns = %{
        id: Enum.to_list(1..10_000),
        value: Enum.map(1..10_000, &(&1 * 2))
      }

      schema = [id: :uint64, value: :uint64]

      assert :ok = Chex.insert(conn, "#{table}", columns, schema)
    end

    test "can insert with string keys in columns", %{conn: conn, table: table} do
      # Create table
      Chex.Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        name String
      ) ENGINE = Memory
      """)

      # Use string keys instead of atoms
      schema = [id: :uint64, name: :string]
      columns = %{"id" => [1], "name" => ["Alice"]}

      assert :ok = Chex.insert(conn, "#{table}", columns, schema)
    end

    test "returns error for invalid table", %{conn: conn, table: _table} do
      schema = [id: :uint64]
      columns = %{id: [1]}

      result = Chex.insert(conn, "nonexistent_table", columns, schema)
      assert {:error, _reason} = result
    end
  end

  describe "Column validation" do
    test "validates column lengths match" do
      schema = [id: :uint64, name: :string]
      columns = %{id: [1, 2], name: ["Alice"]}

      # Mismatched lengths
      assert_raise ArgumentError, ~r/Column length mismatch/, fn ->
        Block.build_block(columns, schema)
      end
    end

    test "validates column types" do
      schema = [id: :uint64, name: :string]
      columns = %{id: ["not", "numbers"], name: ["Alice", "Bob"]}

      # Wrong types
      assert_raise ArgumentError, fn ->
        Block.build_block(columns, schema)
      end
    end

    test "works with string keys" do
      schema = [id: :uint64, name: :string]
      columns = %{"id" => [1], "name" => ["Alice"]}

      block = Block.build_block(columns, schema)
      assert Native.block_row_count(block) == 1
    end
  end

  describe "Multiple sequential inserts" do
    test "can insert multiple batches", %{conn: conn, table: table} do
      # Create table
      Chex.Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        batch UInt64
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, batch: :uint64]

      # First batch
      columns1 = %{id: [1, 2], batch: [1, 1]}
      assert :ok = Chex.insert(conn, "#{table}", columns1, schema)

      # Second batch
      columns2 = %{id: [3, 4], batch: [2, 2]}
      assert :ok = Chex.insert(conn, "#{table}", columns2, schema)

      # Third batch
      columns3 = %{id: [5], batch: [3]}
      assert :ok = Chex.insert(conn, "#{table}", columns3, schema)
    end
  end
end
