defmodule Natch.BlockTest do
  use ExUnit.Case, async: true

  alias Natch.{Block, Native}

  setup do
    # Generate unique table name for this test
    table = "test_#{System.unique_integer([:positive, :monotonic])}_#{:rand.uniform(999_999)}"

    # Start connection
    {:ok, conn} = Natch.Connection.start_link(host: "localhost", port: 9000)

    on_exit(fn ->
      # Clean up test table if it exists
      if Process.alive?(conn) do
        try do
          Natch.Connection.execute(conn, "DROP TABLE IF EXISTS #{table}")
        catch
          :exit, _ -> :ok
        end

        # Use Process.exit to avoid race conditions
        Process.exit(conn, :normal)
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
      col = Natch.Column.new(:uint64)
      Natch.Column.append_bulk(col, [1, 2])

      Native.block_append_column(block, "id", col.ref)

      assert Native.block_row_count(block) == 2
      assert Native.block_column_count(block) == 1
    end

    test "can append multiple columns to block" do
      block = Native.block_create()

      col1 = Natch.Column.new(:uint64)
      Natch.Column.append_bulk(col1, [1, 2])

      col2 = Natch.Column.new(:string)
      Natch.Column.append_bulk(col2, ["first", "second"])

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
      Natch.Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        name String
      ) ENGINE = Memory
      """)

      # Insert data
      schema = [id: :uint64, name: :string]
      columns = %{id: [1], name: ["Alice"]}

      assert :ok = Natch.insert(conn, "#{table}", columns, schema)
    end

    test "can insert multiple rows", %{conn: conn, table: table} do
      # Create table
      Natch.Connection.execute(conn, """
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

      assert :ok = Natch.insert(conn, "#{table}", columns, schema)
    end

    test "can insert with all supported types", %{conn: conn, table: table} do
      # Create table
      Natch.Connection.execute(conn, """
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

      assert :ok = Natch.insert(conn, "#{table}", columns, schema)
    end

    test "can insert large batch", %{conn: conn, table: table} do
      # Create table
      Natch.Connection.execute(conn, """
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

      assert :ok = Natch.insert(conn, "#{table}", columns, schema)
    end

    test "can insert with string keys in columns", %{conn: conn, table: table} do
      # Create table
      Natch.Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        name String
      ) ENGINE = Memory
      """)

      # Use string keys instead of atoms
      schema = [id: :uint64, name: :string]
      columns = %{"id" => [1], "name" => ["Alice"]}

      assert :ok = Natch.insert(conn, "#{table}", columns, schema)
    end

    test "returns error for invalid table", %{conn: conn, table: _table} do
      schema = [id: :uint64]
      columns = %{id: [1]}

      result = Natch.insert(conn, "nonexistent_table", columns, schema)
      assert {:error, _reason} = result
    end
  end

  describe "Type and length validation (integration tests)" do
    # These tests verify that Column.append_bulk and FINE NIFs catch all invalid data
    # that was previously caught by the deprecated Conversion validation functions

    test "catches mismatched column lengths (FINE validates at block append)" do
      schema = [id: :uint64, name: :string]
      columns = %{id: [1, 2, 3], name: ["Alice", "Bob"]}

      assert_raise Natch.ValidationError,
                   ~r/all columns in block must have same count of rows/,
                   fn ->
                     Block.build_block(columns, schema)
                   end
    end

    test "catches missing column" do
      schema = [id: :uint64, name: :string]
      columns = %{id: [1, 2, 3]}

      assert_raise ArgumentError, ~r/Missing column :name/, fn ->
        Block.build_block(columns, schema)
      end
    end

    test "catches non-list column" do
      schema = [id: :uint64, name: :string]
      columns = %{id: [1, 2, 3], name: "not a list"}

      assert_raise ArgumentError, ~r/Column :name must be a list/, fn ->
        Block.build_block(columns, schema)
      end
    end

    test "catches negative uint64 values (Column.append_bulk validates)" do
      schema = [id: :uint64]
      columns = %{id: [1, -1, 3]}

      assert_raise ArgumentError, ~r/non-negative integers/, fn ->
        Block.build_block(columns, schema)
      end
    end

    test "catches non-integer uint64 values (FINE validates at NIF boundary)" do
      schema = [id: :uint64]
      columns = %{id: [1, "string", 3]}

      assert_raise ArgumentError, fn ->
        Block.build_block(columns, schema)
      end
    end

    test "catches non-integer int64 values (Column.append_bulk validates)" do
      schema = [value: :int64]
      columns = %{value: [1, 2.5, 3]}

      assert_raise ArgumentError, ~r/must be integers/, fn ->
        Block.build_block(columns, schema)
      end
    end

    test "catches non-string values (FINE validates at NIF boundary)" do
      schema = [name: :string]
      columns = %{name: ["Alice", 123, "Bob"]}

      assert_raise ArgumentError, fn ->
        Block.build_block(columns, schema)
      end
    end

    test "catches non-numeric float64 values (Column.append_bulk validates)" do
      schema = [amount: :float64]
      columns = %{amount: [1.5, "not a number", 3.14]}

      assert_raise ArgumentError, ~r/must be numbers/, fn ->
        Block.build_block(columns, schema)
      end
    end

    test "catches invalid datetime values (Column.append_bulk validates)" do
      schema = [created_at: :datetime]
      columns = %{created_at: [~U[2024-10-29 10:00:00Z], "not a datetime"]}

      assert_raise ArgumentError, fn ->
        Block.build_block(columns, schema)
      end
    end

    test "accepts valid uint64 values including boundary values" do
      schema = [id: :uint64]
      columns = %{id: [0, 1, 100, 18_446_744_073_709_551_615]}

      block = Block.build_block(columns, schema)
      assert Native.block_row_count(block) == 4
    end

    test "accepts valid int64 values including boundary values" do
      schema = [value: :int64]
      columns = %{value: [-9_223_372_036_854_775_808, 0, 9_223_372_036_854_775_807]}

      block = Block.build_block(columns, schema)
      assert Native.block_row_count(block) == 3
    end

    test "accepts valid string values including unicode and empty strings" do
      schema = [name: :string]
      columns = %{name: ["Alice", "Bob", "", "Hello 世界 🌍"]}

      block = Block.build_block(columns, schema)
      assert Native.block_row_count(block) == 4
    end

    test "accepts valid float64 values including integers" do
      schema = [amount: :float64]
      columns = %{amount: [1.5, 2.0, -3.14, 0.0, 42]}

      block = Block.build_block(columns, schema)
      assert Native.block_row_count(block) == 5
    end

    test "accepts valid datetime values as DateTime structs and integers" do
      schema = [created_at: :datetime]

      columns = %{
        created_at: [
          ~U[2024-10-29 10:00:00Z],
          1_730_220_600,
          ~U[1970-01-01 00:00:00Z]
        ]
      }

      block = Block.build_block(columns, schema)
      assert Native.block_row_count(block) == 3
    end

    test "works with string keys in columns map" do
      schema = [id: :uint64, name: :string]
      columns = %{"id" => [1, 2], "name" => ["Alice", "Bob"]}

      block = Block.build_block(columns, schema)
      assert Native.block_row_count(block) == 2
    end

    test "validates multiple columns with different types" do
      schema = [id: :uint64, name: :string, amount: :float64]

      columns = %{
        id: [1, 2],
        name: ["Alice", "Bob"],
        amount: [100.5, 200.75]
      }

      block = Block.build_block(columns, schema)
      assert Native.block_row_count(block) == 2
    end
  end

  describe "Multiple sequential inserts" do
    test "can insert multiple batches", %{conn: conn, table: table} do
      # Create table
      Natch.Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        batch UInt64
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, batch: :uint64]

      # First batch
      columns1 = %{id: [1, 2], batch: [1, 1]}
      assert :ok = Natch.insert(conn, "#{table}", columns1, schema)

      # Second batch
      columns2 = %{id: [3, 4], batch: [2, 2]}
      assert :ok = Natch.insert(conn, "#{table}", columns2, schema)

      # Third batch
      columns3 = %{id: [5], batch: [3]}
      assert :ok = Natch.insert(conn, "#{table}", columns3, schema)
    end
  end
end
