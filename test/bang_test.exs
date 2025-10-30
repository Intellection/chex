defmodule Chex.BangTest do
  use ExUnit.Case, async: true

  alias Chex.Connection

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
        rescue
          _ -> :ok
        end

        GenServer.stop(conn)
      end
    end)

    {:ok, conn: conn, table: table}
  end

  describe "query!/2" do
    test "returns results on success", %{conn: conn, table: table} do
      # Create and populate table
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        name String
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, name: :string]
      columns = %{id: [1, 2, 3], name: ["Alice", "Bob", "Charlie"]}
      Chex.insert(conn, table, columns, schema)

      # Query with bang function
      result = Chex.query!(conn, "SELECT * FROM #{table} ORDER BY id")

      assert is_list(result)
      assert length(result) == 3
      assert Enum.at(result, 0).id == 1
      assert Enum.at(result, 0).name == "Alice"
    end

    test "raises on invalid query", %{conn: conn, table: _table} do
      assert_raise RuntimeError, ~r/Query failed/, fn ->
        Chex.query!(conn, "SELECT * FROM nonexistent_table")
      end
    end

    test "raises on invalid SQL syntax", %{conn: conn, table: _table} do
      assert_raise RuntimeError, ~r/Query failed/, fn ->
        Chex.query!(conn, "INVALID SQL SYNTAX")
      end
    end
  end

  describe "execute!/2" do
    test "returns :ok on success", %{conn: conn, table: table} do
      result =
        Chex.execute!(conn, """
        CREATE TABLE #{table} (
          id UInt64,
          name String
        ) ENGINE = Memory
        """)

      assert :ok = result
    end

    test "raises on invalid SQL", %{conn: conn, table: _table} do
      assert_raise RuntimeError, ~r/Execute failed/, fn ->
        Chex.execute!(conn, "INVALID SQL SYNTAX")
      end
    end

    test "raises on invalid table operation", %{conn: conn, table: _table} do
      assert_raise RuntimeError, ~r/Execute failed/, fn ->
        Chex.execute!(conn, "DROP TABLE nonexistent_table")
      end
    end
  end

  describe "insert!/4" do
    test "returns :ok on success", %{conn: conn, table: table} do
      # Create table
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        name String
      ) ENGINE = Memory
      """)

      schema = [id: :uint64, name: :string]
      columns = %{id: [1, 2], name: ["Alice", "Bob"]}

      result = Chex.insert!(conn, table, columns, schema)
      assert :ok = result

      # Verify data was inserted
      {:ok, rows} = Connection.select(conn, "SELECT count() as cnt FROM #{table}")
      assert [%{cnt: 2}] = rows
    end

    test "raises on invalid table", %{conn: conn, table: _table} do
      schema = [id: :uint64]
      columns = %{id: [1]}

      assert_raise RuntimeError, ~r/Insert failed/, fn ->
        Chex.insert!(conn, "nonexistent_table", columns, schema)
      end
    end

    test "raises on schema mismatch", %{conn: conn, table: table} do
      # Create table with different schema
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64
      ) ENGINE = Memory
      """)

      # Try to insert with wrong schema
      schema = [id: :uint64, name: :string]
      columns = %{id: [1], name: ["Alice"]}

      assert_raise RuntimeError, ~r/Insert failed/, fn ->
        Chex.insert!(conn, table, columns, schema)
      end
    end
  end

  describe "public API convenience wrappers" do
    test "Chex.start_link/1 works", %{table: _table} do
      {:ok, conn} = Chex.start_link(host: "localhost", port: 9000)
      assert is_pid(conn)
      GenServer.stop(conn)
    end

    test "Chex.ping/1 works", %{conn: conn, table: _table} do
      assert :ok = Chex.ping(conn)
    end

    test "Chex.reset/1 works", %{conn: conn, table: _table} do
      assert :ok = Chex.reset(conn)
      # Connection should still work after reset
      assert :ok = Chex.ping(conn)
    end

    test "Chex.stop/1 works", %{table: _table} do
      {:ok, conn} = Chex.start_link(host: "localhost", port: 9000)
      assert :ok = Chex.stop(conn)
      refute Process.alive?(conn)
    end
  end
end
