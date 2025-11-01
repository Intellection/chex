defmodule Chex.ConnectionTest do
  use ExUnit.Case, async: true

  setup do
    # Generate unique table name for this test
    table = "test_#{System.unique_integer([:positive, :monotonic])}_#{:rand.uniform(999_999)}"

    # Start test connection
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

  describe "Connection management" do
    test "can start connection with default options", %{conn: _test_conn} do
      {:ok, conn} = Chex.Connection.start_link(host: "localhost", port: 9000)
      assert is_pid(conn)
      GenServer.stop(conn)
    end

    test "can start connection with custom options", %{conn: _conn} do
      {:ok, conn} =
        Chex.Connection.start_link(
          host: "localhost",
          port: 9000,
          database: "default",
          user: "default",
          compression: true
        )

      assert is_pid(conn)
      GenServer.stop(conn)
    end

    test "can get client reference", %{conn: conn} do
      {:ok, client} = Chex.Connection.get_client(conn)
      assert is_reference(client)
    end

    test "can ping server", %{conn: conn} do
      assert :ok = Chex.Connection.ping(conn)
    end

    test "can reset connection", %{conn: conn} do
      assert :ok = Chex.Connection.reset(conn)
      # Connection should still work after reset
      assert :ok = Chex.Connection.ping(conn)
    end
  end

  describe "DDL operations" do
    test "can create table", %{conn: conn, table: table} do
      sql = """
      CREATE TABLE IF NOT EXISTS #{table} (
        id UInt64,
        name String,
        value Float64
      ) ENGINE = Memory
      """

      assert :ok = Chex.Connection.execute(conn, sql)
    end

    test "can drop table", %{conn: conn, table: table} do
      # Create table first
      Chex.Connection.execute(conn, """
      CREATE TABLE IF NOT EXISTS #{table} (
        id UInt64
      ) ENGINE = Memory
      """)

      # Drop it
      assert :ok = Chex.Connection.execute(conn, "DROP TABLE #{table}")
    end

    test "can create and drop table in sequence", %{conn: conn, table: table} do
      # Create
      assert :ok =
               Chex.Connection.execute(conn, """
               CREATE TABLE #{table} (
                 id UInt64,
                 name String
               ) ENGINE = Memory
               """)

      # Verify it exists by trying to drop it (won't error if exists)
      assert :ok = Chex.Connection.execute(conn, "DROP TABLE #{table}")
    end

    test "execute returns error for invalid SQL", %{conn: conn} do
      result = Chex.Connection.execute(conn, "INVALID SQL SYNTAX")
      assert {:error, _reason} = result
    end
  end

  describe "Multiple operations" do
    test "can execute multiple DDL statements", %{conn: conn, table: table} do
      assert :ok =
               Chex.Connection.execute(conn, """
               CREATE TABLE #{table} (
                 id UInt64,
                 created DateTime
               ) ENGINE = Memory
               """)

      # Can still ping
      assert :ok = Chex.Connection.ping(conn)

      # Can drop
      assert :ok = Chex.Connection.execute(conn, "DROP TABLE #{table}")

      # Can still ping after drop
      assert :ok = Chex.Connection.ping(conn)
    end
  end
end
