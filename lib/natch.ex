defmodule Natch do
  @moduledoc """
  Elixir client for ClickHouse database using native TCP protocol.

  Natch provides a high-level API for interacting with ClickHouse through
  the clickhouse-cpp library via FINE (Foreign Interface Native Extensions),
  offering high-performance native protocol access.

  ## Quick Start

      # Start a connection
      {:ok, conn} = Natch.start_link(
        host: "localhost",
        port: 9000,
        database: "default"
      )

      # Execute DDL
      :ok = Natch.execute(conn, "CREATE TABLE users (id UInt64, name String) ENGINE = Memory")

      # Insert data (columnar format)
      columns = %{
        id: [1, 2],
        name: ["Alice", "Bob"]
      }
      schema = [id: :uint64, name: :string]
      :ok = Natch.insert(conn, "users", columns, schema)

      # Query data
      {:ok, rows} = Natch.query(conn, "SELECT * FROM users ORDER BY id")
      # => {:ok, [%{id: 1, name: "Alice"}, %{id: 2, name: "Bob"}]}

  ## Connection Options

  - `:host` - ClickHouse server host (default: "localhost")
  - `:port` - Native TCP port (default: 9000)
  - `:database` - Database name (default: "default")
  - `:user` - Username (default: "default")
  - `:password` - Password (default: "")
  - `:compression` - Enable LZ4 compression (default: true)
  - `:name` - Process name for registration (optional)

  ## Supported Types

  Currently supports 5 core ClickHouse types:
  - `:uint64` - UInt64
  - `:int64` - Int64
  - `:string` - String
  - `:float64` - Float64
  - `:datetime` - DateTime (Unix timestamp)

  More types coming in Phase 5 (Nullable, Array, Date, Bool, Decimal, etc.)
  """

  alias Natch.Connection

  @type conn :: pid() | atom()
  @type row :: map()
  @type schema :: [{atom(), atom()}]

  # Connection Management

  @doc """
  Starts a new connection to ClickHouse via native TCP protocol.

  ## Options

  - `:host` - ClickHouse server host (default: "localhost")
  - `:port` - Native TCP port (default: 9000)
  - `:database` - Database name (default: "default")
  - `:user` - Username (default: "default")
  - `:password` - Password (default: "")
  - `:compression` - Enable LZ4 compression (default: true)
  - `:name` - Process name for registration (optional)

  ## Examples

      {:ok, conn} = Natch.start_link(host: "localhost", port: 9000)
      {:ok, conn} = Natch.start_link(database: "analytics", user: "readonly")
      {:ok, conn} = Natch.start_link(name: :my_conn)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    Connection.start_link(opts)
  end

  @doc """
  Stops a connection.

  ## Examples

      :ok = Natch.stop(conn)
  """
  @spec stop(conn()) :: :ok
  def stop(conn) do
    GenServer.stop(conn)
  end

  @doc """
  Pings the ClickHouse server to check if connection is alive.

  ## Examples

      :ok = Natch.ping(conn)
  """
  @spec ping(conn()) :: :ok | {:error, term()}
  def ping(conn) do
    Connection.ping(conn)
  end

  @doc """
  Resets the connection.

  ## Examples

      :ok = Natch.reset(conn)
  """
  @spec reset(conn()) :: :ok | {:error, term()}
  def reset(conn) do
    Connection.reset(conn)
  end

  # Query Operations

  @doc """
  Executes a SELECT query and returns all results as a list of maps.

  Column names become map keys (atoms). Results are materialized in memory.

  ## Examples

      {:ok, rows} = Natch.query(conn, "SELECT * FROM users")
      # => {:ok, [%{id: 1, name: "Alice"}, %{id: 2, name: "Bob"}]}

      {:ok, rows} = Natch.query(conn, "SELECT id, name FROM users WHERE id = 1")
      # => {:ok, [%{id: 1, name: "Alice"}]}

      {:ok, rows} = Natch.query(conn, "SELECT count() as cnt FROM users")
      # => {:ok, [%{cnt: 2}]}
  """
  @spec query(conn(), String.t()) :: {:ok, [row()]} | {:error, term()}
  def query(conn, sql) do
    Connection.select_rows(conn, sql)
  end

  @doc """
  Executes a SELECT query and returns all results, raising on error.

  ## Examples

      rows = Natch.query!(conn, "SELECT * FROM users")
      # => [%{id: 1, name: "Alice"}, %{id: 2, name: "Bob"}]
  """
  @spec query!(conn(), String.t()) :: [row()]
  def query!(conn, sql) do
    case query(conn, sql) do
      {:ok, rows} -> rows
      {:error, reason} -> raise "Query failed: #{inspect(reason)}"
    end
  end

  @doc """
  Executes a DDL or DML statement without returning results.

  Useful for CREATE, DROP, ALTER, INSERT, and DELETE statements.

  ## Examples

      :ok = Natch.execute(conn, "CREATE TABLE users (id UInt64, name String) ENGINE = Memory")
      :ok = Natch.execute(conn, "DROP TABLE users")
      :ok = Natch.execute(conn, "ALTER TABLE users ADD COLUMN age UInt8")
      :ok = Natch.execute(conn, "INSERT INTO users VALUES (1, 'Alice')")
  """
  @spec execute(conn(), String.t()) :: :ok | {:error, term()}
  def execute(conn, sql) do
    Connection.execute(conn, sql)
  end

  @doc """
  Executes a DDL or DML statement, raising on error.

  ## Examples

      Natch.execute!(conn, "CREATE TABLE test (id UInt64) ENGINE = Memory")
  """
  @spec execute!(conn(), String.t()) :: :ok
  def execute!(conn, sql) do
    case execute(conn, sql) do
      :ok -> :ok
      {:error, reason} -> raise "Execute failed: #{inspect(reason)}"
    end
  end

  # Insert Operations

  @doc """
  Inserts data into a table using native columnar format.

  Natch uses **columnar format** for maximum performance (10-1000x faster than row-oriented).
  Columns should be a map of column_name => [values].

  Schema defines the column names and types.

  ## Why Columnar?

  - **Matches ClickHouse native storage** - no transposition needed
  - **10-1000x faster** - 1 NIF call per column (not per value!)
  - **Natural for analytics** - operations work on columns, not rows

  ## Schema Types

  Supported types:
  - `:uint64` - UInt64
  - `:int64` - Int64
  - `:string` - String
  - `:float64` - Float64
  - `:datetime` - DateTime (pass Elixir DateTime, converts to Unix timestamp)

  ## Examples

      # Columnar format (RECOMMENDED)
      columns = %{
        id: [1, 2, 3],
        name: ["Alice", "Bob", "Charlie"]
      }
      schema = [id: :uint64, name: :string]
      :ok = Natch.insert(conn, "users", columns, schema)

      # With all types
      columns = %{
        id: [1, 2, 3],
        count: [-42, 100, -5],
        name: ["test1", "test2", "test3"],
        amount: [99.99, 88.88, 77.77],
        created_at: [~U[2024-10-29 10:00:00Z], ~U[2024-10-29 11:00:00Z], ~U[2024-10-29 12:00:00Z]]
      }
      schema = [
        id: :uint64,
        count: :int64,
        name: :string,
        amount: :float64,
        created_at: :datetime
      ]
      :ok = Natch.insert(conn, "events", columns, schema)

      # Bulk insert (extremely efficient!)
      columns = %{
        id: Enum.to_list(1..100_000),
        value: Enum.map(1..100_000, & &1 * 2)
      }
      schema = [id: :uint64, value: :uint64]
      :ok = Natch.insert(conn, "bulk_table", columns, schema)

      # If you have row-oriented data, convert first:
      rows = [%{id: 1, name: "Alice"}, %{id: 2, name: "Bob"}]
      columns = Natch.Conversion.rows_to_columns(rows, schema)
      :ok = Natch.insert(conn, "users", columns, schema)
  """
  @spec insert(conn(), String.t(), map(), schema()) :: :ok | {:error, term()}
  def insert(conn, table, columns, schema) when is_map(columns) and is_list(schema) do
    GenServer.call(conn, {:insert, table, columns, schema}, :infinity)
  end

  @doc """
  Inserts data into a table, raising on error.

  ## Examples

      columns = %{id: [1, 2], name: ["Alice", "Bob"]}
      schema = [id: :uint64, name: :string]
      Natch.insert!(conn, "users", columns, schema)
  """
  @spec insert!(conn(), String.t(), map(), schema()) :: :ok
  def insert!(conn, table, columns, schema) do
    case insert(conn, table, columns, schema) do
      :ok -> :ok
      {:error, reason} -> raise "Insert failed: #{inspect(reason)}"
    end
  end
end
