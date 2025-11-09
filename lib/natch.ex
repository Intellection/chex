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

      # Query with parameters (prevents SQL injection)
      # Types are automatically inferred from values
      {:ok, rows} = Natch.select_rows(
        conn,
        "SELECT * FROM users WHERE id > {min_id}",
        min_id: 0
      )
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

  # Private: Infer ClickHouse type name from Elixir value
  defp infer_clickhouse_type(v) when is_integer(v) and v >= 0, do: "UInt64"
  defp infer_clickhouse_type(v) when is_integer(v), do: "Int64"
  defp infer_clickhouse_type(v) when is_float(v), do: "Float64"
  defp infer_clickhouse_type(v) when is_binary(v), do: "String"
  defp infer_clickhouse_type(%DateTime{}), do: "DateTime"
  defp infer_clickhouse_type(%Date{}), do: "Date"

  defp infer_clickhouse_type(nil) do
    raise ArgumentError,
          "Cannot infer type for nil parameter. " <>
            "Specify the type explicitly, e.g., {param:Nullable(String)}"
  end

  defp infer_clickhouse_type(value) do
    raise ArgumentError,
          "Cannot infer ClickHouse type for #{inspect(value)}. " <>
            "Use explicit type annotation in SQL, e.g., {param:Type}"
  end

  # Private: Rewrite SQL to add type annotations to untyped placeholders
  # Only rewrites {name} patterns, leaves {name:Type} unchanged
  defp add_parameter_types(sql, params) when is_list(params) do
    add_parameter_types(sql, Map.new(params))
  end

  defp add_parameter_types(sql, params) when is_map(params) do
    # Match {name} but NOT {name:Type}
    # Negative lookahead (?!:) ensures we don't match {name:Type}
    Regex.replace(~r/\{(\w+)\}(?!:)/, sql, fn _, name ->
      try do
        # Try atom key first
        atom_key = String.to_existing_atom(name)

        if Map.has_key?(params, atom_key) do
          value = Map.fetch!(params, atom_key)
          type = infer_clickhouse_type(value)
          "{#{name}:#{type}}"
        else
          # Try string key
          if Map.has_key?(params, name) do
            value = Map.fetch!(params, name)
            type = infer_clickhouse_type(value)
            "{#{name}:#{type}}"
          else
            # Parameter not found - leave as-is, will error during binding
            "{#{name}}"
          end
        end
      rescue
        ArgumentError ->
          # String.to_existing_atom failed, try string key only
          if Map.has_key?(params, name) do
            value = Map.fetch!(params, name)
            type = infer_clickhouse_type(value)
            "{#{name}:#{type}}"
          else
            "{#{name}}"
          end
      end
    end)
  end

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
  Executes a SELECT query and returns results in row format (list of maps).

  Each row is represented as a map with column names as keys (atoms).
  Results are materialized in memory.

  ## Examples

      # Simple query without parameters
      {:ok, rows} = Natch.select_rows(conn, "SELECT * FROM users")
      # => {:ok, [%{id: 1, name: "Alice"}, %{id: 2, name: "Bob"}]}

      # Parameterized query (RECOMMENDED - prevents SQL injection)
      # Types are automatically inferred from values
      {:ok, rows} = Natch.select_rows(
        conn,
        "SELECT * FROM users WHERE id = {id} AND status = {status}",
        id: 42,
        status: "active"
      )

      # With map instead of keyword list
      params = %{id: 42, status: "active"}
      {:ok, rows} = Natch.select_rows(
        conn,
        "SELECT * FROM users WHERE id = {id} AND status = {status}",
        params
      )

      # Explicit types when needed (mixed with inferred)
      {:ok, rows} = Natch.select_rows(
        conn,
        "SELECT * FROM users WHERE count = {count:Int32} AND status = {status}",
        count: 100,
        status: "active"
      )

      # Builder pattern for complex cases
      query = Natch.Query.new("SELECT * FROM users WHERE id = {id:UInt64}")
      |> Natch.Query.bind(:id, 42)
      |> Natch.Query.bind(:status, "active", :string)
      {:ok, rows} = Natch.select_rows(conn, query)
  """
  @spec select_rows(conn(), String.t() | Natch.Query.t()) :: {:ok, [row()]} | {:error, term()}
  @spec select_rows(conn(), String.t(), keyword() | map()) :: {:ok, [row()]} | {:error, term()}
  def select_rows(conn, %Natch.Query{} = query) do
    Connection.select_rows_parameterized(conn, query)
  end

  def select_rows(conn, sql) when is_binary(sql) do
    Connection.select_rows(conn, sql)
  end

  def select_rows(conn, sql, params) when is_binary(sql) and is_list(params) do
    # Infer types for untyped placeholders like {id}
    sql_with_types = add_parameter_types(sql, params)
    query = Natch.Query.new(sql_with_types) |> Natch.Query.bind_all(params)
    select_rows(conn, query)
  end

  def select_rows(conn, sql, params) when is_binary(sql) and is_map(params) do
    # Infer types for untyped placeholders like {id}
    sql_with_types = add_parameter_types(sql, params)
    query = Natch.Query.new(sql_with_types) |> Natch.Query.bind_all(params)
    select_rows(conn, query)
  end

  @doc """
  Executes a SELECT query and returns results in row format, raising on error.

  ## Examples

      # String query
      rows = Natch.select_rows!(conn, "SELECT * FROM users")
      # => [%{id: 1, name: "Alice"}, %{id: 2, name: "Bob"}]

      # Parameterized query
      query = Natch.Query.new("SELECT * FROM users WHERE id = {id:UInt64}")
      |> Natch.Query.bind(:id, 42)
      rows = Natch.select_rows!(conn, query)
  """
  @spec select_rows!(conn(), String.t() | Natch.Query.t()) :: [row()]
  def select_rows!(conn, query_or_sql) do
    case select_rows(conn, query_or_sql) do
      {:ok, rows} -> rows
      {:error, reason} -> raise "Query failed: #{inspect(reason)}"
    end
  end

  @doc """
  Executes a SELECT query and returns results in columnar format.

  Returns a map where keys are column names (atoms) and values are lists of column values.
  This format is efficient for analytics workloads and integrates well with data processing
  libraries.

  ## Examples

      # Simple query without parameters
      {:ok, cols} = Natch.select_cols(conn, "SELECT id, name FROM users")
      # => {:ok, %{id: [1, 2, 3], name: ["Alice", "Bob", "Charlie"]}}

      # Parameterized query (RECOMMENDED - prevents SQL injection)
      # Types are automatically inferred from values
      {:ok, cols} = Natch.select_cols(
        conn,
        "SELECT user_id, revenue FROM events WHERE created_at >= {start}",
        start: ~U[2024-01-01 00:00:00Z]
      )
      # => {:ok, %{user_id: [1, 2, 1], revenue: [100.0, 200.0, 150.0]}}

      # With map instead of keyword list
      params = %{min_id: 100, status: "active"}
      {:ok, cols} = Natch.select_cols(
        conn,
        "SELECT * FROM users WHERE id > {min_id} AND status = {status}",
        params
      )

      # Explicit types when needed
      {:ok, cols} = Natch.select_cols(
        conn,
        "SELECT * FROM metrics WHERE count > {threshold:Int32}",
        threshold: 1000
      )

      # Builder pattern for complex cases
      query = Natch.Query.new("SELECT * FROM users WHERE id > {min_id:UInt64}")
      |> Natch.Query.bind(:min_id, 100)
      {:ok, cols} = Natch.select_cols(conn, query)

      # Easy integration with data analysis
      {:ok, %{user_id: ids, revenue: revenues}} = Natch.select_cols(
        conn,
        "SELECT user_id, revenue FROM events WHERE user_id = {uid:UInt64}",
        uid: 42
      )
      total = Enum.sum(revenues)
  """
  @spec select_cols(conn(), String.t() | Natch.Query.t()) :: {:ok, map()} | {:error, term()}
  @spec select_cols(conn(), String.t(), keyword() | map()) :: {:ok, map()} | {:error, term()}
  def select_cols(conn, %Natch.Query{} = query) do
    Connection.select_cols_parameterized(conn, query)
  end

  def select_cols(conn, sql) when is_binary(sql) do
    Connection.select_cols(conn, sql)
  end

  def select_cols(conn, sql, params) when is_binary(sql) and is_list(params) do
    # Infer types for untyped placeholders like {id}
    sql_with_types = add_parameter_types(sql, params)
    query = Natch.Query.new(sql_with_types) |> Natch.Query.bind_all(params)
    select_cols(conn, query)
  end

  def select_cols(conn, sql, params) when is_binary(sql) and is_map(params) do
    # Infer types for untyped placeholders like {id}
    sql_with_types = add_parameter_types(sql, params)
    query = Natch.Query.new(sql_with_types) |> Natch.Query.bind_all(params)
    select_cols(conn, query)
  end

  @doc """
  Executes a SELECT query and returns results in columnar format, raising on error.

  ## Examples

      # String query
      cols = Natch.select_cols!(conn, "SELECT id, name FROM users")
      # => %{id: [1, 2, 3], name: ["Alice", "Bob", "Charlie"]}

      # Parameterized query
      query = Natch.Query.new("SELECT * FROM users WHERE created_at > {start:DateTime}")
      |> Natch.Query.bind(:start, ~U[2024-01-01 00:00:00Z])
      cols = Natch.select_cols!(conn, query)
  """
  @spec select_cols!(conn(), String.t() | Natch.Query.t()) :: map()
  def select_cols!(conn, query_or_sql) do
    case select_cols(conn, query_or_sql) do
      {:ok, cols} -> cols
      {:error, reason} -> raise "Query failed: #{inspect(reason)}"
    end
  end

  @doc """
  Executes a DDL or DML statement without returning results.

  Useful for CREATE, DROP, ALTER, INSERT, UPDATE, and DELETE statements.

  ## Examples

      # DDL queries (no parameters needed)
      :ok = Natch.execute(conn, "CREATE TABLE users (id UInt64, name String) ENGINE = Memory")
      :ok = Natch.execute(conn, "DROP TABLE users")
      :ok = Natch.execute(conn, "ALTER TABLE users ADD COLUMN age UInt8")

      # Parameterized INSERT (RECOMMENDED - prevents SQL injection)
      :ok = Natch.execute(
        conn,
        "INSERT INTO users VALUES ({id:UInt64}, {name:String}, {age:UInt32})",
        id: 1,
        name: "Alice",
        age: 30
      )

      # Parameterized UPDATE
      :ok = Natch.execute(
        conn,
        "UPDATE users SET status = {status:String} WHERE id = {id:UInt64}",
        status: "active",
        id: 42
      )

      # Parameterized DELETE
      :ok = Natch.execute(
        conn,
        "DELETE FROM users WHERE created_at < {cutoff:DateTime}",
        cutoff: ~U[2020-01-01 00:00:00Z]
      )

      # With map instead of keyword list
      params = %{id: 100, name: "Bob", age: 25}
      :ok = Natch.execute(
        conn,
        "INSERT INTO users VALUES ({id:UInt64}, {name:String}, {age:UInt32})",
        params
      )

      # Builder pattern for complex cases
      query = Natch.Query.new("INSERT INTO users VALUES ({id:UInt64}, {name:String})")
      |> Natch.Query.bind(:id, 1)
      |> Natch.Query.bind(:name, "Alice")
      :ok = Natch.execute(conn, query)
  """
  @spec execute(conn(), String.t() | Natch.Query.t()) :: :ok | {:error, term()}
  @spec execute(conn(), String.t(), keyword() | map()) :: :ok | {:error, term()}
  def execute(conn, %Natch.Query{} = query) do
    Connection.execute_parameterized(conn, query)
  end

  def execute(conn, sql) when is_binary(sql) do
    Connection.execute(conn, sql)
  end

  def execute(conn, sql, params) when is_binary(sql) and is_list(params) do
    query = Natch.Query.new(sql) |> Natch.Query.bind_all(params)
    execute(conn, query)
  end

  def execute(conn, sql, params) when is_binary(sql) and is_map(params) do
    query = Natch.Query.new(sql) |> Natch.Query.bind_all(params)
    execute(conn, query)
  end

  @doc """
  Executes a DDL or DML statement, raising on error.

  ## Examples

      # String query
      Natch.execute!(conn, "CREATE TABLE test (id UInt64) ENGINE = Memory")

      # Parameterized query
      query = Natch.Query.new("DELETE FROM users WHERE id = {id:UInt64}")
      |> Natch.Query.bind(:id, 123)
      Natch.execute!(conn, query)
  """
  @spec execute!(conn(), String.t() | Natch.Query.t()) :: :ok
  def execute!(conn, query_or_sql) do
    case execute(conn, query_or_sql) do
      :ok -> :ok
      {:error, reason} -> raise "Execute failed: #{inspect(reason)}"
    end
  end

  # Insert Operations

  @doc """
  Inserts data in row format (list of maps).

  Convenience wrapper that converts rows to columnar format before inserting.

  **Performance Note:** For bulk operations (1000+ rows), prefer `insert_cols/4`
  which avoids the O(N*M) conversion overhead. The conversion from rows to columns
  requires touching every value, making it less efficient for large datasets.

  ## Examples

      # Row format - convenient for small datasets or single inserts
      rows = [
        %{id: 1, name: "Alice"},
        %{id: 2, name: "Bob"}
      ]
      schema = [id: :uint64, name: :string]
      :ok = Natch.insert_rows(conn, "users", rows, schema)

      # Supports both atom and string keys
      rows = [
        %{"id" => 1, "name" => "Alice"},
        %{"id" => 2, "name" => "Bob"}
      ]
      :ok = Natch.insert_rows(conn, "users", rows, schema)
  """
  @spec insert_rows(conn(), String.t(), [map()], schema()) :: :ok | {:error, term()}
  def insert_rows(conn, table, rows, schema) when is_list(rows) and is_list(schema) do
    columns = Natch.Conversion.rows_to_columns(rows, schema)
    insert_cols(conn, table, columns, schema)
  end

  @doc """
  Inserts data in row format, raising on error.

  ## Examples

      rows = [%{id: 1, name: "Alice"}]
      schema = [id: :uint64, name: :string]
      Natch.insert_rows!(conn, "users", rows, schema)
  """
  @spec insert_rows!(conn(), String.t(), [map()], schema()) :: :ok
  def insert_rows!(conn, table, rows, schema) do
    case insert_rows(conn, table, rows, schema) do
      :ok -> :ok
      {:error, reason} -> raise "Insert failed: #{inspect(reason)}"
    end
  end

  @doc """
  Inserts data in columnar format (map of lists).

  **Recommended for performance:** Columnar format provides maximum performance by
  matching ClickHouse's native storage format. Makes M NIF calls (one per column)
  instead of N*M calls for row-oriented approaches.

  ## Why Columnar?

  - **Matches ClickHouse native storage** - no transposition needed
  - **100x fewer NIF calls** - M calls instead of N*M (e.g., 10 calls for 10k rows × 10 columns)
  - **Natural for analytics** - operations work on columns, not rows
  - **Better compression** - columnar data compresses more efficiently

  ## Performance Comparison

  For 10,000 rows × 10 columns:
  - `insert_cols`: 10 NIF calls (one per column)
  - `insert_rows`: 10 NIF calls + 100,000 cell conversions

  ## Examples

      # Columnar format (RECOMMENDED)
      columns = %{
        id: [1, 2, 3],
        name: ["Alice", "Bob", "Charlie"]
      }
      schema = [id: :uint64, name: :string]
      :ok = Natch.insert_cols(conn, "users", columns, schema)

      # Bulk insert (extremely efficient!)
      columns = %{
        id: Enum.to_list(1..100_000),
        value: Enum.map(1..100_000, & &1 * 2)
      }
      schema = [id: :uint64, value: :uint64]
      :ok = Natch.insert_cols(conn, "bulk_table", columns, schema)

      # All ClickHouse types supported
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
      :ok = Natch.insert_cols(conn, "events", columns, schema)
  """
  @spec insert_cols(conn(), String.t(), map(), schema()) :: :ok | {:error, term()}
  def insert_cols(conn, table, columns, schema) when is_map(columns) and is_list(schema) do
    GenServer.call(conn, {:insert, table, columns, schema}, :infinity)
  end

  @doc """
  Inserts data in columnar format, raising on error.

  ## Examples

      columns = %{id: [1, 2], name: ["Alice", "Bob"]}
      schema = [id: :uint64, name: :string]
      Natch.insert_cols!(conn, "users", columns, schema)
  """
  @spec insert_cols!(conn(), String.t(), map(), schema()) :: :ok
  def insert_cols!(conn, table, columns, schema) do
    case insert_cols(conn, table, columns, schema) do
      :ok -> :ok
      {:error, reason} -> raise "Insert failed: #{inspect(reason)}"
    end
  end
end
