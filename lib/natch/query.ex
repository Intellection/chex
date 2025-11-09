defmodule Natch.Query do
  @moduledoc """
  Type-safe parameterized query builder for ClickHouse.

  This module provides protection against SQL injection by using ClickHouse's
  native parameter binding. Parameters are transmitted separately from the SQL
  text over the wire, ensuring values cannot be interpreted as SQL commands.

  ## Parameter Syntax

  Use `{name:Type}` syntax in your SQL to define parameters:

      "SELECT * FROM users WHERE id = {id:UInt64}"
      "INSERT INTO events VALUES ({user_id:UInt32}, {event:String}, {ts:DateTime})"

  ## Type Inference

  The `bind/3` function automatically infers ClickHouse types from Elixir values:

  - Non-negative integers → `UInt64`
  - Negative integers → `Int64`
  - Floats → `Float64`
  - Strings → `String`
  - `DateTime` structs → `DateTime`
  - `Date` structs → `Date`
  - `nil` → `NULL` (works with any `Nullable(T)` type)

  Use `bind/4` for explicit type control when needed (e.g., `Int32`, `UInt32`, `Float32`).

  ## Examples

      # Basic parameterized SELECT
      query = Natch.Query.new("SELECT * FROM users WHERE id = {id:UInt64}")
      |> Natch.Query.bind(:id, 42)

      {:ok, rows} = Natch.select_rows(conn, query)

      # Multiple parameters with automatic type inference
      query = Natch.Query.new(\"""
        SELECT * FROM events
        WHERE user_id = {uid:UInt64}
        AND created_at > {start:DateTime}
        AND status = {status:String}
      \""")
      |> Natch.Query.bind(:uid, 123)
      |> Natch.Query.bind(:start, ~U[2024-01-01 00:00:00Z])
      |> Natch.Query.bind(:status, "active")

      {:ok, results} = Natch.select_rows(conn, query)

      # NULL parameter support
      query = Natch.Query.new("INSERT INTO users VALUES ({id:UInt64}, {name:Nullable(String)})")
      |> Natch.Query.bind(:id, 100)
      |> Natch.Query.bind(:name, nil)

      :ok = Natch.execute(conn, query)

      # Explicit type specification
      query = Natch.Query.new("SELECT * FROM metrics WHERE count > {min:Int32}")
      |> Natch.Query.bind(:min, 1000, :int32)

  ## Security Best Practices

  **Use parameterized queries when:**
  - Query contains user input (form data, API parameters, etc.)
  - Building dynamic WHERE clauses
  - Inserting user-generated content

  **String queries are okay for:**
  - Static SQL with no variables
  - Queries constructed entirely from trusted constants
  - DDL operations (CREATE TABLE, etc.)

  ## Type Mapping

  | Elixir Type | ClickHouse Type (inferred) |
  |-------------|---------------------------|
  | `integer() >= 0` | `UInt64` |
  | `integer() < 0` | `Int64` |
  | `float()` | `Float64` |
  | `String.t()` | `String` |
  | `DateTime.t()` | `DateTime` |
  | `Date.t()` | `Date` |
  | `nil` | `NULL` |

  Use explicit type binding for other types:
  - `Int32`, `Int16`, `Int8`
  - `UInt32`, `UInt16`, `UInt8`
  - `Float32`
  - `DateTime64`
  """

  @type t :: %__MODULE__{
          sql: String.t(),
          params: %{optional(atom()) => param_value()},
          ref: reference()
        }

  @type param_value :: integer() | float() | String.t() | DateTime.t() | Date.t() | nil
  @type param_type ::
          :uint64
          | :uint32
          | :uint16
          | :uint8
          | :int64
          | :int32
          | :int16
          | :int8
          | :float64
          | :float32
          | :string
          | :datetime
          | :datetime64
          | :date

  defstruct [:sql, :params, :ref]

  @doc """
  Creates a new parameterized query.

  The SQL string should contain parameter placeholders using the syntax `{name:Type}`.

  ## Examples

      iex> query = Natch.Query.new("SELECT * FROM users WHERE id = {id:UInt64}")
      %Natch.Query{sql: "SELECT * FROM users WHERE id = {id:UInt64}", ...}

      iex> query = Natch.Query.new(\"""
      ...>   SELECT user_id, count(*) as cnt
      ...>   FROM events
      ...>   WHERE created_at > {start:DateTime}
      ...>   GROUP BY user_id
      ...> \""")
  """
  @spec new(String.t()) :: t()
  def new(sql) when is_binary(sql) do
    ref = Natch.Native.query_create(sql)
    %__MODULE__{sql: sql, params: %{}, ref: ref}
  end

  @doc """
  Binds a parameter value with automatic type inference.

  The type is inferred from the Elixir value:
  - Non-negative integers become `UInt64`
  - Negative integers become `Int64`
  - Floats become `Float64`
  - Strings remain `String`
  - DateTime structs become `DateTime` (Unix timestamp)
  - Date structs become `Date` (days since epoch)
  - `nil` becomes `NULL`

  ## Examples

      query = Natch.Query.new("SELECT * FROM users WHERE id = {id:UInt64}")
      |> Natch.Query.bind(:id, 42)

      query = Natch.Query.new("INSERT INTO logs VALUES ({msg:String}, {ts:DateTime})")
      |> Natch.Query.bind(:msg, "User login")
      |> Natch.Query.bind(:ts, DateTime.utc_now())

      # NULL support
      query = Natch.Query.new("UPDATE users SET notes = {notes:Nullable(String)}")
      |> Natch.Query.bind(:notes, nil)
  """
  @spec bind(t(), atom() | String.t(), param_value()) :: t()
  def bind(%__MODULE__{} = query, name, value) do
    param_name = to_string(name)

    case bind_value(query.ref, param_name, value) do
      :ok ->
        %{query | params: Map.put(query.params, name, value)}

      {:error, reason} ->
        raise ArgumentError, "Failed to bind parameter #{name}: #{inspect(reason)}"
    end
  end

  @doc """
  Binds a parameter with explicit type specification.

  Useful when automatic type inference doesn't match your needs, such as:
  - Using `Int32` instead of `Int64` for memory efficiency
  - Forcing `UInt32` when the value might be negative in edge cases
  - Using `Float32` instead of `Float64`
  - Using `DateTime64` for microsecond precision

  ## Examples

      # Force Int32 for smaller integers
      query |> Natch.Query.bind(:count, 100, :int32)

      # Explicit unsigned integer
      query |> Natch.Query.bind(:id, user_id, :uint32)

      # Single precision float
      query |> Natch.Query.bind(:ratio, 0.5, :float32)

      # Microsecond precision timestamp
      query |> Natch.Query.bind(:ts, DateTime.utc_now(), :datetime64)
  """
  @spec bind(t(), atom() | String.t(), param_value(), param_type()) :: t()
  def bind(%__MODULE__{} = query, name, value, type) when is_atom(type) do
    param_name = to_string(name)

    case bind_value_typed(query.ref, param_name, value, type) do
      :ok ->
        %{query | params: Map.put(query.params, name, {value, type})}

      {:error, reason} ->
        raise ArgumentError, "Failed to bind parameter #{name} as #{type}: #{inspect(reason)}"
    end
  end

  @doc """
  Binds multiple parameters from a keyword list or map.

  This is a convenience function that calls `bind/3` for each parameter,
  using automatic type inference. For explicit type control, use `bind/4`
  individually.

  ## Examples

      # With keyword list
      query = Natch.Query.new("SELECT * FROM users WHERE id = {id:UInt64} AND status = {status:String}")
      |> Natch.Query.bind_all(id: 42, status: "active")

      # With map
      params = %{id: 42, status: "active"}
      query = Natch.Query.new("SELECT * FROM users WHERE id = {id:UInt64} AND status = {status:String}")
      |> Natch.Query.bind_all(params)

      # Empty params are valid (no-op)
      query = Natch.Query.new("SELECT * FROM users")
      |> Natch.Query.bind_all([])
  """
  @spec bind_all(t(), keyword() | map()) :: t()
  def bind_all(%__MODULE__{} = query, params) when is_list(params) do
    Enum.reduce(params, query, fn {key, value}, acc ->
      bind(acc, key, value)
    end)
  end

  def bind_all(%__MODULE__{} = query, params) when is_map(params) do
    Enum.reduce(params, query, fn {key, value}, acc ->
      bind(acc, key, value)
    end)
  end

  # Private: Bind value with automatic type inference
  defp bind_value(ref, name, value) when is_integer(value) and value >= 0 do
    Natch.Native.query_bind_uint64(ref, name, value)
  end

  defp bind_value(ref, name, value) when is_integer(value) do
    Natch.Native.query_bind_int64(ref, name, value)
  end

  defp bind_value(ref, name, value) when is_binary(value) do
    Natch.Native.query_bind_string(ref, name, value)
  end

  defp bind_value(ref, name, value) when is_float(value) do
    Natch.Native.query_bind_float64(ref, name, value)
  end

  defp bind_value(ref, name, %DateTime{} = value) do
    timestamp = DateTime.to_unix(value)
    Natch.Native.query_bind_datetime(ref, name, timestamp)
  end

  defp bind_value(ref, name, %Date{} = value) do
    # ClickHouse Date is days since 1970-01-01
    days = Date.to_gregorian_days(value) - 719_528
    Natch.Native.query_bind_date(ref, name, days)
  end

  defp bind_value(ref, name, nil) do
    Natch.Native.query_bind_null(ref, name)
  end

  defp bind_value(_ref, _name, value) do
    {:error,
     "Unsupported parameter type: #{inspect(value)}. Use bind/4 for explicit type control."}
  end

  # Private: Bind value with explicit type
  defp bind_value_typed(ref, name, value, :uint64) when is_integer(value) do
    Natch.Native.query_bind_uint64(ref, name, value)
  end

  defp bind_value_typed(ref, name, value, :uint32) when is_integer(value) do
    Natch.Native.query_bind_uint32(ref, name, value)
  end

  defp bind_value_typed(ref, name, value, :int64) when is_integer(value) do
    Natch.Native.query_bind_int64(ref, name, value)
  end

  defp bind_value_typed(ref, name, value, :int32) when is_integer(value) do
    Natch.Native.query_bind_int32(ref, name, value)
  end

  defp bind_value_typed(ref, name, value, :float64) when is_float(value) or is_integer(value) do
    Natch.Native.query_bind_float64(ref, name, value * 1.0)
  end

  defp bind_value_typed(ref, name, value, :float32) when is_float(value) or is_integer(value) do
    Natch.Native.query_bind_float32(ref, name, value * 1.0)
  end

  defp bind_value_typed(ref, name, value, :string) when is_binary(value) do
    Natch.Native.query_bind_string(ref, name, value)
  end

  defp bind_value_typed(ref, name, %DateTime{} = value, :datetime) do
    timestamp = DateTime.to_unix(value)
    Natch.Native.query_bind_datetime(ref, name, timestamp)
  end

  defp bind_value_typed(ref, name, %DateTime{} = value, :datetime64) do
    microseconds = DateTime.to_unix(value, :microsecond)
    Natch.Native.query_bind_datetime64(ref, name, microseconds)
  end

  defp bind_value_typed(ref, name, value, :datetime64) when is_integer(value) do
    Natch.Native.query_bind_datetime64(ref, name, value)
  end

  defp bind_value_typed(ref, name, %Date{} = value, :date) do
    days = Date.to_gregorian_days(value) - 719_528
    Natch.Native.query_bind_date(ref, name, days)
  end

  defp bind_value_typed(ref, name, value, :date) when is_integer(value) do
    Natch.Native.query_bind_date(ref, name, value)
  end

  defp bind_value_typed(ref, name, nil, _type) do
    Natch.Native.query_bind_null(ref, name)
  end

  defp bind_value_typed(_ref, _name, value, type) do
    {:error, "Cannot bind #{inspect(value)} as #{type}. Type mismatch."}
  end
end
