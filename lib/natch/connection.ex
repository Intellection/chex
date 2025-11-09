defmodule Natch.Connection do
  @moduledoc false
  # Private implementation module for Natch.
  # Use the public API on the Natch module instead.

  use GenServer
  alias Natch.Native

  @type option ::
          {:host, String.t()}
          | {:port, non_neg_integer()}
          | {:database, String.t()}
          | {:user, String.t()}
          | {:password, String.t()}
          | {:compression, boolean()}
          | {:ssl, boolean()}
          | {:connect_timeout, non_neg_integer()}
          | {:recv_timeout, non_neg_integer()}
          | {:send_timeout, non_neg_integer()}
          | {:name, atom()}

  @doc """
  Starts a new connection GenServer.

  ## Examples

      {:ok, conn} = Natch.Connection.start_link(
        host: "localhost",
        port: 9000,
        database: "default",
        user: "default"
      )
  """
  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {gen_opts, client_opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, client_opts, gen_opts)
  end

  @doc """
  Gets the native client reference from the connection.
  """
  @spec get_client(GenServer.server()) :: {:ok, reference()} | {:error, term()}
  def get_client(conn) do
    GenServer.call(conn, :get_client)
  end

  @doc """
  Executes a query (DDL/DML) without returning results.
  """
  @spec execute(GenServer.server(), String.t()) :: :ok | {:error, term()}
  def execute(conn, sql) do
    GenServer.call(conn, {:execute, sql})
  end

  @doc """
  Pings the ClickHouse server.
  """
  @spec ping(GenServer.server()) :: :ok | {:error, term()}
  def ping(conn) do
    GenServer.call(conn, :ping)
  end

  @doc """
  Resets the connection.
  """
  @spec reset(GenServer.server()) :: :ok | {:error, term()}
  def reset(conn) do
    GenServer.call(conn, :reset)
  end

  @doc """
  Executes a SELECT query and returns results in row-major format (list of maps).

  Each row is represented as a map with column names as keys.

  ## Examples

      {:ok, rows} = Natch.Connection.select_rows(conn, "SELECT id, name FROM users")
      # => {:ok, [%{id: 1, name: "Alice"}, %{id: 2, name: "Bob"}]}

  """
  @spec select_rows(GenServer.server(), String.t()) :: {:ok, [map()]} | {:error, term()}
  def select_rows(conn, query) do
    GenServer.call(conn, {:select_rows, query}, :infinity)
  end

  @doc """
  Executes a SELECT query and returns results in columnar format (map of lists).

  Each column is represented as a list of values, with column names as map keys.
  This format is more efficient for large result sets and enables easier integration
  with data analysis tools.

  ## Examples

      {:ok, cols} = Natch.Connection.select_cols(conn, "SELECT id, name FROM users")
      # => {:ok, %{id: [1, 2], name: ["Alice", "Bob"]}}

  """
  @spec select_cols(GenServer.server(), String.t()) :: {:ok, map()} | {:error, term()}
  def select_cols(conn, query) do
    GenServer.call(conn, {:select_cols, query}, :infinity)
  end

  # Phase 6C - Parameterized Query API

  @doc """
  Executes a parameterized query (DDL/DML) without returning results.
  """
  @spec execute_parameterized(GenServer.server(), Natch.Query.t()) :: :ok | {:error, term()}
  def execute_parameterized(conn, query) do
    GenServer.call(conn, {:execute_parameterized, query})
  end

  @doc """
  Executes a parameterized SELECT query and returns results in row-major format.
  """
  @spec select_rows_parameterized(GenServer.server(), Natch.Query.t()) ::
          {:ok, [map()]} | {:error, term()}
  def select_rows_parameterized(conn, query) do
    GenServer.call(conn, {:select_rows_parameterized, query}, :infinity)
  end

  @doc """
  Executes a parameterized SELECT query and returns results in columnar format.
  """
  @spec select_cols_parameterized(GenServer.server(), Natch.Query.t()) ::
          {:ok, map()} | {:error, term()}
  def select_cols_parameterized(conn, query) do
    GenServer.call(conn, {:select_cols_parameterized, query}, :infinity)
  end

  # GenServer callbacks

  @impl true
  def init(opts) do
    {:ok, client} = build_client(opts)
    {:ok, %{client: client, opts: opts}}
  end

  @impl true
  def handle_call(:get_client, _from, state) do
    {:reply, {:ok, state.client}, state}
  end

  @impl true
  def handle_call({:execute, sql}, _from, state) do
    try do
      Native.client_execute(state.client, sql)
      {:reply, :ok, state}
    rescue
      e -> {:reply, error_tuple(e), state}
    end
  end

  @impl true
  def handle_call(:ping, _from, state) do
    try do
      Native.client_ping(state.client)
      {:reply, :ok, state}
    rescue
      e -> {:reply, error_tuple(e), state}
    end
  end

  @impl true
  def handle_call(:reset, _from, state) do
    try do
      Native.client_reset_connection(state.client)
      {:reply, :ok, state}
    rescue
      e -> {:reply, error_tuple(e), state}
    end
  end

  @impl true
  def handle_call({:insert, table, columns, schema}, _from, state) do
    try do
      # Build block from columnar data
      block = Natch.Block.build_block(columns, schema)

      # Insert block
      Native.client_insert(state.client, table, block)

      {:reply, :ok, state}
    rescue
      e -> {:reply, error_tuple(e), state}
    end
  end

  @impl true
  def handle_call({:select_rows, query}, _from, state) do
    try do
      # client_select returns list of maps directly
      rows = Native.client_select(state.client, query)

      {:reply, {:ok, rows}, state}
    rescue
      e -> {:reply, error_tuple(e), state}
    end
  end

  @impl true
  def handle_call({:select_cols, query}, _from, state) do
    try do
      # client_select_cols returns map of column lists
      cols = Native.client_select_cols(state.client, query)

      {:reply, {:ok, cols}, state}
    rescue
      e -> {:reply, error_tuple(e), state}
    end
  end

  # Phase 6C - Parameterized Query Support

  @impl true
  def handle_call({:execute_parameterized, query}, _from, state) do
    try do
      Native.client_execute_parameterized(state.client, query.ref)
      {:reply, :ok, state}
    rescue
      e -> {:reply, error_tuple(e), state}
    end
  end

  @impl true
  def handle_call({:select_rows_parameterized, query}, _from, state) do
    try do
      rows = Native.client_select_parameterized(state.client, query.ref)
      {:reply, {:ok, rows}, state}
    rescue
      e -> {:reply, error_tuple(e), state}
    end
  end

  @impl true
  def handle_call({:select_cols_parameterized, query}, _from, state) do
    try do
      cols = Native.client_select_cols_parameterized(state.client, query.ref)
      {:reply, {:ok, cols}, state}
    rescue
      e -> {:reply, error_tuple(e), state}
    end
  end

  # Private functions

  # Delegate to shared error handling
  defp handle_error(exception_struct) do
    Natch.Error.handle_nif_error(exception_struct)
  end

  defp error_tuple(exception_struct) do
    Natch.Error.handle_callback_error(exception_struct)
  end

  defp build_client(opts) do
    host = Keyword.get(opts, :host, "localhost")
    port = Keyword.get(opts, :port, 9000)
    database = Keyword.get(opts, :database, "default")
    user = Keyword.get(opts, :user, "default")
    password = Keyword.get(opts, :password, "")
    compression = Keyword.get(opts, :compression, true)
    ssl = Keyword.get(opts, :ssl, false)

    # Timeout options - match C++ library defaults
    connect_timeout = Keyword.get(opts, :connect_timeout, 5000)
    recv_timeout = Keyword.get(opts, :recv_timeout, 0)
    send_timeout = Keyword.get(opts, :send_timeout, 0)

    try do
      client =
        Native.client_create(
          host,
          port,
          database,
          user,
          password,
          compression,
          ssl,
          connect_timeout,
          recv_timeout,
          send_timeout
        )

      {:ok, client}
    rescue
      e -> handle_error(e)
    end
  end
end
