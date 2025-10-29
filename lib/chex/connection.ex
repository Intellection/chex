defmodule Chex.Connection do
  @moduledoc """
  GenServer that manages a ClickHouse client connection via native TCP protocol.

  ## Configuration Options

  - `:host` - ClickHouse server host (default: "localhost")
  - `:port` - Native TCP port (default: 9000)
  - `:database` - Database name (default: "default")
  - `:user` - Username (default: "default")
  - `:password` - Password (optional)
  - `:compression` - Enable LZ4 compression (default: true)
  - `:name` - Process name for registration (optional)
  """

  use GenServer
  alias Chex.Native

  @type option ::
          {:host, String.t()}
          | {:port, non_neg_integer()}
          | {:database, String.t()}
          | {:user, String.t()}
          | {:password, String.t()}
          | {:compression, boolean()}
          | {:name, atom()}

  @doc """
  Starts a new connection GenServer.

  ## Examples

      {:ok, conn} = Chex.Connection.start_link(
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
  Executes a SELECT query and returns results as a list of maps.

  ## Examples

      {:ok, rows} = Chex.Connection.select(conn, "SELECT id, name FROM users")
      # => {:ok, [%{id: 1, name: "Alice"}, %{id: 2, name: "Bob"}]}

  """
  @spec select(GenServer.server(), String.t()) :: {:ok, [map()]} | {:error, term()}
  def select(conn, query) do
    GenServer.call(conn, {:select, query}, :infinity)
  end

  # GenServer callbacks

  @impl true
  def init(opts) do
    case build_client(opts) do
      {:ok, client} ->
        {:ok, %{client: client, opts: opts}}

      {:error, reason} ->
        {:stop, reason}
    end
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
      e -> {:reply, {:error, Exception.message(e)}, state}
    end
  end

  @impl true
  def handle_call(:ping, _from, state) do
    try do
      Native.client_ping(state.client)
      {:reply, :ok, state}
    rescue
      e -> {:reply, {:error, Exception.message(e)}, state}
    end
  end

  @impl true
  def handle_call(:reset, _from, state) do
    try do
      Native.client_reset_connection(state.client)
      {:reply, :ok, state}
    rescue
      e -> {:reply, {:error, Exception.message(e)}, state}
    end
  end

  @impl true
  def handle_call({:insert, table, rows, schema}, _from, state) do
    try do
      # Build block from rows
      block = Chex.Insert.build_block(rows, schema)

      # Insert block
      Native.client_insert(state.client, table, block)

      {:reply, :ok, state}
    rescue
      e -> {:reply, {:error, Exception.message(e)}, state}
    end
  end

  @impl true
  def handle_call({:select, query}, _from, state) do
    try do
      # client_select returns list of maps directly
      rows = Native.client_select(state.client, query)

      {:reply, {:ok, rows}, state}
    rescue
      e -> {:reply, {:error, Exception.message(e)}, state}
    end
  end

  # Private functions

  defp build_client(opts) do
    host = Keyword.get(opts, :host, "localhost")
    port = Keyword.get(opts, :port, 9000)
    database = Keyword.get(opts, :database, "default")
    user = Keyword.get(opts, :user, "default")
    password = Keyword.get(opts, :password, "")
    compression = Keyword.get(opts, :compression, true)

    try do
      client =
        Native.client_create(
          host,
          port,
          database,
          user,
          password,
          compression
        )

      {:ok, client}
    rescue
      e -> {:error, Exception.message(e)}
    end
  end
end
