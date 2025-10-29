defmodule Chex.Native do
  @moduledoc false
  # Private module for FINE NIF declarations

  @on_load :load_nifs

  def load_nifs do
    path = :filename.join(:code.priv_dir(:chex), ~c"chex_fine")
    :ok = :erlang.load_nif(path, 0)
  end

  # Minimal PoC NIFs (backwards compatibility)
  def create_client, do: :erlang.nif_error(:nif_not_loaded)
  def ping(_client), do: :erlang.nif_error(:nif_not_loaded)

  # Phase 1 - Foundation NIFs
  def client_create(_host, _port, _database, _user, _password, _compression),
    do: :erlang.nif_error(:nif_not_loaded)

  def client_ping(_client), do: :erlang.nif_error(:nif_not_loaded)
  def client_execute(_client, _sql), do: :erlang.nif_error(:nif_not_loaded)
  def client_reset_connection(_client), do: :erlang.nif_error(:nif_not_loaded)

  # Phase 2 - Column NIFs
  def column_create(_type_name), do: :erlang.nif_error(:nif_not_loaded)
  def column_uint64_append(_col, _value), do: :erlang.nif_error(:nif_not_loaded)
  def column_int64_append(_col, _value), do: :erlang.nif_error(:nif_not_loaded)
  def column_string_append(_col, _value), do: :erlang.nif_error(:nif_not_loaded)
  def column_float64_append(_col, _value), do: :erlang.nif_error(:nif_not_loaded)
  def column_datetime_append(_col, _timestamp), do: :erlang.nif_error(:nif_not_loaded)
  def column_size(_col), do: :erlang.nif_error(:nif_not_loaded)

  # Phase 3 - Block NIFs
  def block_create(), do: :erlang.nif_error(:nif_not_loaded)
  def block_append_column(_block, _name, _column), do: :erlang.nif_error(:nif_not_loaded)
  def block_row_count(_block), do: :erlang.nif_error(:nif_not_loaded)
  def block_column_count(_block), do: :erlang.nif_error(:nif_not_loaded)
  def client_insert(_client, _table_name, _block), do: :erlang.nif_error(:nif_not_loaded)

  # Phase 4 - SELECT NIFs
  def client_select(_client, _query), do: :erlang.nif_error(:nif_not_loaded)
end
