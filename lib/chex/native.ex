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

  # Single-value append (deprecated, use bulk append for better performance)
  def column_uint64_append(_col, _value), do: :erlang.nif_error(:nif_not_loaded)
  def column_int64_append(_col, _value), do: :erlang.nif_error(:nif_not_loaded)
  def column_string_append(_col, _value), do: :erlang.nif_error(:nif_not_loaded)
  def column_float64_append(_col, _value), do: :erlang.nif_error(:nif_not_loaded)
  def column_datetime_append(_col, _timestamp), do: :erlang.nif_error(:nif_not_loaded)

  def column_size(_col), do: :erlang.nif_error(:nif_not_loaded)

  # Phase 5 - Bulk Append NIFs (Performance Optimization)
  def column_uint64_append_bulk(_col, _values), do: :erlang.nif_error(:nif_not_loaded)
  def column_int64_append_bulk(_col, _values), do: :erlang.nif_error(:nif_not_loaded)
  def column_string_append_bulk(_col, _values), do: :erlang.nif_error(:nif_not_loaded)
  def column_float64_append_bulk(_col, _values), do: :erlang.nif_error(:nif_not_loaded)
  def column_datetime_append_bulk(_col, _timestamps), do: :erlang.nif_error(:nif_not_loaded)

  # Phase 5C - Additional Type Support
  def column_datetime64_append_bulk(_col, _ticks), do: :erlang.nif_error(:nif_not_loaded)
  def column_date_append_bulk(_col, _days), do: :erlang.nif_error(:nif_not_loaded)
  def column_decimal_append_bulk(_col, _scaled_values), do: :erlang.nif_error(:nif_not_loaded)
  def column_uint8_append_bulk(_col, _values), do: :erlang.nif_error(:nif_not_loaded)
  def column_uint32_append_bulk(_col, _values), do: :erlang.nif_error(:nif_not_loaded)
  def column_uint16_append_bulk(_col, _values), do: :erlang.nif_error(:nif_not_loaded)
  def column_int32_append_bulk(_col, _values), do: :erlang.nif_error(:nif_not_loaded)
  def column_int16_append_bulk(_col, _values), do: :erlang.nif_error(:nif_not_loaded)
  def column_int8_append_bulk(_col, _values), do: :erlang.nif_error(:nif_not_loaded)
  def column_float32_append_bulk(_col, _values), do: :erlang.nif_error(:nif_not_loaded)
  def column_uuid_append_bulk(_col, _highs, _lows), do: :erlang.nif_error(:nif_not_loaded)

  # Array column NIF
  def column_array_append_from_column(_array_col, _nested_col, _offsets),
    do: :erlang.nif_error(:nif_not_loaded)

  # Tuple column NIF
  def column_tuple_append_from_columns(_tuple_col, _nested_cols),
    do: :erlang.nif_error(:nif_not_loaded)

  # Map column NIF
  def column_map_append_from_array(_map_col, _array_tuple_col),
    do: :erlang.nif_error(:nif_not_loaded)

  # LowCardinality column NIF
  def column_lowcardinality_append_from_column(_lc_col, _source_col),
    do: :erlang.nif_error(:nif_not_loaded)

  # Nullable type NIFs
  def column_nullable_uint64_append_bulk(_col, _values, _nulls),
    do: :erlang.nif_error(:nif_not_loaded)

  def column_nullable_int64_append_bulk(_col, _values, _nulls),
    do: :erlang.nif_error(:nif_not_loaded)

  def column_nullable_string_append_bulk(_col, _values, _nulls),
    do: :erlang.nif_error(:nif_not_loaded)

  def column_nullable_float64_append_bulk(_col, _values, _nulls),
    do: :erlang.nif_error(:nif_not_loaded)

  # Phase 3 - Block NIFs
  def block_create(), do: :erlang.nif_error(:nif_not_loaded)
  def block_append_column(_block, _name, _column), do: :erlang.nif_error(:nif_not_loaded)
  def block_row_count(_block), do: :erlang.nif_error(:nif_not_loaded)
  def block_column_count(_block), do: :erlang.nif_error(:nif_not_loaded)
  def client_insert(_client, _table_name, _block), do: :erlang.nif_error(:nif_not_loaded)

  # Phase 4 - SELECT NIFs
  def client_select(_client, _query), do: :erlang.nif_error(:nif_not_loaded)
  def client_select_cols(_client, _query), do: :erlang.nif_error(:nif_not_loaded)
end
