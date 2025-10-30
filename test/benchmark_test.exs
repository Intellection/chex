defmodule Chex.BenchmarkTest do
  @moduledoc """
  Performance benchmarks for Chex.

  These tests are excluded by default. Run them with:
      mix test --include benchmark

  ## Current Performance Baseline (2024-10-30)
  - Bulk insert 100k rows: ~18ms
  - Query 100k rows: ~40ms
  - rows_to_columns (10k): ~0.6ms
  - columns_to_rows (10k): ~400ms

  Note: Streaming was removed. For large datasets, use Chex.insert/4 directly
  as clickhouse-cpp handles wire-level chunking (64KB blocks) automatically.
  """

  use ExUnit.Case, async: false

  @moduletag :benchmark

  alias Chex.Connection

  setup do
    # Generate unique table name for this test
    table = "bench_#{System.unique_integer([:positive, :monotonic])}_#{:rand.uniform(999_999)}"

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

  describe "Large dataset performance" do
    test "benchmark: bulk insert 100k rows", %{conn: conn, table: table} do
      # Create table
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        value UInt64
      ) ENGINE = Memory
      """)

      # Generate 100k rows
      columns = %{
        id: Enum.to_list(1..100_000),
        value: Enum.map(1..100_000, &(&1 * 2))
      }

      schema = [id: :uint64, value: :uint64]

      # Benchmark single bulk insert
      {time_us, :ok} =
        :timer.tc(fn ->
          Chex.insert(conn, table, columns, schema)
        end)

      time_ms = time_us / 1000

      IO.puts("\n  Bulk inserted 100k rows in #{Float.round(time_ms, 2)}ms")

      # Verify count
      {:ok, result} = Connection.select(conn, "SELECT count() as cnt FROM #{table}")
      assert [%{cnt: 100_000}] = result
    end

    test "benchmark: query 100k rows", %{conn: conn, table: table} do
      # Create and populate table
      Connection.execute(conn, """
      CREATE TABLE #{table} (
        id UInt64,
        value UInt64
      ) ENGINE = Memory
      """)

      columns = %{
        id: Enum.to_list(1..100_000),
        value: Enum.map(1..100_000, &(&1 * 2))
      }

      schema = [id: :uint64, value: :uint64]
      Chex.insert(conn, table, columns, schema)

      # Benchmark query
      {time_us, {:ok, rows}} =
        :timer.tc(fn ->
          Connection.select(conn, "SELECT * FROM #{table}")
        end)

      time_ms = time_us / 1000

      IO.puts("\n  Queried 100k rows in #{Float.round(time_ms, 2)}ms")

      assert length(rows) == 100_000
    end

    test "benchmark: conversion roundtrip 10k rows", %{conn: _conn, table: _table} do
      # Generate 10k rows
      rows =
        for i <- 1..10_000 do
          %{id: i, name: "user_#{i}", amount: i * 10.5}
        end

      schema = [id: :uint64, name: :string, amount: :float64]

      # Benchmark rows -> columns
      {time_us_to_cols, columns} =
        :timer.tc(fn ->
          Chex.Conversion.rows_to_columns(rows, schema)
        end)

      # Benchmark columns -> rows
      {time_us_to_rows, result_rows} =
        :timer.tc(fn ->
          Chex.Conversion.columns_to_rows(columns, schema)
        end)

      time_ms_to_cols = time_us_to_cols / 1000
      time_ms_to_rows = time_us_to_rows / 1000

      IO.puts("\n  rows_to_columns (10k rows): #{Float.round(time_ms_to_cols, 2)}ms")
      IO.puts("  columns_to_rows (10k rows): #{Float.round(time_ms_to_rows, 2)}ms")

      assert result_rows == rows
    end
  end
end
