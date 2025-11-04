# Natch-Only Benchmark Suite
#
# Usage:
#   mix run bench/natch_only_bench.exs
#
# Requires ClickHouse running:
#   docker-compose up -d

Code.require_file("helpers.ex", __DIR__)

alias Bench.Helpers

defmodule NatchOnlyBench do
  @moduledoc """
  Benchmark Natch performance in isolation.
  """

  def run do
    IO.puts("\n=== Natch Performance Benchmark ===\n")
    IO.puts("Starting ClickHouse connection...")

    # Setup connection
    {:ok, natch_conn} =
      Natch.Connection.start_link(
        host: "localhost",
        port: 9000,
        database: "default"
      )

    IO.puts("✓ Connection established\n")

    # Generate test data
    IO.puts("Generating test data...")
    {columns_10k, schema} = Helpers.generate_test_data(10_000)
    {columns_100k, _} = Helpers.generate_test_data(100_000)
    {columns_1m, _} = Helpers.generate_test_data(1_000_000)

    IO.puts("✓ Test data generated\n")

    # Run INSERT benchmarks
    IO.puts("=== INSERT Benchmarks ===\n")

    Benchee.run(
      %{
        "Natch INSERT 10k rows" => fn ->
          table = Helpers.unique_table_name("natch_insert_10k")
          Natch.Connection.execute(natch_conn, Helpers.create_test_table(table))
          :ok = Natch.insert(natch_conn, table, columns_10k, schema)
          Natch.Connection.execute(natch_conn, Helpers.drop_test_table(table))
        end,
        "Natch INSERT 100k rows" => fn ->
          table = Helpers.unique_table_name("natch_insert_100k")
          Natch.Connection.execute(natch_conn, Helpers.create_test_table(table))
          :ok = Natch.insert(natch_conn, table, columns_100k, schema)
          Natch.Connection.execute(natch_conn, Helpers.drop_test_table(table))
        end,
        "Natch INSERT 1M rows" => fn ->
          table = Helpers.unique_table_name("natch_insert_1m")
          Natch.Connection.execute(natch_conn, Helpers.create_test_table(table))
          :ok = Natch.insert(natch_conn, table, columns_1m, schema)
          Natch.Connection.execute(natch_conn, Helpers.drop_test_table(table))
        end
      },
      warmup: 1,
      time: 5,
      memory_time: 2,
      formatters: [
        Benchee.Formatters.Console,
        {Benchee.Formatters.HTML, file: "bench/results_natch_insert.html"}
      ],
      print: [
        benchmarking: true,
        configuration: true,
        fast_warning: false
      ]
    )

    # Setup table for SELECT benchmarks
    IO.puts("\n=== Setting up SELECT benchmark table ===\n")
    select_table = "natch_select_bench"

    Natch.Connection.execute(natch_conn, Helpers.drop_test_table(select_table))
    Natch.Connection.execute(natch_conn, Helpers.create_test_table(select_table))
    IO.puts("Inserting 1M rows for SELECT benchmarks...")
    :ok = Natch.insert(natch_conn, select_table, columns_1m, schema)

    IO.puts("✓ Table populated with 1M rows\n")

    # Run SELECT benchmarks
    IO.puts("=== SELECT Benchmarks ===\n")

    Benchee.run(
      %{
        "Natch SELECT all 1M rows (row-major)" => fn ->
          {:ok, _rows} = Natch.Connection.select_rows(natch_conn, "SELECT * FROM #{select_table}")
        end,
        "Natch SELECT all 1M rows (columnar)" => fn ->
          {:ok, _cols} = Natch.Connection.select_cols(natch_conn, "SELECT * FROM #{select_table}")
        end,
        "Natch SELECT filtered 10k rows (row-major)" => fn ->
          {:ok, _rows} =
            Natch.Connection.select_rows(
              natch_conn,
              "SELECT * FROM #{select_table} WHERE user_id < 1000"
            )
        end,
        "Natch SELECT filtered 10k rows (columnar)" => fn ->
          {:ok, _cols} =
            Natch.Connection.select_cols(
              natch_conn,
              "SELECT * FROM #{select_table} WHERE user_id < 1000"
            )
        end,
        "Natch SELECT aggregation (row-major)" => fn ->
          {:ok, _rows} =
            Natch.Connection.select_rows(
              natch_conn,
              "SELECT event_type, count(*) as cnt FROM #{select_table} GROUP BY event_type"
            )
        end,
        "Natch SELECT aggregation (columnar)" => fn ->
          {:ok, _cols} =
            Natch.Connection.select_cols(
              natch_conn,
              "SELECT event_type, count(*) as cnt FROM #{select_table} GROUP BY event_type"
            )
        end
      },
      warmup: 1,
      time: 5,
      memory_time: 2,
      formatters: [
        Benchee.Formatters.Console,
        {Benchee.Formatters.HTML, file: "bench/results_natch_select.html"}
      ],
      print: [
        benchmarking: true,
        configuration: true,
        fast_warning: false
      ]
    )

    # Cleanup
    IO.puts("\n=== Cleaning up ===\n")
    Natch.Connection.execute(natch_conn, Helpers.drop_test_table(select_table))

    GenServer.stop(natch_conn)

    IO.puts("✓ Benchmark complete!\n")
    IO.puts("HTML reports generated:")
    IO.puts("  - bench/results_natch_insert.html")
    IO.puts("  - bench/results_natch_select.html\n")
  end
end

# Run the benchmark
NatchOnlyBench.run()
