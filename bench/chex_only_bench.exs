# Chex-Only Benchmark Suite
#
# Usage:
#   mix run bench/chex_only_bench.exs
#
# Requires ClickHouse running:
#   docker-compose up -d

Code.require_file("helpers.ex", __DIR__)

alias Bench.Helpers

defmodule ChexOnlyBench do
  @moduledoc """
  Benchmark Chex performance in isolation.
  """

  def run do
    IO.puts("\n=== Chex Performance Benchmark ===\n")
    IO.puts("Starting ClickHouse connection...")

    # Setup connection
    {:ok, chex_conn} =
      Chex.Connection.start_link(
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
        "Chex INSERT 10k rows" => fn ->
          table = Helpers.unique_table_name("chex_insert_10k")
          Chex.Connection.execute(chex_conn, Helpers.create_test_table(table))
          :ok = Chex.insert(chex_conn, table, columns_10k, schema)
          Chex.Connection.execute(chex_conn, Helpers.drop_test_table(table))
        end,
        "Chex INSERT 100k rows" => fn ->
          table = Helpers.unique_table_name("chex_insert_100k")
          Chex.Connection.execute(chex_conn, Helpers.create_test_table(table))
          :ok = Chex.insert(chex_conn, table, columns_100k, schema)
          Chex.Connection.execute(chex_conn, Helpers.drop_test_table(table))
        end,
        "Chex INSERT 1M rows" => fn ->
          table = Helpers.unique_table_name("chex_insert_1m")
          Chex.Connection.execute(chex_conn, Helpers.create_test_table(table))
          :ok = Chex.insert(chex_conn, table, columns_1m, schema)
          Chex.Connection.execute(chex_conn, Helpers.drop_test_table(table))
        end
      },
      warmup: 1,
      time: 5,
      memory_time: 2,
      formatters: [
        Benchee.Formatters.Console,
        {Benchee.Formatters.HTML, file: "bench/results_chex_insert.html"}
      ],
      print: [
        benchmarking: true,
        configuration: true,
        fast_warning: false
      ]
    )

    # Setup table for SELECT benchmarks
    IO.puts("\n=== Setting up SELECT benchmark table ===\n")
    select_table = "chex_select_bench"

    Chex.Connection.execute(chex_conn, Helpers.drop_test_table(select_table))
    Chex.Connection.execute(chex_conn, Helpers.create_test_table(select_table))
    IO.puts("Inserting 1M rows for SELECT benchmarks...")
    :ok = Chex.insert(chex_conn, select_table, columns_1m, schema)

    IO.puts("✓ Table populated with 1M rows\n")

    # Run SELECT benchmarks
    IO.puts("=== SELECT Benchmarks ===\n")

    Benchee.run(
      %{
        "Chex SELECT all 1M rows" => fn ->
          {:ok, _rows} = Chex.Connection.select(chex_conn, "SELECT * FROM #{select_table}")
        end,
        "Chex SELECT filtered (10k rows)" => fn ->
          {:ok, _rows} =
            Chex.Connection.select(
              chex_conn,
              "SELECT * FROM #{select_table} WHERE user_id < 1000"
            )
        end,
        "Chex SELECT aggregation" => fn ->
          {:ok, _rows} =
            Chex.Connection.select(
              chex_conn,
              "SELECT event_type, count(*) as cnt FROM #{select_table} GROUP BY event_type"
            )
        end
      },
      warmup: 1,
      time: 5,
      memory_time: 2,
      formatters: [
        Benchee.Formatters.Console,
        {Benchee.Formatters.HTML, file: "bench/results_chex_select.html"}
      ],
      print: [
        benchmarking: true,
        configuration: true,
        fast_warning: false
      ]
    )

    # Cleanup
    IO.puts("\n=== Cleaning up ===\n")
    Chex.Connection.execute(chex_conn, Helpers.drop_test_table(select_table))

    GenServer.stop(chex_conn)

    IO.puts("✓ Benchmark complete!\n")
    IO.puts("HTML reports generated:")
    IO.puts("  - bench/results_chex_insert.html")
    IO.puts("  - bench/results_chex_select.html\n")
  end
end

# Run the benchmark
ChexOnlyBench.run()
