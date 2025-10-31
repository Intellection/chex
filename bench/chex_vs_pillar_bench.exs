# Chex vs Pillar Benchmark Suite
#
# Usage:
#   mix run bench/chex_vs_pillar_bench.exs
#
# Requires ClickHouse running:
#   docker-compose up -d

Code.require_file("helpers.ex", __DIR__)

alias Bench.Helpers

defmodule ChexVsPillarBench do
  @moduledoc """
  Comprehensive benchmark comparing Chex (native TCP) vs Pillar (HTTP).
  """

  def run do
    IO.puts("\n=== Chex vs Pillar Benchmark Suite ===\n")
    IO.puts("Starting ClickHouse connections...")

    # Setup connections
    {:ok, chex_conn} = setup_chex()
    pillar_conn = setup_pillar()

    IO.puts("✓ Connections established\n")

    # Generate test data
    IO.puts("Generating test data...")
    {columns_10k, schema} = Helpers.generate_test_data(10_000)
    {columns_100k, _} = Helpers.generate_test_data(100_000)
    {columns_1m, _} = Helpers.generate_test_data(1_000_000)

    IO.puts("Generating row-format data for Pillar...")
    rows_10k = Helpers.generate_test_data_rows(10_000)
    rows_100k = Helpers.generate_test_data_rows(100_000)
    rows_1m = Helpers.generate_test_data_rows(1_000_000)

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
        "Pillar INSERT 10k rows" => fn ->
          table = Helpers.unique_table_name("pillar_insert_10k")
          {:ok, _} = Pillar.query(pillar_conn, Helpers.create_test_table(table))
          {:ok, _} = Pillar.insert_to_table(pillar_conn, table, rows_10k)
          {:ok, _} = Pillar.query(pillar_conn, Helpers.drop_test_table(table))
        end,
        "Chex INSERT 100k rows" => fn ->
          table = Helpers.unique_table_name("chex_insert_100k")
          Chex.Connection.execute(chex_conn, Helpers.create_test_table(table))
          :ok = Chex.insert(chex_conn, table, columns_100k, schema)
          Chex.Connection.execute(chex_conn, Helpers.drop_test_table(table))
        end,
        "Pillar INSERT 100k rows" => fn ->
          table = Helpers.unique_table_name("pillar_insert_100k")
          {:ok, _} = Pillar.query(pillar_conn, Helpers.create_test_table(table))
          {:ok, _} = Pillar.insert_to_table(pillar_conn, table, rows_100k)
          {:ok, _} = Pillar.query(pillar_conn, Helpers.drop_test_table(table))
        end,
        "Chex INSERT 1M rows" => fn ->
          table = Helpers.unique_table_name("chex_insert_1m")
          Chex.Connection.execute(chex_conn, Helpers.create_test_table(table))
          :ok = Chex.insert(chex_conn, table, columns_1m, schema)
          Chex.Connection.execute(chex_conn, Helpers.drop_test_table(table))
        end,
        "Pillar INSERT 1M rows" => fn ->
          table = Helpers.unique_table_name("pillar_insert_1m")
          {:ok, _} = Pillar.query(pillar_conn, Helpers.create_test_table(table))
          {:ok, _} = Pillar.insert_to_table(pillar_conn, table, rows_1m)
          {:ok, _} = Pillar.query(pillar_conn, Helpers.drop_test_table(table))
        end
      },
      warmup: 1,
      time: 5,
      memory_time: 2,
      formatters: [
        Benchee.Formatters.Console,
        {Benchee.Formatters.HTML, file: "bench/results_insert.html"}
      ],
      print: [
        benchmarking: true,
        configuration: true,
        fast_warning: false
      ]
    )

    # Setup tables for SELECT benchmarks
    IO.puts("\n=== Setting up SELECT benchmark tables ===\n")
    chex_select_table = "chex_select_bench"
    pillar_select_table = "pillar_select_bench"

    Chex.Connection.execute(chex_conn, Helpers.drop_test_table(chex_select_table))
    Chex.Connection.execute(chex_conn, Helpers.create_test_table(chex_select_table))
    :ok = Chex.insert(chex_conn, chex_select_table, columns_1m, schema)

    {:ok, _} = Pillar.query(pillar_conn, Helpers.drop_test_table(pillar_select_table))
    {:ok, _} = Pillar.query(pillar_conn, Helpers.create_test_table(pillar_select_table))
    {:ok, _} = Pillar.insert_to_table(pillar_conn, pillar_select_table, rows_1m)

    IO.puts("✓ Tables populated with 1M rows\n")

    # Run SELECT benchmarks
    IO.puts("=== SELECT Benchmarks ===\n")

    Benchee.run(
      %{
        "Chex SELECT all 1M rows" => fn ->
          {:ok, _cols} =
            Chex.Connection.select_cols(chex_conn, "SELECT * FROM #{chex_select_table}")
        end,
        "Pillar SELECT all 1M rows" => fn ->
          {:ok, _rows} = Pillar.select(pillar_conn, "SELECT * FROM #{pillar_select_table}")
        end,
        "Chex SELECT filtered (10k rows)" => fn ->
          {:ok, _cols} =
            Chex.Connection.select_cols(
              chex_conn,
              "SELECT * FROM #{chex_select_table} WHERE user_id < 1000"
            )
        end,
        "Pillar SELECT filtered (10k rows)" => fn ->
          {:ok, _rows} =
            Pillar.select(
              pillar_conn,
              "SELECT * FROM #{pillar_select_table} WHERE user_id < 1000"
            )
        end,
        "Chex SELECT aggregation" => fn ->
          {:ok, _cols} =
            Chex.Connection.select_cols(
              chex_conn,
              "SELECT event_type, count(*) as cnt FROM #{chex_select_table} GROUP BY event_type"
            )
        end,
        "Pillar SELECT aggregation" => fn ->
          {:ok, _rows} =
            Pillar.select(
              pillar_conn,
              "SELECT event_type, count(*) as cnt FROM #{pillar_select_table} GROUP BY event_type"
            )
        end
      },
      warmup: 1,
      time: 5,
      memory_time: 2,
      formatters: [
        Benchee.Formatters.Console,
        {Benchee.Formatters.HTML, file: "bench/results_select.html"}
      ],
      print: [
        benchmarking: true,
        configuration: true,
        fast_warning: false
      ]
    )

    # Cleanup
    IO.puts("\n=== Cleaning up ===\n")
    Chex.Connection.execute(chex_conn, Helpers.drop_test_table(chex_select_table))
    {:ok, _} = Pillar.query(pillar_conn, Helpers.drop_test_table(pillar_select_table))

    GenServer.stop(chex_conn)

    IO.puts("✓ Benchmark complete!\n")
    IO.puts("HTML reports generated:")
    IO.puts("  - bench/results_insert.html")
    IO.puts("  - bench/results_select.html\n")
  end

  defp setup_chex do
    Chex.Connection.start_link(
      host: "localhost",
      port: 9000,
      database: "default"
    )
  end

  defp setup_pillar do
    Pillar.Connection.new("http://localhost:8123/default")
  end
end

# Run the benchmark
ChexVsPillarBench.run()
