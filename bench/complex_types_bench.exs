# Benchmark for complex ClickHouse types (Nullable, Map, Tuple)
# This validates Phase 1 optimizations that target these specific types

defmodule ComplexTypesBench do
  def run() do
    # Start ClickHouse connection
    {:ok, pid} = Natch.start_link(host: "localhost", port: 9000, database: "default")

    IO.puts("\n=== Setting up benchmark tables ===\n")

    setup_nullable_bench(pid)
    setup_map_bench(pid)
    setup_tuple_bench(pid)

    IO.puts("\n=== Running benchmarks ===\n")

    # Benchmark SELECT queries
    Benchee.run(
      %{
        "Nullable SELECT 1M rows" => fn ->
          {:ok, _rows} = Natch.query(pid, "SELECT * FROM bench_nullable")
        end,
        "Map SELECT 100K rows" => fn ->
          {:ok, _rows} = Natch.query(pid, "SELECT * FROM bench_map")
        end,
        "Tuple SELECT 500K rows" => fn ->
          {:ok, _rows} = Natch.query(pid, "SELECT * FROM bench_tuple")
        end
      },
      time: 10,
      memory_time: 2,
      formatters: [
        {Benchee.Formatters.HTML, file: "bench/results_complex_types.html"},
        Benchee.Formatters.Console
      ]
    )

    IO.puts("\n=== Cleaning up ===\n")
    Natch.query(pid, "DROP TABLE IF EXISTS bench_nullable")
    Natch.query(pid, "DROP TABLE IF EXISTS bench_map")
    Natch.query(pid, "DROP TABLE IF EXISTS bench_tuple")

    IO.puts("✓ Benchmark complete!")
    IO.puts("\nHTML report generated: bench/results_complex_types.html")
  end

  defp setup_nullable_bench(pid) do
    Natch.query(pid, "DROP TABLE IF EXISTS bench_nullable")

    Natch.query(
      pid,
      """
      CREATE TABLE bench_nullable (
        id UInt64,
        nullable_int Nullable(UInt64),
        nullable_str Nullable(String),
        nullable_float Nullable(Float64),
        value UInt32
      ) ENGINE = MergeTree()
      ORDER BY id
      """
    )

    row_count = 1_000_000
    IO.puts("Generating #{row_count} rows with nullable columns (30% nulls)...")

    ids = Enum.to_list(1..row_count)

    nullable_ints =
      Enum.map(1..row_count, fn i ->
        if rem(i, 10) < 3, do: nil, else: i * 100
      end)

    nullable_strs =
      Enum.map(1..row_count, fn i ->
        if rem(i, 10) < 3, do: nil, else: "value_#{i}"
      end)

    nullable_floats =
      Enum.map(1..row_count, fn i ->
        if rem(i, 10) < 3, do: nil, else: i * 1.5
      end)

    values = Enum.map(1..row_count, fn i -> rem(i, 1000) end)

    block = %{
      id: ids,
      nullable_int: nullable_ints,
      nullable_str: nullable_strs,
      nullable_float: nullable_floats,
      value: values
    }

    schema = [
      id: :uint64,
      nullable_int: {:nullable, :uint64},
      nullable_str: {:nullable, :string},
      nullable_float: {:nullable, :float64},
      value: :uint32
    ]

    IO.puts("Inserting nullable data...")
    :ok = Natch.insert(pid, "bench_nullable", block, schema)
    IO.puts("✓ Nullable table populated with #{row_count} rows")
  end

  defp setup_map_bench(pid) do
    Natch.query(pid, "DROP TABLE IF EXISTS bench_map")

    Natch.query(
      pid,
      """
      CREATE TABLE bench_map (
        id UInt64,
        metadata Map(String, UInt64),
        tags Map(String, String)
      ) ENGINE = MergeTree()
      ORDER BY id
      """
    )

    row_count = 100_000
    IO.puts("Generating #{row_count} rows with Map columns (10 keys per map)...")

    ids = Enum.to_list(1..row_count)

    metadata =
      Enum.map(1..row_count, fn i ->
        Map.new(1..10, fn j -> {"key_#{j}", i * j} end)
      end)

    tags =
      Enum.map(1..row_count, fn i ->
        Map.new(1..10, fn j -> {"tag_#{j}", "value_#{i}_#{j}"} end)
      end)

    block = %{
      id: ids,
      metadata: metadata,
      tags: tags
    }

    schema = [
      id: :uint64,
      metadata: {:map, :string, :uint64},
      tags: {:map, :string, :string}
    ]

    IO.puts("Inserting map data...")
    :ok = Natch.insert(pid, "bench_map", block, schema)
    IO.puts("✓ Map table populated with #{row_count} rows")
  end

  defp setup_tuple_bench(pid) do
    Natch.query(pid, "DROP TABLE IF EXISTS bench_tuple")

    Natch.query(
      pid,
      """
      CREATE TABLE bench_tuple (
        id UInt64,
        coordinates Tuple(Float64, Float64),
        user_info Tuple(UInt64, String, DateTime),
        measurement Tuple(Float64, Float64, Float64, Float64)
      ) ENGINE = MergeTree()
      ORDER BY id
      """
    )

    row_count = 500_000
    IO.puts("Generating #{row_count} rows with Tuple columns...")

    ids = Enum.to_list(1..row_count)
    coordinates = Enum.map(1..row_count, fn i -> {i * 0.1, i * 0.2} end)

    user_info =
      Enum.map(1..row_count, fn i ->
        {i, "user_#{i}", 1_700_000_000 + i}
      end)

    measurement = Enum.map(1..row_count, fn i -> {i * 1.1, i * 2.2, i * 3.3, i * 4.4} end)

    block = %{
      id: ids,
      coordinates: coordinates,
      user_info: user_info,
      measurement: measurement
    }

    schema = [
      id: :uint64,
      coordinates: {:tuple, [:float64, :float64]},
      user_info: {:tuple, [:uint64, :string, :datetime]},
      measurement: {:tuple, [:float64, :float64, :float64, :float64]}
    ]

    IO.puts("Inserting tuple data...")
    :ok = Natch.insert(pid, "bench_tuple", block, schema)
    IO.puts("✓ Tuple table populated with #{row_count} rows")
  end
end

ComplexTypesBench.run()
