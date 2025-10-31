defmodule Bench.Helpers do
  @moduledoc """
  Helper functions for generating benchmark test data.
  """

  @doc """
  Generates deterministic test data for benchmarking.

  Returns a map with columnar data and the schema.
  """
  def generate_test_data(row_count) do
    # Deterministic seed
    :rand.seed(:exsss, {1, 2, 3})

    columns = %{
      id: Enum.to_list(1..row_count),
      user_id: for(_ <- 1..row_count, do: :rand.uniform(100_000)),
      event_type:
        for(_ <- 1..row_count, do: Enum.random(["click", "view", "purchase", "signup", "logout"])),
      timestamp:
        for(
          _ <- 1..row_count,
          do: DateTime.add(~U[2024-01-01 00:00:00Z], :rand.uniform(86400 * 365), :second)
        ),
      value: for(_ <- 1..row_count, do: :rand.uniform() * 1000.0),
      count: for(_ <- 1..row_count, do: :rand.uniform(1000) - 500),
      metadata: for(_ <- 1..row_count, do: "metadata_#{:rand.uniform(100)}")
    }

    schema = [
      id: :uint64,
      user_id: :uint32,
      event_type: :string,
      timestamp: :datetime,
      value: :float64,
      count: :int64,
      metadata: :string
    ]

    {columns, schema}
  end

  @doc """
  Generates deterministic test data in row format (for Pillar).

  Returns a list of maps, where each map represents a row.
  Uses the same random seed as generate_test_data/1 for consistency.
  """
  def generate_test_data_rows(row_count) do
    # Same seed as columnar version
    :rand.seed(:exsss, {1, 2, 3})

    event_types = ["click", "view", "purchase", "signup", "logout"]

    for id <- 1..row_count do
      %{
        id: id,
        user_id: :rand.uniform(100_000),
        event_type: Enum.random(event_types),
        timestamp: DateTime.add(~U[2024-01-01 00:00:00Z], :rand.uniform(86400 * 365), :second),
        value: :rand.uniform() * 1000.0,
        count: :rand.uniform(1000) - 500,
        metadata: "metadata_#{:rand.uniform(100)}"
      }
    end
  end

  @doc """
  Creates a test table in ClickHouse.
  """
  def create_test_table(table_name) do
    """
    CREATE TABLE IF NOT EXISTS #{table_name} (
      id UInt64,
      user_id UInt32,
      event_type String,
      timestamp DateTime,
      value Float64,
      count Int64,
      metadata String
    ) ENGINE = MergeTree()
    ORDER BY id
    """
  end

  @doc """
  Drops a test table from ClickHouse.
  """
  def drop_test_table(table_name) do
    "DROP TABLE IF EXISTS #{table_name}"
  end

  @doc """
  Generates a unique table name for benchmarking.
  """
  def unique_table_name(prefix \\ "bench") do
    "#{prefix}_#{System.unique_integer([:positive, :monotonic])}"
  end
end
