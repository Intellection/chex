defmodule Chex.NestingTest do
  use ExUnit.Case, async: true
  alias Chex.Column

  describe "Priority 1: Array(Nullable(T)) - Nullable elements in arrays" do
    test "Array(Nullable(String)) - creates column" do
      col = Column.new({:array, {:nullable, :string}})
      assert col.type == {:array, {:nullable, :string}}
      assert col.clickhouse_type == "Array(Nullable(String))"
      assert Column.size(col) == 0
    end

    test "Array(Nullable(String)) - appends arrays with interspersed nulls" do
      col = Column.new({:array, {:nullable, :string}})

      arrays = [
        ["hello", nil, "world"],
        [nil, nil],
        ["foo"],
        []
      ]

      Column.append_bulk(col, arrays)
      assert Column.size(col) == 4
    end

    test "Array(Nullable(UInt64)) - all nulls scenario" do
      col = Column.new({:array, {:nullable, :uint64}})
      arrays = [[nil, nil, nil], [nil]]
      Column.append_bulk(col, arrays)
      assert Column.size(col) == 2
    end

    test "Array(Nullable(UInt64)) - no nulls scenario" do
      col = Column.new({:array, {:nullable, :uint64}})
      arrays = [[1, 2, 3], [4, 5]]
      Column.append_bulk(col, arrays)
      assert Column.size(col) == 2
    end

    test "Array(Nullable(UInt64)) - mixed null patterns" do
      col = Column.new({:array, {:nullable, :uint64}})

      arrays = [
        [1, nil, 3, nil, 5],
        [nil, 2, nil],
        [1, 2, 3, 4, 5],
        [nil, nil, nil]
      ]

      Column.append_bulk(col, arrays)
      assert Column.size(col) == 4
    end

    test "Array(Nullable(Float64)) - with nulls and floats" do
      col = Column.new({:array, {:nullable, :float64}})
      arrays = [[1.5, nil, 2.5], [nil], [3.14, 2.71]]
      Column.append_bulk(col, arrays)
      assert Column.size(col) == 3
    end

    test "Array(Nullable(String)) - single element arrays" do
      col = Column.new({:array, {:nullable, :string}})
      arrays = [[nil], ["one"], [nil], ["two"]]
      Column.append_bulk(col, arrays)
      assert Column.size(col) == 4
    end

    test "Array(Nullable(String)) - multiple batches" do
      col = Column.new({:array, {:nullable, :string}})

      Column.append_bulk(col, [["a", nil, "b"]])
      assert Column.size(col) == 1

      Column.append_bulk(col, [[nil, "c"], ["d", nil, nil]])
      assert Column.size(col) == 3
    end
  end

  describe "Priority 1: LowCardinality(Nullable(String)) - Double wrapper" do
    test "creates LowCardinality(Nullable(String)) column" do
      col = Column.new({:low_cardinality, {:nullable, :string}})
      assert col.type == {:low_cardinality, {:nullable, :string}}
      assert col.clickhouse_type == "LowCardinality(Nullable(String))"
      assert Column.size(col) == 0
    end

    test "appends values with interspersed nulls and duplicates" do
      col = Column.new({:low_cardinality, {:nullable, :string}})

      # Many duplicates - perfect for LowCardinality, with nulls mixed in
      values = ["apple", nil, "banana", "apple", nil, "cherry", "banana", "apple", nil, "banana"]
      Column.append_bulk(col, values)

      assert Column.size(col) == 10
    end

    test "handles all nulls scenario" do
      col = Column.new({:low_cardinality, {:nullable, :string}})
      values = [nil, nil, nil, nil, nil]
      Column.append_bulk(col, values)
      assert Column.size(col) == 5
    end

    test "handles no nulls scenario" do
      col = Column.new({:low_cardinality, {:nullable, :string}})
      values = ["a", "b", "a", "c", "b", "a"]
      Column.append_bulk(col, values)
      assert Column.size(col) == 6
    end

    test "single row with null" do
      col = Column.new({:low_cardinality, {:nullable, :string}})
      Column.append_bulk(col, [nil])
      assert Column.size(col) == 1
    end

    test "single row with value" do
      col = Column.new({:low_cardinality, {:nullable, :string}})
      Column.append_bulk(col, ["value"])
      assert Column.size(col) == 1
    end

    test "multiple batches with nulls" do
      col = Column.new({:low_cardinality, {:nullable, :string}})

      Column.append_bulk(col, ["a", nil, "b"])
      assert Column.size(col) == 3

      Column.append_bulk(col, [nil, "a", "c"])
      assert Column.size(col) == 6

      Column.append_bulk(col, ["b", nil, nil])
      assert Column.size(col) == 9
    end
  end

  describe "Priority 1: Tuple with Nullable elements" do
    test "Tuple(Nullable(String), UInt64) - creates column" do
      col = Column.new({:tuple, [{:nullable, :string}, :uint64]})
      assert col.type == {:tuple, [{:nullable, :string}, :uint64]}
      assert col.clickhouse_type == "Tuple(Nullable(String), UInt64)"
      assert Column.size(col) == 0
    end

    test "Tuple(Nullable(String), UInt64) - appends with nulls in first element" do
      col = Column.new({:tuple, [{:nullable, :string}, :uint64]})

      names = [nil, "Bob", nil, "Dave"]
      scores = [100, 200, 300, 400]

      Column.append_tuple_columns(col, [names, scores])
      assert Column.size(col) == 4
    end

    test "Tuple(String, Nullable(UInt64), Nullable(Float64)) - multiple nullable elements" do
      col = Column.new({:tuple, [:string, {:nullable, :uint64}, {:nullable, :float64}]})

      names = ["Alice", "Bob", "Charlie"]
      scores = [100, nil, 300]
      prices = [99.99, nil, 150.50]

      Column.append_tuple_columns(col, [names, scores, prices])
      assert Column.size(col) == 3
    end

    test "Tuple(Nullable(String), Nullable(UInt64)) - all elements nullable" do
      col = Column.new({:tuple, [{:nullable, :string}, {:nullable, :uint64}]})

      col1 = [nil, "test", nil]
      col2 = [100, nil, nil]

      Column.append_tuple_columns(col, [col1, col2])
      assert Column.size(col) == 3
    end

    test "Tuple with nullable elements - multiple batches" do
      col = Column.new({:tuple, [{:nullable, :string}, :uint64]})

      Column.append_tuple_columns(col, [[nil, "a"], [1, 2]])
      assert Column.size(col) == 2

      Column.append_tuple_columns(col, [["b", nil], [3, 4]])
      assert Column.size(col) == 4
    end
  end

  describe "Priority 1: Map with Nullable values" do
    test "Map(String, Nullable(UInt64)) - creates column" do
      col = Column.new({:map, :string, {:nullable, :uint64}})
      assert col.type == {:map, :string, {:nullable, :uint64}}
      assert col.clickhouse_type == "Map(String, Nullable(UInt64))"
      assert Column.size(col) == 0
    end

    test "Map(String, Nullable(UInt64)) - appends maps with null values" do
      col = Column.new({:map, :string, {:nullable, :uint64}})

      keys_arrays = [["k1", "k2"], ["k3"], ["k4", "k5"]]
      values_arrays = [[1, nil], [nil], [100, 200]]

      Column.append_map_arrays(col, keys_arrays, values_arrays)
      assert Column.size(col) == 3
    end

    test "Map(String, Nullable(String)) - all null values" do
      col = Column.new({:map, :string, {:nullable, :string}})

      keys_arrays = [["a", "b"], ["c"]]
      values_arrays = [[nil, nil], [nil]]

      Column.append_map_arrays(col, keys_arrays, values_arrays)
      assert Column.size(col) == 2
    end

    test "Map(String, Nullable(Float64)) - mixed nulls" do
      col = Column.new({:map, :string, {:nullable, :float64}})

      keys_arrays = [["price", "discount", "tax"]]
      values_arrays = [[99.99, nil, 5.5]]

      Column.append_map_arrays(col, keys_arrays, values_arrays)
      assert Column.size(col) == 1
    end

    test "Map with nullable values - empty maps" do
      col = Column.new({:map, :string, {:nullable, :uint64}})
      Column.append_map_arrays(col, [[], []], [[], []])
      assert Column.size(col) == 2
    end

    test "Map with nullable values - multiple batches" do
      col = Column.new({:map, :string, {:nullable, :uint64}})

      Column.append_map_arrays(col, [["a"]], [[1]])
      assert Column.size(col) == 1

      Column.append_map_arrays(col, [["b"], ["c"]], [[nil], [3]])
      assert Column.size(col) == 3
    end
  end

  describe "Priority 1: Nullable wrapping composite types" do
    test "Nullable(Array(UInt64)) - nullable arrays" do
      # Note: This might not be supported directly - testing to see
      # In ClickHouse, typically you'd use Array(Nullable(T)) instead
      # This test will help us understand the behavior
    end
  end

  describe "Priority 2: Array(LowCardinality(T)) - LC in arrays" do
    test "Array(LowCardinality(String)) - creates column" do
      col = Column.new({:array, {:low_cardinality, :string}})
      assert col.type == {:array, {:low_cardinality, :string}}
      assert col.clickhouse_type == "Array(LowCardinality(String))"
      assert Column.size(col) == 0
    end

    test "Array(LowCardinality(String)) - arrays with duplicated values" do
      col = Column.new({:array, {:low_cardinality, :string}})

      # Arrays with many duplicates - optimal for LowCardinality
      arrays = [
        ["apple", "banana", "apple"],
        ["cherry", "banana"],
        ["apple", "apple", "cherry", "banana"]
      ]

      Column.append_bulk(col, arrays)
      assert Column.size(col) == 3
    end

    test "Array(LowCardinality(String)) - empty arrays within structure" do
      col = Column.new({:array, {:low_cardinality, :string}})
      arrays = [[], ["a", "b"], [], ["a"]]
      Column.append_bulk(col, arrays)
      assert Column.size(col) == 4
    end

    test "Array(LowCardinality(String)) - multiple batches with dictionary merging" do
      col = Column.new({:array, {:low_cardinality, :string}})

      Column.append_bulk(col, [["a", "b"]])
      assert Column.size(col) == 1

      Column.append_bulk(col, [["c", "a"], ["b", "c"]])
      assert Column.size(col) == 3
    end
  end

  describe "Priority 2: Map with LowCardinality keys/values" do
    test "Map(LowCardinality(String), UInt64) - LC keys" do
      col = Column.new({:map, {:low_cardinality, :string}, :uint64})
      assert col.type == {:map, {:low_cardinality, :string}, :uint64}
      assert col.clickhouse_type == "Map(LowCardinality(String), UInt64)"
      assert Column.size(col) == 0
    end

    test "Map(LowCardinality(String), UInt64) - appends with duplicate keys" do
      col = Column.new({:map, {:low_cardinality, :string}, :uint64})

      # Many duplicate keys across maps - good for LowCardinality
      keys_arrays = [["status", "count"], ["status", "count"], ["status"]]
      values_arrays = [[1, 100], [2, 200], [3]]

      Column.append_map_arrays(col, keys_arrays, values_arrays)
      assert Column.size(col) == 3
    end

    test "Map(String, LowCardinality(String)) - LC values" do
      col = Column.new({:map, :string, {:low_cardinality, :string}})
      assert col.clickhouse_type == "Map(String, LowCardinality(String))"

      keys_arrays = [["k1", "k2"], ["k3"]]
      # Duplicate values - good for LowCardinality
      values_arrays = [["active", "active"], ["inactive"]]

      Column.append_map_arrays(col, keys_arrays, values_arrays)
      assert Column.size(col) == 2
    end

    test "Map(LowCardinality(String), LowCardinality(String)) - LC keys and values" do
      col = Column.new({:map, {:low_cardinality, :string}, {:low_cardinality, :string}})

      keys_arrays = [["status", "category"], ["status"]]
      values_arrays = [["active", "A"], ["inactive"]]

      Column.append_map_arrays(col, keys_arrays, values_arrays)
      assert Column.size(col) == 2
    end
  end

  describe "Priority 2: Complex LowCardinality combinations" do
    test "Array(LowCardinality(Nullable(String))) - triple wrapper!" do
      col = Column.new({:array, {:low_cardinality, {:nullable, :string}}})

      assert col.clickhouse_type == "Array(LowCardinality(Nullable(String)))"

      arrays = [
        ["apple", nil, "banana"],
        [nil, "apple"],
        ["banana", "cherry", nil]
      ]

      Column.append_bulk(col, arrays)
      assert Column.size(col) == 3
    end

    test "Tuple with LowCardinality elements" do
      col = Column.new({:tuple, [{:low_cardinality, :string}, :uint64]})
      assert col.clickhouse_type == "Tuple(LowCardinality(String), UInt64)"

      names = ["active", "inactive", "active", "pending"]
      counts = [100, 200, 300, 400]

      Column.append_tuple_columns(col, [names, counts])
      assert Column.size(col) == 4
    end
  end

  describe "Priority 3: Map with structured values" do
    test "Map(String, Array(UInt64)) - arrays as map values" do
      col = Column.new({:map, :string, {:array, :uint64}})
      assert col.clickhouse_type == "Map(String, Array(UInt64))"

      keys_arrays = [["ids", "counts"], ["values"]]
      # Each value is an array
      values_arrays = [[[1, 2, 3], [10, 20]], [[100, 200, 300]]]

      Column.append_map_arrays(col, keys_arrays, values_arrays)
      assert Column.size(col) == 2
    end

    test "Map(String, Array(String)) - string arrays as values" do
      col = Column.new({:map, :string, {:array, :string}})

      keys_arrays = [["tags", "labels"]]
      values_arrays = [[["tag1", "tag2"], ["label1"]]]

      Column.append_map_arrays(col, keys_arrays, values_arrays)
      assert Column.size(col) == 1
    end
  end

  describe "Priority 3: Tuple with structured elements" do
    test "Tuple(String, Array(UInt64)) - array as tuple element" do
      col = Column.new({:tuple, [:string, {:array, :uint64}]})
      assert col.clickhouse_type == "Tuple(String, Array(UInt64))"

      names = ["Alice", "Bob", "Charlie"]
      scores_arrays = [[100, 200, 300], [50], [75, 85, 95, 105]]

      Column.append_tuple_columns(col, [names, scores_arrays])
      assert Column.size(col) == 3
    end

    test "Tuple(String, Array(Nullable(UInt64))) - array with nullables" do
      col = Column.new({:tuple, [:string, {:array, {:nullable, :uint64}}]})

      names = ["Test1", "Test2"]
      scores_arrays = [[100, nil, 200], [nil, 50]]

      Column.append_tuple_columns(col, [names, scores_arrays])
      assert Column.size(col) == 2
    end
  end

  describe "Priority 3: Deep array nesting" do
    test "Array(Array(Nullable(UInt64))) - triple nesting with nulls" do
      col = Column.new({:array, {:array, {:nullable, :uint64}}})
      assert col.clickhouse_type == "Array(Array(Nullable(UInt64)))"

      arrays = [
        [[1, nil, 3], [nil, 5]],
        [[nil]],
        [[10, 20], [], [nil, nil]]
      ]

      Column.append_bulk(col, arrays)
      assert Column.size(col) == 3
    end

    test "Array(Array(Nullable(String))) - triple nesting with nulls" do
      col = Column.new({:array, {:array, {:nullable, :string}}})

      arrays = [
        [["a", nil], [nil, "b"]],
        [[nil, nil, nil]],
        [["x"], [], ["y", nil, "z"]]
      ]

      Column.append_bulk(col, arrays)
      assert Column.size(col) == 3
    end

    test "Array(Array(Array(Array(UInt64)))) - 4-level nesting (stress test)" do
      col = Column.new({:array, {:array, {:array, {:array, :uint64}}}})
      assert col.clickhouse_type == "Array(Array(Array(Array(UInt64))))"

      # Deep nesting: level 1 -> level 2 -> level 3 -> level 4
      arrays = [
        [[[[1, 2], [3]], [[4]]]],
        [[[[5, 6, 7]]], [[[8]], [[9, 10]]]]
      ]

      Column.append_bulk(col, arrays)
      assert Column.size(col) == 2
    end

    test "Array(Array(Array(String))) - empty arrays at various levels" do
      col = Column.new({:array, {:array, {:array, :string}}})

      arrays = [
        [[["a", "b"], []], [[]], []],
        [[], [["c"]], [[]]],
        [[[]], [[]]]
      ]

      Column.append_bulk(col, arrays)
      assert Column.size(col) == 3
    end
  end

  describe "Priority 3: Mixed complex nesting" do
    test "Array(Tuple(String, UInt64)) - tuples in arrays" do
      col = Column.new({:array, {:tuple, [:string, :uint64]}})
      assert col.clickhouse_type == "Array(Tuple(String, UInt64))"

      # Each array contains tuples
      # For arrays of tuples, we need arrays of tuples
      # But our columnar API expects column-oriented data
      # This might not work directly - let's see
    end

    test "Tuple(Array(String), Array(UInt64)) - multiple arrays in tuple" do
      col = Column.new({:tuple, [{:array, :string}, {:array, :uint64}]})
      assert col.clickhouse_type == "Tuple(Array(String), Array(UInt64))"

      names_arrays = [["Alice", "Bob"], ["Charlie"], []]
      scores_arrays = [[100, 200], [300], []]

      Column.append_tuple_columns(col, [names_arrays, scores_arrays])
      assert Column.size(col) == 3
    end

    test "Map(String, Tuple(UInt64, String)) - tuple as map value" do
      col = Column.new({:map, :string, {:tuple, [:uint64, :string]}})
      assert col.clickhouse_type == "Map(String, Tuple(UInt64, String))"

      # Maps with tuple values are complex
      # Each map entry has a tuple as the value
      # This requires special handling in append_map_arrays
    end
  end

  describe "Priority 4: Enum in nested structures" do
    test "Array(Enum8(...)) - enums in arrays" do
      enum_def = [{"small", 1}, {"medium", 2}, {"large", 3}]
      col = Column.new({:array, {:enum8, enum_def}})
      assert col.clickhouse_type == "Array(Enum8('small' = 1, 'medium' = 2, 'large' = 3))"

      arrays = [
        ["small", "large"],
        ["medium"],
        ["small", "medium", "large"]
      ]

      Column.append_bulk(col, arrays)
      assert Column.size(col) == 3
    end

    test "Array(Enum8(...)) - with integer values" do
      enum_def = [{"active", 1}, {"inactive", 0}]
      col = Column.new({:array, {:enum8, enum_def}})

      # Using integer values directly
      arrays = [[1, 0, 1], [0], [1, 1]]

      Column.append_bulk(col, arrays)
      assert Column.size(col) == 3
    end

    test "Nullable(Enum8(...)) - nullable enums" do
      enum_def = [{"yes", 1}, {"no", 0}]
      col = Column.new({:nullable, {:enum8, enum_def}})
      assert col.clickhouse_type == "Nullable(Enum8('yes' = 1, 'no' = 0))"

      # Note: This requires extending our nullable handling for enum types
      # For now, this test documents the desired behavior
    end

    test "Tuple with Enum8 elements" do
      enum_def = [{"low", 1}, {"high", 2}]
      col = Column.new({:tuple, [{:enum8, enum_def}, :uint64]})
      assert col.clickhouse_type == "Tuple(Enum8('low' = 1, 'high' = 2), UInt64)"

      priorities = ["low", "high", "low"]
      scores = [100, 200, 150]

      Column.append_tuple_columns(col, [priorities, scores])
      assert Column.size(col) == 3
    end

    test "Map(Enum8(...), String) - enum as map key" do
      enum_def = [{"status_ok", 1}, {"status_error", 2}]
      col = Column.new({:map, {:enum8, enum_def}, :string})
      assert col.clickhouse_type == "Map(Enum8('status_ok' = 1, 'status_error' = 2), String)"

      keys_arrays = [["status_ok", "status_error"], ["status_ok"]]
      values_arrays = [["Success", "Failed"], ["OK"]]

      Column.append_map_arrays(col, keys_arrays, values_arrays)
      assert Column.size(col) == 2
    end

    test "Map(String, Enum16(...)) - enum as map value" do
      enum_def = [{"bronze", 100}, {"silver", 200}, {"gold", 300}]
      col = Column.new({:map, :string, {:enum16, enum_def}})

      assert col.clickhouse_type ==
               "Map(String, Enum16('bronze' = 100, 'silver' = 200, 'gold' = 300))"

      keys_arrays = [["player1", "player2"], ["player3"]]
      values_arrays = [["gold", "silver"], ["bronze"]]

      Column.append_map_arrays(col, keys_arrays, values_arrays)
      assert Column.size(col) == 2
    end

    test "Array(Array(Enum8(...))) - nested enum arrays" do
      enum_def = [{"a", 1}, {"b", 2}]
      col = Column.new({:array, {:array, {:enum8, enum_def}}})
      assert col.clickhouse_type == "Array(Array(Enum8('a' = 1, 'b' = 2)))"

      arrays = [
        [["a", "b"], ["a"]],
        [[], ["b", "b"]],
        [["a"]]
      ]

      Column.append_bulk(col, arrays)
      assert Column.size(col) == 3
    end
  end
end
