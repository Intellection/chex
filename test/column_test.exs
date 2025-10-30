defmodule Chex.ColumnTest do
  use ExUnit.Case, async: true

  alias Chex.Column

  describe "Column creation" do
    test "can create UInt64 column" do
      col = Column.new(:uint64)
      assert %Column{type: :uint64, clickhouse_type: "UInt64"} = col
      assert is_reference(col.ref)
    end

    test "can create Int64 column" do
      col = Column.new(:int64)
      assert %Column{type: :int64, clickhouse_type: "Int64"} = col
      assert is_reference(col.ref)
    end

    test "can create String column" do
      col = Column.new(:string)
      assert %Column{type: :string, clickhouse_type: "String"} = col
      assert is_reference(col.ref)
    end

    test "can create Float64 column" do
      col = Column.new(:float64)
      assert %Column{type: :float64, clickhouse_type: "Float64"} = col
      assert is_reference(col.ref)
    end

    test "can create DateTime column" do
      col = Column.new(:datetime)
      assert %Column{type: :datetime, clickhouse_type: "DateTime"} = col
      assert is_reference(col.ref)
    end

    test "raises on unsupported type" do
      assert_raise ArgumentError, ~r/Unsupported column type/, fn ->
        Column.new(:invalid_type)
      end
    end
  end

  describe "UInt64 column operations" do
    test "can append single value" do
      col = Column.new(:uint64)
      assert :ok = Column.append_bulk(col, [42])
      assert Column.size(col) == 1
    end

    test "can append multiple values" do
      col = Column.new(:uint64)
      assert :ok = Column.append_bulk(col, [1, 2, 3])
      assert Column.size(col) == 3
    end

    test "can append large values" do
      col = Column.new(:uint64)
      max_uint64 = 18_446_744_073_709_551_615
      assert :ok = Column.append_bulk(col, [max_uint64])
      assert Column.size(col) == 1
    end

    test "raises on negative values" do
      col = Column.new(:uint64)

      assert_raise ArgumentError, ~r/All values must be non-negative integers/, fn ->
        Column.append_bulk(col, [-1])
      end
    end

    test "raises on invalid type" do
      col = Column.new(:uint64)

      assert_raise ArgumentError, fn ->
        Column.append_bulk(col, ["string"])
      end
    end
  end

  describe "Int64 column operations" do
    test "can append positive values" do
      col = Column.new(:int64)
      assert :ok = Column.append_bulk(col, [42])
      assert Column.size(col) == 1
    end

    test "can append negative values" do
      col = Column.new(:int64)
      assert :ok = Column.append_bulk(col, [-42])
      assert Column.size(col) == 1
    end

    test "can append zero" do
      col = Column.new(:int64)
      assert :ok = Column.append_bulk(col, [0])
      assert Column.size(col) == 1
    end

    test "can append min/max values" do
      col = Column.new(:int64)

      assert :ok =
               Column.append_bulk(col, [-9_223_372_036_854_775_808, 9_223_372_036_854_775_807])

      assert Column.size(col) == 2
    end
  end

  describe "String column operations" do
    test "can append single string" do
      col = Column.new(:string)
      assert :ok = Column.append_bulk(col, ["hello"])
      assert Column.size(col) == 1
    end

    test "can append multiple strings" do
      col = Column.new(:string)
      assert :ok = Column.append_bulk(col, ["hello", "world"])
      assert Column.size(col) == 2
    end

    test "can append empty string" do
      col = Column.new(:string)
      assert :ok = Column.append_bulk(col, [""])
      assert Column.size(col) == 1
    end

    test "can append UTF-8 strings" do
      col = Column.new(:string)
      assert :ok = Column.append_bulk(col, ["Hello 世界 🌍"])
      assert Column.size(col) == 1
    end

    test "can append long strings" do
      col = Column.new(:string)
      long_string = String.duplicate("a", 10_000)
      assert :ok = Column.append_bulk(col, [long_string])
      assert Column.size(col) == 1
    end
  end

  describe "Float64 column operations" do
    test "can append float values" do
      col = Column.new(:float64)
      assert :ok = Column.append_bulk(col, [3.14159])
      assert Column.size(col) == 1
    end

    test "can append integer values (auto-converted)" do
      col = Column.new(:float64)
      assert :ok = Column.append_bulk(col, [42])
      assert Column.size(col) == 1
    end

    test "can append negative values" do
      col = Column.new(:float64)
      assert :ok = Column.append_bulk(col, [-123.456])
      assert Column.size(col) == 1
    end

    test "can append zero" do
      col = Column.new(:float64)
      assert :ok = Column.append_bulk(col, [0.0])
      assert Column.size(col) == 1
    end

    test "can append scientific notation" do
      col = Column.new(:float64)
      assert :ok = Column.append_bulk(col, [1.23e10, 4.56e-5])
      assert Column.size(col) == 2
    end
  end

  describe "DateTime column operations" do
    test "can append DateTime struct" do
      col = Column.new(:datetime)
      dt = ~U[2024-10-29 16:30:00Z]
      assert :ok = Column.append_bulk(col, [dt])
      assert Column.size(col) == 1
    end

    test "can append Unix timestamp" do
      col = Column.new(:datetime)
      timestamp = 1_730_220_600
      assert :ok = Column.append_bulk(col, [timestamp])
      assert Column.size(col) == 1
    end

    test "can append multiple DateTime values" do
      col = Column.new(:datetime)
      assert :ok = Column.append_bulk(col, [~U[2024-01-01 00:00:00Z], ~U[2024-12-31 23:59:59Z]])
      assert Column.size(col) == 2
    end

    test "can append epoch (1970-01-01)" do
      col = Column.new(:datetime)
      assert :ok = Column.append_bulk(col, [~U[1970-01-01 00:00:00Z]])
      assert Column.size(col) == 1
    end
  end

  describe "Mixed operations" do
    test "can work with multiple columns simultaneously" do
      col1 = Column.new(:uint64)
      col2 = Column.new(:string)
      col3 = Column.new(:float64)

      Column.append_bulk(col1, [1, 2])
      Column.append_bulk(col2, ["first", "second"])
      Column.append_bulk(col3, [1.1, 2.2])

      assert Column.size(col1) == 2
      assert Column.size(col2) == 2
      assert Column.size(col3) == 2
    end

    test "columns are independent" do
      col1 = Column.new(:uint64)
      col2 = Column.new(:uint64)

      Column.append_bulk(col1, [1, 2])

      # col2 should be empty
      assert Column.size(col1) == 2
      assert Column.size(col2) == 0
    end
  end

  describe "Column size tracking" do
    test "size starts at zero" do
      col = Column.new(:uint64)
      assert Column.size(col) == 0
    end

    test "size increments with each append" do
      col = Column.new(:string)
      assert Column.size(col) == 0

      Column.append_bulk(col, ["a"])
      assert Column.size(col) == 1

      Column.append_bulk(col, ["b", "c"])
      assert Column.size(col) == 3
    end
  end
end
