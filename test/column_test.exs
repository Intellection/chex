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

  describe "Bool column operations" do
    test "can create Bool column" do
      col = Column.new(:bool)
      assert %Column{type: :bool, clickhouse_type: "Bool"} = col
      assert is_reference(col.ref)
    end

    test "can append true values" do
      col = Column.new(:bool)
      assert :ok = Column.append_bulk(col, [true])
      assert Column.size(col) == 1
    end

    test "can append false values" do
      col = Column.new(:bool)
      assert :ok = Column.append_bulk(col, [false])
      assert Column.size(col) == 1
    end

    test "can append mixed boolean values" do
      col = Column.new(:bool)
      assert :ok = Column.append_bulk(col, [true, false, true, false])
      assert Column.size(col) == 4
    end

    test "raises on non-boolean values" do
      col = Column.new(:bool)

      assert_raise ArgumentError, ~r/All values must be booleans/, fn ->
        Column.append_bulk(col, [1])
      end
    end
  end

  describe "Date column operations" do
    test "can create Date column" do
      col = Column.new(:date)
      assert %Column{type: :date, clickhouse_type: "Date"} = col
      assert is_reference(col.ref)
    end

    test "can append Date struct" do
      col = Column.new(:date)
      date = ~D[2024-10-29]
      assert :ok = Column.append_bulk(col, [date])
      assert Column.size(col) == 1
    end

    test "can append days since epoch as integer" do
      col = Column.new(:date)
      days = 19_000
      assert :ok = Column.append_bulk(col, [days])
      assert Column.size(col) == 1
    end

    test "can append multiple dates" do
      col = Column.new(:date)
      assert :ok = Column.append_bulk(col, [~D[2024-01-01], ~D[2024-12-31]])
      assert Column.size(col) == 2
    end

    test "can append epoch date (1970-01-01)" do
      col = Column.new(:date)
      assert :ok = Column.append_bulk(col, [~D[1970-01-01]])
      assert Column.size(col) == 1
    end
  end

  describe "Float32 column operations" do
    test "can create Float32 column" do
      col = Column.new(:float32)
      assert %Column{type: :float32, clickhouse_type: "Float32"} = col
      assert is_reference(col.ref)
    end

    test "can append float values" do
      col = Column.new(:float32)
      assert :ok = Column.append_bulk(col, [3.14])
      assert Column.size(col) == 1
    end

    test "can append integer values (auto-converted)" do
      col = Column.new(:float32)
      assert :ok = Column.append_bulk(col, [42])
      assert Column.size(col) == 1
    end

    test "can append negative values" do
      col = Column.new(:float32)
      assert :ok = Column.append_bulk(col, [-123.45])
      assert Column.size(col) == 1
    end
  end

  describe "UInt32 column operations" do
    test "can create UInt32 column" do
      col = Column.new(:uint32)
      assert %Column{type: :uint32, clickhouse_type: "UInt32"} = col
      assert is_reference(col.ref)
    end

    test "can append values in range" do
      col = Column.new(:uint32)
      assert :ok = Column.append_bulk(col, [0, 100, 4_294_967_295])
      assert Column.size(col) == 3
    end

    test "raises on negative values" do
      col = Column.new(:uint32)

      assert_raise ArgumentError, ~r/All values must be non-negative integers/, fn ->
        Column.append_bulk(col, [-1])
      end
    end

    test "raises on out of range values" do
      col = Column.new(:uint32)

      assert_raise ArgumentError, ~r/All values must be non-negative integers/, fn ->
        Column.append_bulk(col, [4_294_967_296])
      end
    end
  end

  describe "UInt16 column operations" do
    test "can create UInt16 column" do
      col = Column.new(:uint16)
      assert %Column{type: :uint16, clickhouse_type: "UInt16"} = col
      assert is_reference(col.ref)
    end

    test "can append values in range" do
      col = Column.new(:uint16)
      assert :ok = Column.append_bulk(col, [0, 100, 65_535])
      assert Column.size(col) == 3
    end

    test "raises on out of range values" do
      col = Column.new(:uint16)

      assert_raise ArgumentError, ~r/All values must be non-negative integers/, fn ->
        Column.append_bulk(col, [65_536])
      end
    end
  end

  describe "Int32 column operations" do
    test "can create Int32 column" do
      col = Column.new(:int32)
      assert %Column{type: :int32, clickhouse_type: "Int32"} = col
      assert is_reference(col.ref)
    end

    test "can append values in range" do
      col = Column.new(:int32)
      assert :ok = Column.append_bulk(col, [-2_147_483_648, 0, 2_147_483_647])
      assert Column.size(col) == 3
    end

    test "raises on out of range positive values" do
      col = Column.new(:int32)

      assert_raise ArgumentError, ~r/All values must be integers/, fn ->
        Column.append_bulk(col, [2_147_483_648])
      end
    end

    test "raises on out of range negative values" do
      col = Column.new(:int32)

      assert_raise ArgumentError, ~r/All values must be integers/, fn ->
        Column.append_bulk(col, [-2_147_483_649])
      end
    end
  end

  describe "Int16 column operations" do
    test "can create Int16 column" do
      col = Column.new(:int16)
      assert %Column{type: :int16, clickhouse_type: "Int16"} = col
      assert is_reference(col.ref)
    end

    test "can append values in range" do
      col = Column.new(:int16)
      assert :ok = Column.append_bulk(col, [-32_768, 0, 32_767])
      assert Column.size(col) == 3
    end

    test "raises on out of range values" do
      col = Column.new(:int16)

      assert_raise ArgumentError, ~r/All values must be integers/, fn ->
        Column.append_bulk(col, [32_768])
      end
    end
  end

  describe "Int8 column operations" do
    test "can create Int8 column" do
      col = Column.new(:int8)
      assert %Column{type: :int8, clickhouse_type: "Int8"} = col
      assert is_reference(col.ref)
    end

    test "can append values in range" do
      col = Column.new(:int8)
      assert :ok = Column.append_bulk(col, [-128, 0, 127])
      assert Column.size(col) == 3
    end

    test "raises on out of range positive values" do
      col = Column.new(:int8)

      assert_raise ArgumentError, ~r/All values must be integers/, fn ->
        Column.append_bulk(col, [128])
      end
    end

    test "raises on out of range negative values" do
      col = Column.new(:int8)

      assert_raise ArgumentError, ~r/All values must be integers/, fn ->
        Column.append_bulk(col, [-129])
      end
    end
  end

  describe "UUID column operations" do
    test "can create UUID column" do
      col = Column.new(:uuid)
      assert %Column{type: :uuid, clickhouse_type: "UUID"} = col
      assert is_reference(col.ref)
    end

    test "can append UUID strings" do
      col = Column.new(:uuid)
      uuid = "550e8400-e29b-41d4-a716-446655440000"
      assert :ok = Column.append_bulk(col, [uuid])
      assert Column.size(col) == 1
    end

    test "can append multiple UUIDs" do
      col = Column.new(:uuid)

      uuids = [
        "550e8400-e29b-41d4-a716-446655440000",
        "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
        "6ba7b811-9dad-11d1-80b4-00c04fd430c8"
      ]

      assert :ok = Column.append_bulk(col, uuids)
      assert Column.size(col) == 3
    end

    test "can append UUID as 16-byte binary" do
      col = Column.new(:uuid)
      # 550e8400-e29b-41d4-a716-446655440000
      uuid_bin =
        <<0x55, 0x0E, 0x84, 0x00, 0xE2, 0x9B, 0x41, 0xD4, 0xA7, 0x16, 0x44, 0x66, 0x55, 0x44,
          0x00, 0x00>>

      assert :ok = Column.append_bulk(col, [uuid_bin])
      assert Column.size(col) == 1
    end

    test "accepts UUID with uppercase hex" do
      col = Column.new(:uuid)
      uuid = "550E8400-E29B-41D4-A716-446655440000"
      assert :ok = Column.append_bulk(col, [uuid])
      assert Column.size(col) == 1
    end

    test "accepts UUID without hyphens" do
      col = Column.new(:uuid)
      uuid = "550e8400e29b41d4a716446655440000"
      assert :ok = Column.append_bulk(col, [uuid])
      assert Column.size(col) == 1
    end

    test "raises on invalid UUID format" do
      col = Column.new(:uuid)

      assert_raise ArgumentError, ~r/Invalid UUID format/, fn ->
        Column.append_bulk(col, ["not-a-uuid"])
      end
    end

    test "raises on wrong length UUID" do
      col = Column.new(:uuid)

      assert_raise ArgumentError, ~r/Invalid UUID format/, fn ->
        Column.append_bulk(col, ["550e8400-e29b-41d4-a716"])
      end
    end

    test "raises on non-hex characters" do
      col = Column.new(:uuid)

      assert_raise ArgumentError, ~r/Invalid UUID format/, fn ->
        Column.append_bulk(col, ["550e8400-e29b-41d4-a716-44665544000g"])
      end
    end
  end

  describe "DateTime64 column operations" do
    test "can create DateTime64 column" do
      col = Column.new(:datetime64)
      assert %Column{type: :datetime64, clickhouse_type: "DateTime64(6)"} = col
      assert Column.size(col) == 0
    end

    test "can append DateTime structs with microsecond precision" do
      col = Column.new(:datetime64)
      dt1 = ~U[2024-01-01 10:00:00.123456Z]
      dt2 = ~U[2024-01-02 15:30:45.987654Z]

      :ok = Column.append_bulk(col, [dt1, dt2])
      assert Column.size(col) == 2
    end

    test "can append multiple DateTime64 values" do
      col = Column.new(:datetime64)

      values = [
        ~U[2024-01-01 10:00:00.000001Z],
        ~U[2024-01-02 11:00:00.000002Z],
        ~U[2024-01-03 12:00:00.000003Z]
      ]

      :ok = Column.append_bulk(col, values)
      assert Column.size(col) == 3
    end

    test "can append integer microsecond timestamps" do
      col = Column.new(:datetime64)
      # January 1, 2024 00:00:00 UTC in microseconds
      timestamp_us = 1_704_067_200_000_000

      :ok = Column.append_bulk(col, [timestamp_us])
      assert Column.size(col) == 1
    end

    test "accepts mix of DateTime and integers" do
      col = Column.new(:datetime64)
      dt = ~U[2024-01-01 10:00:00.123456Z]
      timestamp_us = 1_704_110_400_000_000

      :ok = Column.append_bulk(col, [dt, timestamp_us])
      assert Column.size(col) == 2
    end

    test "raises on invalid datetime64 value" do
      col = Column.new(:datetime64)

      assert_raise ArgumentError, ~r/Invalid datetime64 value/, fn ->
        Column.append_bulk(col, ["invalid"])
      end
    end
  end

  describe "Decimal column operations" do
    test "can create Decimal column" do
      col = Column.new(:decimal)
      assert %Column{type: :decimal, clickhouse_type: "Decimal64(9)"} = col
      assert Column.size(col) == 0
    end

    test "can append Decimal structs" do
      col = Column.new(:decimal)
      dec1 = Decimal.new("123.456789012")
      dec2 = Decimal.new("987.654321098")

      :ok = Column.append_bulk(col, [dec1, dec2])
      assert Column.size(col) == 2
    end

    test "can append multiple Decimal values" do
      col = Column.new(:decimal)

      values = [
        Decimal.new("1.123456789"),
        Decimal.new("2.234567890"),
        Decimal.new("3.345678901")
      ]

      :ok = Column.append_bulk(col, values)
      assert Column.size(col) == 3
    end

    test "can append integer values" do
      col = Column.new(:decimal)
      :ok = Column.append_bulk(col, [100, 200, 300])
      assert Column.size(col) == 3
    end

    test "can append float values" do
      col = Column.new(:decimal)
      :ok = Column.append_bulk(col, [1.5, 2.75, 3.125])
      assert Column.size(col) == 3
    end

    test "accepts mix of Decimal, integers, and floats" do
      col = Column.new(:decimal)
      dec = Decimal.new("100.5")

      :ok = Column.append_bulk(col, [dec, 200, 300.75])
      assert Column.size(col) == 3
    end

    test "handles negative Decimal values" do
      col = Column.new(:decimal)
      dec1 = Decimal.new("-123.456")
      dec2 = Decimal.new("-987.654")

      :ok = Column.append_bulk(col, [dec1, dec2])
      assert Column.size(col) == 2
    end

    test "raises on invalid decimal value" do
      col = Column.new(:decimal)

      assert_raise ArgumentError, ~r/Invalid decimal value/, fn ->
        Column.append_bulk(col, ["invalid"])
      end
    end
  end

  describe "Nullable column operations" do
    test "can create Nullable(UInt64) column" do
      col = Column.new(:nullable_uint64)
      assert %Column{type: :nullable_uint64, clickhouse_type: "Nullable(UInt64)"} = col
      assert Column.size(col) == 0
    end

    test "can append values with nils to Nullable(UInt64)" do
      col = Column.new(:nullable_uint64)
      :ok = Column.append_bulk(col, [1, nil, 3, nil, 5])
      assert Column.size(col) == 5
    end

    test "can append values with nils to Nullable(String)" do
      col = Column.new(:nullable_string)
      :ok = Column.append_bulk(col, ["hello", nil, "world", nil])
      assert Column.size(col) == 4
    end

    test "can append all nils to Nullable column" do
      col = Column.new(:nullable_int64)
      :ok = Column.append_bulk(col, [nil, nil, nil])
      assert Column.size(col) == 3
    end

    test "can append all values to Nullable column" do
      col = Column.new(:nullable_float64)
      :ok = Column.append_bulk(col, [1.5, 2.5, 3.5])
      assert Column.size(col) == 3
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

  describe "Array column operations - Fast Path" do
    test "can create Array(UInt64) column" do
      col = Column.new({:array, :uint64})
      assert %Column{type: {:array, :uint64}, clickhouse_type: "Array(UInt64)"} = col
      assert is_reference(col.ref)
    end

    test "can append Array(UInt64) values - fast path" do
      col = Column.new({:array, :uint64})
      arrays = [[1, 2, 3], [4, 5], [6, 7, 8, 9]]
      assert :ok = Column.append_bulk(col, arrays)
      assert Column.size(col) == 3
    end

    test "can append Array(Int64) values - fast path" do
      col = Column.new({:array, :int64})
      arrays = [[-1, -2, -3], [0], [1, 2]]
      assert :ok = Column.append_bulk(col, arrays)
      assert Column.size(col) == 3
    end

    test "can append Array(Float64) values - fast path" do
      col = Column.new({:array, :float64})
      arrays = [[1.1, 2.2], [3.3, 4.4, 5.5], []]
      assert :ok = Column.append_bulk(col, arrays)
      assert Column.size(col) == 3
    end

    test "can append Array(String) values - fast path" do
      col = Column.new({:array, :string})
      arrays = [["hello", "world"], ["foo"], ["bar", "baz", "qux"]]
      assert :ok = Column.append_bulk(col, arrays)
      assert Column.size(col) == 3
    end

    test "can append empty arrays" do
      col = Column.new({:array, :uint64})
      arrays = [[], [1, 2], [], [3]]
      assert :ok = Column.append_bulk(col, arrays)
      assert Column.size(col) == 4
    end

    test "can append large arrays" do
      col = Column.new({:array, :uint64})
      large_array = Enum.to_list(1..1000)
      assert :ok = Column.append_bulk(col, [large_array])
      assert Column.size(col) == 1
    end

    test "can append UTF-8 strings in arrays" do
      col = Column.new({:array, :string})
      arrays = [["Hello 世界", "🌍"], ["Привет", "مرحبا"]]
      assert :ok = Column.append_bulk(col, arrays)
      assert Column.size(col) == 2
    end
  end

  describe "Array column operations - Generic Path" do
    test "can append Array(Date) values - generic path" do
      col = Column.new({:array, :date})
      arrays = [[~D[2024-01-01], ~D[2024-01-02]], [~D[2024-12-31]]]
      assert :ok = Column.append_bulk(col, arrays)
      assert Column.size(col) == 2
    end

    test "can append Array(DateTime) values - generic path" do
      col = Column.new({:array, :datetime})
      arrays = [[~U[2024-01-01 10:00:00Z]], [~U[2024-12-31 23:59:59Z], ~U[2024-06-15 12:30:00Z]]]
      assert :ok = Column.append_bulk(col, arrays)
      assert Column.size(col) == 2
    end

    test "can append Array(UUID) values - generic path" do
      col = Column.new({:array, :uuid})

      arrays = [
        ["550e8400-e29b-41d4-a716-446655440000", "6ba7b810-9dad-11d1-80b4-00c04fd430c8"],
        []
      ]

      assert :ok = Column.append_bulk(col, arrays)
      assert Column.size(col) == 2
    end

    test "can append Array(Decimal) values - generic path" do
      col = Column.new({:array, :decimal})
      arrays = [[Decimal.new("123.45"), Decimal.new("678.90")], [Decimal.new("0.01")]]
      assert :ok = Column.append_bulk(col, arrays)
      assert Column.size(col) == 2
    end

    test "can append Array(Bool) values - generic path" do
      col = Column.new({:array, :bool})
      arrays = [[true, false, true], [false], []]
      assert :ok = Column.append_bulk(col, arrays)
      assert Column.size(col) == 3
    end

    test "can append Array(UInt32) values - generic path" do
      col = Column.new({:array, :uint32})
      arrays = [[1, 2, 3], [4_294_967_295], []]
      assert :ok = Column.append_bulk(col, arrays)
      assert Column.size(col) == 3
    end

    test "can append Array(Int32) values - generic path" do
      col = Column.new({:array, :int32})
      arrays = [[-2_147_483_648, 0, 2_147_483_647], [100]]
      assert :ok = Column.append_bulk(col, arrays)
      assert Column.size(col) == 2
    end

    test "can append Array(Float32) values - generic path" do
      col = Column.new({:array, :float32})
      arrays = [[1.5, 2.5], [3.14159]]
      assert :ok = Column.append_bulk(col, arrays)
      assert Column.size(col) == 2
    end
  end

  describe "Array column operations - Nested Arrays" do
    test "can create Array(Array(UInt64)) column" do
      col = Column.new({:array, {:array, :uint64}})

      assert %Column{type: {:array, {:array, :uint64}}, clickhouse_type: "Array(Array(UInt64))"} =
               col

      assert is_reference(col.ref)
    end

    test "can append Array(Array(UInt64)) values" do
      col = Column.new({:array, {:array, :uint64}})
      # Each element is an array of arrays
      arrays = [
        # First element: array containing two arrays
        [[1, 2], [3, 4, 5]],
        # Second element: array containing one array
        [[6]],
        # Third element: array containing empty array and non-empty array
        [[], [7, 8]]
      ]

      assert :ok = Column.append_bulk(col, arrays)
      assert Column.size(col) == 3
    end

    test "can append Array(Array(String)) values" do
      col = Column.new({:array, {:array, :string}})

      arrays = [
        [["hello", "world"], ["foo"]],
        [["bar"], ["baz", "qux"]]
      ]

      assert :ok = Column.append_bulk(col, arrays)
      assert Column.size(col) == 2
    end

    test "can append Array(Array(Array(UInt64))) values - triple nesting!" do
      col = Column.new({:array, {:array, {:array, :uint64}}})

      arrays = [
        # First element: array of array of arrays
        [[[1, 2], [3]], [[4, 5]]],
        # Second element: with empty arrays at various levels
        [[[]], [[6, 7, 8]]]
      ]

      assert :ok = Column.append_bulk(col, arrays)
      assert Column.size(col) == 2
    end

    test "can append Array(Array(Float64)) values" do
      col = Column.new({:array, {:array, :float64}})
      arrays = [[[1.1, 2.2], [3.3]], [[4.4, 5.5, 6.6]]]
      assert :ok = Column.append_bulk(col, arrays)
      assert Column.size(col) == 2
    end

    test "nested arrays can contain empty arrays" do
      col = Column.new({:array, {:array, :int64}})
      arrays = [[[], [1], [], [2, 3]], [[]]]
      assert :ok = Column.append_bulk(col, arrays)
      assert Column.size(col) == 2
    end
  end

  describe "Array column operations - Error Handling" do
    test "raises on non-list values" do
      col = Column.new({:array, :uint64})

      assert_raise ArgumentError, ~r/All values must be lists/, fn ->
        Column.append_bulk(col, [123])
      end
    end

    test "raises on mixed types (expected arrays, got scalar)" do
      col = Column.new({:array, :uint64})

      assert_raise ArgumentError, ~r/All values must be lists/, fn ->
        Column.append_bulk(col, [[1, 2], 3])
      end
    end

    test "raises on invalid inner type" do
      col = Column.new({:array, :uint64})

      assert_raise ArgumentError, fn ->
        Column.append_bulk(col, [["string"]])
      end
    end
  end

  describe "Tuple column operations - Columnar API" do
    test "creates Tuple(String, UInt64) column" do
      col = Column.new({:tuple, [:string, :uint64]})
      assert col.type == {:tuple, [:string, :uint64]}
      assert col.clickhouse_type == "Tuple(String, UInt64)"
      assert Column.size(col) == 0
    end

    test "appends Tuple(String, UInt64) values using columnar API" do
      col = Column.new({:tuple, [:string, :uint64]})

      names = ["Alice", "Bob", "Charlie"]
      scores = [100, 200, 300]

      Column.append_tuple_columns(col, [names, scores])
      assert Column.size(col) == 3
    end

    test "appends Tuple(String, UInt64, Date) values using columnar API" do
      col = Column.new({:tuple, [:string, :uint64, :date]})

      names = ["Alice", "Bob"]
      scores = [100, 200]
      dates = [~D[2024-01-01], ~D[2024-01-02]]

      Column.append_tuple_columns(col, [names, scores, dates])
      assert Column.size(col) == 2
    end

    test "appends Tuple(Float64, String, Bool) values" do
      col = Column.new({:tuple, [:float64, :string, :bool]})

      prices = [99.99, 149.50, 19.99]
      names = ["Widget", "Gadget", "Doohickey"]
      in_stock = [true, false, true]

      Column.append_tuple_columns(col, [prices, names, in_stock])
      assert Column.size(col) == 3
    end

    test "handles empty tuple columns" do
      col = Column.new({:tuple, [:string, :uint64]})
      Column.append_tuple_columns(col, [[], []])
      assert Column.size(col) == 0
    end

    test "raises on column count mismatch" do
      col = Column.new({:tuple, [:string, :uint64]})

      assert_raise ArgumentError, ~r/Column count mismatch/, fn ->
        Column.append_tuple_columns(col, [["Alice"]])
      end
    end

    test "raises on column length mismatch" do
      col = Column.new({:tuple, [:string, :uint64]})

      assert_raise ArgumentError, ~r/same length/, fn ->
        Column.append_tuple_columns(col, [["Alice", "Bob"], [100]])
      end
    end

    test "raises when used with non-tuple column" do
      col = Column.new(:uint64)

      assert_raise ArgumentError, ~r/only works with tuple columns/, fn ->
        Column.append_tuple_columns(col, [[1], [2]])
      end
    end
  end

  describe "Map column operations - Columnar API" do
    test "creates Map(String, UInt64) column" do
      col = Column.new({:map, :string, :uint64})
      assert col.type == {:map, :string, :uint64}
      assert col.clickhouse_type == "Map(String, UInt64)"
      assert Column.size(col) == 0
    end

    test "appends Map(String, UInt64) values using columnar API" do
      col = Column.new({:map, :string, :uint64})

      keys_arrays = [["k1", "k2"], ["k3"], ["k4", "k5", "k6"]]
      values_arrays = [[1, 2], [3], [4, 5, 6]]

      Column.append_map_arrays(col, keys_arrays, values_arrays)
      assert Column.size(col) == 3
    end

    test "appends Map(String, Float64) values" do
      col = Column.new({:map, :string, :float64})

      keys_arrays = [["price", "discount"], ["tax"]]
      values_arrays = [[99.99, 10.0], [5.5]]

      Column.append_map_arrays(col, keys_arrays, values_arrays)
      assert Column.size(col) == 2
    end

    test "handles empty maps" do
      col = Column.new({:map, :string, :uint64})
      Column.append_map_arrays(col, [[], []], [[], []])
      assert Column.size(col) == 2
    end

    test "raises on keys/values array length mismatch" do
      col = Column.new({:map, :string, :uint64})

      assert_raise ArgumentError, ~r/must have the same length/, fn ->
        Column.append_map_arrays(col, [["k1"]], [[1], [2]])
      end
    end

    test "raises on keys/values pair length mismatch" do
      col = Column.new({:map, :string, :uint64})

      assert_raise ArgumentError, ~r/must match its values array length/, fn ->
        Column.append_map_arrays(col, [["k1", "k2"]], [[1]])
      end
    end

    test "raises when used with non-map column" do
      col = Column.new(:uint64)

      assert_raise ArgumentError, ~r/only works with map columns/, fn ->
        Column.append_map_arrays(col, [[]], [[]])
      end
    end
  end
end
