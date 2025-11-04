defmodule Natch.ArrayIntegrationTest do
  use ExUnit.Case, async: true

  alias Natch.Column

  test "Array(UInt64) fast path works" do
    col = Column.new({:array, :uint64})
    arrays = [[1, 2, 3], [4, 5], [6, 7, 8, 9]]
    assert :ok = Column.append_bulk(col, arrays)
    assert Column.size(col) == 3
  end

  test "Array(Array(UInt64)) nested arrays work" do
    col = Column.new({:array, {:array, :uint64}})
    arrays = [[[1, 2], [3, 4]], [[5], [6, 7]]]
    assert :ok = Column.append_bulk(col, arrays)
    assert Column.size(col) == 2
  end

  test "Array(Date) generic path works" do
    col = Column.new({:array, :date})
    arrays = [[~D[2024-01-01], ~D[2024-01-02]]]
    assert :ok = Column.append_bulk(col, arrays)
    assert Column.size(col) == 1
  end
end
