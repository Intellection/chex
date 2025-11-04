defmodule Natch.TimeoutIntegrationTest do
  use ExUnit.Case, async: false
  @moduletag :integration
  import ExUnit.CaptureLog

  describe "recv_timeout" do
    test "query completes when within timeout" do
      # Start connection with 10 second recv timeout
      {:ok, conn} =
        Natch.Connection.start_link(
          host: "localhost",
          port: 9000,
          recv_timeout: 10_000
        )

      # Quick query should succeed
      assert {:ok, _rows} = Natch.Connection.select_rows(conn, "SELECT 1")

      GenServer.stop(conn)
    end

    @tag :slow
    test "recv_timeout can be configured" do
      # This test just verifies recv_timeout can be set without errors
      # Actually triggering a recv timeout reliably is difficult in a test environment
      {:ok, conn} =
        Natch.Connection.start_link(
          host: "localhost",
          port: 9000,
          recv_timeout: 5_000
        )

      # Basic operation should work
      assert {:ok, _rows} = Natch.Connection.select_rows(conn, "SELECT 1")

      GenServer.stop(conn)
    end

    test "default recv_timeout of 0 allows long queries" do
      # Start connection with default timeout (0 = infinite)
      {:ok, conn} =
        Natch.Connection.start_link(
          host: "localhost",
          port: 9000
        )

      # Query should complete without timeout concerns
      assert {:ok, _rows} = Natch.Connection.select_rows(conn, "SELECT sleep(1)")

      GenServer.stop(conn)
    end
  end

  describe "connect_timeout" do
    @tag :slow
    test "connection fails quickly when host is unreachable" do
      # Use TEST-NET-1 (192.0.2.0/24) - reserved for documentation/testing, not routable
      # With 1 second connect timeout, this should fail in ~1 second (not hang forever)
      # Capture logs to suppress GenServer crash output
      capture_log(fn ->
        start_time = System.monotonic_time(:millisecond)

        # The connection will fail to start (GenServer crashes during init)
        # We just want to verify it fails within the timeout period
        Process.flag(:trap_exit, true)

        result =
          Natch.Connection.start_link(
            host: "192.0.2.1",
            port: 9000,
            connect_timeout: 1_000
          )

        duration = System.monotonic_time(:millisecond) - start_time

        # Should fail (not return {:ok, _})
        refute match?({:ok, _}, result)

        # Should fail within ~1 second (plus some margin for overhead)
        # If connect_timeout wasn't working, this would hang much longer
        assert duration < 3_000,
               "Connection attempt took #{duration}ms, expected < 3000ms (timeout + margin)"
      end)
    end

    test "connection succeeds with sufficient timeout" do
      # Normal connection with default timeout should work
      {:ok, conn} =
        Natch.Connection.start_link(
          host: "localhost",
          port: 9000,
          connect_timeout: 5_000
        )

      assert is_pid(conn)
      GenServer.stop(conn)
    end
  end

  describe "send_timeout" do
    test "send_timeout can be configured" do
      # send_timeout is hard to test without large data transfers
      # but we can verify it can be set without errors
      {:ok, conn} =
        Natch.Connection.start_link(
          host: "localhost",
          port: 9000,
          send_timeout: 10_000
        )

      # Basic operation should work
      assert :ok = Natch.Connection.ping(conn)

      GenServer.stop(conn)
    end
  end

  describe "timeout configuration" do
    test "all timeouts can be configured together" do
      {:ok, conn} =
        Natch.Connection.start_link(
          host: "localhost",
          port: 9000,
          connect_timeout: 5_000,
          recv_timeout: 60_000,
          send_timeout: 60_000
        )

      assert is_pid(conn)
      assert :ok = Natch.Connection.ping(conn)

      GenServer.stop(conn)
    end

    test "default timeout values work correctly" do
      # Default: connect_timeout=5000, recv_timeout=0, send_timeout=0
      {:ok, conn} =
        Natch.Connection.start_link(
          host: "localhost",
          port: 9000
        )

      assert is_pid(conn)
      assert :ok = Natch.Connection.ping(conn)

      GenServer.stop(conn)
    end
  end
end
