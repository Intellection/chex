defmodule Natch.ConnectionErrorTest do
  use ExUnit.Case, async: true

  alias Natch.ConnectionError

  describe "connection errors" do
    test "raises ConnectionError for invalid host" do
      Process.flag(:trap_exit, true)

      {:error, {%ConnectionError{} = error, _stacktrace}} =
        Natch.Connection.start_link(host: "invalid.nonexistent.host.example", port: 9999)

      assert error.reason == :connection_failed
      # Error message varies by platform, but we got the right error type
      assert is_binary(error.message)
    end
  end

  describe "server errors" do
    setup do
      {:ok, conn} = Natch.Connection.start_link(host: "localhost", port: 9000)
      {:ok, conn: conn}
    end

    test "returns structured error for syntax errors", %{conn: conn} do
      # Invalid SQL syntax should return server error with code/name
      result = Natch.Connection.execute(conn, "INVALID SQL SYNTAX")

      assert {:error, error} = result
      # The error should be a structured map
      assert is_map(error)
      assert error.type == "server"
      assert error.message =~ "Syntax error"

      # Check details map contains all fields
      assert error.details["type"] == "server"
      # SYNTAX_ERROR code
      assert error.details["code"] == 62
      assert error.details["name"] == "DB::Exception"
      assert error.details["message"] =~ "Syntax error"
      # Stack trace should be present for server errors
      assert is_binary(error.details["stack_trace"])
    end
  end
end
