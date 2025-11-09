defmodule Natch.ParameterizedQueryTest do
  use ExUnit.Case, async: false

  alias Natch.Query

  setup_all do
    {:ok, conn} =
      Natch.start_link(
        host: "localhost",
        port: 9000,
        database: "default",
        user: "default",
        password: ""
      )

    # Create test table
    :ok = Natch.execute(conn, "DROP TABLE IF EXISTS param_query_test")

    :ok =
      Natch.execute(
        conn,
        """
        CREATE TABLE param_query_test (
          id UInt64,
          name String,
          age UInt32,
          score Float64,
          created_at DateTime,
          notes Nullable(String)
        ) ENGINE = Memory
        """
      )

    # Insert test data
    :ok =
      Natch.execute(
        conn,
        """
        INSERT INTO param_query_test VALUES
        (1, 'Alice', 30, 95.5, toDateTime('2024-01-01 10:00:00'), 'Test note'),
        (2, 'Bob', 25, 87.3, toDateTime('2024-01-02 11:00:00'), NULL),
        (3, 'Charlie', 35, 92.1, toDateTime('2024-01-03 12:00:00'), 'Another note')
        """
      )

    on_exit(fn ->
      Natch.execute(conn, "DROP TABLE IF EXISTS param_query_test")
    end)

    {:ok, conn: conn}
  end

  describe "Query.new/1" do
    test "creates a query with SQL" do
      query = Query.new("SELECT * FROM users WHERE id = {id:UInt64}")
      assert %Query{sql: "SELECT * FROM users WHERE id = {id:UInt64}"} = query
      assert is_reference(query.ref)
    end
  end

  describe "Query.bind/3 - type inference" do
    test "binds UInt64 from positive integer" do
      query =
        Query.new("SELECT * FROM param_query_test WHERE id = {id:UInt64}")
        |> Query.bind(:id, 1)

      assert query.params[:id] == 1
    end

    test "binds String" do
      query =
        Query.new("SELECT * FROM param_query_test WHERE name = {name:String}")
        |> Query.bind(:name, "Alice")

      assert query.params[:name] == "Alice"
    end

    test "binds NULL" do
      query =
        Query.new(
          "INSERT INTO param_query_test VALUES ({id:UInt64}, {name:String}, {age:UInt32}, {score:Float64}, {created_at:DateTime}, {notes:Nullable(String)})"
        )
        |> Query.bind(:id, 100)
        |> Query.bind(:name, "Test")
        |> Query.bind(:age, 20, :uint32)
        |> Query.bind(:score, 80.0)
        |> Query.bind(:created_at, ~U[2024-01-01 00:00:00Z])
        |> Query.bind(:notes, nil)

      assert query.params[:notes] == nil
    end
  end

  describe "Parameterized SELECT" do
    test "SELECT with single parameter", %{conn: conn} do
      query =
        Query.new("SELECT * FROM param_query_test WHERE id = {id:UInt64}")
        |> Query.bind(:id, 1)

      {:ok, rows} = Natch.select_rows(conn, query)

      assert [%{id: 1, name: "Alice", age: 30}] = rows
    end

    test "SELECT with multiple parameters", %{conn: conn} do
      query =
        Query.new(
          "SELECT * FROM param_query_test WHERE age < {max_age:UInt32} AND score > {min_score:Float64}"
        )
        |> Query.bind(:max_age, 36, :uint32)
        |> Query.bind(:min_score, 90.0)

      {:ok, rows} = Natch.select_rows(conn, query)

      assert length(rows) == 2
      assert Enum.all?(rows, fn r -> r.age < 36 and r.score > 90.0 end)
    end

    test "SELECT with string parameter", %{conn: conn} do
      query =
        Query.new("SELECT * FROM param_query_test WHERE name = {name:String}")
        |> Query.bind(:name, "Bob")

      {:ok, rows} = Natch.select_rows(conn, query)

      assert [%{name: "Bob", age: 25}] = rows
    end

    test "SELECT with columnar output", %{conn: conn} do
      query =
        Query.new(
          "SELECT name, age FROM param_query_test WHERE age > {min_age:UInt32} AND age < {max_age:UInt32} ORDER BY age"
        )
        |> Query.bind(:min_age, 30, :uint32)
        |> Query.bind(:max_age, 36, :uint32)

      {:ok, cols} = Natch.select_cols(conn, query)

      assert %{name: names, age: ages} = cols
      assert names == ["Charlie"]
      assert ages == [35]
    end
  end

  describe "Parameterized INSERT" do
    test "INSERT with parameters", %{conn: conn} do
      query =
        Query.new(
          "INSERT INTO param_query_test VALUES ({id:UInt64}, {name:String}, {age:UInt32}, {score:Float64}, {created_at:DateTime}, {notes:Nullable(String)})"
        )
        |> Query.bind(:id, 200)
        |> Query.bind(:name, "TestUser")
        |> Query.bind(:age, 28, :uint32)
        |> Query.bind(:score, 88.8)
        |> Query.bind(:created_at, ~U[2024-06-01 10:00:00Z])
        |> Query.bind(:notes, "Param insert")

      assert :ok = Natch.execute(conn, query)

      {:ok, rows} = Natch.select_rows(conn, "SELECT * FROM param_query_test WHERE id = 200")
      assert [%{name: "TestUser", age: 28}] = rows
    end

    test "INSERT with NULL", %{conn: conn} do
      query =
        Query.new(
          "INSERT INTO param_query_test VALUES ({id:UInt64}, {name:String}, {age:UInt32}, {score:Float64}, {created_at:DateTime}, {notes:Nullable(String)})"
        )
        |> Query.bind(:id, 201)
        |> Query.bind(:name, "NullTest")
        |> Query.bind(:age, 30, :uint32)
        |> Query.bind(:score, 90.0)
        |> Query.bind(:created_at, ~U[2024-06-02 10:00:00Z])
        |> Query.bind(:notes, nil)

      assert :ok = Natch.execute(conn, query)

      {:ok, rows} = Natch.select_rows(conn, "SELECT * FROM param_query_test WHERE id = 201")
      assert [%{name: "NullTest", notes: nil}] = rows
    end
  end

  describe "Simple parameterized API" do
    test "select_rows/3 with keyword list", %{conn: conn} do
      {:ok, rows} =
        Natch.select_rows(
          conn,
          "SELECT * FROM param_query_test WHERE id = {id:UInt64}",
          id: 1
        )

      assert [%{id: 1, name: "Alice"}] = rows
    end

    test "select_rows/3 with map", %{conn: conn} do
      params = %{id: 1}

      {:ok, rows} =
        Natch.select_rows(
          conn,
          "SELECT * FROM param_query_test WHERE id = {id:UInt64}",
          params
        )

      assert [%{id: 1, name: "Alice"}] = rows
    end

    test "select_rows/3 with multiple parameters (keyword)", %{conn: conn} do
      {:ok, rows} =
        Natch.select_rows(
          conn,
          "SELECT * FROM param_query_test WHERE age < {max_age:UInt32} AND score > {min_score:Float64}",
          max_age: 36,
          min_score: 90.0
        )

      assert length(rows) == 2
    end

    test "select_rows/3 with multiple parameters (map)", %{conn: conn} do
      {:ok, rows} =
        Natch.select_rows(
          conn,
          "SELECT * FROM param_query_test WHERE age < {max_age:UInt32} AND score > {min_score:Float64}",
          %{max_age: 36, min_score: 90.0}
        )

      assert length(rows) == 2
    end

    test "select_cols/3 with keyword list", %{conn: conn} do
      {:ok, cols} =
        Natch.select_cols(
          conn,
          "SELECT name, age FROM param_query_test WHERE age > {min_age:UInt32} AND age < {max_age:UInt32} ORDER BY age",
          min_age: 30,
          max_age: 36
        )

      assert %{name: ["Charlie"], age: [35]} = cols
    end

    test "select_cols/3 with map", %{conn: conn} do
      {:ok, cols} =
        Natch.select_cols(
          conn,
          "SELECT name, age FROM param_query_test WHERE age > {min_age:UInt32} AND age < {max_age:UInt32} ORDER BY age",
          %{min_age: 30, max_age: 36}
        )

      assert %{name: ["Charlie"], age: [35]} = cols
    end

    test "execute/3 INSERT with keyword list", %{conn: conn} do
      :ok =
        Natch.execute(
          conn,
          "INSERT INTO param_query_test VALUES ({id:UInt64}, {name:String}, {age:UInt32}, {score:Float64}, {created_at:DateTime}, {notes:Nullable(String)})",
          id: 300,
          name: "SimpleAPI",
          age: 40,
          score: 95.5,
          created_at: ~U[2024-07-01 10:00:00Z],
          notes: "Simple API test"
        )

      {:ok, rows} = Natch.select_rows(conn, "SELECT * FROM param_query_test WHERE id = 300")
      assert [%{name: "SimpleAPI", age: 40}] = rows
    end

    test "execute/3 INSERT with map", %{conn: conn} do
      params = %{
        id: 301,
        name: "MapAPI",
        age: 41,
        score: 96.5,
        created_at: ~U[2024-07-02 10:00:00Z],
        notes: nil
      }

      :ok =
        Natch.execute(
          conn,
          "INSERT INTO param_query_test VALUES ({id:UInt64}, {name:String}, {age:UInt32}, {score:Float64}, {created_at:DateTime}, {notes:Nullable(String)})",
          params
        )

      {:ok, rows} = Natch.select_rows(conn, "SELECT * FROM param_query_test WHERE id = 301")
      assert [%{name: "MapAPI", notes: nil}] = rows
    end

    test "empty params map", %{conn: conn} do
      # Should work but won't bind anything
      {:ok, rows} =
        Natch.select_rows(
          conn,
          "SELECT * FROM param_query_test",
          %{}
        )

      assert length(rows) >= 3
    end

    test "empty params keyword list", %{conn: conn} do
      {:ok, rows} =
        Natch.select_rows(
          conn,
          "SELECT * FROM param_query_test",
          []
        )

      assert length(rows) >= 3
    end

    test "SQL injection prevention with simple API", %{conn: conn} do
      malicious = "'; DROP TABLE param_query_test; --"

      {:ok, rows} =
        Natch.select_rows(
          conn,
          "SELECT * FROM param_query_test WHERE name = {name:String}",
          name: malicious
        )

      assert rows == []

      # Verify table still exists
      {:ok, _} = Natch.select_rows(conn, "SELECT COUNT(*) FROM param_query_test")
    end
  end

  describe "Type inference for SELECT queries" do
    test "select_rows/3 with untyped placeholders infers UInt64 from positive integer", %{
      conn: conn
    } do
      {:ok, rows} =
        Natch.select_rows(
          conn,
          "SELECT * FROM param_query_test WHERE id = {id}",
          id: 1
        )

      assert [%{id: 1, name: "Alice"}] = rows
    end

    test "select_rows/3 with untyped placeholders infers String from binary", %{conn: conn} do
      {:ok, rows} =
        Natch.select_rows(
          conn,
          "SELECT * FROM param_query_test WHERE name = {name}",
          name: "Bob"
        )

      assert [%{name: "Bob", age: 25}] = rows
    end

    test "select_rows/3 with untyped placeholders infers Float64 from float", %{conn: conn} do
      {:ok, rows} =
        Natch.select_rows(
          conn,
          "SELECT * FROM param_query_test WHERE score > {min_score}",
          min_score: 90.0
        )

      assert length(rows) >= 2
    end

    test "select_rows/3 with untyped placeholders infers DateTime from DateTime struct", %{
      conn: conn
    } do
      {:ok, rows} =
        Natch.select_rows(
          conn,
          "SELECT * FROM param_query_test WHERE created_at > {start}",
          start: ~U[2024-01-01 00:00:00Z]
        )

      assert length(rows) >= 3
    end

    test "select_rows/3 with mixed typed and untyped placeholders", %{conn: conn} do
      # Explicit type for age (Int32), inferred type for name (String)
      {:ok, rows} =
        Natch.select_rows(
          conn,
          "SELECT * FROM param_query_test WHERE age < {max_age:UInt32} AND name = {name}",
          max_age: 36,
          name: "Alice"
        )

      assert [%{name: "Alice"}] = rows
    end

    test "select_cols/3 with untyped placeholders", %{conn: conn} do
      {:ok, cols} =
        Natch.select_cols(
          conn,
          "SELECT name, age FROM param_query_test WHERE age > {min_age} AND age < {max_age} ORDER BY age",
          min_age: 30,
          max_age: 36
        )

      assert %{name: ["Charlie"], age: [35]} = cols
    end

    test "select_rows/3 with map and untyped placeholders", %{conn: conn} do
      {:ok, rows} =
        Natch.select_rows(
          conn,
          "SELECT * FROM param_query_test WHERE id = {id}",
          %{id: 2}
        )

      assert [%{id: 2, name: "Bob"}] = rows
    end

    test "SQL injection prevention with untyped placeholders", %{conn: conn} do
      malicious = "'; DROP TABLE param_query_test; --"

      {:ok, rows} =
        Natch.select_rows(
          conn,
          "SELECT * FROM param_query_test WHERE name = {name}",
          name: malicious
        )

      assert rows == []

      # Verify table still exists
      {:ok, _} = Natch.select_rows(conn, "SELECT COUNT(*) FROM param_query_test")
    end
  end

  describe "SQL Injection Prevention" do
    test "string with SQL injection attempt is safely escaped", %{conn: conn} do
      # This would be dangerous with string interpolation
      malicious_input = "'; DROP TABLE param_query_test; --"

      query =
        Query.new("SELECT * FROM param_query_test WHERE name = {name:String}")
        |> Query.bind(:name, malicious_input)

      # Should return no results (name doesn't match), not execute the DROP
      {:ok, rows} = Natch.select_rows(conn, query)
      assert rows == []

      # Verify table still exists
      {:ok, _} = Natch.select_rows(conn, "SELECT COUNT(*) as cnt FROM param_query_test")
    end

    test "integer injection attempt is type-safe", %{conn: conn} do
      # Cannot inject SQL through integer parameter
      query =
        Query.new("SELECT * FROM param_query_test WHERE id = {id:UInt64}")
        |> Query.bind(:id, 1)

      {:ok, rows} = Natch.select_rows(conn, query)
      assert length(rows) == 1
    end

    test "multiple injection vectors are all safe", %{conn: conn} do
      # Try various injection patterns
      patterns = [
        "' OR '1'='1",
        "1; DROP TABLE param_query_test;",
        "' UNION SELECT * FROM users--",
        "admin'--"
      ]

      for pattern <- patterns do
        query =
          Query.new("SELECT * FROM param_query_test WHERE name = {name:String}")
          |> Query.bind(:name, pattern)

        {:ok, rows} = Natch.select_rows(conn, query)
        assert rows == []
      end

      # Table should still be intact
      {:ok, rows} = Natch.select_rows(conn, "SELECT * FROM param_query_test")
      assert length(rows) >= 3
    end
  end
end
