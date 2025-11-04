defmodule Natch.ConnectionError do
  @moduledoc """
  Raised when a connection to ClickHouse cannot be established.

  This error is raised when the client fails to connect to the ClickHouse server,
  typically due to network issues, invalid host/port, or DNS resolution failures.

  ## Fields

  - `:message` - Human-readable error description
  - `:reason` - Underlying system error reason (e.g., `:econnrefused`, `:nxdomain`)
  """

  defexception [:message, :reason]

  @impl true
  def exception(opts) when is_list(opts) do
    message = Keyword.fetch!(opts, :message)
    reason = Keyword.get(opts, :reason)

    %__MODULE__{
      message: message,
      reason: reason
    }
  end
end

defmodule Natch.ServerError do
  @moduledoc """
  Raised when ClickHouse server returns an error.

  This includes SQL syntax errors, table not found, permission errors, and other
  server-side errors reported by ClickHouse.

  ## Fields

  - `:message` - Human-readable error description from server
  - `:code` - ClickHouse error code (integer)
  - `:name` - Exception name from server (e.g., "DB::Exception")
  - `:stack_trace` - Server-side stack trace (optional)
  """

  defexception [:message, :code, :name, :stack_trace]

  @impl true
  def exception(opts) when is_list(opts) do
    message = Keyword.fetch!(opts, :message)
    code = Keyword.get(opts, :code)
    name = Keyword.get(opts, :name)
    stack_trace = Keyword.get(opts, :stack_trace)

    %__MODULE__{
      message: message,
      code: code,
      name: name,
      stack_trace: stack_trace
    }
  end
end

defmodule Natch.ValidationError do
  @moduledoc """
  Raised when client-side validation fails.

  This includes invalid column types, mismatched row counts, empty endpoint lists,
  and other client-side validation failures before data is sent to the server.

  ## Fields

  - `:message` - Human-readable validation error description
  """

  defexception [:message]

  @impl true
  def exception(opts) when is_list(opts) do
    message = Keyword.fetch!(opts, :message)

    %__MODULE__{
      message: message
    }
  end
end

defmodule Natch.ProtocolError do
  @moduledoc """
  Raised when there's a protocol-level communication error.

  This includes I/O errors, serialization/deserialization failures, checksum
  mismatches, and other protocol-level issues.

  ## Fields

  - `:message` - Human-readable protocol error description
  """

  defexception [:message]

  @impl true
  def exception(opts) when is_list(opts) do
    message = Keyword.fetch!(opts, :message)

    %__MODULE__{
      message: message
    }
  end
end

defmodule Natch.CompressionError do
  @moduledoc """
  Raised when compression or decompression fails.

  This includes LZ4/ZSTD compression failures, checksum mismatches in compressed
  data, and other compression-related errors.

  ## Fields

  - `:message` - Human-readable compression error description
  """

  defexception [:message]

  @impl true
  def exception(opts) when is_list(opts) do
    message = Keyword.fetch!(opts, :message)

    %__MODULE__{
      message: message
    }
  end
end

defmodule Natch.UnimplementedError do
  @moduledoc """
  Raised when a requested feature is not implemented.

  This occurs when trying to use features not supported by the current ClickHouse
  server version or the client library.

  ## Fields

  - `:message` - Human-readable description of unimplemented feature
  """

  defexception [:message]

  @impl true
  def exception(opts) when is_list(opts) do
    message = Keyword.fetch!(opts, :message)

    %__MODULE__{
      message: message
    }
  end
end

defmodule Natch.OpenSSLError do
  @moduledoc """
  Raised when SSL/TLS operations fail.

  This includes SSL context initialization failures, certificate errors, and other
  OpenSSL-related errors.

  ## Fields

  - `:message` - Human-readable SSL error description
  """

  defexception [:message]

  @impl true
  def exception(opts) when is_list(opts) do
    message = Keyword.fetch!(opts, :message)

    %__MODULE__{
      message: message
    }
  end
end

# Shared error handling functions
defmodule Natch.Error do
  @moduledoc false

  @doc """
  Handle NIF errors by parsing JSON and raising appropriate exceptions.

  Used by modules that call NIFs directly (Block, Column, etc.) to transform
  generic RuntimeError with JSON payload into typed exceptions.
  """
  def handle_nif_error(exception_struct) do
    message = Exception.message(exception_struct)

    case Jason.decode(message) do
      {:ok, %{"type" => "validation"} = error} ->
        raise Natch.ValidationError, message: error["message"]

      {:ok, %{"type" => "protocol"} = error} ->
        raise Natch.ProtocolError, message: error["message"]

      {:ok, %{"type" => "server"} = error} ->
        raise Natch.ServerError,
          message: error["message"],
          code: error["code"],
          name: error["name"],
          stack_trace: error["stack_trace"]

      {:ok, %{"type" => "connection"} = error} ->
        raise Natch.ConnectionError,
          message: error["message"],
          reason: :connection_failed

      {:ok, %{"type" => "compression"} = error} ->
        raise Natch.CompressionError, message: error["message"]

      {:ok, %{"type" => "unimplemented"} = error} ->
        raise Natch.UnimplementedError, message: error["message"]

      {:ok, %{"type" => "openssl"} = error} ->
        raise Natch.OpenSSLError, message: error["message"]

      {:ok, %{"type" => "unknown"} = error} ->
        raise RuntimeError, message: error["message"]

      _ ->
        # Fallback for non-JSON errors - just raise original
        raise exception_struct
    end
  end

  @doc """
  Handle GenServer callback errors by parsing JSON and returning error tuples.

  Used by Connection.handle_call to return structured errors without raising.
  """
  def handle_callback_error(exception_struct) do
    message = Exception.message(exception_struct)

    case Jason.decode(message) do
      {:ok, %{"type" => type} = error} when type in ["server", "validation", "protocol"] ->
        # Return structured error with type and details
        {:error, %{type: type, message: error["message"], details: error}}

      {:ok, error} ->
        # Other error types as simple error tuple
        {:error, error["message"]}

      _ ->
        # Fallback for non-JSON errors
        {:error, message}
    end
  end
end
