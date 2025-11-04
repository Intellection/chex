#include <fine.hpp>
#include <clickhouse/client.h>
#include <clickhouse/exceptions.h>
#include <string>
#include <stdexcept>
#include <system_error>
#include <map>

using namespace clickhouse;

// Declare Client as a FINE resource
FINE_RESOURCE(Client);

// Helper to escape JSON strings
std::string escape_json_string(const std::string& input) {
  std::string output;
  output.reserve(input.size());

  for (char c : input) {
    switch (c) {
      case '"': output += "\\\""; break;
      case '\\': output += "\\\\"; break;
      case '\b': output += "\\b"; break;
      case '\f': output += "\\f"; break;
      case '\n': output += "\\n"; break;
      case '\r': output += "\\r"; break;
      case '\t': output += "\\t"; break;
      default:
        if (c < 0x20) {
          // Control characters
          char buf[7];
          snprintf(buf, sizeof(buf), "\\u%04x", (unsigned char)c);
          output += buf;
        } else {
          output += c;
        }
    }
  }
  return output;
}

// Generic exception encoder - extracts rich error information from any clickhouse exception
std::string encode_clickhouse_error(const std::exception& e) {
  std::string error_json;

  // Try to cast to specific exception types and extract their rich information
  if (const auto* server_ex = dynamic_cast<const clickhouse::ServerException*>(&e)) {
    // ServerException has code, name, display_text, stack_trace
    const auto& exception = server_ex->GetException();
    error_json = "{\"type\":\"server\",";
    error_json += "\"code\":" + std::to_string(exception.code) + ",";
    error_json += "\"name\":\"" + escape_json_string(exception.name) + "\",";
    error_json += "\"message\":\"" + escape_json_string(exception.display_text) + "\"";

    if (!exception.stack_trace.empty()) {
      error_json += ",\"stack_trace\":\"" + escape_json_string(exception.stack_trace) + "\"";
    }
    error_json += "}";

  } else if (dynamic_cast<const clickhouse::ValidationError*>(&e)) {
    error_json = "{\"type\":\"validation\",\"message\":\"" + escape_json_string(e.what()) + "\"}";

  } else if (dynamic_cast<const clickhouse::ProtocolError*>(&e)) {
    error_json = "{\"type\":\"protocol\",\"message\":\"" + escape_json_string(e.what()) + "\"}";

  } else if (dynamic_cast<const clickhouse::UnimplementedError*>(&e)) {
    error_json = "{\"type\":\"unimplemented\",\"message\":\"" + escape_json_string(e.what()) + "\"}";

  } else if (dynamic_cast<const clickhouse::OpenSSLError*>(&e)) {
    error_json = "{\"type\":\"openssl\",\"message\":\"" + escape_json_string(e.what()) + "\"}";

  } else if (dynamic_cast<const clickhouse::CompressionError*>(&e)) {
    error_json = "{\"type\":\"compression\",\"message\":\"" + escape_json_string(e.what()) + "\"}";

  } else if (const auto* sys_err = dynamic_cast<const std::system_error*>(&e)) {
    // System errors (DNS, network, etc.)
    error_json = "{\"type\":\"connection\",";
    error_json += "\"message\":\"" + escape_json_string(sys_err->what()) + "\",";
    error_json += "\"code\":" + std::to_string(sys_err->code().value()) + "}";

  } else {
    // Unknown exception type
    error_json = "{\"type\":\"unknown\",\"message\":\"" + escape_json_string(e.what()) + "\"}";
  }

  return error_json;
}

// Helper to handle nullable strings from Elixir (nil becomes empty string)
std::string get_optional_string(const std::string& value) {
  return value;
}

// Create a ClickHouse client with full options
// Args: host, port, database (nil/empty for none), user (nil/empty for none),
//       password (nil/empty for none), compression_enabled, ssl_enabled,
//       connect_timeout_ms, recv_timeout_ms, send_timeout_ms
// Note: FINE converts Elixir nil to empty string for string params
fine::ResourcePtr<Client> client_create(
    ErlNifEnv *env,
    std::string host,
    uint64_t port,
    std::string database,
    std::string user,
    std::string password,
    bool compression,
    bool ssl,
    uint64_t connect_timeout,
    uint64_t recv_timeout,
    uint64_t send_timeout) {
  try {
    ClientOptions opts;
    opts.SetHost(host);
    opts.SetPort(static_cast<uint16_t>(port));

    if (!database.empty()) {
      opts.SetDefaultDatabase(database);
    }

    if (!user.empty()) {
      opts.SetUser(user);
    }

    if (!password.empty()) {
      opts.SetPassword(password);
    }

    if (compression) {
      opts.SetCompressionMethod(CompressionMethod::LZ4);
    }

    if (ssl) {
      // Enable SSL with default settings:
      // - Use system CA certificates for verification
      // - Verify peer certificate
      // - Enable SNI
      ClientOptions::SSLOptions ssl_opts;
      ssl_opts.SetUseDefaultCALocations(true);
      ssl_opts.SetUseSNI(true);
      opts.SetSSLOptions(ssl_opts);
    }

    // Set socket-level timeouts
    opts.SetConnectionConnectTimeout(std::chrono::milliseconds(connect_timeout));
    opts.SetConnectionRecvTimeout(std::chrono::milliseconds(recv_timeout));
    opts.SetConnectionSendTimeout(std::chrono::milliseconds(send_timeout));

    return fine::make_resource<Client>(opts);
  } catch (const std::exception& e) {
    // Use generic encoder to extract rich error information
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(client_create, 0);

// Simple client creation (for PoC compatibility)
fine::ResourcePtr<Client> create_client(ErlNifEnv *env) {
  return client_create(env, "localhost", 9000, "", "", "", false, false, 5000, 0, 0);
}
FINE_NIF(create_client, 0);

// Ping the ClickHouse server
std::string client_ping(ErlNifEnv *env, fine::ResourcePtr<Client> client) {
  try {
    client->Ping();
    return "pong";
  } catch (const std::exception& e) {
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(client_ping, 0);

// Alias for backwards compatibility with PoC
std::string ping(ErlNifEnv *env, fine::ResourcePtr<Client> client) {
  return client_ping(env, client);
}
FINE_NIF(ping, 0);

// Execute a query (DDL/DML without results)
// Returns :ok atom on success
fine::Atom client_execute(
    ErlNifEnv *env,
    fine::ResourcePtr<Client> client,
    std::string sql) {
  try {
    client->Execute(sql);
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(client_execute, 0);

// Reset connection
// Returns :ok atom on success
fine::Atom client_reset_connection(ErlNifEnv *env, fine::ResourcePtr<Client> client) {
  try {
    client->ResetConnection();
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(encode_clickhouse_error(e));
  }
}
FINE_NIF(client_reset_connection, 0);

// Initialize the NIF module
FINE_INIT("Elixir.Natch.Native");
