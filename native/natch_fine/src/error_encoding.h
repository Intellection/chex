#pragma once

#include <clickhouse/exceptions.h>
#include <string>
#include <system_error>

// Helper to escape JSON strings
inline std::string escape_json_string(const std::string& input) {
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
inline std::string encode_clickhouse_error(const std::exception& e) {
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
