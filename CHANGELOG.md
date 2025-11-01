# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2025-01-01

### Added
- Full ClickHouse type system support (316 tests passing)
- Columnar-first API for optimal analytics performance
- SSL/TLS support via OpenSSL for ClickHouse Cloud
- Socket-level timeout configuration
- Complete type roundtrip fidelity for all ClickHouse types
- Support for complex nested types (Array, Map, Tuple, Nullable, LowCardinality, Enum)
- Advanced types: UUID, DateTime64, Decimal64
- Comprehensive error handling with structured error types
- Memory-safe NIFs built with FINE framework
- Valgrind-verified zero memory leaks
- GitHub Actions CI with valgrind testing
- Prebuilt binaries for major platforms

### Changed
- Migrated to clickhouse-cpp git submodule for better dependency management
- Improved build system with flexible C++ library paths
- Enhanced test suite with 316 tests including platform-agnostic assertions

### Technical
- Built with FINE (Fast Interop Native Extensions) for crash-proof NIFs
- Uses clickhouse-cpp v2.6.0 for native TCP protocol (port 9000)
- Columnar format provides 100x performance improvement over row-major
- All ClickHouse types supported with full roundtrip fidelity

## [0.1.0] - Initial Development

### Added
- Initial proof of concept
- Basic connection management
- Simple query execution
- Basic type support

[0.2.0]: https://github.com/Intellection/chex/releases/tag/v0.2.0
[0.1.0]: https://github.com/Intellection/chex/releases/tag/v0.1.0
