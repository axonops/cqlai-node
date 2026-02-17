# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**cqlai-node** is a Node.js native bindings package for Apache Cassandra (@axonops/cqlai-node). It provides high-performance Cassandra connectivity by compiling Go source code (using the gocql driver) into a native shared library that is loaded via FFI (Koffi).

**Architecture**: Hybrid JS + Go
- **JavaScript layer** (`index.js`, `session.js`, `native.js`): Promise-based API, session management, FFI bindings
- **Go layer** (`go/`): Compiled to C-shared library (`lib/libcqlai.so` ~12MB) with C-exported functions for FFI

## Common Commands

### Building

```bash
# Automatic build (runs postinstall)
npm install

# Manual build via npm script
npm run build

# Manual build (from go/ directory)
cd go && go build -buildmode=c-shared -o ../lib/libcqlai.so ./bindings/

# Build for current platform (auto-detects extension)
node scripts/build.js
```

### Running Tests

```bash
# Basic JS test (only verifies module loads)
npm test

# Go tests (from go/ directory)
cd go && go test ./internal/...

# Test with live Cassandra instance
node -e "const { CQLSession } = require('.'); CQLSession.testConnection({ host: '127.0.0.1' }).then(r => console.log(r))"
```

### Development Workflow

```bash
# Quick verification after Go changes
cd go && go build -buildmode=c-shared -o ../lib/libcqlai.so ./bindings/ && cd .. && node -e "const {CQLSession} = require('.'); console.log('OK')"

# Enable debug logging for Go code
export CQLAI_DEBUG_LOG_PATH=/tmp/cqlai_debug.log
tail -f /tmp/cqlai_debug.log
```

## High-Level Architecture

### FFI Layer (`native.js`)

Uses **Koffi** to load the shared library and define C function signatures. Two calling modes:

- **Synchronous** (`callNative`): For quick operations, runs on main thread
- **Asynchronous** (`callNativeTrueAsync`): Uses Koffi's worker threads for non-blocking I/O operations

All native functions return JSON strings for cross-language compatibility. C functions follow this signature pattern:
```c
char* SomeFunction(const char* jsonInput);
```

### Session Management (`session.js`)

- **Handle-based**: Go stores sessions in a map with integer handles; JS stores only the handle
- **Static methods** (`CQLSession.connect`, `CQLSession.testConnection`): Create new sessions
- **Instance methods**: All operations on an existing session
- **Result wrapper**: All methods return `{ success: boolean, data?, error?, code? }`

### Go Architecture

Key packages under `go/internal/`:

- **`db/`**: Core database operations, query execution, schema caching, UDT handling, type parsing
- **`bindings/`**: C-exported functions (FFI entry points) - one file per feature area (astra.go, ddl.go, metadata.go, etc.)
- **`config/`**: Configuration management, cqlshrc parsing
- **`batch/`**: CQL statement parsing and splitting
- **`session/`**: Session lifecycle management

### Query Execution Flow

```
session.execute(cql) ->
  native.SplitCQL() [parses statements] ->
  For each statement:
    - Shell command? → _do_<command>() (CONSISTENCY, PAGING, TRACING, etc.)
    - SELECT with paging? → ExecuteQueryPaged() → queryId for pagination
    - Regular query? → ExecuteQuery()
  Return formatted result
```

### Paged Query Support

- Iterator-based pagination for large result sets
- Query state stored in Go with unique `queryId`
- `hasMore` flag indicates additional pages
- `fetchNextPage(queryId)` retrieves subsequent pages

### Schema/DDL Operations

Extensive metadata support in `go/internal/db/`:
- `describe_*.go` files: Tables, keyspaces, indexes, materialized views, aggregates, functions
- `schema_cache.go`: Caches schema for performance
- `ddl.go`: Generates CREATE statements
- `udt_decoder.go`: Custom UDT type handling

## Key Files

| File | Purpose |
|------|---------|
| `index.js` | Package entry, exports CQLSession |
| `session.js` | Main CQLSession class implementation |
| `native.js` | FFI setup, library loading, async/sync call wrappers |
| `scripts/build.js` | Cross-platform build script |
| `go/bindings/exports.go` | Core C-exported FFI functions |
| `go/internal/db/` | Database operations, query execution |
| `docs/API.md` | Complete API documentation |
| `docs/BUILDING.md` | Build instructions and troubleshooting |

## Requirements

- **Node.js** >= 14.0.0
- **Go** >= 1.21
- **CGO enabled** (`CGO_ENABLED=1`)
- **GCC** for CGO compilation
- **Platform**: Linux x64/arm64 (primary)

## Debugging

Enable Go debug logging:
```bash
export CQLAI_DEBUG_LOG_PATH=/tmp/cqlai_debug.log
```

## Troubleshooting Build Issues

- **"undefined symbol" errors**: Rebuild the library (`rm lib/libcqlai.so && npm run build`)
- **"GLIBC_X.XX not found"**: Build on the oldest target system or use a build container
- **Changes not taking effect**: Ensure you rebuilt the Go library after changes
