# Building from Source

## Automatic Build (npm install)

The native library is automatically built during `npm install` via the `postinstall` script:

```bash
npm install cqlai-node
```

This runs `scripts/build.js` which:

1. Checks Go is installed
2. Compiles `go/bindings/` to a shared library
3. Outputs `lib/libcqlai.so` (~12 MB)

## Manual Build

### Prerequisites

```bash
# Verify Go is installed
go version

# Verify CGO is enabled
go env CGO_ENABLED  # Should output: 1
```

### Build the Library

```bash
cd cqlai-node

# Build from the go/ directory
cd go
go build -buildmode=c-shared -o ../lib/libcqlai.so ./bindings/
cd ..

# Verify
ls -la lib/libcqlai.so
```

### Build Script

The build script (`scripts/build.js`) can also be run manually:

```bash
node scripts/build.js
```

## Project Structure

```
cqlai-node/
├── package.json          # npm package configuration
├── index.js              # Entry point
├── native.js             # FFI bindings (koffi)
├── session.js            # CQLSession class
├── scripts/
│   └── build.js          # Build script
├── go/                   # Go source code
│   ├── go.mod            # Go module definition
│   ├── go.sum            # Dependency checksums
│   ├── bindings/         # FFI exports (C exports)
│   │   ├── exports.go    # Main FFI functions
│   │   ├── astra.go      # Astra bundle support
│   │   ├── crypto.go     # Credential decryption
│   │   ├── ddl.go        # DDL generation
│   │   ├── metadata.go   # Cluster metadata
│   │   ├── source.go     # Source file execution
│   │   ├── tls_security.go
│   │   ├── tracing.go
│   │   └── cqlshrc.go
│   └── internal/         # Internal packages
│       ├── batch/        # CQL splitting
│       ├── config/       # Configuration
│       ├── db/           # Database operations
│       ├── logger/       # Debug logging
│       └── session/      # Session management
└── lib/                  # Built library (generated)
    └── libcqlai.so
```

## Go Module

The `go/go.mod` file defines the module:

```go
module github.com/axonops/cqlai-node

go 1.21

require github.com/apache/cassandra-gocql-driver/v2 v2.0.0
```

### Updating Dependencies

```bash
cd go
go get -u ./...
go mod tidy
```

## Build Flags

### Standard Build

```bash
go build -buildmode=c-shared -o ../lib/libcqlai.so ./bindings/
```

### Debug Build

```bash
go build -buildmode=c-shared -gcflags="all=-N -l" -o ../lib/libcqlai.so ./bindings/
```

### Optimized Build

```bash
go build -buildmode=c-shared -ldflags="-s -w" -o ../lib/libcqlai.so ./bindings/
```

The `-ldflags="-s -w"` strips debug information, reducing binary size.

### Cross-Compilation

Cross-compilation for `c-shared` buildmode is limited by CGO requirements.

**For arm64 on x64 host:**

```bash
# Install cross-compiler
sudo apt install gcc-aarch64-linux-gnu

# Build
GOOS=linux GOARCH=arm64 CC=aarch64-linux-gnu-gcc \
  go build -buildmode=c-shared -o lib/libcqlai-arm64.so ./bindings/
```

## Development Workflow

### Making Changes

1. Edit Go source files in `go/`
2. Rebuild the library:

   ```bash
   cd go && go build -buildmode=c-shared -o ../lib/libcqlai.so ./bindings/
   ```

3. Test in Node.js:

   ```bash
   node -e "const {CQLSession} = require('.'); console.log('OK')"
   ```

### Testing

```bash
# Run basic test
npm test

# Test with a Cassandra instance
node -e "
const { CQLSession } = require('.');
CQLSession.testConnection({ host: '127.0.0.1' })
  .then(r => console.log(r))
  .catch(e => console.error(e));
"
```

### Debugging Go Code

Enable debug logging:

```bash
export CQLAI_DEBUG_LOG_PATH=/tmp/cqlai_debug.log
```

Then check logs:

```bash
tail -f /tmp/cqlai_debug.log
```

## Troubleshooting

### "undefined symbol" errors

The library was built with a different Go version or architecture. Rebuild:

```bash
rm lib/libcqlai.so
npm run build
```

### "GLIBC_X.XX not found"

The library was built on a system with a newer glibc. Build on the oldest target system or use a build container.

### Changes not taking effect

Ensure you rebuilt the library after making Go changes:

```bash
cd go && go build -buildmode=c-shared -o ../lib/libcqlai.so ./bindings/
```

### Memory issues during build

Large Go builds can use significant memory. Close other applications or increase swap:

```bash
# Add temporary swap
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
```
