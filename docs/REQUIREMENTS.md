# System Requirements

## Runtime Requirements

### Node.js

- **Minimum version:** 14.0.0
- **Recommended version:** 18.x or 20.x LTS

### Operating System

| OS      | Architecture  | Library          |
| ------- | ------------- | ---------------- |
| Linux   | x64           | `libcqlai.so`    |
| Linux   | arm64         | `libcqlai.so`    |
| macOS   | x64           | `libcqlai.dylib` |
| macOS   | arm64 (M1/M2) | `libcqlai.dylib` |
| Windows | x64           | `cqlai.dll`      |

The library is built from source during `npm install`, so the correct format is automatically generated for your platform.

## Build Requirements

The native library is compiled from Go source during `npm install`. The following are required:

### Go

- **Minimum version:** 1.21
- **Download:** https://golang.org/dl/

**Installation:**

```bash
# Ubuntu/Debian
sudo apt update
sudo apt install golang-go

# Fedora/RHEL
sudo dnf install golang

# macOS (Homebrew)
brew install go

# Manual installation
wget https://go.dev/dl/go1.21.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.21.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
```

**Verify installation:**

```bash
go version
# go version go1.21.x linux/amd64
```

### CGO (C Compiler)

Go requires a C compiler for building shared libraries (`-buildmode=c-shared`).

**Ubuntu/Debian:**

```bash
sudo apt install build-essential
```

**Fedora/RHEL:**

```bash
sudo dnf groupinstall "Development Tools"
```

**macOS:**

```bash
xcode-select --install
```

**Verify CGO is enabled:**

```bash
go env CGO_ENABLED
# Should output: 1
```

## Cassandra Compatibility

### Supported Versions

| Database            | Versions                    | Notes                     |
| ------------------- | --------------------------- | ------------------------- |
| Apache Cassandra    | 3.11.x, 4.0.x, 4.1.x, 5.0.x | Full support              |
| DataStax Enterprise | 6.x, 7.x                    | Full support              |
| DataStax Astra      | -                           | Via secure connect bundle |
| ScyllaDB            | 4.x, 5.x, 6.x               | Compatible                |

### Protocol Support

- CQL Binary Protocol v3, v4, v5
- TLS/SSL connections
- Username/password authentication
- Client certificate authentication

## Dependencies

### npm Dependencies

| Package                                      | Version | Purpose                        |
| -------------------------------------------- | ------- | ------------------------------ |
| [koffi](https://www.npmjs.com/package/koffi) | ^2.9.0  | FFI bindings to native library |

### Go Dependencies

Managed automatically via `go.mod`:

| Package                                                   | Purpose          |
| --------------------------------------------------------- | ---------------- |
| [gocql](https://github.com/apache/cassandra-gocql-driver) | Cassandra driver |

## Disk Space

| Component                     | Size    |
| ----------------------------- | ------- |
| npm package (source)          | ~500 KB |
| Built library (`libcqlai.so`) | ~12 MB  |
| Go module cache (build time)  | ~50 MB  |

## Memory

- Build time: ~200 MB RAM
- Runtime: Depends on query results and connection pool size

## Network

Build requires internet access to download Go dependencies (first build only).

## Troubleshooting

### "Go is not installed"

```
Error: Go is not installed or not in PATH
```

Install Go and ensure it's in your PATH:

```bash
export PATH=$PATH:/usr/local/go/bin
```

### "CGO_ENABLED=0"

```
go build: -buildmode=c-shared requires CGO
```

Enable CGO and install a C compiler:

```bash
export CGO_ENABLED=1
sudo apt install build-essential  # Ubuntu/Debian
```

### "cannot find -lc"

Missing C library. Install development packages:

```bash
sudo apt install libc6-dev  # Ubuntu/Debian
```

### Build hangs or is very slow

First build downloads Go dependencies. Ensure internet connectivity and wait for completion. Subsequent builds are faster (cached dependencies).
