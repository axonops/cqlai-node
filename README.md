# cqlai-node

Native Node.js bindings for Apache Cassandra, powered by Go and the [gocql](https://github.com/apache/cassandra-gocql-driver) driver.

This package provides high-performance Cassandra connectivity for Node.js applications by bundling Go source code that compiles to a native shared library during installation.

## Overview

**cqlai-node** is extracted from [cqlai](https://github.com/axonops/cqlai) - parent project, a full-featured Cassandra CQL shell with AI capabilities. This package provides the core database connectivity layer as a standalone Node.js module.

### Features

- Native performance via Go/gocql driver
- Full CQL support including UDTs, collections, and all Cassandra types
- Proper CQL parsing (handles comments, quoted strings, batch statements)
- Query paging with iterator-based pagination
- DataStax Astra secure bundle support
- TLS/SSL connections with certificate validation
- Query tracing
- DDL generation (CREATE statements)
- Shell commands (CONSISTENCY, PAGING, TRACING, etc.)

## Requirements

- **Node.js** >= 14.0.0
- **Go** >= 1.21 (for building the native library)
- **GCC/CGO** (Go's C compiler support)
- **Linux** x64 or arm64

### Installing Go

If Go is not installed:

```bash
# Ubuntu/Debian
sudo apt install golang-go

# Or download from https://golang.org/dl/
```

## Installation

```bash
npm install cqlai-node
```

The `postinstall` script automatically compiles the Go source into a native shared library (~12 MB).

## Quick Start

```javascript
const { CQLSession } = require('cqlai-node');

async function main() {
  // Connect to Cassandra
  const result = await CQLSession.connect({
    host: '127.0.0.1',
    port: 9042,
    keyspace: 'my_keyspace',
    username: 'cassandra',
    password: 'cassandra'
  });

  if (!result.success) {
    console.error('Connection failed:', result.error);
    return;
  }

  const session = result.data;
  console.log('Connected to Cassandra', session.cassandraVersion);

  try {
    // Execute queries
    const queryResult = await session.execute('SELECT * FROM users LIMIT 10');

    if (queryResult.success) {
      console.log('Columns:', queryResult.data.columns);
      console.log('Rows:', queryResult.data.rows);
    }
  } finally {
    await session.close();
  }
}

main().catch(console.error);
```

## Documentation

- [API Reference](docs/API.md) - Complete API documentation
- [Requirements](docs/REQUIREMENTS.md) - System requirements and compatibility
- [Building](docs/BUILDING.md) - Building from source

## Basic Usage

### Connecting

```javascript
const { CQLSession } = require('cqlai-node');

// Standard connection
const result = await CQLSession.connect({
  host: '192.168.1.100',
  port: 9042,
  keyspace: 'demo',
  username: 'cassandra',
  password: 'cassandra',
  consistency: 'LOCAL_QUORUM'
});

// DataStax Astra connection
const astraResult = await CQLSession.connectWithAstraBundle({
  bundlePath: '/path/to/secure-connect-database.zip',
  username: 'client_id',
  password: 'client_secret'
});
```

### Executing Queries

```javascript
// Single query
const result = await session.execute('SELECT * FROM users WHERE id = ?', {
  params: [userId]
});

// Multiple statements
const result = await session.execute(`
  INSERT INTO users (id, name) VALUES (uuid(), 'Alice');
  INSERT INTO users (id, name) VALUES (uuid(), 'Bob');
  SELECT * FROM users;
`);

// With progress callback
await session.execute(multipleStatements, {
  onProgress: (result) => {
    console.log(`Statement ${result.index + 1}: ${result.identifier}`);
  }
});
```

### Paging

```javascript
// Enable paging
await session.setPaging(100);

// Execute - first page returned
const result = await session.execute('SELECT * FROM large_table');

// Fetch more pages
if (result.data.hasMore) {
  const page2 = await session.fetchNextPage(result.data.queryId);
}
```

### Session Configuration

```javascript
// Set consistency level
await session.setConsistency('QUORUM');

// Enable tracing
await session.setTracing(true);

// Change keyspace
await session.setKeyspace('another_keyspace');

// Get session info
const info = await session.getInfo();
console.log(info.data);
// { cassandraVersion, keyspace, consistency, pageSize, tracing, ... }
```

### Schema Operations

```javascript
// Get cluster metadata
const metadata = await session.getClusterMetadata();

// Generate DDL
const ddl = await session.getDDL({
  keyspace: 'my_keyspace',
  table: 'users'
});
console.log(ddl.data.ddl);
```

## Error Handling

All methods return `{ success: boolean, data?, error?, code? }`:

```javascript
const result = await session.execute('SELECT * FROM users');

if (!result.success) {
  console.error('Error:', result.error);
  console.error('Code:', result.code); // e.g., 'PARSE_ERROR', 'QUERY_ERROR'
  return;
}

// Use result.data
```

## Shell Commands

Execute cqlsh-style commands:

```javascript
await session.execute('CONSISTENCY QUORUM');
await session.execute('PAGING 100');
await session.execute('TRACING ON');
await session.execute('USE my_keyspace');
```

## License

Apache License 2.0 - See [LICENSE](LICENSE)
