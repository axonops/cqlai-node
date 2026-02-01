# cqlai-node API Reference

Complete API documentation for the `cqlai-node` package - native Node.js bindings for Apache Cassandra.

## Table of Contents

- [Installation](#installation)
- [CQLSession Class](#cqlsession-class)
- [Static Methods](#static-methods)
  - [connect()](#cqlsessionconnectoptions)
  - [testConnection()](#cqlsessiontestconnectionoptions)
  - [testConnectionWithID()](#cqlsessiontestconnectionwithidoptions)
  - [cancelTestConnection()](#cqlsessioncanceltestconnectionrequestid)
  - [checkTLSSecurity()](#cqlsessionchecktlssecurityoptions)
  - [decryptCredential()](#cqlsessiondecryptcredentialoptions)
  - [connectWithAstraBundle()](#cqlsessionconnectwithastrundleoptions)
  - [parseAstraBundle()](#cqlsessionparseastrabundleoptions)
  - [validateAstraBundle()](#cqlsessionvalidateastrabundlebundlepath)
  - [cleanupAstraBundle()](#cqlsessioncleanupastrabundleextracteddir)
- [Instance Methods](#instance-methods)
  - [execute()](#sessionexecutecql-options)
  - [executeMulti()](#sessionexecutemulticql-options)
  - [fetchNextPage()](#sessionfetchnextpagequeryid)
  - [cancelPagedQuery()](#sessioncancelpagedqueryqueryid)
  - [cancelQuery()](#sessioncancelquery)
  - [setConsistency()](#sessionsetconsistencylevel)
  - [setPaging()](#sessionsetpagingvalue)
  - [setTracing()](#sessionsettracingenabled)
  - [setExpand()](#sessionsetexpandenabled)
  - [setKeyspace()](#sessionsetkeyspacekeyspace)
  - [getInfo()](#sessiongetinfo)
  - [getClusterMetadata()](#sessiongetclustermetadata)
  - [getDDL()](#sessiongetddloptions)
  - [getQueryTrace()](#sessiongetquerytracesessionid)
  - [executeSourceFiles()](#sessionexecutesourcefilesoptions)
  - [close()](#sessionclose)
- [Instance Properties](#instance-properties)
- [Shell Commands](#shell-commands)
- [Error Handling](#error-handling)
- [Consistency Levels](#consistency-levels)

---

## Installation

```javascript
const { CQLSession } = require('cqlai-node');
```

---

## CQLSession Class

The main class for interacting with Cassandra clusters.

---

## Static Methods

### `CQLSession.connect(options)`

Connect to a Cassandra cluster and create a session.

**Parameters:**

| Name                        | Type     | Default       | Description                                           |
| --------------------------- | -------- | ------------- | ----------------------------------------------------- |
| `options.host`              | `string` | `'127.0.0.1'` | Cassandra host address                                |
| `options.port`              | `number` | `9042`        | Cassandra native protocol port                        |
| `options.keyspace`          | `string` | -             | Initial keyspace to use                               |
| `options.username`          | `string` | -             | Authentication username                               |
| `options.password`          | `string` | -             | Authentication password                               |
| `options.consistency`       | `string` | `'LOCAL_ONE'` | Default consistency level                             |
| `options.connectTimeout`    | `number` | -             | Connection timeout in seconds                         |
| `options.requestTimeout`    | `number` | -             | Request timeout in seconds                            |
| `options.rsaPrivateKey`     | `string` | -             | PEM-encoded RSA private key for credential decryption |
| `options.rsaPrivateKeyFile` | `string` | -             | Path to RSA private key file                          |

**Returns:** `Promise<{ success: boolean, data?: CQLSession, error?: string }>`

**Example:**

```javascript
const result = await CQLSession.connect({
  host: '192.168.1.100',
  port: 9042,
  keyspace: 'my_keyspace',
  username: 'cassandra',
  password: 'cassandra'
});

if (result.success) {
  const session = result.data;
  // Use session...
}
```

---

### `CQLSession.testConnection(options)`

Test connection to a Cassandra cluster without maintaining a session.

**Parameters:** Same as `connect()` (except `keyspace`)

**Returns:** `Promise<{ success: boolean, data?: ClusterInfo, error?: string }>`

**ClusterInfo structure:**

```javascript
{
  build: '4.1.3',        // Cassandra version
  protocol: 5,           // Protocol version
  cql: '3.4.6',          // CQL version
  datacenter: 'dc1',     // Local datacenter
  datacenters: [         // All datacenters
    { address: '192.168.1.100', datacenter: 'dc1' },
    { address: '192.168.1.101', datacenter: 'dc1' }
  ]
}
```

---

### `CQLSession.testConnectionWithID(options)`

Test connection with cancellation support.

**Parameters:**

| Name                | Type     | Required | Description                |
| ------------------- | -------- | -------- | -------------------------- |
| `options.requestID` | `string` | Yes      | Unique ID for cancellation |
| ...other            | -        | -        | Same as `testConnection()` |

**Returns:** `Promise<{ success: boolean, data?: ClusterInfo, error?: string, code?: string }>`

If cancelled: `{ success: false, error: 'Connection cancelled', code: 'CANCELLED' }`

---

### `CQLSession.cancelTestConnection(requestID)`

Cancel a pending connection test.

**Parameters:**

| Name        | Type     | Required | Description                                  |
| ----------- | -------- | -------- | -------------------------------------------- |
| `requestID` | `string` | Yes      | The request ID from `testConnectionWithID()` |

**Returns:** `Promise<{ success: boolean, data?: { cancelled: boolean, reason?: string } }>`

---

### `CQLSession.checkTLSSecurity(options)`

Analyze TLS/SSL security of a connection or certificate files.

**Parameters:**

| Name                 | Type      | Required | Description                                       |
| -------------------- | --------- | -------- | ------------------------------------------------- |
| `options.host`       | `string`  | No*      | Host to connect to (*required unless `filesOnly`) |
| `options.port`       | `number`  | No       | Port (default: 9042)                              |
| `options.caFile`     | `string`  | No       | CA certificate file path                          |
| `options.certFile`   | `string`  | No       | Client certificate file path                      |
| `options.keyFile`    | `string`  | No       | Client key file path                              |
| `options.skipVerify` | `boolean` | No       | Skip certificate verification                     |
| `options.filesOnly`  | `boolean` | No       | Only analyze files, don't connect                 |

**Returns:** `Promise<{ success: boolean, data?: TLSSecurityInfo, error?: string }>`

---

### `CQLSession.decryptCredential(options)`

Decrypt RSA-encrypted credentials.

**Parameters:**

| Name                     | Type     | Required | Description               |
| ------------------------ | -------- | -------- | ------------------------- |
| `options.ciphertext`     | `string` | Yes      | Base64-encoded ciphertext |
| `options.privateKey`     | `string` | No*      | PEM-encoded private key   |
| `options.privateKeyFile` | `string` | No*      | Path to private key file  |

*One of `privateKey` or `privateKeyFile` is required.

**Returns:** `Promise<{ success: boolean, data?: { plaintext: string }, error?: string }>`

---

### `CQLSession.connectWithAstraBundle(options)`

Connect using a DataStax Astra secure connect bundle.

**Parameters:**

| Name                 | Type     | Required | Description                         |
| -------------------- | -------- | -------- | ----------------------------------- |
| `options.bundlePath` | `string` | Yes      | Path to secure-connect-*.zip bundle |
| `options.username`   | `string` | Yes      | Astra client ID                     |
| `options.password`   | `string` | Yes      | Astra client secret                 |
| `options.keyspace`   | `string` | No       | Override keyspace from bundle       |
| `options.extractDir` | `string` | No       | Directory to extract bundle to      |

**Returns:** `Promise<{ success: boolean, data?: { session: CQLSession, bundleInfo: AstraBundleInfo }, error?: string }>`

---

### `CQLSession.parseAstraBundle(options)`

Parse an Astra secure connect bundle without connecting.

**Parameters:**

| Name                 | Type     | Required | Description          |
| -------------------- | -------- | -------- | -------------------- |
| `options.bundlePath` | `string` | Yes      | Path to bundle       |
| `options.extractDir` | `string` | No       | Extraction directory |

**Returns:** `Promise<{ success: boolean, data?: AstraBundleInfo, error?: string }>`

---

### `CQLSession.validateAstraBundle(bundlePath)`

Validate an Astra bundle without extracting.

**Parameters:**

| Name         | Type     | Required | Description    |
| ------------ | -------- | -------- | -------------- |
| `bundlePath` | `string` | Yes      | Path to bundle |

**Returns:** `Promise<{ success: boolean, data?: { valid: boolean, errors: string[] }, error?: string }>`

---

### `CQLSession.cleanupAstraBundle(extractedDir)`

Clean up extracted Astra bundle files.

**Parameters:**

| Name           | Type     | Required | Description                           |
| -------------- | -------- | -------- | ------------------------------------- |
| `extractedDir` | `string` | Yes      | Directory containing extracted bundle |

**Returns:** `Promise<{ success: boolean, error?: string }>`

---

## Instance Methods

### `session.execute(cql, options?)`

Execute CQL query/queries or shell commands. Handles multiple statements separated by semicolons.

**Parameters:**

| Name                  | Type       | Required | Description                                    |
| --------------------- | ---------- | -------- | ---------------------------------------------- |
| `cql`                 | `string`   | Yes      | CQL statement(s) or shell command(s)           |
| `options.stopOnError` | `boolean`  | No       | Stop on first error (default: false)           |
| `options.onProgress`  | `function` | No       | Callback called after each statement completes |

**Returns:** `Promise<ExecuteResult>`

**ExecuteResult structure:**

```javascript
{
  success: true,
  statementsCount: 3,
  statementsExecuted: 3,
  identifiers: ['INSERT', 'SELECT', 'UPDATE'],
  extraTokens: ['INTO', 'users'],
  stopped: false,               // true if stopped early due to error
  data: {
    results: [                  // For multiple statements
      { success, index, identifier, allCompleted, ... },
      { success, index, identifier, allCompleted, ... }
    ]
  },
  promptInfo: {
    username: 'cassandra',
    host: '192.168.1.100',
    keyspace: 'my_keyspace',
    prompt: 'cassandra@cqlsh:my_keyspace'
  }
}
```

**Per-statement result (via onProgress or in results array):**

```javascript
{
  success: true,
  index: 0,                     // Statement index (0-based)
  identifier: 'SELECT',         // Statement type
  allCompleted: false,          // true only for last statement

  // For SELECT queries:
  columns: ['id', 'name'],
  columnTypes: ['uuid', 'text'],
  rows: [...],
  rowCount: 100,
  duration: '2.5ms',
  hasMore: true,                // Paging: more rows available
  queryId: 'abc123',            // Paging: use with fetchNextPage()

  // For non-SELECT:
  message: 'Query executed successfully',

  promptInfo: {...}
}
```

**Examples:**

```javascript
// Single SELECT
const result = await session.execute('SELECT * FROM users LIMIT 10');

// Multiple statements with progress callback
await session.execute(`
  INSERT INTO users (id, name) VALUES (uuid(), 'Alice');
  SELECT * FROM users;
  UPDATE users SET name = 'Bob' WHERE id = ?;
`, {
  onProgress: async (result) => {
    console.log(`Statement ${result.index + 1}: ${result.identifier}`);
    console.log(`  Success: ${result.success}`);
    console.log(`  All completed: ${result.allCompleted}`);

    // For SELECT with paging
    if (result.hasMore) {
      console.log(`  More rows available, queryId: ${result.queryId}`);
    }
  }
});

// SELECT with paging - first page returned, fetch more with queryId
const result = await session.execute('SELECT * FROM large_table');
if (result.data.hasMore) {
  const page2 = await session.fetchNextPage(result.data.queryId);
}

// Stop on first error
const result = await session.execute(`
  INSERT INTO users ...;
  INSERT INTO invalid_table ...;  -- This fails
  INSERT INTO users ...;          -- Not executed
`, { stopOnError: true });
```

**Paging support:**

- Only SELECT statements support paging (Cassandra protocol limitation)
- Set page size via `session.setPaging(100)` before executing
- Result includes `hasMore: true` and `queryId` if more rows available
- Use `fetchNextPage(queryId)` to get next page
- Use `cancelPagedQuery(queryId)` to cancel/cleanup

---

### `session.executeMulti(cql, options?)`

Execute multiple CQL statements using native batch execution. Better performance for pure CQL (no shell commands). Used internally by `execute()` when no `onProgress` callback is provided.

**Parameters:**

| Name                  | Type      | Required | Description                          |
| --------------------- | --------- | -------- | ------------------------------------ |
| `cql`                 | `string`  | Yes      | CQL statement(s)                     |
| `options.stopOnError` | `boolean` | No       | Stop on first error (default: false) |

**Returns:** Same as `execute()`

**Note:** Does not support `onProgress` callback. Use `execute()` with `onProgress` for per-statement progress.

---

### `session.fetchNextPage(queryId)`

Fetch the next page of results for a paged query.

**Parameters:**

| Name      | Type     | Required | Description                                               |
| --------- | -------- | -------- | --------------------------------------------------------- |
| `queryId` | `string` | Yes      | Query ID from `execute()` result (when `hasMore` is true) |

**Returns:** `Promise<{ success: boolean, data?: PagedResult, error?: string }>`

**PagedResult structure:**

```javascript
{
  columns: ['id', 'name'],
  columnTypes: ['uuid', 'text'],
  rows: [...],
  rowCount: 100,
  hasMore: true,          // More pages available
  queryId: 'abc123'       // Same queryId for next fetch
}
```

When `hasMore` is false, the query is automatically closed.

**Example:**

```javascript
// Execute SELECT with paging enabled
await session.setPaging(100);
const result = await session.execute('SELECT * FROM large_table');

// Fetch all pages
let queryId = result.data.queryId;
while (result.data.hasMore) {
  const nextPage = await session.fetchNextPage(queryId);
  console.log('Got', nextPage.data.rows.length, 'more rows');
  if (!nextPage.data.hasMore) break;
}
```

---

### `session.cancelPagedQuery(queryId)`

Cancel/close an active paged query iterator. Call this to clean up resources if you don't need all pages.

**Parameters:**

| Name      | Type     | Required | Description                      |
| --------- | -------- | -------- | -------------------------------- |
| `queryId` | `string` | Yes      | Query ID from `execute()` result |

**Returns:** `Promise<{ success: boolean, data?: { cancelled: boolean, reason?: string }, error?: string }>`

---

### `session.cancelQuery()`

Cancel any active queries on this session (for handling CTRL+C).

**Returns:** `Promise<{ success: boolean, data?: { cancelledQueries: number }, error?: string }>`

---

### `session.setConsistency(level)`

Set the consistency level.

**Parameters:**

| Name    | Type     | Required | Description       |
| ------- | -------- | -------- | ----------------- |
| `level` | `string` | Yes      | Consistency level |

**Valid levels:** `ANY`, `ONE`, `TWO`, `THREE`, `QUORUM`, `ALL`, `LOCAL_QUORUM`, `EACH_QUORUM`, `LOCAL_ONE`

**Returns:** `Promise<{ success: boolean, data?: { consistency: string }, error?: string }>`

---

### `session.setPaging(value)`

Set paging size or disable paging.

**Parameters:**

| Name    | Type               | Required | Description          |
| ------- | ------------------ | -------- | -------------------- |
| `value` | `string \| number` | Yes      | Page size or `'OFF'` |

**Returns:** `Promise<{ success: boolean, data?: { paging: string, pageSize: number }, error?: string }>`

---

### `session.setTracing(enabled)`

Enable or disable query tracing.

**Parameters:**

| Name      | Type      | Required | Description               |
| --------- | --------- | -------- | ------------------------- |
| `enabled` | `boolean` | Yes      | Whether to enable tracing |

**Returns:** `Promise<{ success: boolean, data?: { tracing: boolean }, error?: string }>`

---

### `session.setExpand(enabled)`

Enable or disable expand mode (vertical row display).

**Parameters:**

| Name      | Type      | Required | Description                   |
| --------- | --------- | -------- | ----------------------------- |
| `enabled` | `boolean` | Yes      | Whether to enable expand mode |

**Returns:** `Promise<{ success: boolean, data?: { expand: boolean }, error?: string }>`

---

### `session.setKeyspace(keyspace)`

Change the current keyspace.

**Parameters:**

| Name       | Type     | Required | Description   |
| ---------- | -------- | -------- | ------------- |
| `keyspace` | `string` | Yes      | Keyspace name |

**Returns:** `Promise<{ success: boolean, data?: { keyspace: string }, error?: string }>`

---

### `session.getInfo()`

Get session information.

**Returns:** `Promise<{ success: boolean, data?: SessionInfo, error?: string }>`

**SessionInfo structure:**

```javascript
{
  cassandraVersion: '4.1.3',
  keyspace: 'my_keyspace',
  consistency: 'LOCAL_ONE',
  serialConsistency: 'SERIAL',
  pageSize: 100,
  tracing: false,
  expand: false,
  username: 'cassandra',
  host: '192.168.1.100'
}
```

---

### `session.getClusterMetadata()`

Get full cluster metadata (keyspaces, tables, columns, indexes, types, functions, etc.).

**Returns:** `Promise<{ success: boolean, data?: ClusterMetadata, error?: string }>`

---

### `session.getDDL(options)`

Generate DDL (CREATE statements) for various scopes.

**Parameters:**

| Name                    | Type      | Required | Description                                |
| ----------------------- | --------- | -------- | ------------------------------------------ |
| `options.cluster`       | `boolean` | No       | Generate DDL for entire cluster            |
| `options.includeSystem` | `boolean` | No       | Include system keyspaces (default: true)   |
| `options.keyspace`      | `string`  | No       | Keyspace name                              |
| `options.table`         | `string`  | No       | Table name (requires keyspace)             |
| `options.index`         | `string`  | No       | Index name (requires keyspace and table)   |
| `options.type`          | `string`  | No       | User type name (requires keyspace)         |
| `options.function`      | `string`  | No       | Function name (requires keyspace)          |
| `options.aggregate`     | `string`  | No       | Aggregate name (requires keyspace)         |
| `options.view`          | `string`  | No       | Materialized view name (requires keyspace) |

**Returns:** `Promise<{ success: boolean, data?: { ddl: string, scope: string }, error?: string }>`

**Example:**

```javascript
// Get DDL for entire cluster
const clusterDDL = await session.getDDL({ cluster: true });

// Get DDL for a keyspace
const ksDDL = await session.getDDL({ keyspace: 'my_keyspace' });

// Get DDL for a specific table
const tableDDL = await session.getDDL({ keyspace: 'my_keyspace', table: 'users' });
console.log(tableDDL.data.ddl);
// CREATE TABLE my_keyspace.users (
//   id uuid PRIMARY KEY,
//   name text,
//   email text
// );
```

---

### `session.getQueryTrace(sessionId)`

Get query trace by session ID.

**Parameters:**

| Name        | Type     | Required | Description        |
| ----------- | -------- | -------- | ------------------ |
| `sessionId` | `string` | Yes      | Trace session UUID |

**Returns:** `Promise<{ success: boolean, data?: QueryTraceResult, error?: string }>`

**QueryTraceResult structure:**

```javascript
{
  session: {
    sessionId: '550e8400-...',
    coordinator: '192.168.1.100',
    duration: 2500,           // microseconds
    startedAt: '2024-01-15T10:30:00Z',
    client: '192.168.1.50',
    request: 'Execute CQL3 query',
    command: 'SELECT',
    parameters: '{}'
  },
  events: [
    {
      activity: 'Parsing SELECT * FROM users',
      timestamp: '2024-01-15T10:30:00.001Z',
      source: '192.168.1.100',
      sourceElapsed: 100,     // microseconds
      thread: 'Native-Transport-...'
    }
  ]
}
```

---

### `session.executeSourceFiles(options)`

Execute multiple CQL files (SOURCE command equivalent).

**Parameters:**

| Name                  | Type       | Required | Description         |
| --------------------- | ---------- | -------- | ------------------- |
| `options.files`       | `string[]` | Yes      | Array of file paths |
| `options.stopOnError` | `boolean`  | No       | Stop on first error |
| `options.onProgress`  | `function` | No       | Progress callback   |

**Progress callback receives:**

```javascript
{
  filePath: '/path/to/file.cql',
  fileIndex: 0,
  totalFiles: 3,
  statementsTotal: 50,
  statementsRun: 25,
  statementsOK: 24,
  statementsFailed: 1,
  currentStatement: 'INSERT INTO...',
  errors: ['Error at line 10: ...'],
  isComplete: false,
  duration: 1500  // ms
}
```

**Returns:** `Promise<{ success: boolean, data?: SourceFilesResult, error?: string }>`

---

### `session.close()`

Close the session.

**Returns:** `Promise<{ success: boolean, error?: string }>`

---

### `session.getPrompt()`

Get the cqlsh-style prompt string.

**Returns:** `string` (e.g., `'cassandra@cqlsh:my_keyspace'`)

---

### `session.getPromptInfo()`

Get prompt components for rendering.

**Returns:**

```javascript
{
  username: 'cassandra',
  host: '192.168.1.100',
  keyspace: 'my_keyspace',
  prompt: 'cassandra@cqlsh:my_keyspace'
}
```

---

## Instance Properties

| Property                   | Type     | Description                         |
| -------------------------- | -------- | ----------------------------------- |
| `session.cassandraVersion` | `string` | Cassandra version (e.g., `'4.1.3'`) |
| `session.keyspace`         | `string` | Current keyspace                    |
| `session.handle`           | `number` | Internal session handle             |

---

## Shell Commands

These commands can be executed via `session.execute()`:

| Command                      | Description                 | Example                           |
| ---------------------------- | --------------------------- | --------------------------------- |
| `CONSISTENCY [level]`        | Show/set consistency level  | `CONSISTENCY QUORUM`              |
| `SERIAL CONSISTENCY [level]` | Show/set serial consistency | `SERIAL CONSISTENCY LOCAL_SERIAL` |
| `PAGING [ON\|OFF\|number]`   | Show/set paging             | `PAGING 100`                      |
| `TRACING [ON\|OFF]`          | Show/set tracing            | `TRACING ON`                      |
| `EXPAND [ON\|OFF]`           | Show/set expand mode        | `EXPAND ON`                       |
| `ELAPSED [ON\|OFF]`          | Show/set elapsed time       | `ELAPSED ON`                      |
| `USE <keyspace>`             | Switch keyspace             | `USE my_keyspace`                 |
| `CLEAR` or `CLS`             | Clear screen                | `CLEAR`                           |
| `EXIT` or `QUIT`             | Exit                        | `EXIT`                            |

---

## Error Handling

All methods return an object with `success: boolean`. On failure:

```javascript
{
  success: false,
  error: 'Error message',
  code: 'ERROR_CODE'  // Optional error code
}
```

**Common error codes:**

| Code                   | Description                   |
| ---------------------- | ----------------------------- |
| `PARSE_ERROR`          | CQL syntax error              |
| `INCOMPLETE_STATEMENT` | Unclosed string/comment/batch |
| `CONNECTION_FAILED`    | Failed to connect             |
| `QUERY_ERROR`          | Query execution error         |
| `INVALID_HANDLE`       | Invalid session handle        |
| `CANCELLED`            | Operation was cancelled       |

---

## Consistency Levels

| Level          | Description                                      |
| -------------- | ------------------------------------------------ |
| `ANY`          | Write succeeds after any node acknowledges       |
| `ONE`          | Read/write succeeds after one replica responds   |
| `TWO`          | Read/write succeeds after two replicas respond   |
| `THREE`        | Read/write succeeds after three replicas respond |
| `QUORUM`       | Majority of replicas must respond                |
| `ALL`          | All replicas must respond                        |
| `LOCAL_QUORUM` | Majority in local datacenter                     |
| `EACH_QUORUM`  | Majority in each datacenter                      |
| `LOCAL_ONE`    | One replica in local datacenter                  |

---

## Serial Consistency Levels

Used for lightweight transactions (LWT):

| Level          | Description                          |
| -------------- | ------------------------------------ |
| `SERIAL`       | Serializable across all datacenters  |
| `LOCAL_SERIAL` | Serializable within local datacenter |
