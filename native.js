const koffi = require('koffi');
const path = require('path');
const fs = require('fs');

// Determine library filename based on platform
function getLibraryName() {
  switch (process.platform) {
    case 'win32':
      return 'cqlai.dll';
    case 'darwin':
      return 'libcqlai.dylib';
    default:
      return 'libcqlai.so';
  }
}

// Determine library path, handling Electron ASAR unpacking
function getLibraryPath() {
  const libName = getLibraryName();
  let libPath = path.join(__dirname, 'lib', libName);

  // Handle Electron ASAR: unpacked files are at .asar.unpacked instead of .asar
  if (libPath.includes('.asar') && !libPath.includes('.asar.unpacked')) {
    const unpackedPath = libPath.replace('.asar', '.asar.unpacked');
    if (fs.existsSync(unpackedPath)) {
      return unpackedPath;
    }
  }

  return libPath;
}

const libPath = getLibraryPath();

// Load the native library
const lib = koffi.load(libPath);

// Define function signatures
const native = {
  // Session management
  CreateSession: lib.func('char* CreateSession(const char* optionsJSON)'),
  CloseSession: lib.func('char* CloseSession(int handle)'),

  // Connection test
  TestConnection: lib.func('char* TestConnection(const char* optionsJSON)'),
  TestConnectionWithID: lib.func('char* TestConnectionWithID(const char* optionsJSON)'),
  CancelTestConnection: lib.func('char* CancelTestConnection(const char* requestID)'),

  // Query execution
  ExecuteQuery: lib.func('char* ExecuteQuery(int handle, const char* query)'),
  ExecuteMultiQuery: lib.func('char* ExecuteMultiQuery(int handle, const char* query, const char* optionsJSON)'),

  // CQL parsing
  SplitCQL: lib.func('char* SplitCQL(const char* cql)'),

  // Paged query execution (iterator-based pagination)
  ExecuteQueryPaged: lib.func('char* ExecuteQueryPaged(int handle, const char* query)'),
  FetchNextPage: lib.func('char* FetchNextPage(int handle, const char* queryID)'),
  CancelPagedQuery: lib.func('char* CancelPagedQuery(int handle, const char* queryID)'),
  CancelQuery: lib.func('char* CancelQuery(int handle)'),

  // Session configuration
  SetConsistency: lib.func('char* SetConsistency(int handle, const char* level)'),
  SetKeyspace: lib.func('char* SetKeyspace(int handle, const char* keyspace)'),
  SetPaging: lib.func('char* SetPaging(int handle, const char* value)'),
  SetTracing: lib.func('char* SetTracing(int handle, int enabled)'),
  SetExpand: lib.func('char* SetExpand(int handle, int enabled)'),
  GetSessionInfo: lib.func('char* GetSessionInfo(int handle)'),

  // Metadata
  GetClusterMetadata: lib.func('char* GetClusterMetadata(int handle)'),

  // DDL Generation
  GetDDL: lib.func('char* GetDDL(int handle, const char* scope)'),

  // TLS Security
  CheckTLS: lib.func('char* CheckTLS(const char* optionsJSON)'),

  // RSA Decryption (for standalone use - normally handled automatically in connect)
  DecryptCredential: lib.func('char* DecryptCredential(const char* optionsJSON)'),

  // Astra Secure Bundle
  ParseAstraSecureBundle: lib.func('char* ParseAstraSecureBundle(const char* optionsJSON)'),
  ValidateAstraSecureBundle: lib.func('char* ValidateAstraSecureBundle(const char* bundlePath)'),
  CreateAstraSession: lib.func('char* CreateAstraSession(const char* optionsJSON)'),
  TestAstraConnectionWithID: lib.func('char* TestAstraConnectionWithID(const char* optionsJSON)'),
  CleanupAstraExtracted: lib.func('char* CleanupAstraExtracted(const char* extractedDir)'),

  // Source file execution (CQL files)
  ExecuteSourceFiles: lib.func('char* ExecuteSourceFiles(int handle, const char* optionsJSON)'),
  GetSourceProgress: lib.func('char* GetSourceProgress(int handle)'),
  StopSourceExecution: lib.func('char* StopSourceExecution(int handle)'),

  // Query tracing
  GetQueryTrace: lib.func('char* GetQueryTrace(int handle, const char* sessionID)'),

  // Memory management
  FreeString: lib.func('void FreeString(char* str)'),
};

/**
 * Call a native function synchronously and parse the JSON response
 */
function callNative(fn) {
  const resultStr = fn();
  const result = JSON.parse(resultStr);
  return result;
}

/**
 * Call a native function asynchronously using koffi's worker thread
 * This is truly non-blocking - runs in a separate thread
 */
function callNativeAsync(fn) {
  return new Promise((resolve, reject) => {
    // Get the function reference and arguments from the closure
    // We need to call the function's .async method with a callback
    try {
      const resultStr = fn();
      const result = JSON.parse(resultStr);
      resolve(result);
    } catch (err) {
      reject(err);
    }
  });
}

/**
 * Call a native function truly asynchronously using koffi's async mode
 * This runs in a worker thread and doesn't block the event loop
 * @param {Function} nativeFunc - The native function to call
 * @param {...any} args - Arguments to pass to the function
 * @returns {Promise<Object>} Parsed JSON result
 */
function callNativeTrueAsync(nativeFunc, ...args) {
  return new Promise((resolve, reject) => {
    // koffi's .async() method runs the call in a worker thread
    nativeFunc.async(...args, (err, resultStr) => {
      if (err) {
        reject(err);
        return;
      }
      try {
        const result = JSON.parse(resultStr);
        resolve(result);
      } catch (parseErr) {
        reject(parseErr);
      }
    });
  });
}

module.exports = {
  native,
  callNative,
  callNativeAsync,
  callNativeTrueAsync,
};
