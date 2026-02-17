package main

/*
#include <stdlib.h>
*/
import "C"
import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"github.com/axonops/cqlai-node/internal/batch"
	"github.com/axonops/cqlai-node/internal/config"
	"github.com/axonops/cqlai-node/internal/db"
)

// getTraceIDIfEnabled returns the trace session ID only if tracing is currently enabled
// This prevents returning stale trace IDs from previous traced queries
func getTraceIDIfEnabled(session *db.Session) string {
	if session.Tracing() {
		return session.LastTraceID()
	}
	return ""
}

// parseTableReference extracts keyspace and table from a CQL query
// Supports: SELECT/INSERT/UPDATE/DELETE FROM [keyspace.]table
// Handles quoted identifiers: "Keyspace"."Table"
func parseTableReference(query string, currentKeyspace string) (keyspace, table string) {
	// Normalize whitespace and convert to uppercase for keyword matching
	normalized := strings.Join(strings.Fields(query), " ")
	upper := strings.ToUpper(normalized)

	// Find FROM clause position (for SELECT queries)
	fromIdx := strings.Index(upper, " FROM ")
	if fromIdx == -1 {
		// Try INTO for INSERT
		fromIdx = strings.Index(upper, " INTO ")
	}
	if fromIdx == -1 {
		// Try UPDATE table
		if strings.HasPrefix(upper, "UPDATE ") {
			fromIdx = 6 // Position after "UPDATE"
		}
	}
	if fromIdx == -1 {
		// Try DELETE FROM
		fromIdx = strings.Index(upper, "DELETE FROM ")
		if fromIdx != -1 {
			fromIdx += 6 // Position after "DELETE"
		}
	}

	if fromIdx == -1 {
		return "", ""
	}

	// Extract the part after FROM/INTO/UPDATE
	remainder := strings.TrimSpace(normalized[fromIdx+6:])

	// Parse table reference (handles keyspace.table and quoted identifiers)
	// Pattern: optional_keyspace.table or "keyspace"."table"
	tableRefPattern := regexp.MustCompile(`^("?[a-zA-Z_][a-zA-Z0-9_]*"?\.)?("?[a-zA-Z_][a-zA-Z0-9_]*"?)`)
	match := tableRefPattern.FindString(remainder)

	if match == "" {
		return "", ""
	}

	// Remove quotes and split by dot
	match = strings.ReplaceAll(match, `"`, "")
	parts := strings.Split(match, ".")

	if len(parts) == 2 {
		// keyspace.table format
		keyspace = parts[0]
		table = parts[1]
	} else if len(parts) == 1 {
		// Just table name, use current keyspace
		keyspace = currentKeyspace
		table = parts[0]
	}

	return keyspace, table
}

// Session handle management
var (
	sessions      = make(map[int]*db.Session)
	astraSessions = make(map[int]bool) // Track which sessions are Astra connections
	sessionMutex  sync.RWMutex
	nextHandle    = 1
)

// Pending connection cancellation support
var (
	pendingConnections      = make(map[string]chan struct{})
	pendingConnectionsMutex sync.Mutex
)

// Paged query iterator storage
type pagedQueryState struct {
	Session     *db.Session
	Iterator    interface{ MapScan(map[string]interface{}) bool; Close() error }
	ColumnNames []string
	ColumnTypes []string
	PageSize    int
	PeekedRow   map[string]interface{} // Row peeked ahead to check hasMore
}

var (
	pagedQueries      = make(map[string]*pagedQueryState)
	pagedQueriesMutex sync.Mutex
	nextQueryID       = 1
)

// generateQueryID creates a unique query ID with session handle prefix for isolation
func generateQueryID(handle int) string {
	pagedQueriesMutex.Lock()
	defer pagedQueriesMutex.Unlock()
	id := nextQueryID
	nextQueryID++
	return strconv.Itoa(handle) + ":" + strconv.Itoa(id)
}

// Response represents a JSON response
type Response struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
	Code    string      `json:"code,omitempty"`
}

// SessionOptions represents connection options from JSON
type SessionOptions struct {
	// Direct connection options
	Host           string `json:"host"`
	Port           int    `json:"port"`
	Keyspace       string `json:"keyspace"`
	Username       string `json:"username"`
	Password       string `json:"password"`
	Consistency    string `json:"consistency"`
	ConnectTimeout int    `json:"connectTimeout"`
	RequestTimeout int    `json:"requestTimeout"`

	// cqlshrc-based connection
	Cqlshrc string `json:"cqlshrc"` // Path to cqlshrc file

	// Variable substitution
	VarsManifest string `json:"varsManifest"` // Path to variables manifest JSON
	VarsValues   string `json:"varsValues"`   // Path to variables values JSON
	WorkspaceID  string `json:"workspaceID"`  // Workspace ID for scope filtering

	// Override host/port for display purposes (e.g., when connecting through SSH tunnel)
	// These don't affect the actual connection, only what's shown to the user
	OverrideHost string `json:"overrideHost"` // Display host (original host when tunneling)
	OverridePort int    `json:"overridePort"` // Display port (original port when tunneling)

	// SSL/TLS options (can be from cqlshrc or direct)
	SSLCertfile string `json:"sslCertfile"`
	SSLKeyfile  string `json:"sslKeyfile"`
	SSLCAFile   string `json:"sslCaFile"`
	SSLValidate *bool  `json:"sslValidate"` // Pointer to distinguish unset from false

	// RSA credential decryption
	RSAPrivateKey     string `json:"rsaPrivateKey"`     // PEM-encoded private key
	RSAPrivateKeyFile string `json:"rsaPrivateKeyFile"` // Path to private key file
}

// QueryResult represents query results for JSON serialization
type QueryResult struct {
	Columns        []string                 `json:"columns"`
	ColumnTypes    []string                 `json:"columnTypes"`
	Rows           []map[string]interface{} `json:"rows"`
	RowCount       int                      `json:"rowCount"`
	Duration       string                   `json:"duration"`
	TraceSessionID string                   `json:"traceSessionId,omitempty"` // Present when tracing is enabled
	Keyspace       string                   `json:"keyspace,omitempty"`       // Source keyspace for the query
	Table          string                   `json:"table,omitempty"`          // Source table for the query
}

// StatementResult represents the result of executing a single statement in multi-query
type StatementResult struct {
	Index          int                      `json:"index"`                     // 0-based statement index
	Statement      string                   `json:"statement"`                 // The CQL statement text (truncated)
	Identifier     string                   `json:"identifier"`                // Statement type (SELECT, INSERT, etc.)
	Success        bool                     `json:"success"`
	Error          string                   `json:"error,omitempty"`
	ErrorCode      string                   `json:"errorCode,omitempty"`
	Columns        []string                 `json:"columns,omitempty"`
	ColumnTypes    []string                 `json:"columnTypes,omitempty"`
	Rows           []map[string]interface{} `json:"rows,omitempty"`
	RowCount       int                      `json:"rowCount,omitempty"`
	Duration       string                   `json:"duration,omitempty"`
	Message        string                   `json:"message,omitempty"`         // For non-SELECT statements
	TraceSessionID string                   `json:"traceSessionId,omitempty"`
	Keyspace       string                   `json:"keyspace,omitempty"`
	Table          string                   `json:"table,omitempty"`
}

// MultiQueryOptions contains options for multi-statement execution
type MultiQueryOptions struct {
	StopOnError bool `json:"stopOnError"` // Stop execution on first error
}

// MultiQueryResult represents the result of executing multiple statements
type MultiQueryResult struct {
	StatementsCount    int               `json:"statementsCount"`
	StatementsExecuted int               `json:"statementsExecuted"`
	Identifiers        []string          `json:"identifiers"`        // Statement types (SELECT, INSERT, etc.)
	ExtraTokens        []string          `json:"extraTokens"`        // 2nd/3rd tokens from first statement
	SecondTokens       []string          `json:"secondTokens"`       // 2nd meaningful token of each statement
	ThirdTokens        []string          `json:"thirdTokens"`        // 3rd meaningful token of each statement
	Results            []StatementResult `json:"results"`
	Incomplete         bool              `json:"incomplete"`         // True if input was incomplete
	ParseError         string            `json:"parseError,omitempty"`
	Stopped            bool              `json:"stopped"`            // True if stopped due to error
}

// resolveSessionOptions merges cqlshrc config with direct options
// Direct options override cqlshrc values
func resolveSessionOptions(opts *SessionOptions) error {
	// If cqlshrc is provided and valid, parse it and merge
	if opts.Cqlshrc != "" && opts.Cqlshrc != "undefined" {
		config, err := ParseCqlshrcWithVariables(
			opts.Cqlshrc,
			opts.VarsManifest,
			opts.VarsValues,
			opts.WorkspaceID,
		)
		if err != nil {
			return err
		}

		// Apply cqlshrc values only if not already set by direct options
		if opts.Host == "" && config.Connection.Hostname != "" {
			opts.Host = config.Connection.Hostname
		}
		if opts.Port == 0 && config.Connection.Port != 0 {
			opts.Port = config.Connection.Port
		}
		if opts.ConnectTimeout == 0 && config.Connection.Timeout != 0 {
			opts.ConnectTimeout = config.Connection.Timeout
		}
		if opts.Username == "" && config.Authentication.Username != "" {
			opts.Username = config.Authentication.Username
		}
		if opts.Password == "" && config.Authentication.Password != "" {
			opts.Password = config.Authentication.Password
		}
		if opts.SSLCertfile == "" && config.SSL.Certfile != "" {
			opts.SSLCertfile = config.SSL.Certfile
		}
		if opts.SSLKeyfile == "" && config.SSL.Keyfile != "" {
			opts.SSLKeyfile = config.SSL.Keyfile
		}
		if opts.SSLCAFile == "" && config.SSL.CAFile != "" {
			opts.SSLCAFile = config.SSL.CAFile
		}
		if opts.SSLValidate == nil {
			opts.SSLValidate = &config.SSL.Validate
		}
	}

	// Set defaults
	if opts.Host == "" {
		opts.Host = "127.0.0.1"
	}
	if opts.Port == 0 {
		opts.Port = 9042
	}

	// Attempt to decrypt credentials if RSA private key is provided
	if opts.RSAPrivateKey != "" || opts.RSAPrivateKeyFile != "" {
		opts.Username = tryDecryptCredential(opts.Username, opts.RSAPrivateKey, opts.RSAPrivateKeyFile)
		opts.Password = tryDecryptCredential(opts.Password, opts.RSAPrivateKey, opts.RSAPrivateKeyFile)
	}

	return nil
}

// tryDecryptCredential attempts to decrypt a value using RSA private key
// If decryption fails (e.g., value is plaintext), returns the original value
func tryDecryptCredential(value, privateKeyPEM, privateKeyFile string) string {
	if value == "" {
		return value
	}

	var decrypted string
	var err error

	if privateKeyFile != "" {
		decrypted, err = DecryptWithPrivateKeyFile(value, privateKeyFile)
	} else if privateKeyPEM != "" {
		decrypted, err = DecryptWithPrivateKey(value, privateKeyPEM)
	} else {
		return value
	}

	// If decryption succeeded, use decrypted value
	// If failed (plaintext or invalid), use original value
	if err == nil {
		return decrypted
	}

	return value
}

// Helper to create JSON response
func jsonResponse(success bool, data interface{}, errMsg string, code string) *C.char {
	resp := Response{
		Success: success,
		Data:    data,
		Error:   errMsg,
		Code:    code,
	}
	jsonBytes, err := json.Marshal(resp)
	if err != nil {
		return C.CString(`{"success":false,"error":"JSON marshal error","code":"INTERNAL_ERROR"}`)
	}
	return C.CString(string(jsonBytes))
}

// registerSession stores a session and returns its handle
func registerSession(s *db.Session) int {
	sessionMutex.Lock()
	defer sessionMutex.Unlock()
	handle := nextHandle
	sessions[handle] = s
	nextHandle++
	return handle
}

// getSession retrieves a session by handle
func getSession(handle int) *db.Session {
	sessionMutex.RLock()
	defer sessionMutex.RUnlock()
	return sessions[handle]
}

// removeSession removes a session by handle
func removeSession(handle int) {
	sessionMutex.Lock()
	defer sessionMutex.Unlock()
	delete(sessions, handle)
	delete(astraSessions, handle)
}

// markSessionAsAstra marks a session as an Astra connection
func markSessionAsAstra(handle int) {
	sessionMutex.Lock()
	defer sessionMutex.Unlock()
	astraSessions[handle] = true
}

// isAstraSession checks if a session is an Astra connection
func isAstraSession(handle int) bool {
	sessionMutex.RLock()
	defer sessionMutex.RUnlock()
	return astraSessions[handle]
}

//export CreateSession
func CreateSession(optionsJSON *C.char) *C.char {
	// Parse options JSON
	optStr := C.GoString(optionsJSON)
	var opts SessionOptions
	if err := json.Unmarshal([]byte(optStr), &opts); err != nil {
		return jsonResponse(false, nil, "Invalid options JSON: "+err.Error(), "INVALID_OPTIONS")
	}

	// Resolve options (cqlshrc + variables + defaults)
	if err := resolveSessionOptions(&opts); err != nil {
		return jsonResponse(false, nil, "Failed to parse config: "+err.Error(), "CONFIG_ERROR")
	}

	// Create session options
	dbOpts := db.SessionOptions{
		Host:           opts.Host,
		Port:           opts.Port,
		Keyspace:       opts.Keyspace,
		Username:       opts.Username,
		Password:       opts.Password,
		Consistency:    opts.Consistency,
		ConnectTimeout: opts.ConnectTimeout,
		RequestTimeout: opts.RequestTimeout,
		BatchMode:      false, // Enable schema cache for better performance
	}

	// Apply SSL options if provided
	if opts.SSLCertfile != "" || opts.SSLCAFile != "" {
		sslValidate := true
		if opts.SSLValidate != nil {
			sslValidate = *opts.SSLValidate
		}
		dbOpts.SSL = &config.SSLConfig{
			Enabled:            true,
			CertPath:           opts.SSLCertfile,
			KeyPath:            opts.SSLKeyfile,
			CAPath:             opts.SSLCAFile,
			HostVerification:   sslValidate,
			InsecureSkipVerify: !sslValidate,
		}
	}

	// Create session
	session, err := db.NewSessionWithOptions(dbOpts)
	if err != nil {
		return jsonResponse(false, nil, "Connection failed: "+err.Error(), "CONNECTION_FAILED")
	}

	// Register and return handle
	handle := registerSession(session)

	// Build response with connection info
	responseData := map[string]interface{}{
		"handle":           handle,
		"cassandraVersion": session.CassandraVersion(),
		"keyspace":         session.Keyspace(),
		"host":             opts.Host,
		"port":             opts.Port,
	}

	// Include override values if provided (for display when using SSH tunnel)
	if opts.OverrideHost != "" {
		responseData["overrideHost"] = opts.OverrideHost
	}
	if opts.OverridePort != 0 {
		responseData["overridePort"] = opts.OverridePort
	}

	return jsonResponse(true, responseData, "", "")
}

//export CloseSession
func CloseSession(handle C.int) *C.char {
	h := int(handle)
	session := getSession(h)
	if session == nil {
		return jsonResponse(false, nil, "Invalid session handle", "INVALID_HANDLE")
	}

	session.Close()
	removeSession(h)
	return jsonResponse(true, nil, "", "")
}

//export ExecuteQuery
func ExecuteQuery(handle C.int, query *C.char) *C.char {
	h := int(handle)
	session := getSession(h)
	if session == nil {
		return jsonResponse(false, nil, "Invalid session handle", "INVALID_HANDLE")
	}

	cql := C.GoString(query)

	// WORKAROUND: Astra hangs indefinitely when tracing is enabled for queries.
	// Only apply this workaround for Astra connections (detected via Secure Connect Bundle).
	tracingWasEnabled := false
	if isAstraSession(h) && session.Tracing() {
		tracingWasEnabled = true
		session.SetTracing(false)
	}

	result := session.ExecuteCQLQuery(cql)

	// Re-enable tracing if it was disabled for Astra
	if tracingWasEnabled {
		session.SetTracing(true)
	}

	// Handle nil result - this can happen with authorization failures on managed services like Astra
	if result == nil {
		return jsonResponse(false, nil, "Query returned no result - this may indicate a permission issue or connection problem", "NO_RESULT")
	}

	// Parse keyspace and table from the query for TABLEMETA:INFO support
	keyspace, table := parseTableReference(cql, session.Keyspace())

	// Handle different result types
	switch v := result.(type) {
	case db.QueryResult:
		// Convert to our QueryResult format
		rows := make([]map[string]interface{}, 0, len(v.RawData))
		for _, rawRow := range v.RawData {
			rows = append(rows, rawRow)
		}

		qr := QueryResult{
			Columns:        v.Headers,
			ColumnTypes:    v.ColumnTypes,
			Rows:           rows,
			RowCount:       v.RowCount,
			Duration:       v.Duration.String(),
			TraceSessionID: getTraceIDIfEnabled(session), // Include trace ID if tracing is enabled
			Keyspace:       keyspace,
			Table:          table,
		}
		return jsonResponse(true, qr, "", "")

	case db.StreamingQueryResult:
		// For streaming results, we need to fetch all rows
		defer v.Iterator.Close()

		rows := make([]map[string]interface{}, 0)
		for {
			row := make(map[string]interface{})
			if !v.Iterator.MapScan(row) {
				break
			}
			rows = append(rows, row)
		}

		// Check for iterator errors after scanning (important for Astra authorization errors)
		if err := v.Iterator.Close(); err != nil {
			errStr := err.Error()
			// Check for authorization/permission errors common on managed services
			if strings.Contains(strings.ToLower(errStr), "unauthorized") ||
				strings.Contains(strings.ToLower(errStr), "permission") ||
				strings.Contains(strings.ToLower(errStr), "access denied") {
				return jsonResponse(false, nil, "Permission denied: "+errStr, "PERMISSION_DENIED")
			}
			return jsonResponse(false, nil, "Query failed: "+errStr, "QUERY_ERROR")
		}

		qr := QueryResult{
			Columns:        v.ColumnNames,
			ColumnTypes:    v.ColumnTypes,
			Rows:           rows,
			RowCount:       len(rows),
			Duration:       "", // Duration not available for streaming
			TraceSessionID: getTraceIDIfEnabled(session), // Include trace ID if tracing is enabled
			Keyspace:       keyspace,
			Table:          table,
		}
		return jsonResponse(true, qr, "", "")

	case string:
		// Simple string result (e.g., "Query executed successfully", "No results")
		return jsonResponse(true, map[string]interface{}{
			"message": v,
		}, "", "")

	case error:
		errStr := v.Error()
		// Check for authorization/permission errors common on managed services like Astra
		if strings.Contains(strings.ToLower(errStr), "unauthorized") ||
			strings.Contains(strings.ToLower(errStr), "permission") ||
			strings.Contains(strings.ToLower(errStr), "access denied") {
			return jsonResponse(false, nil, "Permission denied: "+errStr, "PERMISSION_DENIED")
		}
		return jsonResponse(false, nil, errStr, "QUERY_ERROR")

	default:
		// Unknown type, try to return as-is
		if result == nil {
			return jsonResponse(false, nil, "Query returned no result", "NO_RESULT")
		}
		return jsonResponse(true, map[string]interface{}{
			"result": v,
		}, "", "")
	}
}

//export ExecuteMultiQuery
func ExecuteMultiQuery(handle C.int, query *C.char, optionsJSON *C.char) *C.char {
	h := int(handle)
	session := getSession(h)
	if session == nil {
		return jsonResponse(false, nil, "Invalid session handle", "INVALID_HANDLE")
	}

	cql := C.GoString(query)

	// Parse options
	var opts MultiQueryOptions
	if optionsJSON != nil {
		optStr := C.GoString(optionsJSON)
		if optStr != "" {
			json.Unmarshal([]byte(optStr), &opts)
		}
	}

	result := executeMultiQuery(session, cql, opts)
	return jsonResponse(true, result, "", "")
}

// executeMultiQuery executes multiple CQL statements and returns combined results
func executeMultiQuery(session *db.Session, cql string, opts MultiQueryOptions) *MultiQueryResult {
	result := &MultiQueryResult{
		Results:      []StatementResult{},
		Identifiers:  []string{},
		ExtraTokens:  []string{},
		SecondTokens: []string{},
		ThirdTokens:  []string{},
	}

	// Handle empty input
	cql = strings.TrimSpace(cql)
	if cql == "" {
		return result
	}

	// Split statements using the CQL splitter from internal/batch
	splitResult, err := batch.SplitStatements(cql)
	if err != nil {
		result.ParseError = err.Error()
		return result
	}

	if splitResult.Incomplete {
		result.Incomplete = true
		result.ParseError = "Incomplete statement"
		return result
	}

	result.StatementsCount = len(splitResult.Statements)
	result.Identifiers = splitResult.Identifiers
	result.ExtraTokens = splitResult.ExtraTokens
	result.SecondTokens = splitResult.SecondTokens
	result.ThirdTokens = splitResult.ThirdTokens

	// Get statement strings
	stmtStrings := splitResult.GetStatementStrings()

	// Execute each statement
	for i, stmtText := range stmtStrings {
		stmtText = strings.TrimSpace(stmtText)
		if stmtText == "" {
			continue
		}

		// Get identifier for this statement
		identifier := ""
		if i < len(result.Identifiers) {
			identifier = result.Identifiers[i]
		}

		stmtResult := executeStatement(session, stmtText, i, identifier)
		result.Results = append(result.Results, stmtResult)
		result.StatementsExecuted++

		// Stop on error if requested
		if !stmtResult.Success && opts.StopOnError {
			result.Stopped = true
			break
		}
	}

	return result
}

// executeStatement executes a single CQL statement and returns the result
func executeStatement(session *db.Session, stmt string, index int, identifier string) StatementResult {
	sr := StatementResult{
		Index:      index,
		Statement:  truncateStmt(stmt, 500),
		Identifier: identifier,
		Success:    true,
	}

	// Parse keyspace and table for TABLEMETA:INFO support
	keyspace, table := parseTableReference(stmt, session.Keyspace())
	sr.Keyspace = keyspace
	sr.Table = table

	// Execute the query
	queryResult := session.ExecuteCQLQuery(stmt)

	switch v := queryResult.(type) {
	case db.QueryResult:
		sr.Columns = v.Headers
		sr.ColumnTypes = v.ColumnTypes
		sr.Rows = v.RawData
		sr.RowCount = v.RowCount
		sr.Duration = v.Duration.String()
		sr.TraceSessionID = getTraceIDIfEnabled(session)

	case db.StreamingQueryResult:
		// For streaming results, fetch all rows (no pagination in multi-query)
		defer v.Iterator.Close()

		rows := make([]map[string]interface{}, 0)
		for {
			row := make(map[string]interface{})
			if !v.Iterator.MapScan(row) {
				break
			}
			rows = append(rows, row)
		}

		sr.Columns = v.ColumnNames
		sr.ColumnTypes = v.ColumnTypes
		sr.Rows = rows
		sr.RowCount = len(rows)
		sr.TraceSessionID = getTraceIDIfEnabled(session)

	case string:
		sr.Message = v

	case error:
		sr.Success = false
		sr.Error = v.Error()
		sr.ErrorCode = "QUERY_ERROR"

	default:
		sr.Message = ""
	}

	return sr
}

// truncateStmt truncates a statement to maxLen characters for display
func truncateStmt(stmt string, maxLen int) string {
	if len(stmt) <= maxLen {
		return stmt
	}
	return stmt[:maxLen] + "..."
}

//export SetConsistency
func SetConsistency(handle C.int, level *C.char) *C.char {
	h := int(handle)
	session := getSession(h)
	if session == nil {
		return jsonResponse(false, nil, "Invalid session handle", "INVALID_HANDLE")
	}

	levelStr := C.GoString(level)
	if err := session.SetConsistency(levelStr); err != nil {
		return jsonResponse(false, nil, err.Error(), "INVALID_CONSISTENCY")
	}

	return jsonResponse(true, map[string]interface{}{
		"consistency": levelStr,
	}, "", "")
}

//export SetKeyspace
func SetKeyspace(handle C.int, keyspace *C.char) *C.char {
	h := int(handle)
	session := getSession(h)
	if session == nil {
		return jsonResponse(false, nil, "Invalid session handle", "INVALID_HANDLE")
	}

	ks := C.GoString(keyspace)
	if err := session.SetKeyspace(ks); err != nil {
		return jsonResponse(false, nil, err.Error(), "KEYSPACE_ERROR")
	}

	return jsonResponse(true, map[string]interface{}{
		"keyspace": ks,
	}, "", "")
}

//export SetPaging
func SetPaging(handle C.int, value *C.char) *C.char {
	h := int(handle)
	session := getSession(h)
	if session == nil {
		return jsonResponse(false, nil, "Invalid session handle", "INVALID_HANDLE")
	}

	valueStr := C.GoString(value)
	upperVal := strings.ToUpper(strings.TrimSpace(valueStr))

	if upperVal == "OFF" {
		// Page size 0 disables paging (use server default)
		session.SetPageSize(0)
		return jsonResponse(true, map[string]interface{}{
			"paging":   "OFF",
			"pageSize": 0,
		}, "", "")
	}

	// Try to parse as a number
	size, err := strconv.Atoi(valueStr)
	if err != nil {
		return jsonResponse(false, nil, "Invalid paging value: use a number or 'OFF'", "INVALID_VALUE")
	}
	if size < 0 {
		return jsonResponse(false, nil, "Page size must be non-negative", "INVALID_VALUE")
	}

	session.SetPageSize(size)
	return jsonResponse(true, map[string]interface{}{
		"paging":   "ON",
		"pageSize": size,
	}, "", "")
}

//export SetTracing
func SetTracing(handle C.int, enabled C.int) *C.char {
	h := int(handle)
	session := getSession(h)
	if session == nil {
		return jsonResponse(false, nil, "Invalid session handle", "INVALID_HANDLE")
	}

	isEnabled := enabled != 0
	session.SetTracing(isEnabled)

	return jsonResponse(true, map[string]interface{}{
		"tracing": isEnabled,
	}, "", "")
}

//export SetExpand
func SetExpand(handle C.int, enabled C.int) *C.char {
	h := int(handle)
	session := getSession(h)
	if session == nil {
		return jsonResponse(false, nil, "Invalid session handle", "INVALID_HANDLE")
	}

	isEnabled := enabled != 0
	session.SetExpand(isEnabled)

	return jsonResponse(true, map[string]interface{}{
		"expand": isEnabled,
	}, "", "")
}

//export GetSessionInfo
func GetSessionInfo(handle C.int) *C.char {
	h := int(handle)
	session := getSession(h)
	if session == nil {
		return jsonResponse(false, nil, "Invalid session handle", "INVALID_HANDLE")
	}

	info := map[string]interface{}{
		"cassandraVersion":  session.CassandraVersion(),
		"keyspace":          session.Keyspace(),
		"consistency":       session.Consistency(),
		"serialConsistency": "SERIAL", // Default serial consistency
		"pageSize":          session.PageSize(),
		"tracing":           session.Tracing(),
		"expand":            session.Expand(),
		"username":          session.Username(),
		"host":              session.Host(),
	}

	return jsonResponse(true, info, "", "")
}

// DatacenterInfo represents a node's datacenter info
type DatacenterInfo struct {
	Address    string `json:"address"`
	Datacenter string `json:"datacenter"`
}

// ClusterInfo represents cluster connection test results
type ClusterInfo struct {
	Build       string           `json:"build"`
	Protocol    int              `json:"protocol"`
	CQL         string           `json:"cql"`
	Datacenter  string           `json:"datacenter"`
	Datacenters []DatacenterInfo `json:"datacenters"`
}

//export TestConnection
func TestConnection(optionsJSON *C.char) *C.char {
	// Parse options JSON
	optStr := C.GoString(optionsJSON)
	var opts SessionOptions
	if err := json.Unmarshal([]byte(optStr), &opts); err != nil {
		return jsonResponse(false, nil, "Invalid options JSON: "+err.Error(), "INVALID_OPTIONS")
	}

	// Resolve options (cqlshrc + variables + defaults)
	if err := resolveSessionOptions(&opts); err != nil {
		return jsonResponse(false, nil, "Failed to parse config: "+err.Error(), "CONFIG_ERROR")
	}

	// Create session options - use batch mode to skip schema cache for faster connection
	dbOpts := db.SessionOptions{
		Host:           opts.Host,
		Port:           opts.Port,
		Keyspace:       opts.Keyspace,
		Username:       opts.Username,
		Password:       opts.Password,
		Consistency:    opts.Consistency,
		ConnectTimeout: opts.ConnectTimeout,
		RequestTimeout: opts.RequestTimeout,
		BatchMode:      true, // Skip schema cache for faster test
	}

	// Apply SSL options if provided
	if opts.SSLCertfile != "" || opts.SSLCAFile != "" {
		sslValidate := true
		if opts.SSLValidate != nil {
			sslValidate = *opts.SSLValidate
		}
		dbOpts.SSL = &config.SSLConfig{
			Enabled:            true,
			CertPath:           opts.SSLCertfile,
			KeyPath:            opts.SSLKeyfile,
			CAPath:             opts.SSLCAFile,
			HostVerification:   sslValidate,
			InsecureSkipVerify: !sslValidate,
		}
	}

	// Create session
	session, err := db.NewSessionWithOptions(dbOpts)
	if err != nil {
		return jsonResponse(false, nil, "Connection failed: "+err.Error(), "CONNECTION_FAILED")
	}
	defer session.Close()

	// Query local node info
	var releaseVersion, cqlVersion, datacenter string
	localQuery := session.Query("SELECT release_version, cql_version, data_center FROM system.local")
	if err := localQuery.Scan(&releaseVersion, &cqlVersion, &datacenter); err != nil {
		return jsonResponse(false, nil, "Failed to query system.local: "+err.Error(), "QUERY_ERROR")
	}

	// Determine display host (use override if provided, for SSH tunnel scenarios)
	displayHost := opts.Host
	if opts.OverrideHost != "" {
		displayHost = opts.OverrideHost
	}

	// Collect datacenters - start with local node
	datacenters := []DatacenterInfo{
		{
			Address:    displayHost,
			Datacenter: datacenter,
		},
	}

	// Query peers for other nodes
	peersIter := session.Query("SELECT peer, data_center FROM system.peers").Iter()
	var peerAddr, peerDC string
	for peersIter.Scan(&peerAddr, &peerDC) {
		datacenters = append(datacenters, DatacenterInfo{
			Address:    peerAddr,
			Datacenter: peerDC,
		})
	}
	peersIter.Close()

	// Build result
	info := ClusterInfo{
		Build:       releaseVersion,
		Protocol:    5, // We try protocol 5 first in NewSessionWithOptions
		CQL:         cqlVersion,
		Datacenter:  datacenter,
		Datacenters: datacenters,
	}

	return jsonResponse(true, info, "", "")
}

// TestConnectionOptions extends SessionOptions with request ID for cancellation
type TestConnectionOptions struct {
	SessionOptions
	RequestID string `json:"requestID"` // Unique ID for cancellation
}

//export TestConnectionWithID
func TestConnectionWithID(optionsJSON *C.char) *C.char {
	// Parse options JSON
	optStr := C.GoString(optionsJSON)
	var opts TestConnectionOptions
	if err := json.Unmarshal([]byte(optStr), &opts); err != nil {
		return jsonResponse(false, nil, "Invalid options JSON: "+err.Error(), "INVALID_OPTIONS")
	}

	// Create cancellation channel if requestID provided
	var cancelChan chan struct{}
	if opts.RequestID != "" {
		cancelChan = make(chan struct{})
		pendingConnectionsMutex.Lock()
		pendingConnections[opts.RequestID] = cancelChan
		pendingConnectionsMutex.Unlock()

		// Cleanup when done
		defer func() {
			pendingConnectionsMutex.Lock()
			delete(pendingConnections, opts.RequestID)
			pendingConnectionsMutex.Unlock()
		}()
	}

	// Check if already cancelled before starting
	if cancelChan != nil {
		select {
		case <-cancelChan:
			return jsonResponse(false, nil, "Connection cancelled", "CANCELLED")
		default:
		}
	}

	// Resolve options (cqlshrc + variables + defaults)
	if err := resolveSessionOptions(&opts.SessionOptions); err != nil {
		return jsonResponse(false, nil, "Failed to parse config: "+err.Error(), "CONFIG_ERROR")
	}

	// Check if cancelled after config resolution
	if cancelChan != nil {
		select {
		case <-cancelChan:
			return jsonResponse(false, nil, "Connection cancelled", "CANCELLED")
		default:
		}
	}

	// Create session options - use batch mode to skip schema cache for faster connection
	dbOpts := db.SessionOptions{
		Host:           opts.Host,
		Port:           opts.Port,
		Keyspace:       opts.Keyspace,
		Username:       opts.Username,
		Password:       opts.Password,
		Consistency:    opts.Consistency,
		ConnectTimeout: opts.ConnectTimeout,
		RequestTimeout: opts.RequestTimeout,
		BatchMode:      true, // Skip schema cache for faster test
	}

	// Apply SSL options if provided
	if opts.SSLCertfile != "" || opts.SSLCAFile != "" {
		sslValidate := true
		if opts.SSLValidate != nil {
			sslValidate = *opts.SSLValidate
		}
		dbOpts.SSL = &config.SSLConfig{
			Enabled:            true,
			CertPath:           opts.SSLCertfile,
			KeyPath:            opts.SSLKeyfile,
			CAPath:             opts.SSLCAFile,
			HostVerification:   sslValidate,
			InsecureSkipVerify: !sslValidate,
		}
	}

	// Create session (this is the blocking part)
	// We run it in a goroutine to allow cancellation
	type sessionResult struct {
		session *db.Session
		err     error
	}
	resultChan := make(chan sessionResult, 1)

	go func() {
		session, err := db.NewSessionWithOptions(dbOpts)
		resultChan <- sessionResult{session, err}
	}()

	// Wait for either session creation or cancellation
	var session *db.Session
	if cancelChan != nil {
		select {
		case <-cancelChan:
			// Cancelled - we can't stop the goroutine but we won't use its result
			return jsonResponse(false, nil, "Connection cancelled", "CANCELLED")
		case res := <-resultChan:
			if res.err != nil {
				return jsonResponse(false, nil, "Connection failed: "+res.err.Error(), "CONNECTION_FAILED")
			}
			session = res.session
		}
	} else {
		res := <-resultChan
		if res.err != nil {
			return jsonResponse(false, nil, "Connection failed: "+res.err.Error(), "CONNECTION_FAILED")
		}
		session = res.session
	}
	defer session.Close()

	// Check cancelled before querying
	if cancelChan != nil {
		select {
		case <-cancelChan:
			return jsonResponse(false, nil, "Connection cancelled", "CANCELLED")
		default:
		}
	}

	// Query local node info
	var releaseVersion, cqlVersion, datacenter string
	localQuery := session.Query("SELECT release_version, cql_version, data_center FROM system.local")
	if err := localQuery.Scan(&releaseVersion, &cqlVersion, &datacenter); err != nil {
		return jsonResponse(false, nil, "Failed to query system.local: "+err.Error(), "QUERY_ERROR")
	}

	// Determine display host (use override if provided, for SSH tunnel scenarios)
	displayHost := opts.Host
	if opts.OverrideHost != "" {
		displayHost = opts.OverrideHost
	}

	// Collect datacenters - start with local node
	datacenters := []DatacenterInfo{
		{
			Address:    displayHost,
			Datacenter: datacenter,
		},
	}

	// Query peers for other nodes
	peersIter := session.Query("SELECT peer, data_center FROM system.peers").Iter()
	var peerAddr, peerDC string
	for peersIter.Scan(&peerAddr, &peerDC) {
		datacenters = append(datacenters, DatacenterInfo{
			Address:    peerAddr,
			Datacenter: peerDC,
		})
	}
	peersIter.Close()

	// Build result
	info := ClusterInfo{
		Build:       releaseVersion,
		Protocol:    5,
		CQL:         cqlVersion,
		Datacenter:  datacenter,
		Datacenters: datacenters,
	}

	return jsonResponse(true, info, "", "")
}

//export CancelTestConnection
func CancelTestConnection(requestID *C.char) *C.char {
	reqID := C.GoString(requestID)
	if reqID == "" {
		return jsonResponse(false, nil, "Request ID is required", "INVALID_OPTIONS")
	}

	pendingConnectionsMutex.Lock()
	cancelChan, exists := pendingConnections[reqID]
	if exists {
		close(cancelChan)
		delete(pendingConnections, reqID)
	}
	pendingConnectionsMutex.Unlock()

	if !exists {
		return jsonResponse(true, map[string]interface{}{
			"cancelled": false,
			"reason":    "No pending connection with this ID",
		}, "", "")
	}

	return jsonResponse(true, map[string]interface{}{
		"cancelled": true,
	}, "", "")
}

//export GetClusterMetadata
func GetClusterMetadata(handle C.int) *C.char {
	h := int(handle)
	session := getSession(h)
	if session == nil {
		return jsonResponse(false, nil, "Invalid session handle", "INVALID_HANDLE")
	}

	metadata, err := GetClusterMetadataFromSession(session)
	if err != nil {
		return jsonResponse(false, nil, "Failed to get cluster metadata: "+err.Error(), "METADATA_ERROR")
	}

	return jsonResponse(true, metadata, "", "")
}

// DDLOptions represents options for DDL generation
type DDLOptions struct {
	Cluster       bool   `json:"cluster"`       // If true, generate DDL for entire cluster
	Keyspace      string `json:"keyspace"`      // Keyspace name (required if not cluster)
	Table         string `json:"table"`         // Table name (optional)
	Index         string `json:"index"`         // Index name (optional, requires table)
	Type          string `json:"type"`          // User type name (optional)
	Function      string `json:"function"`      // Function name (optional)
	Aggregate     string `json:"aggregate"`     // Aggregate name (optional)
	View          string `json:"view"`          // Materialized view name (optional)
	IncludeSystem bool   `json:"includeSystem"` // If true, include system keyspaces in cluster DDL
}

//export GetDDL
func GetDDL(handle C.int, optionsJSON *C.char) *C.char {
	h := int(handle)
	session := getSession(h)
	if session == nil {
		return jsonResponse(false, nil, "Invalid session handle", "INVALID_HANDLE")
	}

	optStr := C.GoString(optionsJSON)
	var opts DDLOptions
	if err := json.Unmarshal([]byte(optStr), &opts); err != nil {
		return jsonResponse(false, nil, "Invalid options JSON: "+err.Error(), "INVALID_OPTIONS")
	}

	ddlResult, err := GenerateDDLWithOptions(session.GocqlSession(), opts)
	if err != nil {
		return jsonResponse(false, nil, "Failed to generate DDL: "+err.Error(), "DDL_ERROR")
	}

	return jsonResponse(true, ddlResult, "", "")
}

// TLSCheckOptions represents options for TLS security check
type TLSCheckOptions struct {
	Host       string `json:"host"`
	Port       int    `json:"port"`
	CAFile     string `json:"caFile"`
	CertFile   string `json:"certFile"`
	KeyFile    string `json:"keyFile"`
	SkipVerify bool   `json:"skipVerify"`
	FilesOnly  bool   `json:"filesOnly"` // Only check certificate files, don't connect
}

//export CheckTLS
func CheckTLS(optionsJSON *C.char) *C.char {
	optStr := C.GoString(optionsJSON)
	var opts TLSCheckOptions
	if err := json.Unmarshal([]byte(optStr), &opts); err != nil {
		return jsonResponse(false, nil, "Invalid options JSON: "+err.Error(), "INVALID_OPTIONS")
	}

	// Set defaults
	if opts.Port == 0 {
		opts.Port = 9042
	}

	var result *TLSSecurityInfo
	var err error

	if opts.FilesOnly {
		// Only analyze certificate files
		result, err = CheckTLSSecurityFromFiles(opts.CAFile, opts.CertFile, opts.KeyFile)
	} else {
		// Connect and analyze
		if opts.Host == "" {
			return jsonResponse(false, nil, "Host is required for TLS connection check", "INVALID_OPTIONS")
		}
		result, err = CheckTLSSecurity(opts.Host, opts.Port, opts.CAFile, opts.CertFile, opts.KeyFile, opts.SkipVerify)
	}

	if err != nil {
		return jsonResponse(false, nil, err.Error(), "TLS_CHECK_ERROR")
	}

	return jsonResponse(true, result, "", "")
}

// DecryptOptions represents options for decryption
type DecryptOptions struct {
	Ciphertext     string `json:"ciphertext"`     // Base64 encoded
	PrivateKey     string `json:"privateKey"`     // PEM string
	PrivateKeyFile string `json:"privateKeyFile"` // Path to PEM file
}

//export DecryptCredential
func DecryptCredential(optionsJSON *C.char) *C.char {
	optStr := C.GoString(optionsJSON)
	var opts DecryptOptions
	if err := json.Unmarshal([]byte(optStr), &opts); err != nil {
		return jsonResponse(false, nil, "Invalid options JSON: "+err.Error(), "INVALID_OPTIONS")
	}

	if opts.Ciphertext == "" {
		return jsonResponse(false, nil, "Ciphertext is required", "INVALID_OPTIONS")
	}

	var plaintext string
	var err error

	if opts.PrivateKeyFile != "" {
		plaintext, err = DecryptWithPrivateKeyFile(opts.Ciphertext, opts.PrivateKeyFile)
	} else if opts.PrivateKey != "" {
		plaintext, err = DecryptWithPrivateKey(opts.Ciphertext, opts.PrivateKey)
	} else {
		return jsonResponse(false, nil, "Either privateKey or privateKeyFile is required", "INVALID_OPTIONS")
	}

	if err != nil {
		return jsonResponse(false, nil, err.Error(), "DECRYPT_ERROR")
	}

	return jsonResponse(true, map[string]string{"plaintext": plaintext}, "", "")
}

// AstraBundleOptions represents options for parsing Astra bundle
type AstraBundleOptions struct {
	BundlePath string `json:"bundlePath"`
	ExtractDir string `json:"extractDir"` // Optional, uses temp dir if empty
}

//export ParseAstraSecureBundle
func ParseAstraSecureBundle(optionsJSON *C.char) *C.char {
	optStr := C.GoString(optionsJSON)
	var opts AstraBundleOptions
	if err := json.Unmarshal([]byte(optStr), &opts); err != nil {
		return jsonResponse(false, nil, "Invalid options JSON: "+err.Error(), "INVALID_OPTIONS")
	}

	if opts.BundlePath == "" {
		return jsonResponse(false, nil, "bundlePath is required", "INVALID_OPTIONS")
	}

	bundleInfo, err := ParseAstraBundle(opts.BundlePath, opts.ExtractDir)
	if err != nil {
		return jsonResponse(false, nil, err.Error(), "BUNDLE_ERROR")
	}

	return jsonResponse(true, bundleInfo, "", "")
}

//export ValidateAstraSecureBundle
func ValidateAstraSecureBundle(bundlePath *C.char) *C.char {
	path := C.GoString(bundlePath)

	valid, errors := ValidateAstraBundle(path)

	result := map[string]interface{}{
		"valid":  valid,
		"errors": errors,
	}

	return jsonResponse(true, result, "", "")
}

// AstraConnectOptions represents options for connecting with Astra bundle
type AstraConnectOptions struct {
	BundlePath string `json:"bundlePath"`
	ExtractDir string `json:"extractDir"`
	Username   string `json:"username"`
	Password   string `json:"password"`
	Keyspace   string `json:"keyspace"` // Override keyspace from bundle
}

//export CreateAstraSession
func CreateAstraSession(optionsJSON *C.char) *C.char {
	optStr := C.GoString(optionsJSON)
	var opts AstraConnectOptions
	if err := json.Unmarshal([]byte(optStr), &opts); err != nil {
		return jsonResponse(false, nil, "Invalid options JSON: "+err.Error(), "INVALID_OPTIONS")
	}

	if opts.BundlePath == "" {
		return jsonResponse(false, nil, "bundlePath is required", "INVALID_OPTIONS")
	}
	if opts.Username == "" || opts.Password == "" {
		return jsonResponse(false, nil, "username and password are required", "INVALID_OPTIONS")
	}

	// Parse the bundle
	bundleInfo, err := ParseAstraBundle(opts.BundlePath, opts.ExtractDir)
	if err != nil {
		return jsonResponse(false, nil, "Failed to parse bundle: "+err.Error(), "BUNDLE_ERROR")
	}

	// Fetch metadata from Astra metadata service to get actual connection endpoints
	if err := FetchAstraMetadata(bundleInfo, 0); err != nil {
		CleanupAstraBundle(bundleInfo.ExtractedDir)
		return jsonResponse(false, nil, "Failed to fetch Astra metadata: "+err.Error(), "METADATA_ERROR")
	}

	// Validate we got the required metadata
	if bundleInfo.SniHost == "" || bundleInfo.SniPort == 0 || len(bundleInfo.ContactPoints) == 0 {
		CleanupAstraBundle(bundleInfo.ExtractedDir)
		return jsonResponse(false, nil, "Invalid metadata: missing SNI proxy or contact points", "METADATA_ERROR")
	}

	// Build session options
	keyspace := opts.Keyspace
	if keyspace == "" {
		keyspace = bundleInfo.Keyspace
	}

	// Create session options for db
	// Use SNI proxy host:port as the connection endpoint
	// Use first contact point (host ID) as the TLS ServerName for SNI routing
	// Note: InsecureSkipVerify is needed because the cert is valid for *.db.astra.datastax.com
	// but SNI uses UUID host IDs for routing. The CA cert still validates the chain.
	dbOpts := db.SessionOptions{
		Host:     bundleInfo.SniHost,
		Port:     bundleInfo.SniPort,
		Keyspace: keyspace,
		Username: opts.Username,
		Password: opts.Password,
		SSL: &config.SSLConfig{
			Enabled:            true,
			CertPath:           bundleInfo.CertPath,
			KeyPath:            bundleInfo.KeyPath,
			CAPath:             bundleInfo.CACertPath,
			HostVerification:   false,                       // SNI proxy uses host IDs, not hostnames
			InsecureSkipVerify: true,                        // Skip hostname verification (UUID != *.db.astra.datastax.com)
			ServerName:         bundleInfo.ContactPoints[0], // Use host ID as SNI for routing
		},
	}

	// Create session
	session, err := db.NewSessionWithOptions(dbOpts)
	if err != nil {
		CleanupAstraBundle(bundleInfo.ExtractedDir)
		return jsonResponse(false, nil, "Connection failed: "+err.Error(), "CONNECTION_FAILED")
	}

	// Register session and mark as Astra connection
	handle := registerSession(session)
	markSessionAsAstra(handle)
	return jsonResponse(true, map[string]interface{}{
		"handle":           handle,
		"cassandraVersion": session.CassandraVersion(),
		"keyspace":         session.Keyspace(),
		"bundleInfo":       bundleInfo,
	}, "", "")
}

// TestAstraConnectionOptions extends AstraConnectOptions with request ID for cancellation
type TestAstraConnectionOptions struct {
	BundlePath string `json:"bundlePath"`
	ExtractDir string `json:"extractDir"`
	Username   string `json:"username"`
	Password   string `json:"password"`
	Keyspace   string `json:"keyspace"`
	RequestID  string `json:"requestID"` // Unique ID for cancellation
}

//export TestAstraConnectionWithID
func TestAstraConnectionWithID(optionsJSON *C.char) *C.char {
	optStr := C.GoString(optionsJSON)
	var opts TestAstraConnectionOptions
	if err := json.Unmarshal([]byte(optStr), &opts); err != nil {
		return jsonResponse(false, nil, "Invalid options JSON: "+err.Error(), "INVALID_OPTIONS")
	}

	if opts.BundlePath == "" {
		return jsonResponse(false, nil, "bundlePath is required", "INVALID_OPTIONS")
	}
	if opts.Username == "" || opts.Password == "" {
		return jsonResponse(false, nil, "username and password are required", "INVALID_OPTIONS")
	}

	// Create cancellation channel if requestID provided
	var cancelChan chan struct{}
	if opts.RequestID != "" {
		cancelChan = make(chan struct{})
		pendingConnectionsMutex.Lock()
		pendingConnections[opts.RequestID] = cancelChan
		pendingConnectionsMutex.Unlock()

		// Cleanup when done
		defer func() {
			pendingConnectionsMutex.Lock()
			delete(pendingConnections, opts.RequestID)
			pendingConnectionsMutex.Unlock()
		}()
	}

	// Check if already cancelled before starting
	if cancelChan != nil {
		select {
		case <-cancelChan:
			return jsonResponse(false, nil, "Connection cancelled", "CANCELLED")
		default:
		}
	}

	// Parse the bundle
	bundleInfo, err := ParseAstraBundle(opts.BundlePath, opts.ExtractDir)
	if err != nil {
		return jsonResponse(false, nil, "Failed to parse bundle: "+err.Error(), "BUNDLE_ERROR")
	}

	// Check if cancelled after bundle parsing
	if cancelChan != nil {
		select {
		case <-cancelChan:
			// Cleanup extracted files
			CleanupAstraBundle(bundleInfo.ExtractedDir)
			return jsonResponse(false, nil, "Connection cancelled", "CANCELLED")
		default:
		}
	}

	// Fetch metadata from Astra metadata service to get actual connection endpoints
	if err := FetchAstraMetadata(bundleInfo, 0); err != nil {
		CleanupAstraBundle(bundleInfo.ExtractedDir)
		return jsonResponse(false, nil, "Failed to fetch Astra metadata: "+err.Error(), "METADATA_ERROR")
	}

	// Check if cancelled after metadata fetch
	if cancelChan != nil {
		select {
		case <-cancelChan:
			CleanupAstraBundle(bundleInfo.ExtractedDir)
			return jsonResponse(false, nil, "Connection cancelled", "CANCELLED")
		default:
		}
	}

	// Validate we got the required metadata
	if bundleInfo.SniHost == "" || bundleInfo.SniPort == 0 || len(bundleInfo.ContactPoints) == 0 {
		CleanupAstraBundle(bundleInfo.ExtractedDir)
		return jsonResponse(false, nil, "Invalid metadata: missing SNI proxy or contact points", "METADATA_ERROR")
	}

	// Build session options
	keyspace := opts.Keyspace
	if keyspace == "" {
		keyspace = bundleInfo.Keyspace
	}

	// Use SNI proxy host:port as the connection endpoint
	// Use first contact point (host ID) as the TLS ServerName for SNI routing
	// Note: InsecureSkipVerify is needed because the cert is valid for *.db.astra.datastax.com
	// but SNI uses UUID host IDs for routing. The CA cert still validates the chain.
	dbOpts := db.SessionOptions{
		Host:     bundleInfo.SniHost,
		Port:     bundleInfo.SniPort,
		Keyspace: keyspace,
		Username: opts.Username,
		Password: opts.Password,
		SSL: &config.SSLConfig{
			Enabled:            true,
			CertPath:           bundleInfo.CertPath,
			KeyPath:            bundleInfo.KeyPath,
			CAPath:             bundleInfo.CACertPath,
			HostVerification:   false,                       // SNI proxy uses host IDs, not hostnames
			InsecureSkipVerify: true,                        // Skip hostname verification (UUID != *.db.astra.datastax.com)
			ServerName:         bundleInfo.ContactPoints[0], // Use host ID as SNI for routing
		},
		BatchMode: true, // Skip schema cache for faster test
	}

	// Create session in a goroutine to allow cancellation
	type sessionResult struct {
		session *db.Session
		err     error
	}
	resultChan := make(chan sessionResult, 1)

	go func() {
		session, err := db.NewSessionWithOptions(dbOpts)
		resultChan <- sessionResult{session, err}
	}()

	// Wait for either session creation or cancellation
	var session *db.Session
	if cancelChan != nil {
		select {
		case <-cancelChan:
			CleanupAstraBundle(bundleInfo.ExtractedDir)
			return jsonResponse(false, nil, "Connection cancelled", "CANCELLED")
		case res := <-resultChan:
			if res.err != nil {
				CleanupAstraBundle(bundleInfo.ExtractedDir)
				return jsonResponse(false, nil, "Connection failed: "+res.err.Error(), "CONNECTION_FAILED")
			}
			session = res.session
		}
	} else {
		res := <-resultChan
		if res.err != nil {
			CleanupAstraBundle(bundleInfo.ExtractedDir)
			return jsonResponse(false, nil, "Connection failed: "+res.err.Error(), "CONNECTION_FAILED")
		}
		session = res.session
	}
	defer session.Close()

	// Check cancelled before querying
	if cancelChan != nil {
		select {
		case <-cancelChan:
			CleanupAstraBundle(bundleInfo.ExtractedDir)
			return jsonResponse(false, nil, "Connection cancelled", "CANCELLED")
		default:
		}
	}

	// Query to verify connection and get version info
	var releaseVersion string
	localQuery := session.Query("SELECT release_version FROM system.local")
	if err := localQuery.Scan(&releaseVersion); err != nil {
		CleanupAstraBundle(bundleInfo.ExtractedDir)
		return jsonResponse(false, nil, "Failed to query system.local: "+err.Error(), "QUERY_ERROR")
	}

	// Cleanup extracted files after successful test
	CleanupAstraBundle(bundleInfo.ExtractedDir)

	// Build result similar to testConnection
	info := ClusterInfo{
		Build:      releaseVersion,
		Protocol:   5,
		Datacenter: bundleInfo.LocalDC,
		Datacenters: []DatacenterInfo{
			{
				Address:    bundleInfo.SniHost,
				Datacenter: bundleInfo.LocalDC,
			},
		},
	}

	return jsonResponse(true, info, "", "")
}

//export CleanupAstraExtracted
func CleanupAstraExtracted(extractedDir *C.char) *C.char {
	dir := C.GoString(extractedDir)
	if err := CleanupAstraBundle(dir); err != nil {
		return jsonResponse(false, nil, err.Error(), "CLEANUP_ERROR")
	}
	return jsonResponse(true, nil, "", "")
}

// SourceFilesRequest represents the request for executing CQL files
type SourceFilesRequest struct {
	Files       []string `json:"files"`
	StopOnError bool     `json:"stopOnError"`
}

// sourceFileProgress tracks progress for a source file execution - keyed by session handle for isolation
var (
	sourceProgress     = make(map[int][]FileExecutionProgress)
	sourceProgressLock sync.Mutex
)

//export ExecuteSourceFiles
func ExecuteSourceFiles(handle C.int, optionsJSON *C.char) *C.char {
	h := int(handle)
	session := getSession(h)
	if session == nil {
		return jsonResponse(false, nil, "Invalid session handle", "INVALID_HANDLE")
	}

	optStr := C.GoString(optionsJSON)
	var opts SourceFilesRequest
	if err := json.Unmarshal([]byte(optStr), &opts); err != nil {
		return jsonResponse(false, nil, "Invalid options JSON: "+err.Error(), "INVALID_OPTIONS")
	}

	if len(opts.Files) == 0 {
		return jsonResponse(false, nil, "No files provided", "INVALID_OPTIONS")
	}

	// Reset progress tracking for this session
	sourceProgressLock.Lock()
	sourceProgress[h] = []FileExecutionProgress{}
	sourceProgressLock.Unlock()

	// Execute with progress callback
	sourceOpts := &SourceFilesOptions{
		Files:       opts.Files,
		StopOnError: opts.StopOnError,
	}

	result, err := executeSourceFiles(h, session, sourceOpts, func(progress FileExecutionProgress) {
		sourceProgressLock.Lock()
		// Update or append progress for this session
		sessionProgress := sourceProgress[h]
		found := false
		for i, p := range sessionProgress {
			if p.FilePath == progress.FilePath {
				sessionProgress[i] = progress
				found = true
				break
			}
		}
		if !found {
			sessionProgress = append(sessionProgress, progress)
		}
		sourceProgress[h] = sessionProgress
		sourceProgressLock.Unlock()
	})

	if err != nil {
		return jsonResponse(false, nil, err.Error(), "EXECUTION_ERROR")
	}

	// Include final progress with result
	sourceProgressLock.Lock()
	sessionProgress := sourceProgress[h]
	finalProgress := make([]FileExecutionProgress, len(sessionProgress))
	copy(finalProgress, sessionProgress)
	sourceProgressLock.Unlock()

	return jsonResponse(true, map[string]interface{}{
		"result":   result,
		"progress": finalProgress,
	}, "", "")
}

//export GetSourceProgress
func GetSourceProgress(handle C.int) *C.char {
	h := int(handle)
	session := getSession(h)
	if session == nil {
		return jsonResponse(false, nil, "Invalid session handle", "INVALID_HANDLE")
	}

	sourceProgressLock.Lock()
	sessionProgress := sourceProgress[h]
	progress := make([]FileExecutionProgress, len(sessionProgress))
	copy(progress, sessionProgress)
	sourceProgressLock.Unlock()

	return jsonResponse(true, progress, "", "")
}

//export StopSourceExecution
func StopSourceExecution(handle C.int) *C.char {
	h := int(handle)
	session := getSession(h)
	if session == nil {
		return jsonResponse(false, nil, "Invalid session handle", "INVALID_HANDLE")
	}

	cancelSourceExecution(h)
	return jsonResponse(true, nil, "", "")
}

//export GetQueryTrace
func GetQueryTrace(handle C.int, sessionID *C.char) *C.char {
	h := int(handle)
	session := getSession(h)
	if session == nil {
		return jsonResponse(false, nil, "Invalid session handle", "INVALID_HANDLE")
	}

	sessionIDStr := C.GoString(sessionID)
	if sessionIDStr == "" {
		return jsonResponse(false, nil, "Session ID is required", "INVALID_OPTIONS")
	}

	trace, err := getQueryTraceBySessionID(session, sessionIDStr)
	if err != nil {
		return jsonResponse(false, nil, err.Error(), "TRACE_ERROR")
	}

	return jsonResponse(true, trace, "", "")
}

// PagedQueryResult represents a page of query results
type PagedQueryResult struct {
	Columns        []string                 `json:"columns"`
	ColumnTypes    []string                 `json:"columnTypes"`
	Rows           []map[string]interface{} `json:"rows"`
	RowCount       int                      `json:"rowCount"`
	HasMore        bool                     `json:"hasMore"`
	AllCompleted   bool                     `json:"allCompleted"`           // True when no more pages (hasMore=false)
	QueryID        string                   `json:"queryId"`
	TraceSessionID string                   `json:"traceSessionId,omitempty"` // Present when tracing is enabled
	Keyspace       string                   `json:"keyspace,omitempty"`     // Source keyspace for the query
	Table          string                   `json:"table,omitempty"`        // Source table for the query
}

//export ExecuteQueryPaged
func ExecuteQueryPaged(handle C.int, query *C.char) *C.char {
	h := int(handle)
	session := getSession(h)
	if session == nil {
		return jsonResponse(false, nil, "Invalid session handle", "INVALID_HANDLE")
	}

	cql := C.GoString(query)

	// WORKAROUND: Astra hangs indefinitely when tracing is enabled for queries.
	// Only apply this workaround for Astra connections (detected via Secure Connect Bundle).
	tracingWasEnabled := false
	if isAstraSession(h) && session.Tracing() {
		tracingWasEnabled = true
		session.SetTracing(false)
	}

	result := session.ExecuteCQLQuery(cql)

	// Re-enable tracing if it was disabled for Astra
	if tracingWasEnabled {
		session.SetTracing(true)
	}

	// Parse keyspace and table from the query for TABLEMETA:INFO support
	keyspace, table := parseTableReference(cql, session.Keyspace())

	// Handle different result types
	switch v := result.(type) {
	case db.QueryResult:
		// Non-streaming result - return all rows, no pagination needed
		rows := make([]map[string]interface{}, 0, len(v.RawData))
		for _, rawRow := range v.RawData {
			rows = append(rows, rawRow)
		}

		qr := PagedQueryResult{
			Columns:        v.Headers,
			ColumnTypes:    v.ColumnTypes,
			Rows:           rows,
			RowCount:       v.RowCount,
			HasMore:        false,
			AllCompleted:   true,
			QueryID:        "",
			TraceSessionID: getTraceIDIfEnabled(session),
			Keyspace:       keyspace,
			Table:          table,
		}
		return jsonResponse(true, qr, "", "")

	case db.StreamingQueryResult:
		// Streaming result - fetch first page and store iterator
		pageSize := session.PageSize()
		if pageSize <= 0 {
			pageSize = 100 // Default page size
		}

		rows := make([]map[string]interface{}, 0, pageSize)

		for i := 0; i < pageSize; i++ {
			row := make(map[string]interface{})
			if !v.Iterator.MapScan(row) {
				break
			}
			rows = append(rows, row)
		}

		// Check if there are more rows by trying to scan one more
		testRow := make(map[string]interface{})
		if v.Iterator.MapScan(testRow) {
			// We read one extra row, store it for next page
			queryID := generateQueryID(h)

			pagedQueriesMutex.Lock()
			pagedQueries[queryID] = &pagedQueryState{
				Session:     session,
				Iterator:    v.Iterator,
				ColumnNames: v.ColumnNames,
				ColumnTypes: v.ColumnTypes,
				PageSize:    pageSize,
				PeekedRow:   testRow, // Store the peeked row for next call
			}
			pagedQueriesMutex.Unlock()

			qr := PagedQueryResult{
				Columns:        v.ColumnNames,
				ColumnTypes:    v.ColumnTypes,
				Rows:           rows,
				RowCount:       len(rows),
				HasMore:        true,
				AllCompleted:   false,
				QueryID:        queryID,
				TraceSessionID: getTraceIDIfEnabled(session),
				Keyspace:       keyspace,
				Table:          table,
			}
			return jsonResponse(true, qr, "", "")
		}

		// No more rows, close iterator
		v.Iterator.Close()

		qr := PagedQueryResult{
			Columns:        v.ColumnNames,
			ColumnTypes:    v.ColumnTypes,
			Rows:           rows,
			RowCount:       len(rows),
			HasMore:        false,
			AllCompleted:   true,
			QueryID:        "",
			TraceSessionID: getTraceIDIfEnabled(session),
			Keyspace:       keyspace,
			Table:          table,
		}
		return jsonResponse(true, qr, "", "")

	case string:
		return jsonResponse(true, map[string]interface{}{
			"message": v,
		}, "", "")

	case error:
		return jsonResponse(false, nil, v.Error(), "QUERY_ERROR")

	default:
		return jsonResponse(true, map[string]interface{}{
			"result": v,
		}, "", "")
	}
}

//export FetchNextPage
func FetchNextPage(handle C.int, queryID *C.char) *C.char {
	h := int(handle)
	session := getSession(h)
	if session == nil {
		return jsonResponse(false, nil, "Invalid session handle", "INVALID_HANDLE")
	}

	qID := C.GoString(queryID)
	if qID == "" {
		return jsonResponse(false, nil, "Query ID is required", "INVALID_OPTIONS")
	}

	pagedQueriesMutex.Lock()
	state, exists := pagedQueries[qID]
	pagedQueriesMutex.Unlock()

	if !exists {
		return jsonResponse(false, nil, "Query not found or already closed", "QUERY_NOT_FOUND")
	}

	// Fetch next page
	pageSize := state.PageSize
	if pageSize <= 0 {
		pageSize = 100
	}

	rows := make([]map[string]interface{}, 0, pageSize)

	// First, include the peeked row from previous call if it exists
	if state.PeekedRow != nil {
		rows = append(rows, state.PeekedRow)
		state.PeekedRow = nil
	}

	// Fetch remaining rows to fill up to pageSize
	for len(rows) < pageSize {
		row := make(map[string]interface{})
		if !state.Iterator.MapScan(row) {
			break
		}
		rows = append(rows, row)
	}

	// Check if there are more rows by peeking ahead
	hasMore := false
	if len(rows) == pageSize {
		testRow := make(map[string]interface{})
		if state.Iterator.MapScan(testRow) {
			hasMore = true
			// Store the peeked row for next call instead of appending
			state.PeekedRow = testRow
		}
	}

	if !hasMore {
		// No more rows, clean up
		state.Iterator.Close()
		pagedQueriesMutex.Lock()
		delete(pagedQueries, qID)
		pagedQueriesMutex.Unlock()
	}

	qr := PagedQueryResult{
		Columns:      state.ColumnNames,
		ColumnTypes:  state.ColumnTypes,
		Rows:         rows,
		RowCount:     len(rows),
		HasMore:      hasMore,
		AllCompleted: !hasMore,
		QueryID:      qID,
	}

	if !hasMore {
		qr.QueryID = "" // Clear query ID when done
	}

	return jsonResponse(true, qr, "", "")
}

//export CancelPagedQuery
func CancelPagedQuery(handle C.int, queryID *C.char) *C.char {
	h := int(handle)
	session := getSession(h)
	if session == nil {
		return jsonResponse(false, nil, "Invalid session handle", "INVALID_HANDLE")
	}

	qID := C.GoString(queryID)
	if qID == "" {
		return jsonResponse(false, nil, "Query ID is required", "INVALID_OPTIONS")
	}

	pagedQueriesMutex.Lock()
	state, exists := pagedQueries[qID]
	if exists {
		state.Iterator.Close()
		delete(pagedQueries, qID)
	}
	pagedQueriesMutex.Unlock()

	if !exists {
		return jsonResponse(true, map[string]interface{}{
			"cancelled": false,
			"reason":    "Query not found or already closed",
		}, "", "")
	}

	return jsonResponse(true, map[string]interface{}{
		"cancelled": true,
	}, "", "")
}

// CancelQuery cancels any active paged queries for the session
// This is used when the user interrupts a running query (e.g., CTRL+C)
//
//export CancelQuery
func CancelQuery(handle C.int) *C.char {
	h := int(handle)
	session := getSession(h)
	if session == nil {
		return jsonResponse(false, nil, "Invalid session handle", "INVALID_HANDLE")
	}

	pagedQueriesMutex.Lock()
	defer pagedQueriesMutex.Unlock()

	// Find and cancel all paged queries for this session
	cancelledCount := 0
	queryIDs := make([]string, 0)

	for qID, state := range pagedQueries {
		if state.Session == session {
			queryIDs = append(queryIDs, qID)
		}
	}

	for _, qID := range queryIDs {
		state := pagedQueries[qID]
		if state.Iterator != nil {
			state.Iterator.Close()
		}
		delete(pagedQueries, qID)
		cancelledCount++
	}

	return jsonResponse(true, map[string]interface{}{
		"cancelledQueries": cancelledCount,
	}, "", "")
}

// SplitCQLResult represents the result of splitting CQL statements
type SplitCQLResult struct {
	Statements   []string `json:"statements"`
	Identifiers  []string `json:"identifiers"`
	ExtraTokens  []string `json:"extraTokens"`
	SecondTokens []string `json:"secondTokens"`
	ThirdTokens  []string `json:"thirdTokens"`
	Incomplete   bool     `json:"incomplete"`
	Error        string   `json:"error,omitempty"`
}

//export SplitCQL
func SplitCQL(cql *C.char) *C.char {
	cqlStr := C.GoString(cql)

	// Handle empty input
	cqlStr = strings.TrimSpace(cqlStr)
	if cqlStr == "" {
		return jsonResponse(true, SplitCQLResult{
			Statements:   []string{},
			Identifiers:  []string{},
			ExtraTokens:  []string{},
			SecondTokens: []string{},
			ThirdTokens:  []string{},
			Incomplete:   false,
		}, "", "")
	}

	// Split statements using the CQL splitter from internal/batch
	splitResult, err := batch.SplitStatements(cqlStr)
	if err != nil {
		return jsonResponse(true, SplitCQLResult{
			Statements:   []string{},
			Identifiers:  []string{},
			ExtraTokens:  []string{},
			SecondTokens: []string{},
			ThirdTokens:  []string{},
			Incomplete:   false,
			Error:        err.Error(),
		}, "", "")
	}

	result := SplitCQLResult{
		Statements:   splitResult.GetStatementStrings(),
		Identifiers:  splitResult.Identifiers,
		ExtraTokens:  splitResult.ExtraTokens,
		SecondTokens: splitResult.SecondTokens,
		ThirdTokens:  splitResult.ThirdTokens,
		Incomplete:   splitResult.Incomplete,
	}

	return jsonResponse(true, result, "", "")
}

//export CopyTo
func CopyTo(handle C.int, paramsJSON *C.char) *C.char {
	session := getSession(int(handle))
	if session == nil {
		return jsonResponse(false, nil, "Invalid session handle", "INVALID_HANDLE")
	}

	var params CopyParams
	if err := json.Unmarshal([]byte(C.GoString(paramsJSON)), &params); err != nil {
		return jsonResponse(false, nil, "Invalid params JSON: "+err.Error(), "INVALID_PARAMS")
	}

	if params.Table == "" || params.Filename == "" {
		return jsonResponse(false, nil, "table and filename are required", "INVALID_PARAMS")
	}

	options := mergeCopyOptions(defaultCopyOptions(), params.Options)
	result, err := executeCopyTo(session, params, options)
	if err != nil {
		return jsonResponse(false, nil, err.Error(), "COPY_ERROR")
	}

	return jsonResponse(true, result, "", "")
}

//export CopyFrom
func CopyFrom(handle C.int, paramsJSON *C.char) *C.char {
	session := getSession(int(handle))
	if session == nil {
		return jsonResponse(false, nil, "Invalid session handle", "INVALID_HANDLE")
	}

	var params CopyParams
	if err := json.Unmarshal([]byte(C.GoString(paramsJSON)), &params); err != nil {
		return jsonResponse(false, nil, "Invalid params JSON: "+err.Error(), "INVALID_PARAMS")
	}

	if params.Table == "" || params.Filename == "" {
		return jsonResponse(false, nil, "table and filename are required", "INVALID_PARAMS")
	}

	options := mergeCopyOptions(defaultCopyOptions(), params.Options)
	result, err := executeCopyFrom(session, params, options)
	if err != nil {
		if result != nil {
			// Partial success - return result with error
			return jsonResponse(false, result, err.Error(), "COPY_ERROR")
		}
		return jsonResponse(false, nil, err.Error(), "COPY_ERROR")
	}

	return jsonResponse(true, result, "", "")
}

//export FreeString
func FreeString(str *C.char) {
	C.free(unsafe.Pointer(str))
}

// Required for c-shared build mode
func main() {}
