package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/axonops/cqlai-node/internal/db"
)

// CopyParams represents parameters for COPY TO/FROM operations
type CopyParams struct {
	Table    string            `json:"table"`
	Columns  []string          `json:"columns,omitempty"`
	Filename string            `json:"filename"`
	Options  map[string]string `json:"options,omitempty"`
}

// CopyResult represents the result of a COPY operation
type CopyResult struct {
	RowsExported int64 `json:"rows_exported,omitempty"`
	RowsImported int64 `json:"rows_imported,omitempty"`
	Errors       int64 `json:"errors,omitempty"`
	ParseErrors  int   `json:"parse_errors,omitempty"`
	SkippedRows  int   `json:"skipped_rows,omitempty"`
}

// batchEntry holds a prepared query and its values for batch execution
type batchEntry struct {
	query  string
	values []interface{}
}

// defaultCopyOptions returns default options for COPY operations
func defaultCopyOptions() map[string]string {
	return map[string]string{
		"HEADER":          "false",
		"NULLVAL":         "null",
		"DELIMITER":       ",",
		"QUOTE":           "\"",
		"ESCAPE":          "\\",
		"ENCODING":        "utf8",
		"PAGESIZE":        "1000",
		"MAXREQUESTS":     "6",
		"CHUNKSIZE":       "5000",
		"MAXROWS":         "-1",
		"SKIPROWS":        "0",
		"MAXPARSEERRORS":  "-1",
		"MAXINSERTERRORS": "1000",
		"MAXBATCHSIZE":    "20",
		"MINBATCHSIZE":    "2",
	}
}

// mergeCopyOptions merges user options into defaults (case-insensitive keys)
func mergeCopyOptions(defaults, userOpts map[string]string) map[string]string {
	for k, v := range userOpts {
		defaults[strings.ToUpper(k)] = v
	}
	return defaults
}

// formatCSVValue formats a value for CSV export, handling complex types
func formatCSVValue(val interface{}) string {
	switch v := val.(type) {
	case []byte:
		return fmt.Sprintf("0x%x", v)
	case time.Time:
		return v.Format(time.RFC3339)
	case gocql.UUID:
		return v.String()
	case map[string]interface{}, []interface{}, map[interface{}]interface{}:
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(jsonBytes)
	default:
		if val == nil {
			return ""
		}
		return fmt.Sprintf("%v", val)
	}
}

// executeCopyTo exports data from a table to a CSV file
func executeCopyTo(session *db.Session, params CopyParams, options map[string]string) (*CopyResult, error) {
	// Build SELECT query
	var query string
	if len(params.Columns) > 0 {
		query = fmt.Sprintf("SELECT %s FROM %s", strings.Join(params.Columns, ", "), params.Table)
	} else {
		query = fmt.Sprintf("SELECT * FROM %s", params.Table)
	}

	// Open output file
	cleanPath := filepath.Clean(params.Filename)
	file, err := os.Create(cleanPath) // #nosec G304 - user-provided path
	if err != nil {
		return nil, fmt.Errorf("error creating file: %v", err)
	}
	defer file.Close()

	// Create CSV writer
	csvWriter := csv.NewWriter(file)
	if delimiter := options["DELIMITER"]; delimiter != "" && len(delimiter) > 0 {
		csvWriter.Comma = rune(delimiter[0])
	}

	maxRows, _ := strconv.Atoi(options["MAXROWS"])
	nullVal := options["NULLVAL"]
	writeHeader := strings.ToLower(options["HEADER"]) == "true"

	// Execute as streaming query for large tables
	result := session.ExecuteStreamingQuery(query)

	switch v := result.(type) {
	case db.StreamingQueryResult:
		defer v.Iterator.Close()

		// Write header
		if writeHeader && len(v.ColumnNames) > 0 {
			if err := csvWriter.Write(v.ColumnNames); err != nil {
				return nil, fmt.Errorf("error writing header: %v", err)
			}
		}

		rowCount := int64(0)
		pageSize, _ := strconv.Atoi(options["PAGESIZE"])
		if pageSize <= 0 {
			pageSize = 1000
		}

		for {
			if maxRows != -1 && rowCount >= int64(maxRows) {
				break
			}

			rowMap := make(map[string]interface{})
			if !v.Iterator.MapScan(rowMap) {
				break
			}

			row := make([]string, len(v.ColumnNames))
			for i, colName := range v.ColumnNames {
				if val, ok := rowMap[colName]; ok {
					if val == nil {
						row[i] = nullVal
					} else {
						row[i] = formatCSVValue(val)
					}
				} else {
					row[i] = nullVal
				}
			}

			if err := csvWriter.Write(row); err != nil {
				return nil, fmt.Errorf("error writing row: %v", err)
			}
			rowCount++

			if rowCount%int64(pageSize) == 0 {
				csvWriter.Flush()
			}
		}

		csvWriter.Flush()
		if err := csvWriter.Error(); err != nil {
			return nil, fmt.Errorf("error flushing CSV: %v", err)
		}

		return &CopyResult{RowsExported: rowCount}, nil

	case db.QueryResult:
		// Write header
		if writeHeader && len(v.Headers) > 0 {
			if err := csvWriter.Write(v.Headers); err != nil {
				return nil, fmt.Errorf("error writing header: %v", err)
			}
		}

		rowCount := int64(0)
		for _, row := range v.Data {
			if maxRows != -1 && rowCount >= int64(maxRows) {
				break
			}
			processedRow := make([]string, len(row))
			for i, cell := range row {
				if nullVal != "" && (cell == "null" || cell == "<null>") {
					processedRow[i] = nullVal
				} else {
					processedRow[i] = cell
				}
			}
			if err := csvWriter.Write(processedRow); err != nil {
				return nil, fmt.Errorf("error writing row: %v", err)
			}
			rowCount++
		}

		csvWriter.Flush()
		if err := csvWriter.Error(); err != nil {
			return nil, fmt.Errorf("error flushing CSV: %v", err)
		}

		return &CopyResult{RowsExported: rowCount}, nil

	case error:
		return nil, fmt.Errorf("query error: %v", v)

	default:
		return nil, fmt.Errorf("unexpected result type: %T", result)
	}
}

// executeCopyFrom imports data from a CSV file into a table
func executeCopyFrom(session *db.Session, params CopyParams, options map[string]string) (*CopyResult, error) {
	// Open CSV file
	cleanPath := filepath.Clean(params.Filename)
	file, err := os.Open(cleanPath) // #nosec G304 - user-provided path
	if err != nil {
		return nil, fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()

	// Create CSV reader
	csvReader := csv.NewReader(file)
	if delimiter := options["DELIMITER"]; delimiter != "" && len(delimiter) > 0 {
		csvReader.Comma = rune(delimiter[0])
	}
	if quote := options["QUOTE"]; quote != "" && len(quote) > 0 {
		csvReader.LazyQuotes = true
	}

	// Parse options
	hasHeader := strings.ToLower(options["HEADER"]) == "true"
	nullVal := options["NULLVAL"]
	chunkSize, _ := strconv.Atoi(options["CHUNKSIZE"])
	maxRows, _ := strconv.Atoi(options["MAXROWS"])
	skipRows, _ := strconv.Atoi(options["SKIPROWS"])
	maxParseErrors, _ := strconv.Atoi(options["MAXPARSEERRORS"])
	maxInsertErrors, _ := strconv.Atoi(options["MAXINSERTERRORS"])
	maxBatchSize, _ := strconv.Atoi(options["MAXBATCHSIZE"])
	maxRequests, _ := strconv.Atoi(options["MAXREQUESTS"])

	if chunkSize <= 0 {
		chunkSize = 5000
	}
	if maxBatchSize <= 0 {
		maxBatchSize = 20
	}
	if maxRequests < 1 {
		maxRequests = 6
	}

	columns := params.Columns

	// Read header if present
	var headerColumns []string
	if hasHeader {
		headerRow, err := csvReader.Read()
		if err != nil {
			return nil, fmt.Errorf("error reading header: %v", err)
		}
		headerColumns = make([]string, len(headerRow))
		for i, col := range headerRow {
			cleanCol := strings.TrimSpace(col)
			// Remove (PK) and (C) suffixes from COPY TO exports
			if idx := strings.Index(cleanCol, " (PK)"); idx != -1 {
				cleanCol = cleanCol[:idx]
			}
			if idx := strings.Index(cleanCol, " (C)"); idx != -1 {
				cleanCol = cleanCol[:idx]
			}
			headerColumns[i] = strings.TrimSpace(cleanCol)
		}
	}

	// Determine columns
	if len(columns) == 0 {
		if hasHeader && len(headerColumns) > 0 {
			columns = headerColumns
		} else {
			// Get columns from table schema
			columns = getTableColumns(session, params.Table)
			if len(columns) == 0 {
				return nil, fmt.Errorf("cannot determine columns for table %s; specify columns explicitly or use HEADER=true", params.Table)
			}
		}
	}

	// Skip rows if requested
	skippedRows := 0
	for i := 0; i < skipRows; i++ {
		_, err := csvReader.Read()
		if err != nil {
			break
		}
		skippedRows++
	}

	// Build INSERT template
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	insertTemplate := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		params.Table, strings.Join(columns, ", "), strings.Join(placeholders, ", "))

	// Concurrent batch execution
	var rowCount int64
	var insertErrorCount int64
	processedRows := 0
	parseErrorCount := 0

	batchChan := make(chan []batchEntry, maxRequests*2)
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < maxRequests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range batchChan {
				errors := executeBatchWithValues(session, batch)
				atomic.AddInt64(&insertErrorCount, int64(errors))
				atomic.AddInt64(&rowCount, int64(len(batch)-errors))
			}
		}()
	}

	batch := make([]batchEntry, 0, maxBatchSize)

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			parseErrorCount++
			if maxParseErrors != -1 && parseErrorCount > maxParseErrors {
				close(batchChan)
				wg.Wait()
				return &CopyResult{
					RowsImported: atomic.LoadInt64(&rowCount),
					Errors:       atomic.LoadInt64(&insertErrorCount),
					ParseErrors:  parseErrorCount,
					SkippedRows:  skippedRows,
				}, fmt.Errorf("too many parse errors (%d)", parseErrorCount)
			}
			continue
		}

		if maxRows != -1 && processedRows >= maxRows {
			break
		}
		processedRows++

		if len(record) != len(columns) {
			parseErrorCount++
			if maxParseErrors != -1 && parseErrorCount > maxParseErrors {
				close(batchChan)
				wg.Wait()
				return &CopyResult{
					RowsImported: atomic.LoadInt64(&rowCount),
					Errors:       atomic.LoadInt64(&insertErrorCount),
					ParseErrors:  parseErrorCount,
					SkippedRows:  skippedRows,
				}, fmt.Errorf("too many parse errors (%d)", parseErrorCount)
			}
			continue
		}

		// Convert values
		values := make([]interface{}, len(record))
		for i, val := range record {
			if val == nullVal {
				values[i] = nil
			} else {
				values[i] = parseValueForBinding(val)
			}
		}

		batch = append(batch, batchEntry{query: insertTemplate, values: values})

		if len(batch) >= maxBatchSize {
			if maxInsertErrors != -1 && atomic.LoadInt64(&insertErrorCount) > int64(maxInsertErrors) {
				close(batchChan)
				wg.Wait()
				return &CopyResult{
					RowsImported: atomic.LoadInt64(&rowCount),
					Errors:       atomic.LoadInt64(&insertErrorCount),
					ParseErrors:  parseErrorCount,
					SkippedRows:  skippedRows,
				}, fmt.Errorf("too many insert errors (%d)", atomic.LoadInt64(&insertErrorCount))
			}
			batchCopy := make([]batchEntry, len(batch))
			copy(batchCopy, batch)
			batchChan <- batchCopy
			batch = batch[:0]
		}
	}

	// Send remaining batch
	if len(batch) > 0 {
		batchCopy := make([]batchEntry, len(batch))
		copy(batchCopy, batch)
		batchChan <- batchCopy
	}

	close(batchChan)
	wg.Wait()

	return &CopyResult{
		RowsImported: atomic.LoadInt64(&rowCount),
		Errors:       atomic.LoadInt64(&insertErrorCount),
		ParseErrors:  parseErrorCount,
		SkippedRows:  skippedRows,
	}, nil
}

// getTableColumns retrieves column names for a table from system_schema
func getTableColumns(session *db.Session, table string) []string {
	parts := strings.Split(table, ".")
	var keyspace, tableName string

	if len(parts) == 2 {
		keyspace = parts[0]
		tableName = parts[1]
	} else {
		keyspace = session.Keyspace()
		if keyspace == "" {
			return []string{}
		}
		tableName = parts[0]
	}

	query := fmt.Sprintf(`SELECT column_name FROM system_schema.columns WHERE keyspace_name = '%s' AND table_name = '%s'`, keyspace, tableName)
	result := session.ExecuteCQLQuery(query)

	switch v := result.(type) {
	case db.QueryResult:
		columns := make([]string, 0, len(v.Data))
		for _, row := range v.Data {
			if len(row) > 0 {
				columns = append(columns, row[0])
			}
		}
		return columns
	default:
		return []string{}
	}
}

// parseValueForBinding converts a CSV string value to the appropriate Go type
func parseValueForBinding(value string) interface{} {
	if value == "" {
		return ""
	}
	if i, err := strconv.ParseInt(value, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(value, 64); err == nil {
		return f
	}
	if value == "true" {
		return true
	}
	if value == "false" {
		return false
	}
	return value
}

// executeBatchWithValues executes a batch of queries, falling back to individual on failure
func executeBatchWithValues(session *db.Session, entries []batchEntry) int {
	if len(entries) == 0 {
		return 0
	}

	batch := session.CreateBatch(gocql.UnloggedBatch)
	for _, entry := range entries {
		batch.Query(entry.query, entry.values...)
	}

	err := session.ExecuteBatch(batch)
	if err != nil {
		errors := 0
		for _, entry := range entries {
			if execErr := session.Query(entry.query, entry.values...).Exec(); execErr != nil {
				errors++
			}
		}
		return errors
	}
	return 0
}
