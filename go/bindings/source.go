package main

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/axonops/cqlai-node/internal/db"
)

// Source execution cancellation - keyed by session handle for isolation
var (
	sourceExecutionCancelled = make(map[int]bool)
	sourceExecutionLock      sync.Mutex
)

// isSourceExecutionCancelled checks if execution was cancelled for a specific session
func isSourceExecutionCancelled(handle int) bool {
	sourceExecutionLock.Lock()
	defer sourceExecutionLock.Unlock()
	return sourceExecutionCancelled[handle]
}

// cancelSourceExecution sets the cancellation flag for a specific session
func cancelSourceExecution(handle int) {
	sourceExecutionLock.Lock()
	defer sourceExecutionLock.Unlock()
	sourceExecutionCancelled[handle] = true
}

// resetSourceExecutionCancellation resets the cancellation flag for a specific session
func resetSourceExecutionCancellation(handle int) {
	sourceExecutionLock.Lock()
	defer sourceExecutionLock.Unlock()
	delete(sourceExecutionCancelled, handle)
}

// FileExecutionProgress represents progress info for a single file
type FileExecutionProgress struct {
	FilePath         string   `json:"filePath"`
	FileIndex        int      `json:"fileIndex"`
	TotalFiles       int      `json:"totalFiles"`
	StatementsTotal  int      `json:"statementsTotal"`
	StatementsRun    int      `json:"statementsRun"`
	StatementsOK     int      `json:"statementsOK"`
	StatementsFailed int      `json:"statementsFailed"`
	CurrentStatement string   `json:"currentStatement,omitempty"`
	Errors           []string `json:"errors,omitempty"`
	IsComplete       bool     `json:"isComplete"`
	Cancelled        bool     `json:"cancelled"` // true if cancelled by user
	Duration         int64    `json:"duration"`  // milliseconds
}

// SourceFilesOptions contains options for executing CQL files
type SourceFilesOptions struct {
	Files       []string `json:"files"`
	StopOnError bool     `json:"stopOnError"`
}

// SourceFilesResult is the final result after all files are executed
type SourceFilesResult struct {
	TotalFiles       int      `json:"totalFiles"`
	FilesCompleted   int      `json:"filesCompleted"`
	FilesFailed      int      `json:"filesFailed"`
	TotalStatements  int      `json:"totalStatements"`
	StatementsOK     int      `json:"statementsOK"`
	StatementsFailed int      `json:"statementsFailed"`
	TotalDuration    int64    `json:"totalDuration"` // milliseconds
	Errors           []string `json:"errors,omitempty"`
	Stopped          bool     `json:"stopped"`   // true if stopped due to error
	Cancelled        bool     `json:"cancelled"` // true if cancelled by user
}

// parseCQLFile reads a CQL file and extracts individual statements
func parseCQLFile(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	var statements []string
	var currentStatement strings.Builder
	scanner := bufio.NewScanner(file)

	// Increase buffer size for large files
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	inString := false
	stringChar := rune(0)
	inBlockComment := false

	for scanner.Scan() {
		line := scanner.Text()

		// Skip empty lines
		trimmedLine := strings.TrimSpace(line)
		if trimmedLine == "" {
			continue
		}

		// Handle single-line comments at start of line
		if !inString && !inBlockComment {
			if strings.HasPrefix(trimmedLine, "--") || strings.HasPrefix(trimmedLine, "//") {
				continue
			}
		}

		// Process character by character to handle strings, comments, and semicolons
		for i := 0; i < len(line); i++ {
			char := rune(line[i])

			// Handle block comments
			if !inString && !inBlockComment && i+1 < len(line) && line[i:i+2] == "/*" {
				inBlockComment = true
				i++ // skip next char
				continue
			}
			if inBlockComment {
				if i+1 < len(line) && line[i:i+2] == "*/" {
					inBlockComment = false
					i++ // skip next char
				}
				continue
			}

			// Handle single-line comments
			if !inString && i+1 < len(line) && (line[i:i+2] == "--" || line[i:i+2] == "//") {
				break // skip rest of line
			}

			if inString {
				currentStatement.WriteRune(char)
				if char == stringChar {
					// Check for escaped quote (doubled)
					if i+1 < len(line) && rune(line[i+1]) == stringChar {
						currentStatement.WriteRune(rune(line[i+1]))
						i++
						continue
					}
					inString = false
				}
			} else {
				if char == '\'' || char == '"' {
					inString = true
					stringChar = char
					currentStatement.WriteRune(char)
				} else if char == ';' {
					// End of statement
					stmt := strings.TrimSpace(currentStatement.String())
					if stmt != "" {
						statements = append(statements, stmt)
					}
					currentStatement.Reset()
				} else {
					currentStatement.WriteRune(char)
				}
			}
		}

		// Add space between lines (for multi-line statements)
		if currentStatement.Len() > 0 {
			currentStatement.WriteRune(' ')
		}
	}

	// Handle last statement without semicolon
	if stmt := strings.TrimSpace(currentStatement.String()); stmt != "" {
		statements = append(statements, stmt)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading file: %v", err)
	}

	return statements, nil
}

// executeSourceFiles executes multiple CQL files and sends progress via callback
// The handle parameter is the session handle used for per-session cancellation isolation
func executeSourceFiles(handle int, session *db.Session, options *SourceFilesOptions, progressCallback func(FileExecutionProgress)) (*SourceFilesResult, error) {
	// Reset cancellation flag at start for this session
	resetSourceExecutionCancellation(handle)

	result := &SourceFilesResult{
		TotalFiles: len(options.Files),
		Errors:     []string{},
	}

	gocqlSession := session.GocqlSession()
	startTime := time.Now()

	for fileIndex, filePath := range options.Files {
		// Check for cancellation before processing each file
		if isSourceExecutionCancelled(handle) {
			result.Cancelled = true
			result.TotalDuration = time.Since(startTime).Milliseconds()
			return result, nil
		}
		fileStartTime := time.Now()

		progress := FileExecutionProgress{
			FilePath:   filePath,
			FileIndex:  fileIndex,
			TotalFiles: len(options.Files),
			Errors:     []string{},
		}

		// Parse the CQL file
		statements, err := parseCQLFile(filePath)
		if err != nil {
			progress.Errors = append(progress.Errors, fmt.Sprintf("Failed to parse file: %v", err))
			progress.IsComplete = true
			progress.Duration = time.Since(fileStartTime).Milliseconds()
			progressCallback(progress)

			result.FilesFailed++
			result.Errors = append(result.Errors, fmt.Sprintf("%s: %v", filePath, err))

			if options.StopOnError {
				result.Stopped = true
				result.TotalDuration = time.Since(startTime).Milliseconds()
				return result, nil
			}
			continue
		}

		progress.StatementsTotal = len(statements)
		result.TotalStatements += len(statements)

		// Execute each statement
		fileHasError := false
		for stmtIndex, stmt := range statements {
			// Check for cancellation before each statement
			if isSourceExecutionCancelled(handle) {
				progress.IsComplete = true
				progress.Cancelled = true
				progress.Duration = time.Since(fileStartTime).Milliseconds()
				progressCallback(progress)

				result.Cancelled = true
				result.TotalDuration = time.Since(startTime).Milliseconds()
				return result, nil
			}

			progress.StatementsRun = stmtIndex + 1
			progress.CurrentStatement = truncateStatement(stmt, 200)

			// Send progress before execution
			progressCallback(progress)

			// Execute the statement
			err := gocqlSession.Query(stmt).Exec()
			if err != nil {
				progress.StatementsFailed++
				result.StatementsFailed++
				errMsg := fmt.Sprintf("Statement %d: %v", stmtIndex+1, err)
				progress.Errors = append(progress.Errors, errMsg)
				fileHasError = true

				if options.StopOnError {
					progress.IsComplete = true
					progress.Duration = time.Since(fileStartTime).Milliseconds()
					progressCallback(progress)

					result.FilesFailed++
					result.Errors = append(result.Errors, fmt.Sprintf("%s: %s", filePath, errMsg))
					result.Stopped = true
					result.TotalDuration = time.Since(startTime).Milliseconds()
					return result, nil
				}
			} else {
				progress.StatementsOK++
				result.StatementsOK++
			}
		}

		progress.IsComplete = true
		progress.Duration = time.Since(fileStartTime).Milliseconds()
		progress.CurrentStatement = ""
		progressCallback(progress)

		if fileHasError {
			result.FilesFailed++
		} else {
			result.FilesCompleted++
		}
	}

	result.TotalDuration = time.Since(startTime).Milliseconds()
	return result, nil
}

// truncateStatement truncates a statement for display purposes
func truncateStatement(stmt string, maxLen int) string {
	// Remove newlines and extra spaces
	re := regexp.MustCompile(`\s+`)
	stmt = re.ReplaceAllString(stmt, " ")
	stmt = strings.TrimSpace(stmt)

	if len(stmt) > maxLen {
		return stmt[:maxLen] + "..."
	}
	return stmt
}
