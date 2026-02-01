package main

import (
	"fmt"
	"time"

	"github.com/axonops/cqlai-node/internal/db"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// TraceEvent represents a single trace event from system_traces.events
type TraceEvent struct {
	Activity      string `json:"activity"`
	EventID       string `json:"event_id"`               // UUID string for the event
	Timestamp     string `json:"timestamp"`              // Extracted from event_id TimeUUID
	Source        string `json:"source"`                 // Source node IP
	SourceElapsed int64  `json:"source_elapsed"`         // microseconds (snake_case for renderer)
	SourcePort    int    `json:"source_port,omitempty"`  // Source port (if available)
	Thread        string `json:"thread,omitempty"`       // Thread name
	SessionID     string `json:"session_id"`             // Parent session ID
}

// TraceSession represents the trace session info from system_traces.sessions
type TraceSession struct {
	SessionID   string `json:"sessionId"`
	Command     string `json:"command,omitempty"`
	Coordinator string `json:"coordinator"`
	Duration    int64  `json:"duration"` // microseconds
	StartedAt   string `json:"startedAt"`
	Client      string `json:"client,omitempty"`
	Request     string `json:"request,omitempty"`
	Parameters  string `json:"parameters,omitempty"`
}

// QueryTraceResult contains the full trace information
type QueryTraceResult struct {
	Session TraceSession `json:"session"`
	Events  []TraceEvent `json:"events"`
}

// getQueryTraceBySessionID retrieves trace information for a given session ID
func getQueryTraceBySessionID(session *db.Session, traceSessionIDStr string) (*QueryTraceResult, error) {
	traceSessionID, err := gocql.ParseUUID(traceSessionIDStr)
	if err != nil {
		return nil, fmt.Errorf("invalid session ID: %v", err)
	}

	gocqlSession := session.GocqlSession()
	result := &QueryTraceResult{
		Events: []TraceEvent{},
	}

	// Get session info from system_traces.sessions
	var coordinator, request, command, client string
	var duration int
	var startedAt time.Time
	var parameters map[string]string

	sessionQuery := `SELECT coordinator, duration, request, command, client, started_at, parameters
		FROM system_traces.sessions WHERE session_id = ?`

	err = gocqlSession.Query(sessionQuery, traceSessionID).Scan(
		&coordinator, &duration, &request, &command, &client, &startedAt, &parameters,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get trace session: %v", err)
	}

	result.Session = TraceSession{
		SessionID:   traceSessionID.String(),
		Coordinator: coordinator,
		Duration:    int64(duration),
		StartedAt:   startedAt.Format(time.RFC3339Nano),
		Request:     request,
		Command:     command,
		Client:      client,
	}

	// Convert parameters map to string if present
	if len(parameters) > 0 {
		paramStr := ""
		for k, v := range parameters {
			if paramStr != "" {
				paramStr += ", "
			}
			paramStr += fmt.Sprintf("%s=%s", k, v)
		}
		result.Session.Parameters = paramStr
	}

	// Get events from system_traces.events
	eventsQuery := `SELECT activity, source, source_elapsed, event_id, thread, source_port
		FROM system_traces.events WHERE session_id = ?`

	iter := gocqlSession.Query(eventsQuery, traceSessionID).Iter()

	var activity, source, thread string
	var sourceElapsed, sourcePort int
	var eventID gocql.UUID

	for iter.Scan(&activity, &source, &sourceElapsed, &eventID, &thread, &sourcePort) {
		// Extract timestamp from eventID (TimeUUID)
		eventTime := eventID.Time()

		event := TraceEvent{
			Activity:      activity,
			EventID:       eventID.String(),
			Timestamp:     eventTime.Format(time.RFC3339Nano),
			Source:        source,
			SourceElapsed: int64(sourceElapsed),
			SourcePort:    sourcePort,
			Thread:        thread,
			SessionID:     traceSessionIDStr,
		}
		result.Events = append(result.Events, event)
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to get trace events: %v", err)
	}

	return result, nil
}
