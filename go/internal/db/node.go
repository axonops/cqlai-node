package db

// Methods the Node bindings need that cqlai does not have. They live in this
// file so the rest of the package can be copied over from cqlai as it is.

import (
	"fmt"
	"net"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// Host returns the connection host, without the port.
func (s *Session) Host() string {
	if s.cluster == nil || len(s.cluster.Hosts) == 0 {
		return ""
	}
	host := s.cluster.Hosts[0]
	if h, _, err := net.SplitHostPort(host); err == nil {
		return h
	}
	return host
}

// GocqlSession returns the underlying gocql.Session.
func (s *Session) GocqlSession() *gocql.Session {
	return s.Session
}

// LastTraceID returns the trace session ID of the last traced request as a
// hex string, or "" when nothing has been traced.
func (s *Session) LastTraceID() string {
	ids, _ := s.tracedRequests()
	if len(ids) == 0 {
		return ""
	}
	return fmt.Sprintf("%x", ids[len(ids)-1])
}
