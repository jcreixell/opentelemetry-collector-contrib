// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package store // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/topologyconnector/internal/store"

import (
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// Edge represents an in-flight client/server span pair being correlated.
// Once both sides are observed the edge is considered complete and forwarded
// to the topology connector's confirmed-edge map.
type Edge struct {
	Key Key

	ClientService   string
	ClientNamespace string
	ServerService   string
	ServerNamespace string

	// Dimensions holds the values of any extra dimensions configured by the
	// user (e.g. deployment.environment), captured from the client resource.
	Dimensions map[string]string

	expiration time.Time
}

func newEdge(key Key, ttl time.Duration) *Edge {
	return &Edge{
		Key:        key,
		Dimensions: make(map[string]string),
		expiration: time.Now().Add(ttl),
	}
}

// isComplete returns true when both client and server service names are known.
func (e *Edge) isComplete() bool {
	return e.ClientService != "" && e.ServerService != ""
}

func (e *Edge) isExpired() bool {
	return time.Now().After(e.expiration)
}

// Key identifies an in-flight edge by its trace ID and the client span's ID.
// The server span uses ParentSpanID to find the same key.
type Key struct {
	tid pcommon.TraceID
	sid pcommon.SpanID
}

func NewKey(tid pcommon.TraceID, sid pcommon.SpanID) Key {
	return Key{tid: tid, sid: sid}
}
