// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package topologyconnector

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/connector/connectortest"
	"go.opentelemetry.io/collector/consumer/consumertest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/topologyconnector/internal/metadata"
)

func newTestLogsConnector(t *testing.T, cfg *Config, sink *consumertest.LogsSink) *logsConnector {
	t.Helper()
	params := connectortest.NewNopSettings(metadata.Type)
	return newLogsConnector(params, cfg, sink)
}

func TestLogsConnector_EdgeDiscovered(t *testing.T) {
	sink := &consumertest.LogsSink{}
	conn := newTestLogsConnector(t, defaultTestConfig(), sink)
	require.NoError(t, conn.Start(context.Background(), nil))
	t.Cleanup(func() { require.NoError(t, conn.Shutdown(context.Background())) })

	td := buildTrace("svc-a", "ns1", "svc-b", "ns1")
	require.NoError(t, conn.ConsumeTraces(context.Background(), td))

	// Edge should be recorded in the edges map.
	conn.edgeMu.RLock()
	_, ok := conn.edges[edgeKey{SourceName: "svc-a", SourceNamespace: "ns1", DestName: "svc-b", DestNamespace: "ns1"}]
	conn.edgeMu.RUnlock()
	assert.True(t, ok)

	// A discovered log record should have been emitted.
	allLogs := sink.AllLogs()
	require.Len(t, allLogs, 1)
	lr := allLogs[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	assert.Equal(t, eventNameDiscovered, lr.Body().Str())
	eventName, _ := lr.Attributes().Get("event.name")
	assert.Equal(t, eventNameDiscovered, eventName.Str())
	srcName, _ := lr.Attributes().Get("source.service.name")
	assert.Equal(t, "svc-a", srcName.Str())
	dstName, _ := lr.Attributes().Get("destination.service.name")
	assert.Equal(t, "svc-b", dstName.Str())
}

func TestLogsConnector_DiscoveredOnlyOnce(t *testing.T) {
	sink := &consumertest.LogsSink{}
	conn := newTestLogsConnector(t, defaultTestConfig(), sink)
	require.NoError(t, conn.Start(context.Background(), nil))
	t.Cleanup(func() { require.NoError(t, conn.Shutdown(context.Background())) })

	// Send the same edge twice — only one discovered event expected.
	td := buildTrace("svc-a", "ns", "svc-b", "ns")
	require.NoError(t, conn.ConsumeTraces(context.Background(), td))
	require.NoError(t, conn.ConsumeTraces(context.Background(), td))

	assert.Len(t, sink.AllLogs(), 1, "discovered event should only be emitted for new edges")
}

func TestLogsConnector_EdgeExpiredEvent(t *testing.T) {
	cfg := defaultTestConfig()
	cfg.EdgeTTL = 50 * time.Millisecond

	sink := &consumertest.LogsSink{}
	conn := newTestLogsConnector(t, cfg, sink)
	require.NoError(t, conn.Start(context.Background(), nil))
	t.Cleanup(func() { require.NoError(t, conn.Shutdown(context.Background())) })

	// Manually insert an already-expired edge.
	k := edgeKey{SourceName: "old-a", DestName: "old-b"}
	conn.edgeMu.Lock()
	conn.edges[k] = time.Now().Add(-(cfg.EdgeTTL + time.Millisecond))
	conn.edgeMu.Unlock()

	conn.evictStaleEdges()

	// Edge should be removed.
	conn.edgeMu.RLock()
	_, stillPresent := conn.edges[k]
	conn.edgeMu.RUnlock()
	assert.False(t, stillPresent)

	// An expired log record should have been emitted.
	allLogs := sink.AllLogs()
	require.Len(t, allLogs, 1)
	lr := allLogs[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	assert.Equal(t, eventNameExpired, lr.Body().Str())
	eventName, _ := lr.Attributes().Get("event.name")
	assert.Equal(t, eventNameExpired, eventName.Str())
}

func TestLogsConnector_DimensionsInDiscoveredEvent(t *testing.T) {
	cfg := defaultTestConfig()
	cfg.Dimensions = []Dimension{
		{Name: "deployment.environment", Default: "unknown"},
	}

	sink := &consumertest.LogsSink{}
	conn := newTestLogsConnector(t, cfg, sink)
	require.NoError(t, conn.Start(context.Background(), nil))
	t.Cleanup(func() { require.NoError(t, conn.Shutdown(context.Background())) })

	td := buildTrace("svc-a", "ns", "svc-b", "ns", "deployment.environment", "staging")
	require.NoError(t, conn.ConsumeTraces(context.Background(), td))

	allLogs := sink.AllLogs()
	require.Len(t, allLogs, 1)
	lr := allLogs[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	env, _ := lr.Attributes().Get("deployment.environment")
	assert.Equal(t, "staging", env.Str())
}
