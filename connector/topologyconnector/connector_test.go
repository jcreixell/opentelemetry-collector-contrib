// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package topologyconnector

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/connector/connectortest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/topologyconnector/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/storagetest" // sub-package of extension/storage module
)

func newTestConnector(t *testing.T, cfg *Config, sink *consumertest.MetricsSink) *topologyConnector {
	t.Helper()
	params := connectortest.NewNopSettings(metadata.Type)
	conn, err := newConnector(params, cfg, sink)
	require.NoError(t, err)
	return conn
}

// newTestConnectorWithFixedID creates a connector with a deterministic component ID,
// required when testing storage persistence across two connector instances.
func newTestConnectorWithFixedID(t *testing.T, cfg *Config, sink *consumertest.MetricsSink) *topologyConnector {
	t.Helper()
	params := connector.Settings{
		ID:                component.MustNewID("topology"),
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
	}
	conn, err := newConnector(params, cfg, sink)
	require.NoError(t, err)
	return conn
}

func defaultTestConfig() *Config {
	return &Config{
		EdgeTTL:      time.Hour,
		EmitInterval: time.Minute,
		Store: InFlightStoreConfig{
			MaxItems: 100,
			TTL:      50 * time.Millisecond,
		},
	}
}

// buildTrace creates a minimal trace with a client span from srcService and a
// server span from dstService, linked by traceID/spanID.
func buildTrace(srcService, srcNS, dstService, dstNS string) ptrace.Traces {
	td := ptrace.NewTraces()
	traceID := pcommon.TraceID([16]byte{1})
	spanID := pcommon.SpanID([8]byte{1})

	// Client resource span (source service)
	clientRS := td.ResourceSpans().AppendEmpty()
	clientRS.Resource().Attributes().PutStr("service.name", srcService)
	if srcNS != "" {
		clientRS.Resource().Attributes().PutStr("service.namespace", srcNS)
	}
	clientSpan := clientRS.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	clientSpan.SetKind(ptrace.SpanKindClient)
	clientSpan.SetTraceID(traceID)
	clientSpan.SetSpanID(spanID)

	// Server resource span (destination service)
	serverRS := td.ResourceSpans().AppendEmpty()
	serverRS.Resource().Attributes().PutStr("service.name", dstService)
	if dstNS != "" {
		serverRS.Resource().Attributes().PutStr("service.namespace", dstNS)
	}
	serverSpan := serverRS.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	serverSpan.SetKind(ptrace.SpanKindServer)
	serverSpan.SetTraceID(traceID)
	serverSpan.SetParentSpanID(spanID)

	return td
}

func TestConsumeTraces_EdgeDiscovered(t *testing.T) {
	sink := &consumertest.MetricsSink{}
	conn := newTestConnector(t, defaultTestConfig(), sink)
	host := storagetest.NewStorageHost()
	require.NoError(t, conn.Start(context.Background(), host))
	t.Cleanup(func() { require.NoError(t, conn.Shutdown(context.Background())) })

	td := buildTrace("service-a", "ns1", "service-b", "ns1")
	require.NoError(t, conn.ConsumeTraces(context.Background(), td))

	conn.edgeMu.RLock()
	_, ok := conn.edges[edgeKey{SourceName: "service-a", SourceNamespace: "ns1", DestName: "service-b", DestNamespace: "ns1"}]
	conn.edgeMu.RUnlock()
	assert.True(t, ok, "edge should be recorded after matching client+server spans")
}

func TestConsumeTraces_NamespaceDefaults(t *testing.T) {
	sink := &consumertest.MetricsSink{}
	conn := newTestConnector(t, defaultTestConfig(), sink)
	host := storagetest.NewStorageHost()
	require.NoError(t, conn.Start(context.Background(), host))
	t.Cleanup(func() { require.NoError(t, conn.Shutdown(context.Background())) })

	// No service.namespace set — should default to empty string
	td := buildTrace("svc-a", "", "svc-b", "")
	require.NoError(t, conn.ConsumeTraces(context.Background(), td))

	conn.edgeMu.RLock()
	_, ok := conn.edges[edgeKey{SourceName: "svc-a", SourceNamespace: "", DestName: "svc-b", DestNamespace: ""}]
	conn.edgeMu.RUnlock()
	assert.True(t, ok)
}

func TestConsumeTraces_IncompleteSpanPair(t *testing.T) {
	sink := &consumertest.MetricsSink{}
	conn := newTestConnector(t, defaultTestConfig(), sink)
	host := storagetest.NewStorageHost()
	require.NoError(t, conn.Start(context.Background(), host))
	t.Cleanup(func() { require.NoError(t, conn.Shutdown(context.Background())) })

	// Only a client span, no server counterpart
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "orphan-client")
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetKind(ptrace.SpanKindClient)
	span.SetTraceID(pcommon.TraceID([16]byte{99}))
	span.SetSpanID(pcommon.SpanID([8]byte{99}))

	require.NoError(t, conn.ConsumeTraces(context.Background(), td))

	// Wait for TTL to expire then trigger eviction
	time.Sleep(100 * time.Millisecond)
	conn.inFlight.Expire()

	conn.edgeMu.RLock()
	edgeCount := len(conn.edges)
	conn.edgeMu.RUnlock()
	assert.Equal(t, 0, edgeCount, "incomplete span pair should not result in an edge")
}

func TestEmitMetrics_EmitsGauge(t *testing.T) {
	sink := &consumertest.MetricsSink{}
	conn := newTestConnector(t, defaultTestConfig(), sink)
	host := storagetest.NewStorageHost()
	require.NoError(t, conn.Start(context.Background(), host))
	t.Cleanup(func() { require.NoError(t, conn.Shutdown(context.Background())) })

	k := edgeKey{SourceName: "svc-a", SourceNamespace: "ns", DestName: "svc-b", DestNamespace: "ns"}
	conn.edgeMu.Lock()
	conn.edges[k] = time.Now()
	conn.edgeMu.Unlock()

	require.NoError(t, conn.emitMetrics(context.Background()))

	allMetrics := sink.AllMetrics()
	require.Len(t, allMetrics, 1)

	rm := allMetrics[0].ResourceMetrics().At(0)
	sm := rm.ScopeMetrics().At(0)
	require.Equal(t, 1, sm.Metrics().Len())
	m := sm.Metrics().At(0)
	assert.Equal(t, metricName, m.Name())

	dp := m.Gauge().DataPoints().At(0)
	assert.Equal(t, int64(1), dp.IntValue())
	srcName, _ := dp.Attributes().Get("source_service_name")
	assert.Equal(t, "svc-a", srcName.Str())
	dstName, _ := dp.Attributes().Get("destination_service_name")
	assert.Equal(t, "svc-b", dstName.Str())
}

func TestEmitMetrics_SkipsEmpty(t *testing.T) {
	sink := &consumertest.MetricsSink{}
	conn := newTestConnector(t, defaultTestConfig(), sink)
	host := storagetest.NewStorageHost()
	require.NoError(t, conn.Start(context.Background(), host))
	t.Cleanup(func() { require.NoError(t, conn.Shutdown(context.Background())) })

	require.NoError(t, conn.emitMetrics(context.Background()))
	assert.Len(t, sink.AllMetrics(), 0)
}

func TestTTLEviction(t *testing.T) {
	cfg := defaultTestConfig()
	cfg.EdgeTTL = 100 * time.Millisecond

	sink := &consumertest.MetricsSink{}
	conn := newTestConnector(t, cfg, sink)
	host := storagetest.NewStorageHost()
	require.NoError(t, conn.Start(context.Background(), host))
	t.Cleanup(func() { require.NoError(t, conn.Shutdown(context.Background())) })

	k := edgeKey{SourceName: "old-src", DestName: "old-dst"}
	conn.edgeMu.Lock()
	conn.edges[k] = time.Now().Add(-(cfg.EdgeTTL + time.Millisecond))
	conn.edgeMu.Unlock()

	conn.evictStaleEdges()

	conn.edgeMu.RLock()
	_, stillPresent := conn.edges[k]
	conn.edgeMu.RUnlock()
	assert.False(t, stillPresent, "edge past TTL should be evicted")
}

func TestStoragePersistence(t *testing.T) {
	storageDir := t.TempDir()
	storageHost := storagetest.NewStorageHost().WithFileBackedStorageExtension("test", storageDir)
	storageID := storagetest.NewStorageID("test")

	cfg := defaultTestConfig()
	cfg.StorageID = &storageID

	// First connector instance: discover an edge then shut down (persists)
	sink1 := &consumertest.MetricsSink{}
	conn1 := newTestConnectorWithFixedID(t, cfg, sink1)
	require.NoError(t, conn1.Start(context.Background(), storageHost))
	require.NoError(t, conn1.ConsumeTraces(context.Background(), buildTrace("a", "ns", "b", "ns")))
	require.NoError(t, conn1.Shutdown(context.Background()))

	// Second connector instance with the same ID: should reload the persisted edge
	sink2 := &consumertest.MetricsSink{}
	conn2 := newTestConnectorWithFixedID(t, cfg, sink2)
	require.NoError(t, conn2.Start(context.Background(), storageHost))
	t.Cleanup(func() { require.NoError(t, conn2.Shutdown(context.Background())) })

	conn2.edgeMu.RLock()
	_, ok := conn2.edges[edgeKey{SourceName: "a", SourceNamespace: "ns", DestName: "b", DestNamespace: "ns"}]
	conn2.edgeMu.RUnlock()
	assert.True(t, ok, "edge should be reloaded from storage after restart")
}

func TestStoragePersistence_ExpiredAtLoad(t *testing.T) {
	storageDir := t.TempDir()
	storageHost := storagetest.NewStorageHost().WithFileBackedStorageExtension("test2", storageDir)
	storageID := storagetest.NewStorageID("test2")

	cfg := defaultTestConfig()
	cfg.StorageID = &storageID
	cfg.EdgeTTL = 100 * time.Millisecond

	// Write an already-expired edge directly
	sink1 := &consumertest.MetricsSink{}
	conn1 := newTestConnectorWithFixedID(t, cfg, sink1)
	require.NoError(t, conn1.Start(context.Background(), storageHost))

	k := edgeKey{SourceName: "stale-src", DestName: "stale-dst"}
	conn1.edgeMu.Lock()
	conn1.edges[k] = time.Now().Add(-(cfg.EdgeTTL + time.Second))
	conn1.edgeMu.Unlock()
	require.NoError(t, conn1.Shutdown(context.Background()))

	// Second instance should not reload expired edge
	sink2 := &consumertest.MetricsSink{}
	conn2 := newTestConnectorWithFixedID(t, cfg, sink2)
	require.NoError(t, conn2.Start(context.Background(), storageHost))
	t.Cleanup(func() { require.NoError(t, conn2.Shutdown(context.Background())) })

	conn2.edgeMu.RLock()
	_, ok := conn2.edges[k]
	conn2.edgeMu.RUnlock()
	assert.False(t, ok, "expired edge should not be reloaded from storage")
}

func TestStorageNil_NoError(t *testing.T) {
	sink := &consumertest.MetricsSink{}
	cfg := defaultTestConfig()
	// StorageID is nil — should work fine without any storage extension

	conn := newTestConnector(t, cfg, sink)
	host := storagetest.NewStorageHost() // no storage extension registered
	require.NoError(t, conn.Start(context.Background(), host))
	require.NoError(t, conn.ConsumeTraces(context.Background(), buildTrace("x", "", "y", "")))
	require.NoError(t, conn.Shutdown(context.Background()))
}

func TestGetStorageClient_NotFound(t *testing.T) {
	storageID := component.MustNewIDWithName("filestorage", "missing")
	host := storagetest.NewStorageHost() // no extension registered
	_, err := getStorageClient(context.Background(), host, &storageID, component.MustNewID("topology"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestGetStorageClient_NotStorageExtension(t *testing.T) {
	host := storagetest.NewStorageHost().WithNonStorageExtension("notstorage")
	nonStorageID := storagetest.NewNonStorageID("notstorage")
	_, err := getStorageClient(context.Background(), host, &nonStorageID, component.MustNewID("topology"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not implement storage.Extension")
}
