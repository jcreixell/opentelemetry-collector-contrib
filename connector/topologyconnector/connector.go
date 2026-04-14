// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package topologyconnector // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/topologyconnector"

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	conventions "go.opentelemetry.io/otel/semconv/v1.38.0"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/topologyconnector/internal/metadata"
	internalstore "github.com/open-telemetry/opentelemetry-collector-contrib/connector/topologyconnector/internal/store"
)

const (
	storageEdgesKey = "topology_edges"
	metricName      = "topology_edge"
	metricDesc      = "Presence of a topology edge between two services (value is always 1)"
)

// edgeKey uniquely identifies a directed service-to-service edge.
// Dimensions holds encoded extra dimension values in config order.
type edgeKey struct {
	SourceName      string
	SourceNamespace string
	DestName        string
	DestNamespace   string
	Dimensions      string
}

// encode returns a NUL-separated string suitable as a map key or storage key.
func (k edgeKey) encode() string {
	if k.Dimensions == "" {
		return strings.Join([]string{k.SourceName, k.SourceNamespace, k.DestName, k.DestNamespace}, "\x00")
	}
	return strings.Join([]string{k.SourceName, k.SourceNamespace, k.DestName, k.DestNamespace, k.Dimensions}, "\x00")
}

func decodeEdgeKey(s string) (edgeKey, error) {
	// SplitN with 5 so that the Dimensions field (which may itself contain
	// NULs when multiple dimensions are configured) is kept intact as a
	// single string in parts[4].
	parts := strings.SplitN(s, "\x00", 5)
	if len(parts) < 4 {
		return edgeKey{}, fmt.Errorf("malformed edge key: %q", s)
	}
	k := edgeKey{SourceName: parts[0], SourceNamespace: parts[1], DestName: parts[2], DestNamespace: parts[3]}
	if len(parts) == 5 {
		k.Dimensions = parts[4]
	}
	return k, nil
}

// encodeDimensions returns the NUL-joined dimension values for the given edge
// in the order of the configured dimensions.
func encodeDimensions(dims []Dimension, values map[string]string) string {
	if len(dims) == 0 {
		return ""
	}
	parts := make([]string, len(dims))
	for i, d := range dims {
		if v, ok := values[d.Name]; ok {
			parts[i] = v
		} else {
			parts[i] = d.Default
		}
	}
	return strings.Join(parts, "\x00")
}

// splitDimensions splits the NUL-joined dimension string back into individual values.
func splitDimensions(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, "\x00")
}

// persistedEdge is the on-disk representation of a topology edge.
type persistedEdge struct {
	LastSeenUnix int64 `json:"last_seen_unix"`
}

var _ connector.Traces = (*topologyConnector)(nil)

type topologyConnector struct {
	config *Config
	logger *zap.Logger
	id     component.ID
	next   consumer.Metrics

	inFlight *internalstore.Store

	edgeMu sync.RWMutex
	edges  map[edgeKey]time.Time // confirmed edge → last seen time

	storageClient storage.Client

	shutdownCh chan struct{}
	wg         sync.WaitGroup

	telemetry *metadata.TelemetryBuilder
}

func newConnector(params connector.Settings, cfg *Config, next consumer.Metrics) (*topologyConnector, error) {
	tb, err := metadata.NewTelemetryBuilder(params.TelemetrySettings)
	if err != nil {
		return nil, err
	}
	return &topologyConnector{
		config:        cfg,
		logger:        params.Logger,
		id:            params.ID,
		next:          next,
		edges:         make(map[edgeKey]time.Time),
		shutdownCh:    make(chan struct{}),
		telemetry:     tb,
		storageClient: storage.NewNopClient(),
	}, nil
}

func (c *topologyConnector) Start(ctx context.Context, host component.Host) error {
	storageClient, err := getStorageClient(ctx, host, c.config.StorageID, c.id)
	if err != nil {
		return fmt.Errorf("failed to get storage client: %w", err)
	}
	c.storageClient = storageClient

	if err := c.loadEdges(ctx); err != nil {
		c.logger.Warn("failed to load persisted edges, starting with empty state", zap.Error(err))
	}

	c.inFlight = internalstore.NewStore(
		c.config.Store.TTL,
		c.config.Store.MaxItems,
		c.onEdgeComplete,
		c.onEdgeExpire,
	)

	c.wg.Add(3)
	go c.emitLoop()
	go c.ttlEvictionLoop()
	go c.storeExpirationLoop()

	c.logger.Info("started topologyconnector")
	return nil
}

func (c *topologyConnector) Shutdown(ctx context.Context) error {
	c.logger.Info("shutting down topologyconnector")
	close(c.shutdownCh)
	c.wg.Wait()

	if err := c.saveEdges(ctx); err != nil {
		c.logger.Error("failed to persist edges on shutdown", zap.Error(err))
	}
	return c.storageClient.Close(ctx)
}

func (*topologyConnector) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (c *topologyConnector) ConsumeTraces(_ context.Context, td ptrace.Traces) error {
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rSpans := rss.At(i)
		rAttrs := rSpans.Resource().Attributes()

		serviceName, ok := getStringAttr(rAttrs, string(conventions.ServiceNameKey))
		if !ok {
			continue
		}
		serviceNamespace, _ := getStringAttr(rAttrs, string(conventions.ServiceNamespaceKey))

		// Capture configured extra dimensions from the resource attributes.
		var dims map[string]string
		if len(c.config.Dimensions) > 0 {
			dims = make(map[string]string, len(c.config.Dimensions))
			for _, d := range c.config.Dimensions {
				if v, ok := getStringAttr(rAttrs, d.Name); ok {
					dims[d.Name] = v
				}
			}
		}

		for j := 0; j < rSpans.ScopeSpans().Len(); j++ {
			spans := rSpans.ScopeSpans().At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				switch span.Kind() {
				case ptrace.SpanKindClient, ptrace.SpanKindProducer:
					key := internalstore.NewKey(span.TraceID(), span.SpanID())
					if _, err := c.inFlight.UpsertEdge(key, func(e *internalstore.Edge) {
						e.ClientService = serviceName
						e.ClientNamespace = serviceNamespace
						for k, v := range dims {
							e.Dimensions[k] = v
						}
					}); err != nil {
						c.telemetry.ConnectorTopologyDroppedSpans.Add(context.Background(), 1)
					}
				case ptrace.SpanKindServer, ptrace.SpanKindConsumer:
					key := internalstore.NewKey(span.TraceID(), span.ParentSpanID())
					if _, err := c.inFlight.UpsertEdge(key, func(e *internalstore.Edge) {
						e.ServerService = serviceName
						e.ServerNamespace = serviceNamespace
					}); err != nil {
						c.telemetry.ConnectorTopologyDroppedSpans.Add(context.Background(), 1)
					}
				}
			}
		}
	}
	return nil
}

func (c *topologyConnector) onEdgeComplete(e *internalstore.Edge) {
	k := edgeKey{
		SourceName:      e.ClientService,
		SourceNamespace: e.ClientNamespace,
		DestName:        e.ServerService,
		DestNamespace:   e.ServerNamespace,
		Dimensions:      encodeDimensions(c.config.Dimensions, e.Dimensions),
	}
	isNew := false
	c.edgeMu.Lock()
	if _, exists := c.edges[k]; !exists {
		isNew = true
	}
	c.edges[k] = time.Now()
	c.edgeMu.Unlock()

	if isNew {
		c.telemetry.ConnectorTopologyTotalEdges.Add(context.Background(), 1)
		c.logger.Debug("new topology edge discovered",
			zap.String("src", e.ClientService),
			zap.String("src_ns", e.ClientNamespace),
			zap.String("dst", e.ServerService),
			zap.String("dst_ns", e.ServerNamespace),
		)
	}
}

// onEdgeExpire is called when a span pair times out without completing.
func (c *topologyConnector) onEdgeExpire(_ *internalstore.Edge) {
	c.telemetry.ConnectorTopologyExpiredEdges.Add(context.Background(), 1)
}

func (c *topologyConnector) emitLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.config.EmitInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := c.emitMetrics(context.Background()); err != nil {
				c.logger.Error("failed to emit topology metrics", zap.Error(err))
			}
		case <-c.shutdownCh:
			return
		}
	}
}

func (c *topologyConnector) ttlEvictionLoop() {
	defer c.wg.Done()
	interval := c.config.EdgeTTL / 10
	if interval < time.Minute {
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.evictStaleEdges()
		case <-c.shutdownCh:
			return
		}
	}
}

func (c *topologyConnector) storeExpirationLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.config.Store.TTL)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.inFlight.Expire()
		case <-c.shutdownCh:
			return
		}
	}
}

func (c *topologyConnector) emitMetrics(ctx context.Context) error {
	c.edgeMu.RLock()
	if len(c.edges) == 0 {
		c.edgeMu.RUnlock()
		return nil
	}
	snapshot := make(map[edgeKey]struct{}, len(c.edges))
	for k := range c.edges {
		snapshot[k] = struct{}{}
	}
	c.edgeMu.RUnlock()

	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	sm.Scope().SetName(metadata.ScopeName)

	m := sm.Metrics().AppendEmpty()
	m.SetName(metricName)
	m.SetDescription(metricDesc)
	m.SetUnit("")
	g := m.SetEmptyGauge()

	now := pcommon.NewTimestampFromTime(time.Now())
	for k := range snapshot {
		dp := g.DataPoints().AppendEmpty()
		dp.SetTimestamp(now)
		dp.SetIntValue(1)
		dp.Attributes().PutStr("source_service_name", k.SourceName)
		dp.Attributes().PutStr("source_service_namespace", k.SourceNamespace)
		dp.Attributes().PutStr("destination_service_name", k.DestName)
		dp.Attributes().PutStr("destination_service_namespace", k.DestNamespace)
		// Emit configured extra dimensions.
		if k.Dimensions != "" {
			vals := splitDimensions(k.Dimensions)
			for i, d := range c.config.Dimensions {
				if i < len(vals) {
					dp.Attributes().PutStr(d.Name, vals[i])
				}
			}
		}
	}

	return c.next.ConsumeMetrics(ctx, md)
}

func (c *topologyConnector) evictStaleEdges() {
	cutoff := time.Now().Add(-c.config.EdgeTTL)
	var evicted []edgeKey

	c.edgeMu.Lock()
	for k, lastSeen := range c.edges {
		if lastSeen.Before(cutoff) {
			delete(c.edges, k)
			evicted = append(evicted, k)
		}
	}
	c.edgeMu.Unlock()

	for _, k := range evicted {
		c.logger.Debug("evicted stale topology edge",
			zap.String("src", k.SourceName),
			zap.String("dst", k.DestName),
		)
	}
}

func (c *topologyConnector) loadEdges(ctx context.Context) error {
	data, err := c.storageClient.Get(ctx, storageEdgesKey)
	if err != nil {
		return fmt.Errorf("storage get: %w", err)
	}
	if len(data) == 0 {
		return nil
	}

	var raw map[string]persistedEdge
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("unmarshal edges: %w", err)
	}

	cutoff := time.Now().Add(-c.config.EdgeTTL)
	c.edgeMu.Lock()
	defer c.edgeMu.Unlock()
	for encoded, pe := range raw {
		lastSeen := time.Unix(pe.LastSeenUnix, 0)
		if lastSeen.Before(cutoff) {
			continue
		}
		k, err := decodeEdgeKey(encoded)
		if err != nil {
			c.logger.Warn("skipping malformed persisted edge", zap.Error(err))
			continue
		}
		c.edges[k] = lastSeen
	}
	c.logger.Info("loaded topology edges from storage", zap.Int("count", len(c.edges)))
	return nil
}

func (c *topologyConnector) saveEdges(ctx context.Context) error {
	c.edgeMu.RLock()
	raw := make(map[string]persistedEdge, len(c.edges))
	for k, lastSeen := range c.edges {
		raw[k.encode()] = persistedEdge{LastSeenUnix: lastSeen.Unix()}
	}
	c.edgeMu.RUnlock()

	data, err := json.Marshal(raw)
	if err != nil {
		return fmt.Errorf("marshal edges: %w", err)
	}
	return c.storageClient.Set(ctx, storageEdgesKey, data)
}

// getStorageClient retrieves a storage.Client from the host extensions.
func getStorageClient(ctx context.Context, host component.Host, storageID *component.ID, id component.ID) (storage.Client, error) {
	if storageID == nil {
		return storage.NewNopClient(), nil
	}
	ext, ok := host.GetExtensions()[*storageID]
	if !ok {
		return nil, fmt.Errorf("storage extension %q not found", storageID)
	}
	storageExt, ok := ext.(storage.Extension)
	if !ok {
		return nil, fmt.Errorf("extension %q does not implement storage.Extension", storageID)
	}
	return storageExt.GetClient(ctx, component.KindConnector, id, "")
}

func getStringAttr(attrs pcommon.Map, key string) (string, bool) {
	v, ok := attrs.Get(key)
	if !ok || v.Type() != pcommon.ValueTypeStr {
		return "", false
	}
	return v.Str(), true
}
