// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package topologyconnector // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/topologyconnector"

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	conventions "go.opentelemetry.io/otel/semconv/v1.38.0"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/topologyconnector/internal/metadata"
	internalstore "github.com/open-telemetry/opentelemetry-collector-contrib/connector/topologyconnector/internal/store"
)

const (
	eventNameDiscovered = "topology.edge.discovered"
	eventNameExpired    = "topology.edge.expired"
)

var _ connector.Traces = (*logsConnector)(nil)

type logsConnector struct {
	config *Config
	logger *zap.Logger
	next   consumer.Logs

	inFlight *internalstore.Store

	edgeMu sync.RWMutex
	edges  map[edgeKey]time.Time

	shutdownCh chan struct{}
	wg         sync.WaitGroup
}

func newLogsConnector(params connector.Settings, cfg *Config, next consumer.Logs) *logsConnector {
	return &logsConnector{
		config:     cfg,
		logger:     params.Logger,
		next:       next,
		edges:      make(map[edgeKey]time.Time),
		shutdownCh: make(chan struct{}),
	}
}

func (c *logsConnector) Start(_ context.Context, _ component.Host) error {
	c.inFlight = internalstore.NewStore(
		c.config.Store.TTL,
		c.config.Store.MaxItems,
		c.onEdgeComplete,
		func(_ *internalstore.Edge) {}, // incomplete pairs are not lifecycle events
	)

	c.wg.Add(2)
	go c.ttlEvictionLoop()
	go c.storeExpirationLoop()

	c.logger.Info("started topologyconnector (logs)")
	return nil
}

func (c *logsConnector) Shutdown(_ context.Context) error {
	c.logger.Info("shutting down topologyconnector (logs)")
	close(c.shutdownCh)
	c.wg.Wait()
	return nil
}

func (*logsConnector) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (c *logsConnector) ConsumeTraces(_ context.Context, td ptrace.Traces) error {
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rSpans := rss.At(i)
		rAttrs := rSpans.Resource().Attributes()

		serviceName, ok := getStringAttr(rAttrs, string(conventions.ServiceNameKey))
		if !ok {
			continue
		}
		serviceNamespace, _ := getStringAttr(rAttrs, string(conventions.ServiceNamespaceKey))

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
						c.logger.Warn("dropped span: in-flight store is full", zap.Error(err))
					}
				case ptrace.SpanKindServer, ptrace.SpanKindConsumer:
					key := internalstore.NewKey(span.TraceID(), span.ParentSpanID())
					if _, err := c.inFlight.UpsertEdge(key, func(e *internalstore.Edge) {
						e.ServerService = serviceName
						e.ServerNamespace = serviceNamespace
					}); err != nil {
						c.logger.Warn("dropped span: in-flight store is full", zap.Error(err))
					}
				}
			}
		}
	}
	return nil
}

func (c *logsConnector) onEdgeComplete(e *internalstore.Edge) {
	k := edgeKey{
		SourceName:      e.ClientService,
		SourceNamespace: e.ClientNamespace,
		DestName:        e.ServerService,
		DestNamespace:   e.ServerNamespace,
		Dimensions:      encodeDimensions(c.config.Dimensions, e.Dimensions),
	}

	c.edgeMu.Lock()
	_, exists := c.edges[k]
	c.edges[k] = time.Now()
	c.edgeMu.Unlock()

	if exists {
		return
	}

	ld := c.buildLogRecord(eventNameDiscovered, k, plog.SeverityNumberInfo)
	if err := c.next.ConsumeLogs(context.Background(), ld); err != nil {
		c.logger.Error("failed to emit edge discovered event", zap.Error(err))
	}
}

func (c *logsConnector) ttlEvictionLoop() {
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

func (c *logsConnector) storeExpirationLoop() {
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

func (c *logsConnector) evictStaleEdges() {
	cutoff := time.Now().Add(-c.config.EdgeTTL)
	var expired []edgeKey

	c.edgeMu.Lock()
	for k, lastSeen := range c.edges {
		if lastSeen.Before(cutoff) {
			delete(c.edges, k)
			expired = append(expired, k)
		}
	}
	c.edgeMu.Unlock()

	for _, k := range expired {
		ld := c.buildLogRecord(eventNameExpired, k, plog.SeverityNumberInfo)
		if err := c.next.ConsumeLogs(context.Background(), ld); err != nil {
			c.logger.Error("failed to emit edge expired event", zap.Error(err))
		}
	}
}

func (c *logsConnector) buildLogRecord(eventName string, k edgeKey, severity plog.SeverityNumber) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName(metadata.ScopeName)

	lr := sl.LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	lr.SetSeverityNumber(severity)
	lr.Body().SetStr(eventName)

	attrs := lr.Attributes()
	attrs.PutStr("event.name", eventName)
	attrs.PutStr("source.service.name", k.SourceName)
	attrs.PutStr("source.service.namespace", k.SourceNamespace)
	attrs.PutStr("destination.service.name", k.DestName)
	attrs.PutStr("destination.service.namespace", k.DestNamespace)

	// Emit configured extra dimensions.
	if k.Dimensions != "" {
		vals := splitDimensions(k.Dimensions)
		for i, d := range c.config.Dimensions {
			if i < len(vals) {
				attrs.PutStr(d.Name, vals[i])
			}
		}
	}

	return ld
}
