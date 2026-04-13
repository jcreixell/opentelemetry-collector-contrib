// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package topologyconnector

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/connector/connectortest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pipeline"

	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/topologyconnector/internal/metadata"
)

func TestFactoryType(t *testing.T) {
	assert.Equal(t, metadata.Type, NewFactory().Type())
}

func TestFactoryDefaultConfig(t *testing.T) {
	cfg := NewFactory().CreateDefaultConfig()
	require.NoError(t, componenttest.CheckConfigStruct(cfg))
	require.NoError(t, cfg.(*Config).Validate())
}

func TestFactoryCreateTracesToMetrics(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	params := connectortest.NewNopSettings(metadata.Type)
	sink := &consumertest.MetricsSink{}

	conn, err := factory.CreateTracesToMetrics(context.Background(), params, cfg, sink)
	require.NoError(t, err)

	host := componenttest.NewNopHost()
	require.NoError(t, conn.Start(context.Background(), host))
	require.NoError(t, conn.Shutdown(context.Background()))
}

func TestFactoryCreateTracesToMetrics_WithRouter(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	params := connectortest.NewNopSettings(metadata.Type)

	router := connector.NewMetricsRouter(map[pipeline.ID]consumer.Metrics{
		pipeline.NewID(pipeline.SignalMetrics): &consumertest.MetricsSink{},
	})
	_, err := factory.CreateTracesToMetrics(context.Background(), params, cfg, router)
	require.NoError(t, err)
}

func TestFactoryCreateTracesToLogs(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	params := connectortest.NewNopSettings(metadata.Type)
	sink := &consumertest.LogsSink{}

	conn, err := factory.CreateTracesToLogs(context.Background(), params, cfg, sink)
	require.NoError(t, err)

	host := componenttest.NewNopHost()
	require.NoError(t, conn.Start(context.Background(), host))
	require.NoError(t, conn.Shutdown(context.Background()))
}
