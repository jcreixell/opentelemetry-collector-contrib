// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate make mdatagen

package topologyconnector // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/topologyconnector"

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"

	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/topologyconnector/internal/metadata"
)

// NewFactory returns a ConnectorFactory.
func NewFactory() connector.Factory {
	return connector.NewFactory(
		metadata.Type,
		createDefaultConfig,
		connector.WithTracesToMetrics(createTracesToMetricsConnector, metadata.TracesToMetricsStability),
		connector.WithTracesToLogs(createTracesToLogsConnector, metadata.TracesToLogsStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		EdgeTTL:      7 * 24 * time.Hour,
		EmitInterval: 60 * time.Second,
		Store: InFlightStoreConfig{
			MaxItems: 1000,
			TTL:      2 * time.Second,
		},
	}
}

func createTracesToMetricsConnector(
	_ context.Context,
	params connector.Settings,
	cfg component.Config,
	next consumer.Metrics,
) (connector.Traces, error) {
	return newConnector(params, cfg.(*Config), next)
}

func createTracesToLogsConnector(
	_ context.Context,
	params connector.Settings,
	cfg component.Config,
	next consumer.Logs,
) (connector.Traces, error) {
	return newLogsConnector(params, cfg.(*Config), next), nil
}
