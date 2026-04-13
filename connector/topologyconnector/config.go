// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package topologyconnector // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/topologyconnector"

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/component"
)

// Config defines the configuration for the topology connector.
type Config struct {
	// StorageID is the component ID of a storage extension used to persist
	// discovered edges across collector restarts. When nil edges are held in
	// memory only and lost on restart.
	StorageID *component.ID `mapstructure:"storage"`

	// EdgeTTL is how long an edge is retained after the last trace that
	// confirmed it. Edges not refreshed within this window are evicted.
	// Default: 168h (7 days).
	EdgeTTL time.Duration `mapstructure:"edge_ttl"`

	// EmitInterval controls how often the full set of known edges is flushed
	// as metrics to the downstream consumer.
	// Default: 60s.
	EmitInterval time.Duration `mapstructure:"emit_interval"`

	// Store configures the in-flight span-pair correlator: the short-lived
	// cache that matches client and server spans within a single trace.
	// This is unrelated to the persistent storage of confirmed edges.
	Store InFlightStoreConfig `mapstructure:"store"`
}

// InFlightStoreConfig controls the short-lived correlator that matches
// client/server span pairs before they are confirmed as topology edges.
type InFlightStoreConfig struct {
	// MaxItems caps the number of unmatched span pairs held at once.
	MaxItems int `mapstructure:"max_items"`
	// TTL is how long to wait for the matching span before expiring an entry.
	TTL time.Duration `mapstructure:"ttl"`
}

func (c *Config) Validate() error {
	if c.EdgeTTL <= 0 {
		return errors.New("edge_ttl must be a positive duration")
	}
	if c.EmitInterval <= 0 {
		return errors.New("emit_interval must be a positive duration")
	}
	if c.Store.MaxItems <= 0 {
		return errors.New("store.max_items must be greater than 0")
	}
	if c.Store.TTL <= 0 {
		return errors.New("store.ttl must be a positive duration")
	}
	return nil
}
