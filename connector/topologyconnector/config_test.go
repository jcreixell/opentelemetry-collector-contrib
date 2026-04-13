// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package topologyconnector

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *Config
		wantErr string
	}{
		{
			name: "valid",
			cfg: &Config{
				EdgeTTL:      7 * 24 * time.Hour,
				EmitInterval: 60 * time.Second,
				Store:        InFlightStoreConfig{MaxItems: 1000, TTL: 2 * time.Second},
			},
		},
		{
			name: "zero EdgeTTL",
			cfg: &Config{
				EdgeTTL:      0,
				EmitInterval: 60 * time.Second,
				Store:        InFlightStoreConfig{MaxItems: 1000, TTL: 2 * time.Second},
			},
			wantErr: "edge_ttl must be a positive duration",
		},
		{
			name: "zero EmitInterval",
			cfg: &Config{
				EdgeTTL:      time.Hour,
				EmitInterval: 0,
				Store:        InFlightStoreConfig{MaxItems: 1000, TTL: 2 * time.Second},
			},
			wantErr: "emit_interval must be a positive duration",
		},
		{
			name: "zero MaxItems",
			cfg: &Config{
				EdgeTTL:      time.Hour,
				EmitInterval: time.Minute,
				Store:        InFlightStoreConfig{MaxItems: 0, TTL: 2 * time.Second},
			},
			wantErr: "store.max_items must be greater than 0",
		},
		{
			name: "zero Store TTL",
			cfg: &Config{
				EdgeTTL:      time.Hour,
				EmitInterval: time.Minute,
				Store:        InFlightStoreConfig{MaxItems: 100, TTL: 0},
			},
			wantErr: "store.ttl must be a positive duration",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConfigDefaults(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	require.NoError(t, cfg.Validate())
	assert.Equal(t, 7*24*time.Hour, cfg.EdgeTTL)
	assert.Equal(t, 60*time.Second, cfg.EmitInterval)
	assert.Equal(t, 1000, cfg.Store.MaxItems)
	assert.Equal(t, 2*time.Second, cfg.Store.TTL)
	assert.Nil(t, cfg.StorageID)
}
