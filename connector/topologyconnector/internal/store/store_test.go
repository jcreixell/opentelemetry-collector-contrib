// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func newKey(b byte) Key {
	return NewKey(pcommon.TraceID([16]byte{b}), pcommon.SpanID([8]byte{b}))
}

func TestStore_Complete(t *testing.T) {
	var completed []*Edge
	s := NewStore(time.Second, 100,
		func(e *Edge) { completed = append(completed, e) },
		func(_ *Edge) {},
	)

	key := newKey(1)

	// First upsert: client side
	isNew, err := s.UpsertEdge(key, func(e *Edge) {
		e.ClientService = "svc-a"
		e.ClientNamespace = "ns"
	})
	require.NoError(t, err)
	assert.True(t, isNew)
	assert.Equal(t, 1, s.Len())

	// Second upsert: server side — should complete the edge
	isNew, err = s.UpsertEdge(key, func(e *Edge) {
		e.ServerService = "svc-b"
		e.ServerNamespace = "ns"
	})
	require.NoError(t, err)
	assert.False(t, isNew)
	assert.Equal(t, 0, s.Len(), "completed edge should be removed from store")

	require.Len(t, completed, 1)
	assert.Equal(t, "svc-a", completed[0].ClientService)
	assert.Equal(t, "svc-b", completed[0].ServerService)
}

func TestStore_CompleteOnFirstUpsert(t *testing.T) {
	// If both sides arrive in a single upsert (e.g. same-process span), it
	// should complete immediately without being added to the list.
	var completed []*Edge
	s := NewStore(time.Second, 100,
		func(e *Edge) { completed = append(completed, e) },
		func(_ *Edge) {},
	)

	key := newKey(2)
	isNew, err := s.UpsertEdge(key, func(e *Edge) {
		e.ClientService = "svc-a"
		e.ClientNamespace = "ns"
		e.ServerService = "svc-b"
		e.ServerNamespace = "ns"
	})
	require.NoError(t, err)
	assert.True(t, isNew)
	assert.Equal(t, 0, s.Len(), "immediately complete edge should never enter the list")
	assert.Len(t, completed, 1)
}

func TestStore_Expire(t *testing.T) {
	var expired []*Edge
	s := NewStore(10*time.Millisecond, 100,
		func(_ *Edge) {},
		func(e *Edge) { expired = append(expired, e) },
	)

	key := newKey(3)
	_, err := s.UpsertEdge(key, func(e *Edge) {
		e.ClientService = "orphan"
	})
	require.NoError(t, err)
	assert.Equal(t, 1, s.Len())

	time.Sleep(20 * time.Millisecond)
	s.Expire()

	assert.Equal(t, 0, s.Len())
	require.Len(t, expired, 1)
	assert.Equal(t, "orphan", expired[0].ClientService)
}

func TestStore_MaxItems(t *testing.T) {
	s := NewStore(time.Second, 2,
		func(_ *Edge) {},
		func(_ *Edge) {},
	)

	_, err := s.UpsertEdge(newKey(1), func(e *Edge) { e.ClientService = "a" })
	require.NoError(t, err)
	_, err = s.UpsertEdge(newKey(2), func(e *Edge) { e.ClientService = "b" })
	require.NoError(t, err)

	// Third entry should be rejected
	_, err = s.UpsertEdge(newKey(3), func(e *Edge) { e.ClientService = "c" })
	assert.ErrorIs(t, err, ErrTooManyItems)
	assert.Equal(t, 2, s.Len())
}

func TestStore_UpdateExisting(t *testing.T) {
	s := NewStore(time.Second, 100,
		func(_ *Edge) {},
		func(_ *Edge) {},
	)

	key := newKey(4)
	_, err := s.UpsertEdge(key, func(e *Edge) { e.ClientService = "svc-a" })
	require.NoError(t, err)

	// Upsert same key again (same side) — should not add a duplicate
	isNew, err := s.UpsertEdge(key, func(e *Edge) { e.ClientService = "svc-a-updated" })
	require.NoError(t, err)
	assert.False(t, isNew)
	assert.Equal(t, 1, s.Len())
}
