// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package store // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/topologyconnector/internal/store"

import (
	"container/list"
	"errors"
	"sync"
	"time"
)

var ErrTooManyItems = errors.New("too many items")

// Callback is invoked when an edge is completed or expired.
type Callback func(e *Edge)

// Store is a short-lived cache that correlates client and server span pairs
// within the same trace. When both sides are observed the edge is completed.
// Edges that never find their counterpart are evicted after TTL.
type Store struct {
	l   *list.List
	mtx sync.Mutex
	m   map[Key]*list.Element

	onComplete Callback
	onExpire   Callback

	ttl      time.Duration
	maxItems int
}

// NewStore creates a Store.
func NewStore(ttl time.Duration, maxItems int, onComplete, onExpire Callback) *Store {
	return &Store{
		l:          list.New(),
		m:          make(map[Key]*list.Element),
		onComplete: onComplete,
		onExpire:   onExpire,
		ttl:        ttl,
		maxItems:   maxItems,
	}
}

// Len returns the number of in-flight edges (for testing).
func (s *Store) Len() int {
	return s.l.Len()
}

// UpsertEdge fetches an Edge from the store and updates it using the given
// callback. If the Edge doesn't exist yet a new one is created with the
// default TTL. If the Edge is complete after the update it is removed and
// onComplete is called.
func (s *Store) UpsertEdge(key Key, update Callback) (isNew bool, err error) {
	var completed *Edge
	defer func() {
		if completed != nil {
			s.onComplete(completed)
		}
	}()

	s.mtx.Lock()
	defer s.mtx.Unlock()

	if storedEdge, ok := s.m[key]; ok {
		edge := storedEdge.Value.(*Edge)
		update(edge)
		if edge.isComplete() {
			delete(s.m, key)
			s.l.Remove(storedEdge)
			completed = edge
		}
		return false, nil
	}

	edge := newEdge(key, s.ttl)
	update(edge)

	if edge.isComplete() {
		completed = edge
		return true, nil
	}

	if s.l.Len() >= s.maxItems {
		return false, ErrTooManyItems
	}

	ele := s.l.PushBack(edge)
	s.m[key] = ele
	return true, nil
}

// Expire evicts all expired items from the store.
func (s *Store) Expire() {
	var expired []*Edge
	defer func() {
		for _, e := range expired {
			s.onExpire(e)
		}
	}()

	s.mtx.Lock()
	defer s.mtx.Unlock()

	for {
		head := s.l.Front()
		if head == nil {
			break
		}
		headEdge := head.Value.(*Edge)
		if !headEdge.isExpired() {
			break
		}
		expired = append(expired, headEdge)
		delete(s.m, headEdge.Key)
		s.l.Remove(head)
	}
}
