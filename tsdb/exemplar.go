// Copyright 2020 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tsdb

import (
	"context"
	"sync"

	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/pkg/exemplar"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/relabel"
	"github.com/prometheus/prometheus/storage"
)

type exemplarList struct {
	next     int
	previous int
	oldest   int
	list     []exemplar.Exemplar
}

// implements storage.ExemplarStorage
type InMemExemplarStorage struct {
	exemplars map[string]*exemplarList
	len       int
}

func newExemplarList(len int) *exemplarList {
	return &exemplarList{
		list: make([]exemplar.Exemplar, 0, len),
	}
}

func (el *exemplarList) add(e exemplar.Exemplar) error {
	// TODO: Don't add an exemplar if we already have it
	if len(el.list) != 0 && el.list[el.previous].Equals(e) {
		return storage.ErrDuplicateExemplar
	}

	if len(el.list) < cap(el.list) {
		el.list = append(el.list, e)
		el.next++
		if el.next >= cap(el.list) {
			el.next = 0
		}
		return nil
	}

	el.list[el.next] = e
	el.previous = el.next
	el.next++
	if el.next >= len(el.list) {
		el.next = 0
	}
	el.oldest++
	if el.oldest >= len(el.list) {
		el.oldest = 0
	}
	return nil
}

func (el *exemplarList) sorted() []exemplar.Exemplar {
	ret := make([]exemplar.Exemplar, 0, cap(el.list))
	// Return exemplars as a new slice from oldest to newest.
	for i := el.oldest; i < len(el.list); i++ {
		ret = append(ret, el.list[i])
	}
	for i := 0; i < el.oldest; i++ {
		ret = append(ret, el.list[i])
	}
	return ret
}

// NewExemplarStorage creates new in-memory storage for exemplars.
func NewInMemExemplarStorage(len int) *InMemExemplarStorage {
	return &InMemExemplarStorage{
		exemplars: make(map[string]*exemplarList),
		len:       len,
	}
}

func (es *InMemExemplarStorage) Appender() storage.ExemplarAppender {
	return es
}

// TODO: separate wrapper struct for queries?
func (es *InMemExemplarStorage) Querier(ctx context.Context) (storage.ExemplarQuerier, error) {
	return es, nil
}

// Select returns exemplars for a given set of series labels hash.
func (es *InMemExemplarStorage) Select(l labels.Labels) ([]exemplar.Exemplar, error) {
	if _, ok := es.exemplars[l.String()]; !ok {
		return nil, nil
	}
	return es.exemplars[l.String()].sorted(), nil
}

func (es *InMemExemplarStorage) AddExemplar(l labels.Labels, t int64, e exemplar.Exemplar) error {
	// todo: if we're doing time brackets for exemplars, ie 1min, 15min, 30min, 60min etc
	// check if t should bump an existing exemplar out of the storage?

	// Ensure no empty labels have gotten through.
	l = l.WithoutEmpty()

	if _, ok := es.exemplars[l.String()]; !ok {
		es.exemplars[l.String()] = newExemplarList(es.len)
	}
	return es.exemplars[l.String()].add(e)
}

// For use in tests, clears the entire exemplar storage
func (es *InMemExemplarStorage) Reset() {
	es.exemplars = make(map[string]*exemplarList)
}

// ***************************************************************
// single circular buffer for all exemplars
type CircularExemplarStorage struct {
	lock           sync.RWMutex
	index          map[string]int
	exemplars      []circularBufferEntry
	nextIndex      int
	len            int
	relabelMtx     sync.RWMutex
	relabelConfigs []*relabel.Config
}

type circularBufferEntry struct {
	exemplar     exemplar.Exemplar
	seriesLabels labels.Labels // need to store labels so we can double check when querying
	prev         int           // index of previous exemplar in circular for the same series
}

// If we assume the average case 95 bytes per exemplar we can fit 5651272 exemplars in
// 1GB of extra memory, accounting for the fact that this is heap allocated space.
func NewCircularExemplarStorage(len int) *CircularExemplarStorage {
	return &CircularExemplarStorage{
		exemplars: make([]circularBufferEntry, len),
		index:     make(map[string]int),
		len:       len,
	}
}

func (ce *CircularExemplarStorage) ApplyConfig(conf *config.Config) error {
	ce.relabelMtx.Lock()
	defer ce.relabelMtx.Unlock()
	ce.relabelConfigs = conf.ExemplarConfig.RelabelConfigs
	return nil
}

func (ce *CircularExemplarStorage) Appender() storage.ExemplarAppender {
	return ce
}

// TODO: separate wrapper struct for queries?
func (ce *CircularExemplarStorage) Querier(ctx context.Context) (storage.ExemplarQuerier, error) {
	return ce, nil
}

// Select returns exemplars for a given set of series labels hash.
func (ce *CircularExemplarStorage) Select(l labels.Labels) ([]exemplar.Exemplar, error) {
	var (
		ret []exemplar.Exemplar
		idx int
		ok  bool
	)

	ce.lock.RLock()
	defer ce.lock.RUnlock()

	if idx, ok = ce.index[l.String()]; !ok {
		return nil, nil
	}
	ret = append(ret, ce.exemplars[idx].exemplar)
	oldestTS := ce.exemplars[idx].exemplar.Ts

	for {
		idx = ce.exemplars[idx].prev
		if idx == -1 || ce.exemplars[idx].seriesLabels.Hash() != l.Hash() {
			break
		}
		// This line is needed to avoid an infinite loop, consider redesign of buffer entry struct.
		if ce.exemplars[idx].exemplar.Ts > oldestTS {
			break
		}
		oldestTS = ce.exemplars[idx].exemplar.Ts
		// Prepend since this exemplar came before the last one we appeneded chronologically.
		ret = append([]exemplar.Exemplar{ce.exemplars[idx].exemplar}, ret...)
	}
	return ret, nil
}

func (ce *CircularExemplarStorage) AddExemplar(l labels.Labels, t int64, e exemplar.Exemplar) error {
	ce.lock.RLock()
	idx, ok := ce.index[l.String()]
	ce.lock.RUnlock()

	ce.lock.Lock()
	defer ce.lock.Unlock()

	ce.relabelMtx.RLock()
	ce.relabelMtx.RUnlock()

	lbls := relabel.Process(l, ce.relabelConfigs...)
	if len(lbls) == 0 {
		return nil
	}

	if ok {
		// Check for duplicate vs last stored exemplar for this series.
		if ce.exemplars[idx].exemplar.Equals(e) {
			return storage.ErrDuplicateExemplar
		}
		ce.exemplars[ce.nextIndex] = circularBufferEntry{exemplar: e, seriesLabels: l, prev: idx}
		ce.index[l.String()] = ce.nextIndex
		ce.nextIndex++
		if ce.nextIndex >= cap(ce.exemplars) {
			ce.nextIndex = 0
		}
		return nil
	}
	ce.exemplars[ce.nextIndex] = circularBufferEntry{exemplar: e, seriesLabels: l, prev: -1}
	ce.index[l.String()] = ce.nextIndex
	ce.nextIndex++
	if ce.nextIndex >= cap(ce.exemplars) {
		ce.nextIndex = 0
	}
	return nil
}

// For use in tests, clears the entire exemplar storage
func (ce *CircularExemplarStorage) Reset() {
	ce.exemplars = make([]circularBufferEntry, ce.len)
	ce.index = make(map[string]int)
}
