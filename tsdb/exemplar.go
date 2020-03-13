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
	"fmt"

	"github.com/prometheus/prometheus/pkg/exemplar"
	"github.com/prometheus/prometheus/pkg/labels"
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

func (e *InMemExemplarStorage) Appender() storage.ExemplarAppender {
	return e
}

// TODO: separate wrapper struct for queries?
func (e *InMemExemplarStorage) Querier(ctx context.Context) (storage.ExemplarQuerier, error) {
	return e, nil
}

// Select returns exemplars for a given set of series labels hash.
func (es *InMemExemplarStorage) Select(l labels.Labels) ([]exemplar.Exemplar, error) {
	if _, ok := es.exemplars[l.String()]; !ok {
		return nil, nil
	}
	return es.exemplars[l.String()].sorted(), nil
}

func (es *InMemExemplarStorage) AddExemplar(l labels.Labels, t int64, e exemplar.Exemplar) error {
	fmt.Println("adding exemplar for labels: ", l)
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
