package tsdb

import (
	"context"

	"github.com/prometheus/prometheus/pkg/exemplar"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

type exemplarList struct {
	next   int
	oldest int
	list   []exemplar.Exemplar
}

// implements storage.ExemplarStorage
type InMemExemplarStorage struct {
	exemplars map[uint64]*exemplarList
	len       int
}

func newExemplarList(len int) *exemplarList {
	return &exemplarList{
		list: make([]exemplar.Exemplar, 0, len),
	}
}

func (el *exemplarList) add(e exemplar.Exemplar) {
	// TODO: Don't add an exemplar if we already have it

	if len(el.list) < cap(el.list) {
		el.list = append(el.list, e)
		el.next++
		if el.next >= cap(el.list) {
			el.next = 0
		}
		return
	}

	el.list[el.next] = e
	el.next++
	if el.next >= len(el.list) {
		el.next = 0
	}
	el.oldest++
	if el.oldest >= len(el.list) {
		el.oldest = 0
	}
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
		exemplars: make(map[uint64]*exemplarList),
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

// Select returns exemplars for a given series labels hash.
func (es *InMemExemplarStorage) Select(hash uint64) ([]exemplar.Exemplar, error) {
	if _, ok := es.exemplars[hash]; !ok {
		return nil, nil
	}
	return es.exemplars[hash].sorted(), nil
}

func (es *InMemExemplarStorage) AddExemplar(l labels.Labels, t int64, e exemplar.Exemplar) error {
	// if we're doing time brackets for exemplars, ie 1min, 15min, 30min, 60min etc
	// check if t should bump an existing exemplar out of the storage?

	// Ensure no empty labels have gotten through.
	l = l.WithoutEmpty()
	hash := l.Hash()

	if _, ok := es.exemplars[hash]; !ok {
		es.exemplars[hash] = newExemplarList(es.len)
	}
	es.exemplars[hash].add(e)
	return nil
}

// For use in tests, clears the entire exemplar storage
func (es *InMemExemplarStorage) Reset() {
	es.exemplars = make(map[uint64]*exemplarList)
}
