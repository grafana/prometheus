package tsdb

import (
	"github.com/prometheus/prometheus/pkg/exemplar"
	"github.com/prometheus/prometheus/pkg/labels"
)

type ExemplarStorage struct {
	exemplars map[uint64][]exemplar.Exemplar
}

// NewExemplarStorage creates new in-memory storage for exemplars.
func NewExemplarStorage() ExemplarStorage {
	return ExemplarStorage{
		exemplars: make(map[uint64][]exemplar.Exemplar),
	}
}

// Select returns exemplars for a given series labels hash.
func (es *ExemplarStorage) Select(hash uint64) ([]exemplar.Exemplar, error) {
	return es.exemplars[hash], nil
}

func (es *ExemplarStorage) AddExemplar(l labels.Labels, t int64, e exemplar.Exemplar) error {
	// if we're doing time brackets for exemplars, ie 1min, 15min, 30min, 60min etc
	// check if t should bump an existing exemplar out of the storage?

	// Ensure no empty labels have gotten through.
	l = l.WithoutEmpty()
	hash := l.Hash()
	es.exemplars[hash] = append(es.exemplars[hash], e)
	return nil
}
