package tsdb

import (
	"reflect"
	"testing"

	"github.com/prometheus/prometheus/pkg/exemplar"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/util/testutil"
)

func TestAddExemplar(t *testing.T) {
	es := NewInMemExemplarStorage(5)

	l := labels.Labels{
		{Name: "service", Value: "asdf"},
	}
	e := exemplar.Exemplar{
		Labels: labels.Labels{
			labels.Label{
				Name:  "traceID",
				Value: "qwerty",
			},
		},
		Value: 0.1,
		HasTs: false,
	}

	es.AddExemplar(l, 0, e)
	testutil.Assert(t, reflect.DeepEqual(es.exemplars[l.Hash()].list[0], e), "exemplar was not stored correctly")
}

func TestAddExtraExemplar(t *testing.T) {
	es := NewInMemExemplarStorage(5)

	l := labels.Labels{
		{Name: "service", Value: "asdf"},
	}
	exemplars := []exemplar.Exemplar{
		exemplar.Exemplar{
			Labels: labels.Labels{
				labels.Label{
					Name:  "traceID",
					Value: "a",
				},
			},
			Value: 0.1,
			HasTs: false,
		},
		exemplar.Exemplar{
			Labels: labels.Labels{
				labels.Label{
					Name:  "traceID",
					Value: "b",
				},
			},
			Value: 0.2,
			HasTs: false,
		},
		exemplar.Exemplar{
			Labels: labels.Labels{
				labels.Label{
					Name:  "traceID",
					Value: "c",
				},
			},
			Value: 0.3,
			HasTs: false,
		},
		exemplar.Exemplar{
			Labels: labels.Labels{
				labels.Label{
					Name:  "traceID",
					Value: "d",
				},
			},
			Value: 0.4,
			HasTs: false,
		},
		exemplar.Exemplar{
			Labels: labels.Labels{
				labels.Label{
					Name:  "traceID",
					Value: "e",
				},
			},
			Value: 0.5,
			HasTs: false,
		},
		exemplar.Exemplar{
			Labels: labels.Labels{
				labels.Label{
					Name:  "traceID",
					Value: "f",
				},
			},
			Value: 0.6,
			HasTs: false,
		},
	}

	for _, e := range exemplars {
		es.AddExemplar(l, 0, e)
	}
	testutil.Assert(t, reflect.DeepEqual(es.exemplars[l.Hash()].list[0], exemplars[5]), "exemplar was not stored correctly")
}

func TestSelectExemplar(t *testing.T) {
	es := NewInMemExemplarStorage(5)

	l := labels.Labels{
		{Name: "service", Value: "asdf"},
	}
	e := exemplar.Exemplar{
		Labels: labels.Labels{
			labels.Label{
				Name:  "traceID",
				Value: "qwerty",
			},
		},
		Value: 0.1,
		HasTs: false,
	}

	es.AddExemplar(l, 0, e)
	testutil.Assert(t, reflect.DeepEqual(es.exemplars[l.Hash()].list[0], e), "exemplar was not stored correctly")

	exemplars, err := es.Select(l.Hash())
	testutil.Ok(t, err)

	testutil.Assert(t, reflect.DeepEqual(es.exemplars[l.Hash()].list, exemplars), "select did not return all exemplars")
}

func TestSelectExemplarOrdering(t *testing.T) {
	es := NewInMemExemplarStorage(5)

	l := labels.Labels{
		{Name: "service", Value: "asdf"},
	}
	exemplars := []exemplar.Exemplar{
		exemplar.Exemplar{
			Labels: labels.Labels{
				labels.Label{
					Name:  "traceID",
					Value: "a",
				},
			},
			Value: 0.1,
			HasTs: false,
		},
		exemplar.Exemplar{
			Labels: labels.Labels{
				labels.Label{
					Name:  "traceID",
					Value: "b",
				},
			},
			Value: 0.2,
			HasTs: false,
		},
		exemplar.Exemplar{
			Labels: labels.Labels{
				labels.Label{
					Name:  "traceID",
					Value: "c",
				},
			},
			Value: 0.3,
			HasTs: false,
		},
		exemplar.Exemplar{
			Labels: labels.Labels{
				labels.Label{
					Name:  "traceID",
					Value: "d",
				},
			},
			Value: 0.4,
			HasTs: false,
		},
		exemplar.Exemplar{
			Labels: labels.Labels{
				labels.Label{
					Name:  "traceID",
					Value: "e",
				},
			},
			Value: 0.5,
			HasTs: false,
		},
		exemplar.Exemplar{
			Labels: labels.Labels{
				labels.Label{
					Name:  "traceID",
					Value: "f",
				},
			},
			Value: 0.6,
			HasTs: false,
		},
	}

	for _, e := range exemplars {
		es.AddExemplar(l, 0, e)
	}
	testutil.Assert(t, reflect.DeepEqual(es.exemplars[l.Hash()].list[0], exemplars[5]), "exemplar was not stored correctly")

	ret, err := es.Select(l.Hash())
	testutil.Ok(t, err)

	testutil.Assert(t, reflect.DeepEqual(exemplars[1:], ret), "select did not return all exemplars")
}
