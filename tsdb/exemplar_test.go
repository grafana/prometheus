// Copyright 2015 The Prometheus Authors
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
	testutil.Assert(t, reflect.DeepEqual(es.exemplars[l.String()].list[0], e), "exemplar was not stored correctly")

	es.AddExemplar(l, 0, e)
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
	testutil.Assert(t, reflect.DeepEqual(es.exemplars[l.String()].list[0], exemplars[5]), "exemplar was not stored correctly")
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
	testutil.Assert(t, reflect.DeepEqual(es.exemplars[l.String()].list[0], e), "exemplar was not stored correctly")

	exemplars, err := es.Select(l)
	testutil.Ok(t, err)

	testutil.Assert(t, reflect.DeepEqual(es.exemplars[l.String()].list, exemplars), "select did not return all exemplars")
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
	testutil.Assert(t, reflect.DeepEqual(es.exemplars[l.String()].list[0], exemplars[5]), "exemplar was not stored correctly")

	ret, err := es.Select(l)
	testutil.Ok(t, err)

	testutil.Assert(t, reflect.DeepEqual(exemplars[1:], ret), "select did not return all exemplars")
}

func TestAddExemplar_Circ(t *testing.T) {
	es := NewCircularExemplarStorage(2)

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

	err := es.AddExemplar(l, 0, e)
	testutil.Ok(t, err)
	testutil.Equals(t, es.index[l.String()], 0, "exemplar was not stored correctly")

	err = es.AddExemplar(l, 0, e)
	testutil.NotOk(t, err)

	e2 := exemplar.Exemplar{
		Labels: labels.Labels{
			labels.Label{
				Name:  "traceID",
				Value: "zxcvb",
			},
		},
		Value: 0.1,
		HasTs: false,
	}

	err = es.AddExemplar(l, 0, e2)
	testutil.Ok(t, err)
	testutil.Equals(t, es.index[l.String()], 1, "exemplar was not stored correctly")
}

func TestAddExemplar_CircOverwrite(t *testing.T) {
	es := NewCircularExemplarStorage(2)

	l1 := labels.Labels{
		{Name: "service", Value: "asdf"},
	}
	l2 := labels.Labels{
		{Name: "service", Value: "xyz"},
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
	e2 := exemplar.Exemplar{
		Labels: labels.Labels{
			labels.Label{
				Name:  "traceID",
				Value: "zxcvbn",
			},
		},
		Value: 0.1,
		HasTs: false,
	}
	e3 := exemplar.Exemplar{
		Labels: labels.Labels{
			labels.Label{
				Name:  "traceID",
				Value: "asdfgh",
			},
		},
		Value: 0.1,
		HasTs: false,
	}

	err := es.AddExemplar(l1, 0, e)
	testutil.Ok(t, err)
	testutil.Equals(t, es.index[l1.String()], 0, "exemplar was not stored correctly")

	err = es.AddExemplar(l1, 0, e2)
	testutil.Ok(t, err)
	testutil.Equals(t, es.index[l1.String()], 1, "exemplar was not stored correctly")

	err = es.AddExemplar(l2, 0, e3)
	testutil.Ok(t, err)
	testutil.Equals(t, es.index[l2.String()], 0, "exemplar was not stored correctly")

}

func TestSelectExemplar_Circ(t *testing.T) {
	es := NewCircularExemplarStorage(3)

	l := labels.Labels{
		{Name: "service", Value: "asdf"},
	}
	exemplars := []exemplar.Exemplar{
		exemplar.Exemplar{
			Labels: labels.Labels{
				labels.Label{
					Name:  "traceID",
					Value: "qwerty",
				},
			},
			Value: 0.1,
			HasTs: false,
		},
		exemplar.Exemplar{
			Labels: labels.Labels{
				labels.Label{
					Name:  "traceID",
					Value: "zxcvbn",
				},
			},
			Value: 0.1,
			HasTs: false,
		},
		exemplar.Exemplar{
			Labels: labels.Labels{
				labels.Label{
					Name:  "traceID",
					Value: "asdfgh",
				},
			},
			Value: 0.1,
			HasTs: false,
		},
	}

	for i, e := range exemplars {
		err := es.AddExemplar(l, 0, e)
		testutil.Ok(t, err)
		testutil.Equals(t, es.index[l.String()], i, "exemplar was not stored correctly")
	}

	el, err := es.Select(l)
	testutil.Ok(t, err)
	testutil.Assert(t, len(el) == 3, "didn't get expected one exemplar")

	for i := range exemplars {
		testutil.Assert(t, el[i].Equals(exemplars[i]), "")
	}
}

// This is a set of stored exemplars I scraped and stored locally that resulted in an infinite loop.
// This test ensures Select doesn't infinitely loop on them anymore.
func TestSelectExemplar_OverwriteLoop(t *testing.T) {
	es := NewCircularExemplarStorage(10)

	l1 := labels.Labels{
		{Name: "__name__", Value: "test_metric"},
		{Name: "service", Value: "asdf"},
	}
	l2 := labels.Labels{
		{Name: "__name__", Value: "test_metric"},

		{Name: "service", Value: "qwer"},
	}

	es.index[l1.String()] = 0
	es.exemplars[0] = circularBufferEntry{
		seriesLabels: l1,
		prev:         6,
	}
	es.exemplars[6] = circularBufferEntry{
		seriesLabels: l1,
		prev:         2,
	}

	es.index[l2.String()] = 2
	es.exemplars[2] = circularBufferEntry{
		exemplar: exemplar.Exemplar{
			Ts: 10,
		},
		seriesLabels: l2,
		prev:         1,
	}
	es.exemplars[1] = circularBufferEntry{
		exemplar: exemplar.Exemplar{
			Ts: 10,
		},
		seriesLabels: l2,
		prev:         9,
	}
	es.exemplars[9] = circularBufferEntry{
		exemplar: exemplar.Exemplar{
			Ts: 9,
		},
		seriesLabels: l2,
		prev:         8,
	}
	es.exemplars[8] = circularBufferEntry{
		exemplar: exemplar.Exemplar{
			Ts: 8,
		},
		seriesLabels: l2,
		prev:         7,
	}
	es.exemplars[7] = circularBufferEntry{
		exemplar: exemplar.Exemplar{
			Ts: 7,
		},
		seriesLabels: l2,
		prev:         5,
	}
	es.exemplars[5] = circularBufferEntry{
		exemplar: exemplar.Exemplar{
			Ts: 6,
		},
		seriesLabels: l2,
		prev:         4,
	}
	es.exemplars[4] = circularBufferEntry{
		exemplar: exemplar.Exemplar{
			Ts: 5,
		},
		seriesLabels: l2,
		prev:         3,
	}
	es.exemplars[3] = circularBufferEntry{
		exemplar: exemplar.Exemplar{
			Ts: 4,
		},
		seriesLabels: l2,
		prev:         1,
	}

	el, err := es.Select(l2)
	testutil.Ok(t, err)
	testutil.Assert(t, len(el) == 8, "didn't get expected 8 exemplars")
}
