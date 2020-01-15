package tsdb

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/prometheus/prometheus/pkg/exemplar"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/util/testutil"
)

func TestAddExemplar(t *testing.T) {
	es := NewExemplarStorage()

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
	testutil.Assert(t, reflect.DeepEqual(es.exemplars[l.Hash()][0], e), "exemplar was not stored correctly")
}

func TestSelectExemplar(t *testing.T) {
	es := NewExemplarStorage()

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
	testutil.Assert(t, reflect.DeepEqual(es.exemplars[l.Hash()][0], e), "exemplar was not stored correctly")

	exemplars, err := es.Select(l.Hash())
	testutil.Ok(t, err)

	fmt.Println("exemplars: ", exemplars)
	fmt.Println("e: ", es.exemplars)

	testutil.Assert(t, reflect.DeepEqual(es.exemplars[l.Hash()], exemplars), "select did not return all exemplars")
}
