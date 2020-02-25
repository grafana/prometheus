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

package promql

import (
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/util/testutil"
)

func TestExtractSelectors(t *testing.T) {
	for _, tc := range [...]struct {
		input    string
		expected []string
	}{
		{
			"foo",
			[]string{`{__name__="foo"}`},
		},
		{
			`foo{bar="baz"}`,
			[]string{`{bar="baz", __name__="foo"}`},
		},
		{
			`foo{bar="baz"} / flip{flop="flap"}`,
			[]string{`{bar="baz", __name__="foo"}`, `{flop="flap", __name__="flip"}`},
		},
	} {
		expr, err := ParseExpr(tc.input)
		testutil.Ok(t, err)

		var expected [][]*labels.Matcher
		for _, s := range tc.expected {
			selector, err := ParseMetricSelector(s)
			testutil.Ok(t, err)

			expected = append(expected, selector)
		}

		actual, err := ExtractSelectors(expr)
		testutil.Ok(t, err)
		testutil.Equals(t, expected, actual)
	}
}
