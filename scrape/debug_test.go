package scrape

import (
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/model/textparse"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
)

// Mock appender for testing
type mockAppender struct {
	samples []sample
}

type sample struct {
	ref    storage.SeriesRef
	labels labels.Labels
	t      int64
	v      float64
}

func (m *mockAppender) Append(ref storage.SeriesRef, lset labels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	if ref == 0 {
		ref = storage.SeriesRef(lset.Hash())
	}
	m.samples = append(m.samples, sample{ref: ref, labels: lset, t: t, v: v})
	return ref, nil
}

func (m *mockAppender) AppendExemplar(ref storage.SeriesRef, lset labels.Labels, e exemplar.Exemplar) (storage.SeriesRef, error) {
	return ref, nil
}

func (m *mockAppender) AppendHistogram(ref storage.SeriesRef, lset labels.Labels, t int64, h *histogram.Histogram, fh *histogram.FloatHistogram) (storage.SeriesRef, error) {
	if ref == 0 {
		ref = storage.SeriesRef(lset.Hash())
	}
	return ref, nil
}

func (m *mockAppender) AppendCTZeroSample(ref storage.SeriesRef, lset labels.Labels, t, ct int64) (storage.SeriesRef, error) {
	return ref, nil
}

func (m *mockAppender) AppendHistogramCTZeroSample(ref storage.SeriesRef, lset labels.Labels, t, ct int64, h *histogram.Histogram, fh *histogram.FloatHistogram) (storage.SeriesRef, error) {
	return ref, nil
}

func (m *mockAppender) UpdateMetadata(ref storage.SeriesRef, lset labels.Labels, meta metadata.Metadata) (storage.SeriesRef, error) {
	return ref, nil
}

func (m *mockAppender) Commit() error                     { return nil }
func (m *mockAppender) Rollback() error                   { return nil }
func (m *mockAppender) SetOptions(*storage.AppendOptions) {}

// Mock target for label mutation
type mockTarget struct {
	labels labels.Labels
}

func (t *mockTarget) Labels() labels.Labels {
	return t.labels
}

func (t *mockTarget) LabelsRange(f func(labels.Label)) {
	t.labels.Range(f)
}

// Label mutator that simulates the behavior of mutateSampleLabels
func mockMutateSampleLabels(lset labels.Labels, target *mockTarget) labels.Labels {
	lb := labels.NewBuilder(lset)

	// Add target labels (simulating non-honor mode)
	target.LabelsRange(func(l labels.Label) {
		lb.Set(l.Name, l.Value)
	})

	// Add some dynamic labels to stress the label mutation - this increases
	// the chance of detecting memory corruption issues
	lb.Set("scrape_iteration", fmt.Sprintf("%d", time.Now().UnixNano()))
	lb.Set("test_label", "test_value_with_some_content")
	for i := 0; i < 20; i++ {
		lb.Set(fmt.Sprintf("foo_%d", i), fmt.Sprintf("bar_%d", i))
	}

	return lb.Labels()
}

func TestDebugScrapeAppendPattern(t *testing.T) {
	cache := newScrapeCache(newTestScrapeMetrics(t))
	symbolTable := labels.NewSymbolTable()

	// Create a mock target with some labels
	mockTgt := &mockTarget{
		labels: labels.FromStrings(
			"job", "test-job",
			"instance", "test-instance:8080",
			"env", "test",
		),
	}

	files := []string{
		"testdata/proto-base64.txt",
		"testdata/proto-base64-2.txt",
		"testdata/proto-base64-3.txt",
	}

	for i := 0; i < 100; i++ {
		for _, file := range files {
			fmt.Printf("=== Iteration %d ===\n", i)

			// Read the base64 encoded test data
			base64Data, err := ioutil.ReadFile(file)
			require.NoError(t, err, "Failed to read test data")

			// Decode the base64 data
			decodedData, err := base64.StdEncoding.DecodeString(string(base64Data))
			require.NoError(t, err, "Failed to decode base64 data")

			// Simulate scrapeLoop.append behavior
			var _ *labels.SymbolTable = symbolTable
			total, added, cached := simulateScrapeAppend(t, cache, decodedData, mockTgt)

			fmt.Printf("Iteration %d: total=%d, added=%d, cached=%d\n", i, total, added, cached)

			// Verify cache integrity after each iteration
			verifyCacheIntegrity(t, cache, i)
		}
	}
}

func simulateScrapeAppend(t *testing.T, cache *scrapeCache, b []byte, target *mockTarget) (total, added, cached int) {
	// Create parser - closely matching scrapeLoop.append
	p, err := textparse.New(
		b,
		"application/vnd.google.protobuf", // contentType
		"",                                // fallbackScrapeProtocol
		true,                              // alwaysScrapeClassicHist
		false,                             // enableCTZeroIngestion
		nil,                               // symbolTable
	)
	if p == nil {
		t.Logf("Failed to create parser: %v", err)
		return
	}

	var (
		lset labels.Labels // escapes to heap so hoisted out of loop - exactly like scrapeLoop.append
		// Add metadata tracking like the real scrape loop
		lastMFName []byte
		lastMeta   *metaEntry
	)

loop:
	for {
		var (
			et          textparse.Entry
			isHistogram bool
			met         []byte
		)

		if et, err = p.Next(); err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}

		switch et {
		case textparse.EntryType:
			lastMFName, lastMeta = cache.setType(p.Type())
			continue
		case textparse.EntryHelp:
			lastMFName, lastMeta = cache.setHelp(p.Help())
			continue
		case textparse.EntryUnit:
			lastMFName, lastMeta = cache.setUnit(p.Unit())
			continue
		case textparse.EntryComment:
			continue
		case textparse.EntryHistogram:
			isHistogram = true
		default:
		}

		total++

		if isHistogram {
			met, _, _, _ = p.Histogram()
		} else {
			met, _, _ = p.Series()
		}

		// This is the critical part - matching scrapeLoop.append exactly
		ce, seriesCached, _ := cache.get(met)
		var (
			ref  storage.SeriesRef
			hash uint64
		)

		if seriesCached {
			cached++
			// Use cached values
			ref = ce.ref
			lset = ce.lset // This is where memory corruption might occur
			hash = ce.hash

			// Validate cached labels - this is where we might catch corruption
			if !lset.IsValid(model.LegacyValidation) {
				t.Errorf("CORRUPTION DETECTED: Cached labels are invalid for series %s: %v", string(met), lset)
				t.Errorf("Cache entry debug info: %s", ce.debugInfo)
				break loop
			}
		} else {
			added++
			// Parse labels and mutate them
			p.Labels(&lset)
			hash = lset.Hash()

			// Make sure we cache this
			ref = storage.SeriesRef(hash)

			lsetBeforeStr := lset.String()

			if !lset.IsValid(model.LegacyValidation) {
				t.Errorf("Invalid labels before mutation for series %s", string(met))
				break loop
			}

			// Mutate labels - this mimics sl.sampleMutator(lset)
			lset = mockMutateSampleLabels(lset, target)

			if !lset.IsValid(model.LegacyValidation) {
				t.Errorf("Invalid labels after mutation for series %s", string(met))
				t.Errorf("Before mutation: %s", lsetBeforeStr)
				t.Errorf("After mutation: %s", lset.String())
				break loop
			}

			// Track staleness and add to cache
			cache.addRef(met, ref, lset, hash)

			// Add the isSeriesPartOfFamily call to simulate metadata processing like the real scrape loop
			if lastMeta != nil {
				// This simulates the metadata check that happens in scrapeLoop.append
				metricName := lset.Get(labels.MetricName)
				// Call isSeriesPartOfFamily to trigger any potential memory corruption
				// during string processing or label access
				partOfFamily := isSeriesPartOfFamily(metricName, lastMFName, lastMeta.Type)

				t.Logf("Metric %s part of family %s (type %v): %v",
					metricName, string(lastMFName), lastMeta.Type, partOfFamily)
			}
		}
		total++
	}

	return
}

func updateStaleMarkers(app *mockAppender, cache *scrapeCache, defTime int64) {
	cache.forEachStale(func(lset labels.Labels) bool {
		_, err := app.Append(0, lset, defTime, math.Float64frombits(value.StaleNaN))
		return err == nil
	})
}

func verifyCacheIntegrity(t *testing.T, cache *scrapeCache, iteration int) {
	// Check all cached series for label validity
	for metricName, entry := range cache.series {
		if !entry.lset.IsValid(model.LegacyValidation) {
			t.Errorf("CORRUPTION: Invalid cached labels for metric %s at iteration %d: %v", metricName, iteration, entry.lset)
			t.Errorf("Debug info: %s", entry.debugInfo)
			t.Errorf("Hash: %d, LastIter: %d", entry.hash, entry.lastIter)
			t.Errorf("Raw label string: %q", entry.lset.String())

			// Try to provide more detail about what makes the labels invalid
			entry.lset.Range(func(l labels.Label) {
				if l.Name == "" {
					t.Errorf("  - Empty label name found with value: %q", l.Value)
				}
				if len(l.Name) > 0 && l.Name[0] == '\x00' {
					t.Errorf("  - Null byte in label name: %q", l.Name)
				}
				if len(l.Value) > 0 && l.Value[0] == '\x00' {
					t.Errorf("  - Null byte in label value for %q: %q", l.Name, l.Value)
				}
			})
		}
	}

	// Also log cache stats periodically
	if iteration%25 == 0 {
		t.Logf("Cache stats at iteration %d: %d series, %d dropped, iter=%d",
			iteration, len(cache.series), len(cache.droppedSeries), cache.iter)
	}
}
