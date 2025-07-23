package textparse

import (
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
)

func TestDebugProtobufParser(t *testing.T) {
	// Read the base64 encoded test data
	base64Data, err := ioutil.ReadFile("testdata/invalid-proto-base64.txt")
	if err != nil {
		t.Fatalf("Failed to read test data: %v", err)
	}

	// Decode the base64 data
	decodedData, err := base64.StdEncoding.DecodeString(string(base64Data))
	if err != nil {
		t.Fatalf("Failed to decode base64 data: %v", err)
	}

	fmt.Printf("Decoded data length: %d bytes\n", len(decodedData))

	// Create a new parser with protobuf content type
	parser, err := New(
		decodedData,
		"application/vnd.google.protobuf", // contentType
		"",                                // no fallback
		true,                              // parseClassicHistograms
		false,                             // skipOMCTSeries
		nil,                               // nil symbol table
	)

	if err != nil {
		fmt.Printf("Parser creation error: %v\n", err)
		if parser == nil {
			return
		}
	} else {
		fmt.Printf("Parser created successfully\n")
	}

	fmt.Printf("Parser type: %T\n", parser)

	// Parse and print all entries
	entryCount := 0
	for {
		entry, err := parser.Next()
		if err != nil {
			if err == io.EOF {
				fmt.Printf("Reached EOF after %d entries\n", entryCount)
				break
			}
			fmt.Printf("Parse error at entry %d: %v\n", entryCount, err)
			break
		}

		entryCount++
		// fmt.Printf("\n--- Entry %d ---\n", entryCount)
		// fmt.Printf("Entry type: %v\n", entry)

		switch entry {
		case EntryType:
			parser.Type()
			// metricName, metricType := parser.Type()
			// fmt.Printf("TYPE: %s %v\n", string(metricName), metricType)

		case EntryHelp:
			parser.Help()
			// metricName, helpText := parser.Help()
			// fmt.Printf("HELP: %s %s\n", string(metricName), string(helpText))

		case EntryUnit:
			parser.Unit()
			// metricName, unit := parser.Unit()
			// fmt.Printf("UNIT: %s %s\n", string(metricName), string(unit))

		case EntrySeries:
			// parser.Series()
			seriesBytes, timestamp, value := parser.Series()
			fmt.Printf("SERIES: %s", string(seriesBytes))
			if timestamp != nil {
				fmt.Printf(" @%d", *timestamp)
			}
			fmt.Printf(" = %g\n", value)

			// Get labels
			var lbls labels.Labels
			parser.Labels(&lbls)
			fmt.Printf("Labels: %v\n", lbls)
			require.True(t, lbls.IsValid(model.LegacyValidation))

			// Check for exemplars
			var ex exemplar.Exemplar
			exemplarCount := 0
			for parser.Exemplar(&ex) {
				exemplarCount++
				fmt.Printf("Exemplar %d: %v\n", exemplarCount, ex)
			}

			// Get created timestamp
			createdTs := parser.CreatedTimestamp()
			if createdTs != 0 {
				fmt.Printf("Created timestamp: %d\n", createdTs)
			}

		case EntryHistogram:
			seriesBytes, timestamp, intHist, floatHist := parser.Histogram()
			fmt.Printf("HISTOGRAM: %s", string(seriesBytes))
			if timestamp != nil {
				fmt.Printf(" @%d", *timestamp)
			}
			if intHist != nil {
				fmt.Printf(" IntHistogram: count=%d sum=%g\n", intHist.Count, intHist.Sum)
			}
			if floatHist != nil {
				fmt.Printf(" FloatHistogram: count=%g sum=%g\n", floatHist.Count, floatHist.Sum)
			}

			// Get labels
			var lbls labels.Labels
			parser.Labels(&lbls)
			fmt.Printf("Labels: %v\n", lbls)
			require.True(t, lbls.IsValid(model.LegacyValidation))

			// Check for exemplars
			var ex exemplar.Exemplar
			exemplarCount := 0
			for parser.Exemplar(&ex) {
				exemplarCount++
				fmt.Printf("Exemplar %d: %v\n", exemplarCount, ex)
			}

			// Get created timestamp
			createdTs := parser.CreatedTimestamp()
			if createdTs != 0 {
				fmt.Printf("Created timestamp: %d\n", createdTs)
			}

		case EntryComment:
			parser.Comment()
			// comment := parser.Comment()
			// fmt.Printf("COMMENT: %s\n", string(comment))

		case EntryInvalid:
			fmt.Printf("INVALID ENTRY\n")

		default:
			fmt.Printf("UNKNOWN ENTRY TYPE: %v\n", entry)
		}
	}

	fmt.Printf("\nParsing completed. Total entries processed: %d\n", entryCount)
}
