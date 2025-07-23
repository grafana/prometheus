package scrape

import (
	"fmt"
	"strings"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/textparse"
)

// hexStringToBytes converts a hex-escaped string like "\x08\x01\x12\x07" to bytes
func hexStringToBytes(hexStr string) ([]byte, error) {
	// Remove the \x prefix and convert to bytes
	hexStr = strings.ReplaceAll(hexStr, "\\x", "")
	if len(hexStr)%2 != 0 {
		return nil, fmt.Errorf("hex string must have even length")
	}

	result := make([]byte, len(hexStr)/2)
	for i := 0; i < len(hexStr); i += 2 {
		hexByte := hexStr[i : i+2]
		var b byte
		_, err := fmt.Sscanf(hexByte, "%02x", &b)
		if err != nil {
			return nil, fmt.Errorf("invalid hex byte %s: %v", hexByte, err)
		}
		result[i/2] = b
	}
	return result, nil
}

// DebugProtobufData is a standalone function that can be called with hex-escaped protobuf data
// to debug parsing issues with native histogram metrics
// Usage: DebugProtobufData("\\x08\\x01\\x12\\x07...")
func DebugProtobufData(hexData string) {
	fmt.Println("=== DEBUGGING PROTOBUF DATA ===")
	fmt.Printf("Input hex data: %s\n", hexData)

	// Convert hex string to bytes
	data, err := hexStringToBytes(hexData)
	if err != nil {
		fmt.Printf("ERROR: Failed to convert hex string to bytes: %v\n", err)
		return
	}

	fmt.Printf("Converted to %d bytes: %x\n", len(data), data)

	// Create parser
	symbolTable := labels.NewSymbolTable()
	contentType := "application/vnd.google.protobuf"
	fallbackType := ""
	parseClassicHistograms := true
	skipOMCTSeries := false

	p, err := textparse.New(data, contentType, fallbackType, parseClassicHistograms, skipOMCTSeries, symbolTable)
	if err != nil {
		fmt.Printf("ERROR: Failed to create parser: %v\n", err)
		return
	}

	fmt.Printf("Parser created successfully\n")

	// Parse entries
	var lset labels.Labels
	entryCount := 0

	for {
		et, err := p.Next()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			fmt.Printf("ERROR parsing entry %d: %v\n", entryCount, err)
			break
		}

		entryCount++
		fmt.Printf("\nEntry %d: Type=%v\n", entryCount, et)

		switch et {
		case textparse.EntryType:
			name, typ := p.Type()
			fmt.Printf("  TYPE: name=%s, type=%v\n", string(name), typ)
		case textparse.EntryHelp:
			name, help := p.Help()
			fmt.Printf("  HELP: name=%s, help=%s\n", string(name), string(help))
		case textparse.EntryUnit:
			name, unit := p.Unit()
			fmt.Printf("  UNIT: name=%s, unit=%s\n", string(name), string(unit))
		case textparse.EntryComment:
			fmt.Printf("  COMMENT: %s\n", string(p.Comment()))
		case textparse.EntryHistogram:
			met, ts, h, fh := p.Histogram()
			fmt.Printf("  HISTOGRAM: metric=%s, timestamp=%v\n", string(met), ts)
			if h != nil {
				fmt.Printf("    Integer Histogram: count=%d, sum=%f\n", h.Count, h.Sum)
			}
			if fh != nil {
				fmt.Printf("    Float Histogram: count=%f, sum=%f\n", fh.Count, fh.Sum)
			}
		case textparse.EntrySeries:
			met, ts, val := p.Series()
			fmt.Printf("  SERIES: metric=%s, timestamp=%v, value=%f\n", string(met), ts, val)
		}

		// Get labels for all entry types
		p.Labels(&lset)
		if !lset.IsEmpty() {
			fmt.Printf("  LABELS: %s\n", lset.String())

			// Special check for bucket metrics
			if strings.Contains(lset.Get(labels.MetricName), "bucket") {
				fmt.Printf("  *** BUCKET METRIC DETECTED ***\n")
				lset.Range(func(l labels.Label) {
					if l.Name == "le" {
						fmt.Printf("    'le' label: %q (len=%d)\n", l.Value, len(l.Value))
					}
				})
			}
		}
	}

	fmt.Printf("\n=== PARSING COMPLETE ===\n")
	fmt.Printf("Total entries processed: %d\n", entryCount)
}

// ConvertBytesToHexString converts raw bytes to hex-escaped string format
// Use this helper function to convert your response body bytes to the format
// expected by the debug functions
func ConvertBytesToHexString(data []byte) string {
	result := ""
	for _, b := range data {
		result += fmt.Sprintf("\\x%02x", b)
	}
	return result
}
