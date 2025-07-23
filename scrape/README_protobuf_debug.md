# Protobuf Debug Functions

This file contains debug functions to help troubleshoot protobuf parsing issues with native histogram metrics in Prometheus scrape loops.

## Overview

When processing protobuf responses that contain native histogram metrics, you may encounter errors with invalid labels, particularly on bucket metrics. These debug functions help you:

1. Parse protobuf data step-by-step
2. Examine label parsing in detail
3. Identify issues with bucket metrics and their labels
4. Debug native histogram parsing problems

## Functions

### `DebugProtobufData(hexData string)`

A standalone function that can be called with hex-escaped protobuf data to debug parsing issues.

**Usage:**
```go
// Call with hex-escaped protobuf data
DebugProtobufData("\\x08\\x01\\x12\\x07\\x74\\x65\\x73\\x74\\x5f\\x6d\\x65\\x74\\x72\\x69\\x63")
```

**Output:**
- Detailed parsing information for each entry
- Label details for each metric
- Special highlighting for bucket metrics
- Histogram information (both integer and float)
- Exemplar data
- Created timestamps

### `debugProtobufParse(t *testing.T, inputData string)`

A test function that mimics the scrape loop's parsing logic to help debug protobuf parsing issues.

**Usage:**
```go
func TestMyProtobufIssue(t *testing.T) {
    problematicData := "\\x08\\x01\\x12\\x07\\x74\\x65\\x73\\x74\\x5f\\x6d\\x65\\x74\\x72\\x69\\x63"
    debugProtobufParse(t, problematicData)
}
```

## How to Use

### 1. Get the Problematic Protobuf Data

When you encounter the error in your scrape loop:
```
example of invalid labels parsed from response
```

The response body will be logged. You need to convert this to a hex-escaped string.

### 2. Convert Response to Hex-Escaped String

If you have the raw response bytes, convert them to hex-escaped format:
```go
// Example conversion
rawBytes := []byte{0x08, 0x01, 0x12, 0x07, 0x74, 0x65, 0x73, 0x74, 0x5f, 0x6d, 0x65, 0x74, 0x72, 0x69, 0x63}
hexString := ""
for _, b := range rawBytes {
    hexString += fmt.Sprintf("\\x%02x", b)
}
// Result: "\\x08\\x01\\x12\\x07\\x74\\x65\\x73\\x74\\x5f\\x6d\\x65\\x74\\x72\\x69\\x63"
```

### 3. Debug the Data

```go
// Option 1: Use the standalone function
DebugProtobufData("\\x08\\x01\\x12\\x07\\x74\\x65\\x73\\x74\\x5f\\x6d\\x65\\x74\\x72\\x69\\x63")

// Option 2: Use in a test
func TestDebugMyIssue(t *testing.T) {
    debugProtobufParse(t, "\\x08\\x01\\x12\\x07\\x74\\x65\\x73\\x74\\x5f\\x6d\\x65\\x74\\x72\\x69\\x63")
}
```

### 4. Analyze the Output

The debug output will show:
- **Entry types**: TYPE, HELP, UNIT, SERIES, HISTOGRAM
- **Label details**: Each label with name, value, and length
- **Bucket metrics**: Special highlighting for metrics containing "bucket"
- **Histogram data**: Count, sum, spans, buckets
- **Exemplars**: Any exemplar data present
- **Timestamps**: Both sample and created timestamps

## Common Issues to Look For

### 1. Invalid Label Names
Look for labels with invalid characters or names that don't follow Prometheus naming conventions.

### 2. Bucket Metric Issues
Bucket metrics should have an `le` (less than or equal) label. Check if:
- The `le` label is missing
- The `le` label value is malformed
- The metric name doesn't end with `_bucket`

### 3. Native Histogram Problems
For native histograms, check:
- Schema values
- Span configurations
- Bucket counts
- Zero threshold and count

### 4. Protobuf Format Issues
- Malformed protobuf messages
- Incorrect field tags
- Missing required fields

## Example Output

```
=== DEBUGGING PROTOBUF DATA ===
Input hex data: \x08\x01\x12\x07\x74\x65\x73\x74\x5f\x6d\x65\x74\x72\x69\x63
Converted to 15 bytes: 08011207746573745f6d6574726963
Parser created successfully

Entry 1: Type=0
  TYPE: name=test_metric, type=1

Entry 2: Type=2
  SERIES: metric=test_metric, timestamp=<nil>, value=1.000000
  LABELS: {__name__="test_metric"}
```

## Integration with Scrape Loop

The debug functions mimic the exact parsing logic used in the scrape loop:

1. **Parser Creation**: Uses the same `textparse.New()` call
2. **Entry Processing**: Follows the same switch statement logic
3. **Label Handling**: Uses the same `p.Labels()` calls
4. **Histogram Processing**: Handles both integer and float histograms
5. **Error Handling**: Catches and reports the same errors

This ensures that what you see in the debug output matches exactly what happens in the actual scrape loop.

## Troubleshooting Tips

1. **Start with small data**: Use minimal protobuf data to isolate the issue
2. **Compare with working data**: Debug both problematic and working protobuf responses
3. **Check content type**: Ensure the content type is correctly set to `application/vnd.google.protobuf`
4. **Validate protobuf format**: Use protobuf tools to validate the message format
5. **Check for corruption**: Look for truncated or corrupted data in the hex output

## Running the Tests

```bash
# Run all protobuf debug tests
go test -v ./scrape -run "TestDebugProtobuf"

# Run specific test
go test -v ./scrape -run "TestDebugProtobufParse/simple_protobuf_example"

# Run with verbose output
go test -v ./scrape -run "TestDebugProtobuf" -test.v
``` 