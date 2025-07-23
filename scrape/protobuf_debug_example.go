package scrape

import (
	"fmt"
	"log"
)

// ExampleUsage demonstrates how to use the protobuf debug functions
// with actual problematic data from your scrape loop
func ExampleUsage() {
	// This is an example of how to use the debug functions when you encounter
	// the "example of invalid labels parsed from response" error in your scrape loop

	// Step 1: When you see this error in your logs:
	// "example of invalid labels parsed from response"
	// The response body will be logged. You need to convert it to hex-escaped format.

	// Step 2: Convert your problematic response to hex-escaped string
	// For example, if your response body is logged as:
	// "response body: \x08\x01\x12\x07..."

	// Step 3: Use the debug function
	problematicData := "\\x08\\x01\\x12\\x07\\x74\\x65\\x73\\x74\\x5f\\x6d\\x65\\x74\\x72\\x69\\x63"

	fmt.Println("=== DEBUGGING PROBLEMATIC PROTOBUF DATA ===")
	DebugProtobufData(problematicData)
}

// ExampleWithRealData shows how to debug actual problematic protobuf data
func ExampleWithRealData() {
	// Replace this with your actual problematic protobuf data
	// This should be the hex-escaped string from your scrape loop logs

	// Example: If your logs show:
	// "example of invalid labels parsed from response"
	// "labels: {__name__=\"test_histogram_bucket\",le=\"+Inf\"}"
	// "response body: \x08\x01\x12\x07..."

	// Extract the hex data and use it here:
	realProblematicData := "\\x08\\x01\\x12\\x07\\x74\\x65\\x73\\x74\\x5f\\x6d\\x65\\x74\\x72\\x69\\x63"

	log.Println("Debugging real problematic protobuf data...")
	DebugProtobufData(realProblematicData)
}

// ExampleBucketMetricIssue demonstrates debugging a specific bucket metric issue
func ExampleBucketMetricIssue() {
	// This example shows how to debug issues specifically with bucket metrics
	// that have invalid labels

	// Example problematic bucket metric data
	bucketMetricData := "\\x08\\x01\\x12\\x07\\x74\\x65\\x73\\x74\\x5f\\x68\\x69\\x73\\x74\\x6f\\x67\\x72\\x61\\x6d\\x5f\\x62\\x75\\x63\\x6b\\x65\\x74"

	fmt.Println("=== DEBUGGING BUCKET METRIC ISSUE ===")
	DebugProtobufData(bucketMetricData)

	// Look for:
	// 1. Missing 'le' label
	// 2. Invalid 'le' label value
	// 3. Malformed metric name
	// 4. Invalid label names or values
}

// ExampleNativeHistogramIssue demonstrates debugging native histogram issues
func ExampleNativeHistogramIssue() {
	// This example shows how to debug issues with native histograms

	// Example problematic native histogram data
	nativeHistogramData := "\\x08\\x01\\x12\\x07\\x74\\x65\\x73\\x74\\x5f\\x68\\x69\\x73\\x74\\x6f\\x67\\x72\\x61\\x6d"

	fmt.Println("=== DEBUGGING NATIVE HISTOGRAM ISSUE ===")
	DebugProtobufData(nativeHistogramData)

	// Look for:
	// 1. Invalid schema values
	// 2. Malformed spans
	// 3. Incorrect bucket counts
	// 4. Zero threshold issues
}



// ExampleConvertResponseBody shows how to convert a response body to hex string
func ExampleConvertResponseBody() {
	// If you have the raw response body bytes from your scrape loop
	responseBody := []byte{0x08, 0x01, 0x12, 0x07, 0x74, 0x65, 0x73, 0x74, 0x5f, 0x6d, 0x65, 0x74, 0x72, 0x69, 0x63}

	// Convert to hex-escaped string
	hexString := ConvertBytesToHexString(responseBody)
	fmt.Printf("Converted response body to hex string: %s\n", hexString)

	// Now you can use it with the debug function
	DebugProtobufData(hexString)
}
