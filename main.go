package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	runID      = kingpin.Flag("run-id", "Arbitrary string to identify the run, will be output as the run_id field").Default("test").String()
	recordSize = kingpin.Flag("record-size", "Size of each log record").Default("1KiB").Bytes()
	rps        = kingpin.Flag("records-per-second", "Number of log records per second to emit").Required().Int()
	duration   = kingpin.Flag("duration", "Number of seconds to emit logs for").Default("10s").Duration()
)

const (
	// Use an underscored field for data, to prevent it being analyzed in ES
	logLine = `{"ts":"%s","run_id":"%s","seq":"%06d","_data":"%s"}`

	// maxSeq should equal the maximum size of the seq field in the printf directive above, e.g. %06d -> 10^6
	maxSeq = 1000000

	// timeFmt is almost RFC3339Nano, but doesn't strip trailing zeros
	timeFmt = `2006-01-02T15:04:05.000000000-07:00`
)

func main() {
	kingpin.Version("0.0.1")
	kingpin.Parse()

	seq := 1
	padding := buildLogLinePadding(int(*recordSize), *runID)

	rate := time.Second / time.Duration(*rps)
	throttle := time.Tick(rate)

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(*duration))
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			os.Exit(0)
		case <-throttle:
			fmt.Printf(logLine, time.Now().Format(timeFmt), *runID, seq, padding)

			// We don't include the newline character in the size of the log record,
			// because we expect that the logging driver (e.g. docker) will use this as
			// the record delimiter.
			fmt.Println()

			seq = (seq + 1) % maxSeq
		}
	}

}

func buildLogLinePadding(desiredSize int, runID string) string {
	exampleLogLine := fmt.Sprintf(logLine, time.Now().Format(timeFmt), runID, 1, "")
	size := len(exampleLogLine)

	if size > desiredSize {
		panic(fmt.Sprintf("Desired size of %d bytes is less than minimum size of %d bytes", desiredSize, size))
	}

	bytesToAdd := desiredSize - size
	if bytesToAdd == 0 {
		return ""
	}

	// Trailing space to ensure word separation, making it a bit easier on Elasticsearch
	paddingStr := "some data "

	var paddingData strings.Builder
	for i := 0; i < (bytesToAdd / len(paddingStr)); i++ {
		paddingData.Write([]byte(paddingStr))
	}

	// Add the remainder, if any, by taking a substring of the example string, to
	// make up the desired size.
	bytesToAdd = bytesToAdd - len(paddingData.String())
	paddingData.Write([]byte(paddingStr[:bytesToAdd]))

	return paddingData.String()
}
