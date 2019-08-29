package main

import (
	"fmt"
	"strings"
	"time"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	debug      = kingpin.Flag("debug", "Enable debug mode.").Bool()
	recordSize = kingpin.Flag("record-size", "Size of each log record").Default("1KiB").Bytes()
	rps        = kingpin.Flag("records-per-second", "Number of log records per second to emit").Required().Int()
	duration   = kingpin.Flag("duration", "Number of seconds to emit logs for").Default("10s").Duration()
)

const (
	// Use an underscored field for data, to prevent it being analyzed in ES
	logLine = `{"ts":"%s","seq":"%06d","_data":"%s"}`

	// maxSeq should equal the maximum size of the seq field in the printf directive above, e.g. %06d -> 10^6
	maxSeq = 1000000

	// timeFmt is almost RFC3339Nano, but doesn't strip trailing zeros
	timeFmt = `2006-01-02T15:04:05.000000-07:00`
)

func main() {
	kingpin.Version("0.0.1")
	kingpin.Parse()

	seq := 0
	padding := buildLogLinePadding(int(*recordSize))

	rate := time.Second / time.Duration(*rps)
	throttle := time.Tick(rate)
	for {
		<-throttle
		fmt.Printf(logLine, time.Now().Format(timeFmt), seq, padding)

		// We don't include the newline character in the size of the log record,
		// because we expect that the logging driver (e.g. docker) will use this as
		// the record delimiter.
		fmt.Println()

		seq = (seq + 1) % maxSeq
	}

}

func buildLogLinePadding(desiredSize int) string {
	exampleLogLine := fmt.Sprintf(logLine, time.Now().Format(timeFmt), 1, "")
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
