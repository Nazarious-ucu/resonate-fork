package bench

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

const flushThreshold = 100

// OpRecord holds per-operation metrics for a single store call.
type OpRecord struct {
	TimestampNs   int64   `json:"timestamp_ns"`
	LatencyMs     float64 `json:"latency_ms"`
	DbType        string  `json:"db_type"`        // "postgres" or "yugabyte"
	OperationType string  `json:"operation_type"` // e.g. "create_promise"
	TestName      string  `json:"test_name"`
	Success       bool    `json:"success"`
}

// MetricsCollector records individual operation measurements and flushes them
// to CSV and JSONL files every flushThreshold rows.
//
// It is safe for concurrent use.
//
// Create with NewMetricsCollector; always call Close when done.
type MetricsCollector struct {
	mu  sync.Mutex
	buf []OpRecord

	// sliding window: UnixNano timestamps of recent successful ops
	window []int64

	csvW  *csv.Writer
	csvF  *os.File
	jsonF *os.File
}

// NewMetricsCollector opens (or creates) csvPath for writing and returns a
// ready-to-use collector.  A JSONL sidecar file is written at the same path
// with a ".jsonl" extension.
func NewMetricsCollector(csvPath string) (*MetricsCollector, error) {
	if err := os.MkdirAll(dirOf(csvPath), 0o755); err != nil {
		return nil, fmt.Errorf("MetricsCollector mkdir: %w", err)
	}

	csvF, err := os.Create(csvPath)
	if err != nil {
		return nil, fmt.Errorf("MetricsCollector csv create: %w", err)
	}

	jsonlPath := csvPath[:len(csvPath)-len(".csv")] + ".jsonl"
	jsonF, err := os.Create(jsonlPath)
	if err != nil {
		csvF.Close()
		return nil, fmt.Errorf("MetricsCollector jsonl create: %w", err)
	}

	c := &MetricsCollector{
		csvF:  csvF,
		jsonF: jsonF,
		csvW:  csv.NewWriter(csvF),
	}
	_ = c.csvW.Write([]string{
		"timestamp_ns", "latency_ms", "db_type", "operation_type", "test_name", "success",
	})
	return c, nil
}

// NewMetricsCollectorFromEnv reads BENCH_OPS_CSV_PATH from the environment and
// calls NewMetricsCollector.  Returns nil, nil if the env var is not set so
// callers can treat a nil collector as "disabled".
func NewMetricsCollectorFromEnv() (*MetricsCollector, error) {
	path := os.Getenv("BENCH_OPS_CSV_PATH")
	if path == "" {
		return nil, nil
	}
	return NewMetricsCollector(path)
}

// Record adds one completed operation to the in-memory buffer and flushes to
// disk every flushThreshold rows.
func (c *MetricsCollector) Record(dbType, opType, testName string, latencyMs float64, success bool) {
	if c == nil {
		return
	}
	now := time.Now().UnixNano()
	row := OpRecord{
		TimestampNs:   now,
		LatencyMs:     latencyMs,
		DbType:        dbType,
		OperationType: opType,
		TestName:      testName,
		Success:       success,
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.buf = append(c.buf, row)
	if success {
		c.window = append(c.window, now)
	}
	if len(c.buf) >= flushThreshold {
		c.flushLocked()
	}
}

// OpsPerSec returns the number of successful operations recorded within the
// last windowSec seconds.  Pass 1.0 for a standard 1-second sliding window.
func (c *MetricsCollector) OpsPerSec(windowSec float64) float64 {
	if c == nil {
		return 0
	}
	cutoff := time.Now().UnixNano() - int64(windowSec*float64(time.Second))
	c.mu.Lock()
	defer c.mu.Unlock()
	// trim entries that have left the window
	i := 0
	for i < len(c.window) && c.window[i] < cutoff {
		i++
	}
	c.window = c.window[i:]
	return float64(len(c.window)) / windowSec
}

// Close flushes any remaining buffered records and closes the output files.
func (c *MetricsCollector) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	c.flushLocked()
	c.mu.Unlock()
	c.csvW.Flush()
	if err := c.csvW.Error(); err != nil {
		return err
	}
	if err := c.csvF.Close(); err != nil {
		return err
	}
	return c.jsonF.Close()
}

// flushLocked writes c.buf to disk.  Must be called with c.mu held.
func (c *MetricsCollector) flushLocked() {
	for _, r := range c.buf {
		_ = c.csvW.Write([]string{
			strconv.FormatInt(r.TimestampNs, 10),
			strconv.FormatFloat(r.LatencyMs, 'f', 3, 64),
			r.DbType,
			r.OperationType,
			r.TestName,
			strconv.FormatBool(r.Success),
		})
		b, _ := json.Marshal(r)
		_, _ = fmt.Fprintf(c.jsonF, "%s\n", b)
	}
	c.csvW.Flush()
	_ = c.jsonF.Sync()
	c.buf = c.buf[:0]
}

func dirOf(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' || path[i] == '\\' {
			return path[:i]
		}
	}
	return "."
}
