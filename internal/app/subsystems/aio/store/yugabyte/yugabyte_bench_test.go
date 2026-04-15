package yugabyte

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/bench"
	"github.com/resonatehq/resonate/internal/metrics"
)

// newYugabyteBenchStore creates a store using the same env vars as the regular tests.
// Returns nil and calls b.Skip if the env vars are not set.
func newYugabyteBenchStore(b *testing.B) *YugabyteStore {
	b.Helper()
	host := os.Getenv("TEST_AIO_SUBSYSTEMS_STORE_YUGABYTE_CONFIG_HOST")
	fallbackHosts := os.Getenv("TEST_AIO_SUBSYSTEMS_STORE_YUGABYTE_CONFIG_FALLBACK_HOSTS")
	loadBalance := true
	if raw := os.Getenv("TEST_AIO_SUBSYSTEMS_STORE_YUGABYTE_CONFIG_LOAD_BALANCE"); raw != "" {
		if parsed, err := strconv.ParseBool(raw); err == nil {
			loadBalance = parsed
		}
	}
	if host == "" {
		b.Skip("YugabyteDB not configured (TEST_AIO_SUBSYSTEMS_STORE_YUGABYTE_CONFIG_HOST unset)")
	}

	m := metrics.New(prometheus.NewRegistry())
	s, err := New(nil, m, &Config{
		Workers:       4,
		BatchSize:     1000,
		MaxOpenConns:  16, // match the highest goroutine tier in the load test
		MaxIdleConns:  16,
		Host:          host,
		FallbackHosts: fallbackHosts,
		Port:          os.Getenv("TEST_AIO_SUBSYSTEMS_STORE_YUGABYTE_CONFIG_PORT"),
		Username:      os.Getenv("TEST_AIO_SUBSYSTEMS_STORE_YUGABYTE_CONFIG_USERNAME"),
		Password:      os.Getenv("TEST_AIO_SUBSYSTEMS_STORE_YUGABYTE_CONFIG_PASSWORD"),
		Database:      os.Getenv("TEST_AIO_SUBSYSTEMS_STORE_YUGABYTE_CONFIG_DATABASE"),
		Query:         map[string]string{"sslmode": "disable"},
		TxTimeout:     5 * time.Second,
		LoadBalance:   loadBalance,
		MaxRetries:    3,
	})
	if err != nil {
		b.Fatal("New:", err)
	}
	if err := s.Start(nil); err != nil {
		b.Fatal("Start:", err)
	}
	b.Cleanup(func() {
		_ = s.Reset()
		_ = s.Stop()
	})
	return s
}

// BenchmarkYugabyteStore runs all micro-benchmarks against a live YugabyteDB instance.
//
// Run with:
//
//	TEST_AIO_SUBSYSTEMS_STORE_YUGABYTE_CONFIG_HOST=localhost \
//	TEST_AIO_SUBSYSTEMS_STORE_YUGABYTE_CONFIG_PORT=5433 \
//	TEST_AIO_SUBSYSTEMS_STORE_YUGABYTE_CONFIG_USERNAME=yugabyte \
//	TEST_AIO_SUBSYSTEMS_STORE_YUGABYTE_CONFIG_PASSWORD=yugabyte \
//	TEST_AIO_SUBSYSTEMS_STORE_YUGABYTE_CONFIG_DATABASE=resonate \
//	go test -bench=BenchmarkYugabyteStore -benchtime=30s -count=3 -benchmem \
//	    -timeout 20m ./internal/app/subsystems/aio/store/yugabyte/
func BenchmarkYugabyteStore(b *testing.B) {
	s := newYugabyteBenchStore(b)
	bench.RunAll(b, s.workers[0])
}

// BenchmarkYugabyteStoreLoad runs the sustained load test and prints stats per
// worker-count tier.  Results are written to the same CSV as the Postgres run so
// you can compare both backends side-by-side.
//
// Run with the same env vars as above, plus optional:
//
//	BENCH_LOAD_DURATION=60s
//	BENCH_CSV_PATH=results/load.csv
func BenchmarkYugabyteStoreLoad(b *testing.B) {
	s := newYugabyteBenchStore(b)

	duration := 30 * time.Second
	if v := os.Getenv("BENCH_LOAD_DURATION"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			duration = d
		}
	}

	txBatchSize := 1
	if raw := os.Getenv("BENCH_BATCH_SIZE"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			txBatchSize = parsed
		}
	}

	csvPath := os.Getenv("BENCH_CSV_PATH")

	collector, err := bench.NewMetricsCollectorFromEnv()
	if err != nil {
		b.Logf("WARNING: could not create metrics collector: %v", err)
	}
	defer func() {
		if collector != nil {
			if cerr := collector.Close(); cerr != nil {
				b.Logf("WARNING: metrics collector close: %v", cerr)
			}
		}
	}()

	tiers := []int{1, 2, 4, 8, 16}
	var results []bench.Stats

	for _, workers := range tiers {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			b.ResetTimer()
			stats := bench.RunLoadTest(context.Background(), s.workers[0], bench.LoadConfig{
				NumWorkers:  workers,
				Duration:    duration,
				Backend:     "yugabyte",
				Collector:   collector,
				TxBatchSize: txBatchSize,
			})
			b.ReportMetric(stats.OpsPerSec, "ops/s")
			b.ReportMetric(float64(stats.LatencyP99.Microseconds()), "p99_us")
			b.ReportMetric(float64(stats.Errors), "errors")
			fmt.Println(stats)
			results = append(results, stats)
		})
	}

	if csvPath != "" {
		if err := bench.WriteCSV(csvPath, results); err != nil {
			b.Logf("WARNING: could not write CSV: %v", err)
		} else {
			b.Logf("results written to %s", csvPath)
		}
	}
}
