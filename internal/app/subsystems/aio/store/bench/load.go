package bench

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/pkg/promise"
)

// LoadConfig controls the load test parameters.
type LoadConfig struct {
	// NumWorkers is the number of concurrent goroutines issuing requests.
	NumWorkers int
	// Duration is how long to run the test.
	Duration time.Duration
	// Backend is a label written into results (e.g. "postgres", "yugabyte-3node").
	Backend string
}

// Stats contains the aggregated results of a load test run.
type Stats struct {
	Backend     string
	Workers     int
	Duration    time.Duration
	TotalOps    int64
	Errors      int64
	OpsPerSec   float64
	LatencyMean time.Duration
	LatencyP50  time.Duration
	LatencyP95  time.Duration
	LatencyP99  time.Duration
	LatencyMax  time.Duration
}

func (s Stats) String() string {
	return fmt.Sprintf(
		"backend=%-16s workers=%2d  ops=%7d  errors=%d  ops/s=%8.0f  "+
			"mean=%-8s  p50=%-8s  p95=%-8s  p99=%-8s  max=%s",
		s.Backend, s.Workers, s.TotalOps, s.Errors, s.OpsPerSec,
		s.LatencyMean, s.LatencyP50, s.LatencyP95, s.LatencyP99, s.LatencyMax,
	)
}

// RunLoadTest runs a sustained write load against the store and returns aggregated stats.
// Each goroutine creates promises in a tight loop for cfg.Duration.
func RunLoadTest(ctx context.Context, s store.Store, cfg LoadConfig) Stats {
	var (
		totalOps  atomic.Int64
		errors    atomic.Int64
		mu        sync.Mutex
		latencies []time.Duration
	)

	ctx, cancel := context.WithTimeout(ctx, cfg.Duration)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < cfg.NumWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make([]time.Duration, 0, 512)

			for ctx.Err() == nil {
				id := uid("load")
				now := time.Now().UnixMilli()
				tx := &t_aio.Transaction{
					Commands: []t_aio.Command{
						&t_aio.CreatePromiseCommand{
							Id:      id,
							State:   promise.Pending,
							Timeout: now + int64(time.Hour.Milliseconds()),
							Param: promise.Value{
								Headers: map[string]string{},
								Data:    []byte(`{"load":true}`),
							},
							Tags:      map[string]string{},
							CreatedOn: now,
						},
					},
				}

				start := time.Now()
				_, err := s.Execute([]*t_aio.Transaction{tx})
				elapsed := time.Since(start)

				if err != nil {
					errors.Add(1)
					continue
				}
				totalOps.Add(1)
				local = append(local, elapsed)
			}

			mu.Lock()
			latencies = append(latencies, local...)
			mu.Unlock()
		}()
	}

	start := time.Now()
	wg.Wait()
	actualDuration := time.Since(start)

	ops := totalOps.Load()
	errs := errors.Load()

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	pct := func(p float64) time.Duration {
		if len(latencies) == 0 {
			return 0
		}
		idx := int(float64(len(latencies)) * p)
		if idx >= len(latencies) {
			idx = len(latencies) - 1
		}
		return latencies[idx]
	}

	var sum time.Duration
	for _, l := range latencies {
		sum += l
	}
	var mean time.Duration
	if len(latencies) > 0 {
		mean = sum / time.Duration(len(latencies))
	}

	return Stats{
		Backend:     cfg.Backend,
		Workers:     cfg.NumWorkers,
		Duration:    actualDuration,
		TotalOps:    ops,
		Errors:      errs,
		OpsPerSec:   float64(ops) / actualDuration.Seconds(),
		LatencyMean: mean,
		LatencyP50:  pct(0.50),
		LatencyP95:  pct(0.95),
		LatencyP99:  pct(0.99),
		LatencyMax:  pct(1.00),
	}
}

// WriteCSV appends stats rows to a CSV file at path (creates the file if needed).
// Use this to collect results from multiple runs for thesis charts.
func WriteCSV(path string, rows []Stats) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			fmt.Printf("failed to close file: %v\n", err)
		}
	}(f)

	w := csv.NewWriter(f)

	// Write header only if the file is empty.
	info, _ := f.Stat()
	if info.Size() == 0 {
		if err := w.Write([]string{
			"backend", "workers", "duration_s", "total_ops", "errors",
			"ops_per_sec", "mean_us", "p50_us", "p95_us", "p99_us", "max_us",
		}); err != nil {
			return err
		}
	}

	for _, s := range rows {
		if err := w.Write([]string{
			s.Backend,
			fmt.Sprintf("%d", s.Workers),
			fmt.Sprintf("%.2f", s.Duration.Seconds()),
			fmt.Sprintf("%d", s.TotalOps),
			fmt.Sprintf("%d", s.Errors),
			fmt.Sprintf("%.2f", s.OpsPerSec),
			fmt.Sprintf("%d", s.LatencyMean.Microseconds()),
			fmt.Sprintf("%d", s.LatencyP50.Microseconds()),
			fmt.Sprintf("%d", s.LatencyP95.Microseconds()),
			fmt.Sprintf("%d", s.LatencyP99.Microseconds()),
			fmt.Sprintf("%d", s.LatencyMax.Microseconds()),
		}); err != nil {
			return err
		}
	}

	w.Flush()
	return w.Error()
}
