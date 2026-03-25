// Package bench provides shared benchmark helpers for comparing storage backends.
// Use RunAll from within a backend's _bench_test.go file, passing the backend's
// store.Store worker directly.
package bench

import (
	"encoding/csv"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/pkg/promise"
)

// MicroResult holds the result of a single micro-benchmark sub-run.
type MicroResult struct {
	// Name is the sub-benchmark name (e.g. "CreatePromise").
	Name string
	// Backend label, populated by RunAll via BENCH_BACKEND env var (optional).
	Backend string
	// N is the number of iterations the testing framework chose.
	N int
	// NsPerOp is the framework-measured nanoseconds per operation.
	NsPerOp float64
	// OpsPerSec is derived from NsPerOp.
	OpsPerSec float64
	// Timestamp records when the result was collected.
	Timestamp time.Time
}

// WriteMicroCSV appends MicroResult rows to a CSV file at path (creates if absent).
// A header row is written only when the file is empty.
func WriteMicroCSV(path string, rows []MicroResult) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			fmt.Printf("WARNING: failed to close CSV file: %v\n", err)
		}
	}(f)

	info, err := f.Stat()
	if err != nil {
		return err
	}

	w := csv.NewWriter(f)
	if info.Size() == 0 {
		if err := w.Write([]string{
			"timestamp", "backend", "benchmark", "n", "ns_per_op", "ops_per_sec",
		}); err != nil {
			return err
		}
	}

	for _, r := range rows {
		if err := w.Write([]string{
			r.Timestamp.UTC().Format(time.RFC3339),
			r.Backend,
			r.Name,
			fmt.Sprintf("%d", r.N),
			fmt.Sprintf("%.2f", r.NsPerOp),
			fmt.Sprintf("%.2f", r.OpsPerSec),
		}); err != nil {
			return err
		}
	}

	w.Flush()
	return w.Error()
}

// globalCounter ensures every benchmark iteration uses a unique promise ID,
// even across parallel goroutines and sub-benchmarks.
var globalCounter atomic.Int64

func uid(prefix string) string {
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), globalCounter.Add(1))
}

func createPromiseTx(id string) *t_aio.Transaction {
	now := time.Now().UnixMilli()
	return &t_aio.Transaction{
		Commands: []t_aio.Command{
			&t_aio.CreatePromiseCommand{
				Id:      id,
				State:   promise.Pending,
				Timeout: now + int64(time.Hour.Milliseconds()),
				Param: promise.Value{
					Headers: map[string]string{"content-type": "application/json"},
					Data:    []byte(`{"benchmark":true}`),
				},
				Tags:      map[string]string{"env": "bench"},
				CreatedOn: now,
			},
		},
	}
}

func readPromiseTx(id string) *t_aio.Transaction {
	return &t_aio.Transaction{
		Commands: []t_aio.Command{
			&t_aio.ReadPromiseCommand{Id: id},
		},
	}
}

func resolvePromiseTx(id string) *t_aio.Transaction {
	return &t_aio.Transaction{
		Commands: []t_aio.Command{
			&t_aio.UpdatePromiseCommand{
				Id:    id,
				State: promise.Resolved,
				Value: promise.Value{
					Headers: map[string]string{},
					Data:    []byte(`{"result":"ok"}`),
				},
				CompletedOn: time.Now().UnixMilli(),
			},
		},
	}
}

// BenchmarkCreatePromise measures sequential promise creation throughput.
func BenchmarkCreatePromise(b *testing.B, s store.Store) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.Execute([]*t_aio.Transaction{createPromiseTx(uid("create"))}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkReadPromise measures read throughput for a pre-existing promise.
// Only the reads are measured; seeding happens before b.ResetTimer.
func BenchmarkReadPromise(b *testing.B, s store.Store) {
	seedID := uid("read-seed")
	if _, err := s.Execute([]*t_aio.Transaction{createPromiseTx(seedID)}); err != nil {
		b.Fatal("seed:", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.Execute([]*t_aio.Transaction{readPromiseTx(seedID)}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkUpdatePromise measures state-transition throughput (pending → resolved).
// Only the UPDATE is timed; CREATE is done outside the timer.
func BenchmarkUpdatePromise(b *testing.B, s store.Store) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := uid("update")

		b.StopTimer()
		if _, err := s.Execute([]*t_aio.Transaction{createPromiseTx(id)}); err != nil {
			b.Fatal("seed:", err)
		}
		b.StartTimer()

		if _, err := s.Execute([]*t_aio.Transaction{resolvePromiseTx(id)}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCreatePromiseParallel measures concurrent promise creation throughput.
// Run with -cpu 1,2,4,8 to observe scaling behaviour.
func BenchmarkCreatePromiseParallel(b *testing.B, s store.Store) {
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := s.Execute([]*t_aio.Transaction{createPromiseTx(uid("par"))}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkSearchPromises measures search throughput across a pre-populated table.
//
// Each run seeds 200 promises under a unique ID prefix so the search only
// touches those 200 rows, regardless of how many rows prior benchmarks have
// inserted into the same table. This keeps latency stable across -count runs.
func BenchmarkSearchPromises(b *testing.B, s store.Store) {
	// Unique prefix for this run — used in both the seeded IDs and the search
	// pattern so the query can use the B-tree index on `id`.
	runPrefix := uid("srch")

	for i := 0; i < 200; i++ {
		id := fmt.Sprintf("%s-%04d", runPrefix, i)
		if _, err := s.Execute([]*t_aio.Transaction{createPromiseTx(id)}); err != nil {
			b.Fatal("seed:", err)
		}
	}

	searchPattern := runPrefix + "*"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.Execute([]*t_aio.Transaction{{
			Commands: []t_aio.Command{
				&t_aio.SearchPromisesCommand{
					Id:     searchPattern,
					States: []promise.State{promise.Pending},
					Tags:   map[string]string{},
					Limit:  100,
				},
			},
		}}); err != nil {
			b.Fatal(err)
		}
	}
}

// RunAll runs every benchmark as a sub-benchmark of b.
// Call this from each backend's *_bench_test.go file.
//
// Optional env vars:
//
//	BENCH_MICRO_CSV_PATH — path to a CSV file where per-benchmark results are
//	                       appended after all sub-benchmarks finish.
//	BENCH_BACKEND        — label written into the CSV "backend" column.
//
// Example command (postgres, 30 s per sub-benchmark, 3 statistical runs):
//
//	BENCH_BACKEND=postgres BENCH_MICRO_CSV_PATH=results/micro.csv \
//	go test -bench=BenchmarkPostgresStore -benchtime=30s -count=3 -benchmem \
//	    -timeout 20m ./internal/app/subsystems/aio/store/postgres/
//
// The -timeout 20m flag is required: 5 sub-benchmarks × 3 counts × 30 s = 7.5 min
// plus calibration, which exceeds Go's default 10-minute test timeout.
func RunAll(b *testing.B, s store.Store) {
	b.Helper()

	csvPath := os.Getenv("BENCH_MICRO_CSV_PATH")
	backend := os.Getenv("BENCH_BACKEND")
	var results []MicroResult

	run := func(name string, fn func(*testing.B, store.Store)) {
		b.Run(name, func(b *testing.B) {
			fn(b, s)
			// b.Elapsed() returns the framework-measured time (honours
			// ResetTimer / StopTimer / StartTimer), so ns/op is accurate.
			elapsed := b.Elapsed()
			if csvPath != "" && b.N > 0 && elapsed > 0 {
				nsPerOp := float64(elapsed.Nanoseconds()) / float64(b.N)
				results = append(results, MicroResult{
					Name:      name,
					Backend:   backend,
					N:         b.N,
					NsPerOp:   nsPerOp,
					OpsPerSec: 1e9 / nsPerOp,
					Timestamp: time.Now(),
				})
			}
		})
	}

	run("CreatePromise", BenchmarkCreatePromise)
	run("ReadPromise", BenchmarkReadPromise)
	run("UpdatePromise", BenchmarkUpdatePromise)
	run("CreatePromiseParallel", BenchmarkCreatePromiseParallel)
	run("SearchPromises", BenchmarkSearchPromises)

	if csvPath != "" && len(results) > 0 {
		if err := WriteMicroCSV(csvPath, results); err != nil {
			b.Logf("WARNING: could not write micro CSV: %v", err)
		} else {
			b.Logf("micro-benchmark results written to %s", csvPath)
		}
	}
}
