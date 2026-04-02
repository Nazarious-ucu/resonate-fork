// Package yugabyte — Prometheus metrics infrastructure for direct-store fault-tolerance tests.
//
// This file wires Prometheus counters/histograms/gauges into the store FT tests
// (store_fault_tolerance_test.go) and exposes them on a /metrics HTTP endpoint
// so Prometheus can scrape them live while the tests run.
//
// Metric naming: resonate_store_ft_*
// Job name used in Prometheus/Grafana: store_fault_tests
// Default scrape port: 9093  (configure with STORE_FT_METRICS_ADDR env var)
//
// The key distinction from the HTTP-level fault tests (benchmarks/) is the
// result label:
//
//	"success"     — Execute() returned nil (happy path)
//	"net_error"   — connection refused / EOF / timeout (expected during fault injection)
//	"store_error" — transaction error after all retries (unexpected; signals data-integrity risk)
//
// Separating net_error from store_error lets the Grafana dashboard show that
// connection failures during node kill are normal, while any store_error is a
// genuine problem requiring investigation.
package yugabyte

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/push"
)

// ── registry + metric definitions ─────────────────────────────────────────────

var storeFTRegistry = prometheus.NewRegistry()

var (
	// storeFTOpsTotal counts every store Execute() call by outcome.
	// result label values: "success" | "store_error" | "net_error"
	storeFTOpsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "resonate_store_ft_ops_total",
		Help: "Total store Execute() calls during fault-tolerance tests, by outcome.",
	}, []string{"test_name", "phase", "operation", "result"})

	// storeFTLatencySeconds records per-call latency for successful ops only.
	storeFTLatencySeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "resonate_store_ft_latency_seconds",
		Help:    "Store Execute() round-trip latency for successful calls.",
		Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
	}, []string{"test_name", "phase", "operation"})

	// storeFTOpsPerSecond is set once at the end of each phase.
	storeFTOpsPerSecond = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "resonate_store_ft_ops_per_second",
		Help: "Successful store ops/s measured at the end of each test phase.",
	}, []string{"test_name", "phase"})

	// storeFTAvailabilityRatio is the fraction of successful calls within a phase.
	storeFTAvailabilityRatio = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "resonate_store_ft_availability_ratio",
		Help: "Fraction of successful Execute() calls within each test phase (0–1).",
	}, []string{"test_name", "phase"})

	// storeFTDataIntegrityRows records expected vs actual DB row counts after each test.
	// row_type label: "expected" | "actual"
	storeFTDataIntegrityRows = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "resonate_store_ft_data_integrity_rows",
		Help: "DB row counts after each test: expected (=successful writes) vs actual (=SELECT COUNT).",
	}, []string{"test_name", "row_type"})

	// storeFTTestActive is 1 while a named test is running, 0 after it finishes.
	// Used by Grafana annotations to draw vertical lines at test boundaries.
	storeFTTestActive = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "resonate_store_ft_test_active",
		Help: "1 while the named store fault-tolerance test is running, 0 after it finishes.",
	}, []string{"test_name"})

	// storeFTTestStartsTotal is incremented exactly once when a test STARTS (never on end).
	// Grafana annotations use increase(resonate_store_ft_test_starts_total[30s]) > 0
	// so each annotation fires only at the start boundary, not at both edges.
	storeFTTestStartsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "resonate_store_ft_test_starts_total",
		Help: "Incremented once when a store fault-tolerance test starts. Used for start-only Grafana annotations.",
	}, []string{"test_name"})
)

func init() {
	storeFTRegistry.MustRegister(
		storeFTOpsTotal,
		storeFTLatencySeconds,
		storeFTOpsPerSecond,
		storeFTAvailabilityRatio,
		storeFTDataIntegrityRows,
		storeFTTestActive,
		storeFTTestStartsTotal,
	)
}

// ── per-operation recorder ─────────────────────────────────────────────────────

// recordStoreFTOp records one completed store Execute() call to all relevant metrics.
// Safe for concurrent use.
func recordStoreFTOp(testName, phaseName, opType string, lat time.Duration, err error) {
	if testName == "" {
		return
	}
	result := storeFTResultLabel(err)
	storeFTOpsTotal.WithLabelValues(testName, phaseName, opType, result).Inc()
	if err == nil {
		storeFTLatencySeconds.WithLabelValues(testName, phaseName, opType).Observe(lat.Seconds())
	}
}

func storeFTResultLabel(err error) string {
	if err == nil {
		return "success"
	}
	if isNetworkOrConnectionError(err) {
		return "net_error"
	}
	return "store_error"
}

// recordStoreFTPhase updates per-phase summary gauges after a phase completes.
// Called from logStoreFTPhase.
func recordStoreFTPhase(testName string, p *storeFTPhase) {
	if testName == "" {
		return
	}
	storeFTOpsPerSecond.WithLabelValues(testName, p.Name).Set(p.opsPerSec())
	total := float64(p.totalOps())
	if total > 0 {
		storeFTAvailabilityRatio.WithLabelValues(testName, p.Name).Set(float64(p.sOK) / total)
	}
}

// recordStoreFTDataIntegrity records expected and actual DB row counts for a test.
func recordStoreFTDataIntegrity(testName string, expected, actual int) {
	if testName == "" {
		return
	}
	storeFTDataIntegrityRows.WithLabelValues(testName, "expected").Set(float64(expected))
	storeFTDataIntegrityRows.WithLabelValues(testName, "actual").Set(float64(actual))
}

// markStoreFTTestActive increments storeFTTestStartsTotal (for start-only annotations),
// sets storeFTTestActive to 1, and registers a cleanup to set it back to 0 on finish.
// The counter is the annotation source — it only ever increases, so Grafana fires only
// at the start edge. The active gauge is used for the duration highlight panel.
func markStoreFTTestActive(t *testing.T, testName string) {
	t.Helper()
	storeFTTestStartsTotal.WithLabelValues(testName).Inc()
	storeFTTestActive.WithLabelValues(testName).Set(1)
	t.Cleanup(func() { storeFTTestActive.WithLabelValues(testName).Set(0) })
}

// ── metrics HTTP server ────────────────────────────────────────────────────────

const (
	defaultStoreFTMetricsAddr = ":9093"
	defaultStoreFTPushGWURL   = "http://localhost:9091"
	storeFTPushGWJob          = "resonate_store_fault_tolerance"
)

func initStoreFTMetrics() (*http.Server, error) {
	addr := envFT("STORE_FT_METRICS_ADDR", defaultStoreFTMetricsAddr)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("store FT metrics server listen %s: %w", addr, err)
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(storeFTRegistry, promhttp.HandlerOpts{}))
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	fmt.Printf("store FT metrics server ready → http://127.0.0.1%s/metrics\n", addr)
	return srv, nil
}

func finalizeStoreFTMetrics(srv *http.Server) {
	gwURL := envFT("PUSHGATEWAY_URL", defaultStoreFTPushGWURL)
	if err := push.New(gwURL, storeFTPushGWJob).Gatherer(storeFTRegistry).Push(); err != nil {
		fmt.Fprintf(os.Stderr, "WARNING: store FT Pushgateway push to %s failed: %v\n", gwURL, err)
	} else {
		fmt.Printf("store FT metrics pushed to Pushgateway → %s\n", gwURL)
	}
	if srv != nil {
		_ = srv.Shutdown(context.Background())
	}
}

// ── TestMain ─────────────────────────────────────────────────────────────────

// TestMain controls the lifecycle of all tests in this package:
//  1. Starts the Prometheus /metrics HTTP server (if STORE_FAULT_TEST_ENABLED=true).
//  2. Runs all tests (including the regular TestYugabyteStore and benchmarks).
//  3. Flushes JSON/CSV results and pushes metrics to the Pushgateway.
func TestMain(m *testing.M) {
	var srv *http.Server

	if os.Getenv("STORE_FAULT_TEST_ENABLED") == "true" {
		var err error
		srv, err = initStoreFTMetrics()
		if err != nil {
			fmt.Fprintf(os.Stderr, "WARNING: store FT metrics server: %v\n", err)
		}
	}

	code := m.Run()

	if os.Getenv("STORE_FAULT_TEST_ENABLED") == "true" {
		globalStoreFTWriter.flush()
		finalizeStoreFTMetrics(srv)
	}

	os.Exit(code)
}
