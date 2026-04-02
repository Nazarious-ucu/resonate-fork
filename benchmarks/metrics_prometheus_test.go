package benchmarks

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

// ── configuration ─────────────────────────────────────────────────────────────

const (
	defaultMetricsAddr = ":9092"
	defaultPushGWURL   = "http://localhost:9091"
	pushGWJob          = "resonate_fault_tolerance"
)

// ── registry + metric definitions ─────────────────────────────────────────────

var ftRegistry = prometheus.NewRegistry()

var (
	// ftOpsTotal counts every DB/HTTP operation by outcome.
	// result labels: "success" | "http_4xx" | "http_5xx" | "net_error"
	ftOpsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "resonate_ft_ops_total",
		Help: "Total DB/HTTP operations during fault-tolerance tests, by result.",
	}, []string{"test_name", "phase", "operation", "result"})

	// ftLatencySeconds records per-operation latency (successful ops only).
	ftLatencySeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "resonate_ft_latency_seconds",
		Help:    " Round-triplatency per operation in fault-tolerance tests.",
		Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
	}, []string{"test_name", "phase", "operation"})

	// ftOpsPerSecond is set once per phase at phase end.
	ftOpsPerSecond = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "resonate_ft_ops_per_second",
		Help: "Throughput (successful ops/s) at the end of each test phase.",
	}, []string{"test_name", "phase"})

	// ftAvailabilityRatio is the fraction of 2xx responses in a phase (0–1).
	ftAvailabilityRatio = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "resonate_ft_availability_ratio",
		Help: "Fraction of HTTP 2xx responses within a test phase.",
	}, []string{"test_name", "phase"})

	// ftRecoverySeconds records the time from docker-start to first successful
	// write in the RecoveryLatency test.
	ftRecoverySeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "resonate_ft_recovery_duration_seconds",
		Help: "Time from docker-start to first successful write (RecoveryLatency test).",
	}, []string{"test_name"})

	// ftDataIntegrityRows records expected vs actual DB row counts.
	// row_type labels: "expected" | "actual"
	ftDataIntegrityRows = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "resonate_ft_data_integrity_rows",
		Help: "DB row counts at each data-integrity checkpoint.",
	}, []string{"test_name", "checkpoint", "row_type"})

	// ftTestActive is 1 while a named test is running, 0 when it finishes.
	// Used by Grafana annotations to draw vertical lines at test boundaries.
	ftTestActive = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "resonate_ft_test_active",
		Help: "1 while the named fault-tolerance test is running, 0 after it finishes.",
	}, []string{"test_name"})
)

func init() {
	ftRegistry.MustRegister(
		ftOpsTotal,
		ftLatencySeconds,
		ftOpsPerSecond,
		ftAvailabilityRatio,
		ftRecoverySeconds,
		ftDataIntegrityRows,
		ftTestActive,
	)
}

// markTestActive sets ftTestActive to 1 for testName and registers a cleanup
// that sets it back to 0 when the test ends.
func markTestActive(t *testing.T, testName string) {
	t.Helper()
	ftTestActive.WithLabelValues(testName).Set(1)
	t.Cleanup(func() { ftTestActive.WithLabelValues(testName).Set(0) })
}

// ── per-operation recorder ─────────────────────────────────────────────────────

// recordOp records one completed operation to all relevant metrics.
// It is safe for concurrent use and cheap enough to call on every op.
func recordOp(testName, phaseName, opType string, lat time.Duration, statusCode int, err error) {
	result := opResultLabel(statusCode, err)
	ftOpsTotal.WithLabelValues(testName, phaseName, opType, result).Inc()
	// Latency histogram only for ops that reached the server (no net_error).
	if err == nil {
		ftLatencySeconds.WithLabelValues(testName, phaseName, opType).Observe(lat.Seconds())
	}
}

func opResultLabel(statusCode int, err error) string {
	if err != nil {
		return "net_error"
	}
	switch {
	case statusCode >= 200 && statusCode < 300:
		return "success"
	case statusCode >= 400 && statusCode < 500:
		return "http_4xx"
	default:
		return "http_5xx"
	}
}

// recordPhaseSummary updates the per-phase gauge metrics.
// Called from logPhase so it runs automatically after every phase.
func recordPhaseSummary(p *phase) {
	if p.TestName == "" {
		return
	}
	ftOpsPerSecond.WithLabelValues(p.TestName, p.Name).Set(p.opsPerSec())

	total := float64(p.s2xx + p.s4xx + p.s5xx + p.sNet)
	if total > 0 {
		ftAvailabilityRatio.WithLabelValues(p.TestName, p.Name).Set(float64(p.s2xx) / total)
	}
}

// ── metrics HTTP server ────────────────────────────────────────────────────────

// startMetricsServer binds addr synchronously and serves /metrics in the background.
// Returns the server so the caller can shut it down via Shutdown.
func startMetricsServer(addr string) (*http.Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("metrics server listen %s: %w", addr, err)
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(ftRegistry, promhttp.HandlerOpts{}))
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	return srv, nil
}

// checkMetricsEndpoint verifies the /metrics endpoint returns HTTP 200.
// addr is in ":port" form; the check uses 127.0.0.1.
func checkMetricsEndpoint(addr string) error {
	url := "http://127.0.0.1" + addr + "/metrics"
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return fmt.Errorf("GET %s: %w", url, err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s returned %d", url, resp.StatusCode)
	}
	return nil
}

// ── Pushgateway ───────────────────────────────────────────────────────────────

// pushToGateway sends all metrics in ftRegistry to the Pushgateway at gwURL.
// gwURL defaults to http://localhost:9091.
func pushToGateway(gwURL string) error {
	return push.New(gwURL, pushGWJob).
		Gatherer(ftRegistry).
		Push()
}

// ── TestMain helper ──────────────────────────────────────────────────────────

// initPrometheus starts the metrics server and verifies the /metrics endpoint.
//
// Live observability is provided by Prometheus scraping :9092 directly
// (the fault_tests job, 2 s interval).  No periodic Pushgateway push is
// started here — that would duplicate every counter in Prometheus while
// both sources are scraped simultaneously.  A single final push is made in
// finalizePrometheus once :9092 has been shut down, so post-run values are
// preserved in the Pushgateway for later review.
//
// Failures are non-fatal: the caller logs a warning and continues.
func initPrometheus() (*http.Server, func()) {
	addr := envOr("METRICS_ADDR", defaultMetricsAddr)
	srv, err := startMetricsServer(addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "WARNING: metrics server: %v\n", err)
		return nil, func() {}
	}

	if err := checkMetricsEndpoint(addr); err != nil {
		fmt.Fprintf(os.Stderr, "WARNING: metrics endpoint check failed: %v\n", err)
		return srv, func() {}
	}

	fmt.Printf("metrics server ready → http://127.0.0.1%s/metrics\n", addr)
	return srv, func() {}
}

// finalizePrometheus stops the periodic push loop, does a final push, and
// shuts down the metrics HTTP server.
func finalizePrometheus(srv *http.Server, stopPush func()) {
	stopPush()

	gwURL := envOr("PUSHGATEWAY_URL", defaultPushGWURL)
	if err := pushToGateway(gwURL); err != nil {
		fmt.Fprintf(os.Stderr, "WARNING: Pushgateway final push to %s failed: %v\n", gwURL, err)
	} else {
		fmt.Printf("final metrics pushed to Pushgateway → %s\n", gwURL)
	}

	if srv != nil {
		_ = srv.Shutdown(context.Background())
	}
}
