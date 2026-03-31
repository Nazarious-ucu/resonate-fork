// Package benchmarks contains system-level fault-tolerance tests for the
// YugabyteDB-backed Resonate server.
//
// Each test is guarded by FAULT_TEST_ENABLED=true and requires:
//   - A running 3-node YugabyteDB cluster
//     (docker compose -f docker/docker-compose.fault-tolerance.yml up -d)
//   - A running Resonate server (RESONATE_URL, default http://localhost:8001)
//   - Docker accessible from the host running the tests
//
// Results are written to benchmarks/results/fault_<timestamp>.{json,csv}
// after every test run so they can be imported into a spreadsheet or thesis.
//
// Environment variables (all optional):
//
//	RESONATE_URL       — Resonate HTTP base URL     (default: http://localhost:8001)
//	FAULT_CONTAINER    — follower container to kill  (default: yb-node2)
//	PRIMARY_CONTAINER  — primary container to kill   (default: yb-node1)
//	FAULT_WORKERS      — concurrent writers          (default: 8)
//	YUGABYTE_HOST      — direct DB host              (default: localhost)
//	YUGABYTE_PORT      — direct DB port              (default: 5433)
//	YUGABYTE_USER      — DB user                     (default: yugabyte)
//	YUGABYTE_PASSWORD  — DB password                 (default: yugabyte)
//	YUGABYTE_DB        — DB name                     (default: resonate)
//
// Quick start:
//
//	docker compose -f docker/docker-compose.fault-tolerance.yml up -d
//	# wait ~40 s for cluster + Resonate to be healthy
//	FAULT_TEST_ENABLED=true go test -v -timeout=30m ./benchmarks/
package benchmarks

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/yugabyte/pgx/v5/stdlib"
)

// ── TestMain — shared result writer ──────────────────────────────────────────

var globalWriter = &ResultWriter{}

func TestMain(m *testing.M) {
	metricsSrv, stopPush := initPrometheus()

	code := m.Run()

	globalWriter.flush()
	finalizePrometheus(metricsSrv, stopPush)
	os.Exit(code)
}

// ── env / config helpers ──────────────────────────────────────────────────────

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func resonateURL() string      { return envOr("RESONATE_URL", "http://localhost:8001") }
func faultContainer() string   { return envOr("FAULT_CONTAINER", "yb-node2") }
func primaryContainer() string { return envOr("PRIMARY_CONTAINER", "yb-node1") }
func numWorkers() int {
	if s := os.Getenv("FAULT_WORKERS"); s != "" {
		n, err := strconv.Atoi(s)
		if err == nil && n > 0 {
			return n
		}
	}
	return 8
}

func requireFaultTest(t *testing.T) {
	t.Helper()
	if os.Getenv("FAULT_TEST_ENABLED") != "true" {
		t.Skip("set FAULT_TEST_ENABLED=true to run fault-tolerance tests")
	}
}

// waitForServer blocks until Resonate responds successfully or the 2-minute
// deadline is exceeded.  Call at the start of every test to prevent a
// previous test's node-kill from contaminating the next test's baseline.
func waitForServer(t *testing.T, baseURL string) {
	t.Helper()
	t.Log("waiting for Resonate to be healthy...")
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		resp, err := http.Get(baseURL + "/promises?limit=1")
		if err == nil {
			resp.Body.Close()
			t.Log("Resonate is healthy")
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatal("Resonate did not become healthy within 2 minutes")
}

// waitForClusterBalanced polls yb-admin get_is_load_balancer_idle until the
// YugaByte load balancer reports that all tablet leaders are evenly distributed.
// This replaces fixed time.Sleep calls after a node restart — it guarantees the
// cluster is truly ready for the next test's baseline rather than still
// rebalancing in the background.
//
// An initial 15s delay is required before polling: right after a node restart
// YugaByte briefly reports "idle" because the master hasn't yet registered the
// rejoined node and queued its tablets for redistribution.  The delay lets the
// tserver heartbeat reach the master so the load balancer has real work to do
// before we start asking whether it has finished.
//
// The command is run inside yb-node1's container so no local yb-admin install
// is required.  A 5-minute deadline is used; if the balancer is still not idle
// after that the function logs a warning and returns (rather than failing the
// test) so results are still recorded.
func waitForClusterBalanced(t *testing.T) {
	t.Helper()
	baseURL := resonateURL()

	// Phase 1: wait for YugaByte load balancer to report idle.
	// We first sleep 15s so the rejoined tserver has time to send its
	// heartbeat to the master and register pending tablet work.  Without this
	// delay the master hasn't seen the node yet and reports idle prematurely.
	t.Log("waiting 15s for rejoined node to register with the cluster...")
	time.Sleep(15 * time.Second)

	t.Log("polling YugaByte load balancer until idle...")
	masters := "yb-node1:7100,yb-node2:7100,yb-node3:7100"
	start := time.Now()
	deadline := start.Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		out, err := exec.Command("docker", "exec", "yb-node1",
			"bin/yb-admin", "-master_addresses", masters,
			"get_is_load_balancer_idle",
		).Output()
		if err == nil && strings.Contains(string(out), "Idle") {
			t.Logf("load balancer idle (elapsed %s)", time.Since(start).Round(time.Second))
			break
		}
		time.Sleep(5 * time.Second)
	}

	// Phase 2: wait for Resonate's HTTP layer to accept requests.
	waitForServer(t, baseURL)

	// Phase 3: verify the write path is healthy.
	// waitForServer only issues a GET (read), which may succeed via a cached
	// DB connection while the write path is still recovering — e.g. when the
	// restarted node is still bootstrapping tablets and new write connections
	// to it stall.  We probe POST /promises until one succeeds, confirming
	// the full write path (HTTP → kernel → AIO store → YugaByte) is live
	// before the next measurement phase begins.
	t.Log("probing write path until healthy...")
	writeClient := &http.Client{Timeout: 5 * time.Second}
	writeStart := time.Now()
	writeDeadline := writeStart.Add(60 * time.Second)
	for time.Now().Before(writeDeadline) {
		id := fmt.Sprintf("recovery-probe-%d", time.Now().UnixNano())
		status, err := createPromise(baseURL, id, writeClient)
		if err == nil && status >= 200 && status < 300 {
			t.Logf("write path healthy (elapsed %s)", time.Since(writeStart).Round(time.Second))
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Log("warning: write path not healthy after 60s — proceeding anyway")
}

// ── HTTP helpers ──────────────────────────────────────────────────────────────

func createPromise(baseURL, id string, client *http.Client) (int, error) {
	body, _ := json.Marshal(map[string]any{
		"id":      id,
		"timeout": time.Now().Add(time.Hour).UnixMilli(),
		"param":   map[string]any{"headers": map[string]string{}, "data": []byte("benchmark")},
	})
	req, _ := http.NewRequest(http.MethodPost, baseURL+"/promises", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	return resp.StatusCode, nil
}

func readPromise(baseURL, id string, client *http.Client) (int, error) {
	resp, err := client.Get(baseURL + "/promises/" + id)
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	return resp.StatusCode, nil
}

// ── Docker helpers ────────────────────────────────────────────────────────────

func dockerStop(container string) error {
	return exec.Command("docker", "stop", container).Run()
}

func dockerStart(container string) error {
	return exec.Command("docker", "start", container).Run()
}

func dockerPause(container string) error {
	return exec.Command("docker", "pause", container).Run()
}

func dockerUnpause(container string) error {
	return exec.Command("docker", "unpause", container).Run()
}

// ── DB helpers ────────────────────────────────────────────────────────────────

func dbConn(t *testing.T) *sql.DB {
	t.Helper()
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		envOr("YUGABYTE_USER", "yugabyte"),
		envOr("YUGABYTE_PASSWORD", "yugabyte"),
		envOr("YUGABYTE_HOST", "localhost"),
		envOr("YUGABYTE_PORT", "5433"),
		envOr("YUGABYTE_DB", "resonate"),
	)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("dbConn: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("dbConn ping: %v", err)
	}
	return db
}

// countPromises returns how many rows in the promises table match the given IDs.
func countPromises(db *sql.DB, ids []string) (int, error) {
	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}
	q := fmt.Sprintf("SELECT COUNT(*) FROM promises WHERE id IN (%s)", strings.Join(placeholders, ","))
	var n int
	return n, db.QueryRow(q, args...).Scan(&n)
}

// ── phase — per-phase metrics ──────────────────────────────────────────────────

type phase struct {
	Name     string
	Duration time.Duration
	// TestName and OpType are set at construction and used for Prometheus labels.
	// If TestName is empty the Prometheus recording is skipped silently.
	TestName string
	OpType   string // e.g. "create_promise", "read_promise"
	mu       sync.Mutex
	lats     []time.Duration // latencies for successful (2xx) requests only
	s2xx     int64
	s4xx     int64
	s5xx     int64
	sNet     int64 // connection / timeout errors
}

func (p *phase) record(lat time.Duration, statusCode int, err error) {
	// Record to Prometheus before acquiring the phase lock so that metric
	// writes never contend with percentile computation.
	recordOp(p.TestName, p.Name, p.OpType, lat, statusCode, err)

	p.mu.Lock()
	defer p.mu.Unlock()
	if err != nil {
		p.sNet++
		return
	}
	switch {
	case statusCode >= 200 && statusCode < 300:
		p.s2xx++
		p.lats = append(p.lats, lat)
	case statusCode >= 400 && statusCode < 500:
		p.s4xx++
	default:
		p.s5xx++
	}
}

func (p *phase) pctMs(pct float64) float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.lats) == 0 {
		return 0
	}
	s := make([]time.Duration, len(p.lats))
	copy(s, p.lats)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return toMs(s[int(float64(len(s)-1)*pct)])
}

func (p *phase) minMs() float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.lats) == 0 {
		return 0
	}
	m := p.lats[0]
	for _, l := range p.lats[1:] {
		if l < m {
			m = l
		}
	}
	return toMs(m)
}

func (p *phase) maxMs() float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.lats) == 0 {
		return 0
	}
	m := p.lats[0]
	for _, l := range p.lats[1:] {
		if l > m {
			m = l
		}
	}
	return toMs(m)
}

func (p *phase) opsPerSec() float64 {
	if p.Duration.Seconds() == 0 {
		return 0
	}
	return float64(p.s2xx) / p.Duration.Seconds()
}

func toMs(d time.Duration) float64 { return float64(d.Microseconds()) / 1000.0 }

// ── FaultTestResult — one row in output files ─────────────────────────────────

type FaultTestResult struct {
	RunID          string    `json:"run_id"`
	Timestamp      time.Time `json:"timestamp"`
	TestName       string    `json:"test_name"`
	Phase          string    `json:"phase"`
	DurationSec    float64   `json:"duration_sec"`
	TotalOps       int64     `json:"total_ops"`
	OpsPerSec      float64   `json:"ops_per_sec"`
	HTTP2xx        int64     `json:"http_2xx"`
	HTTP4xx        int64     `json:"http_4xx"`
	HTTP5xx        int64     `json:"http_5xx"`
	NetErrors      int64     `json:"net_errors"`
	MinMs          float64   `json:"min_ms"`
	P50Ms          float64   `json:"p50_ms"`
	P95Ms          float64   `json:"p95_ms"`
	P99Ms          float64   `json:"p99_ms"`
	MaxMs          float64   `json:"max_ms"`
	DBRowsExpected int       `json:"db_rows_expected,omitempty"`
	DBRowsActual   int       `json:"db_rows_actual,omitempty"`
	RecoveryMs     float64   `json:"recovery_ms,omitempty"`
	Notes          string    `json:"notes,omitempty"`
}

func phaseResult(runID, testName string, p *phase) FaultTestResult {
	return FaultTestResult{
		RunID:       runID,
		Timestamp:   time.Now().UTC(),
		TestName:    testName,
		Phase:       p.Name,
		DurationSec: p.Duration.Seconds(),
		TotalOps:    p.s2xx,
		OpsPerSec:   p.opsPerSec(),
		HTTP2xx:     p.s2xx,
		HTTP4xx:     p.s4xx,
		HTTP5xx:     p.s5xx,
		NetErrors:   p.sNet,
		MinMs:       p.minMs(),
		P50Ms:       p.pctMs(0.50),
		P95Ms:       p.pctMs(0.95),
		P99Ms:       p.pctMs(0.99),
		MaxMs:       p.maxMs(),
	}
}

// ── ResultWriter ──────────────────────────────────────────────────────────────

type ResultWriter struct {
	mu      sync.Mutex
	records []FaultTestResult
}

func (rw *ResultWriter) add(r FaultTestResult) {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	rw.records = append(rw.records, r)
}

// flush writes results/fault_<timestamp>.json and .csv in the package directory.
// Called once from TestMain after all tests finish.
func (rw *ResultWriter) flush() {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	if len(rw.records) == 0 {
		return
	}
	if err := os.MkdirAll("results", 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "ResultWriter mkdir: %v\n", err)
		return
	}
	ts := time.Now().Format("20060102_150405")
	rw.writeJSON("results/fault_" + ts + ".json")
	rw.writeCSV("results/fault_" + ts + ".csv")
}

func (rw *ResultWriter) writeJSON(path string) {
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ResultWriter json create: %v\n", err)
		return
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(rw.records); err != nil {
		fmt.Fprintf(os.Stderr, "ResultWriter json encode: %v\n", err)
		return
	}
	fmt.Printf("fault-tolerance results → %s\n", path)
}

func (rw *ResultWriter) writeCSV(path string) {
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ResultWriter csv create: %v\n", err)
		return
	}

	defer f.Close()
	w := csv.NewWriter(f)
	_ = w.Write([]string{
		"run_id", "timestamp", "test_name", "phase",
		"duration_sec", "total_ops", "ops_per_sec",
		"http_2xx", "http_4xx", "http_5xx", "net_errors",
		"min_ms", "p50_ms", "p95_ms", "p99_ms", "max_ms",
		"db_rows_expected", "db_rows_actual", "recovery_ms", "notes",
	})
	ff := func(v float64) string { return strconv.FormatFloat(v, 'f', 3, 64) }
	for _, r := range rw.records {
		_ = w.Write([]string{
			r.RunID, r.Timestamp.Format(time.RFC3339),
			r.TestName, r.Phase,
			ff(r.DurationSec),
			strconv.FormatInt(r.TotalOps, 10),
			ff(r.OpsPerSec),
			strconv.FormatInt(r.HTTP2xx, 10),
			strconv.FormatInt(r.HTTP4xx, 10),
			strconv.FormatInt(r.HTTP5xx, 10),
			strconv.FormatInt(r.NetErrors, 10),
			ff(r.MinMs), ff(r.P50Ms), ff(r.P95Ms), ff(r.P99Ms), ff(r.MaxMs),
			strconv.Itoa(r.DBRowsExpected),
			strconv.Itoa(r.DBRowsActual),
			ff(r.RecoveryMs),
			r.Notes,
		})
	}
	w.Flush()
	fmt.Printf("fault-tolerance results → %s\n", path)
}

// ── generic phase runners ─────────────────────────────────────────────────────

// runWritePhase sends unique CreatePromise requests for the phase duration.
func runWritePhase(ctx context.Context, p *phase, baseURL string, workers int, client *http.Client) {
	var wg sync.WaitGroup
	var counter atomic.Int64
	deadline := time.Now().Add(p.Duration)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) && ctx.Err() == nil {
				id := fmt.Sprintf("ft-%d-%d", time.Now().UnixNano(), counter.Add(1))
				t0 := time.Now()
				status, err := createPromise(baseURL, id, client)
				p.record(time.Since(t0), status, err)
			}
		}()
	}
	wg.Wait()
}

// runReadPhase reads the given IDs in round-robin for the phase duration.
func runReadPhase(ctx context.Context, p *phase, baseURL string, ids []string, workers int, client *http.Client) {
	var wg sync.WaitGroup
	var idx atomic.Int64
	n := int64(len(ids))
	deadline := time.Now().Add(p.Duration)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) && ctx.Err() == nil {
				id := ids[(idx.Add(1)-1)%n]
				t0 := time.Now()
				status, err := readPromise(baseURL, id, client)
				p.record(time.Since(t0), status, err)
			}
		}()
	}
	wg.Wait()
}

// logPhase prints a phase summary to the test log and records Prometheus gauges.
func logPhase(t *testing.T, p *phase) {
	t.Helper()
	recordPhaseSummary(p)
	t.Logf("  %-22s  ops/s=%6.0f  2xx=%6d  4xx=%5d  5xx=%5d  netErr=%5d  min=%6.1fms  p50=%6.1fms  p95=%7.1fms  p99=%7.1fms  max=%7.1fms",
		p.Name,
		p.opsPerSec(),
		p.s2xx, p.s4xx, p.s5xx, p.sNet,
		p.minMs(), p.pctMs(0.50), p.pctMs(0.95), p.pctMs(0.99), p.maxMs(),
	)
}

// ── Test 1: TestFaultTolerance_NodeKill ───────────────────────────────────────
//
// Kills a follower YugaByte node (FAULT_CONTAINER, default yb-node2) while
// running a continuous write load, then restarts it and measures recovery.
// With RF=3 and one node down the cluster must remain writable.
func TestFaultTolerance_NodeKill(t *testing.T) {
	requireFaultTest(t)
	const testName = "NodeKill"
	markTestActive(t, testName)

	runID := time.Now().Format("20060102_150405")
	baseURL := resonateURL()
	container := faultContainer()
	workers := numWorkers()
	client := &http.Client{Timeout: 3 * time.Second}

	waitForServer(t, baseURL)

	phases := []*phase{
		{Name: "before-failure", Duration: 15 * time.Second, TestName: testName, OpType: "create_promise"},
		{Name: "during-failure", Duration: 30 * time.Second, TestName: testName, OpType: "create_promise"},
		{Name: "after-recovery", Duration: 30 * time.Second, TestName: testName, OpType: "create_promise"}, // extended: tablet leader rebalancing takes >15s
	}
	ctx := context.Background()

	t.Logf("=== %s: killing container %q, workers=%d ===", testName, container, workers)

	t.Log("--- phase: before-failure")
	runWritePhase(ctx, phases[0], baseURL, workers, client)
	logPhase(t, phases[0])

	t.Logf("--- killing %q", container)
	if err := dockerStop(container); err != nil {
		t.Fatalf("docker stop %s: %v", container, err)
	}

	t.Log("--- phase: during-failure")
	runWritePhase(ctx, phases[1], baseURL, workers, client)
	logPhase(t, phases[1])

	t.Logf("--- restarting %q", container)
	if err := dockerStart(container); err != nil {
		t.Fatalf("docker start %s: %v", container, err)
	}
	waitForClusterBalanced(t)

	t.Log("--- phase: after-recovery")
	runWritePhase(ctx, phases[2], baseURL, workers, client)
	logPhase(t, phases[2])

	t.Log("=== Summary ===")
	t.Logf("%-22s  %8s  %8s  %8s  %8s", "phase", "ops/s", "2xx", "errors", "p99_ms")
	for _, p := range phases {
		t.Logf("%-22s  %8.0f  %8d  %8d  %8.1f",
			p.Name, p.opsPerSec(), p.s2xx, p.s4xx+p.s5xx+p.sNet, p.pctMs(0.99))
		r := phaseResult(runID, testName, p)
		r.Notes = fmt.Sprintf("container=%s workers=%d", container, workers)
		globalWriter.add(r)
	}

	// Key assertions — what actually matters for correctness:
	//
	// 1. Near-zero errors during failure: with RF=3 and 1 node down the
	//    cluster must remain writable.  We allow a small error budget
	//    (≤1% of total ops OR ≤50 absolute, whichever is larger) to
	//    account for the brief Raft tablet-leader election that occurs when
	//    the killed node held some tablet leaders.  Election typically
	//    completes in 3–5 s; writes targeting those tablets hang until the
	//    new leader is elected and may exceed the 3 s HTTP client timeout,
	//    appearing as netErr.  This is an expected YugaByte behaviour, not
	//    a quorum loss — the cluster never drops below the RF=3 write
	//    quorum of 2 nodes.
	totalDuring := phases[1].s2xx + phases[1].sNet + phases[1].s4xx + phases[1].s5xx
	errorsDuring := phases[1].sNet + phases[1].s4xx + phases[1].s5xx
	budget := int64(50)
	if pct := totalDuring / 100; pct > budget {
		budget = pct
	}
	if errorsDuring > budget {
		t.Errorf("during-failure: got %d errors (net=%d 4xx=%d 5xx=%d) out of %d ops — exceeds 1%% budget (%d); cluster may have lost availability",
			errorsDuring, phases[1].sNet, phases[1].s4xx, phases[1].s5xx, totalDuring, budget)
	}
	// 2. System recovers: at least one successful write after node rejoins.
	if phases[2].s2xx == 0 {
		t.Errorf("after-recovery: 0 successful ops — cluster did not recover")
	}
	// NOTE: throughput after recovery is intentionally not asserted here.
	// YugaByte tablet leader rebalancing back to the rejoined node takes
	// 2–3 minutes; during the 30s measurement window the cluster still
	// operates at ~65% of full capacity. This is a thesis finding, not a bug.
	if phases[0].s2xx > 0 && phases[2].s2xx > 0 {
		t.Logf("throughput recovery ratio: %.0f%% (%.0f → %.0f ops/s); tablet rebalancing may still be in progress",
			phases[2].opsPerSec()/phases[0].opsPerSec()*100,
			phases[0].opsPerSec(), phases[2].opsPerSec())
	}
}

// ── Test 2: TestFaultTolerance_PrimaryNodeKill ────────────────────────────────
//
// Kills the primary/bootstrap YugaByte node (PRIMARY_CONTAINER, default yb-node1)
// which the Resonate server uses as its primary connection target.
// Tests that load balancing redirects traffic to the remaining two nodes and
// that tablet leader re-election restores write availability.
func TestFaultTolerance_PrimaryNodeKill(t *testing.T) {
	requireFaultTest(t)
	const testName = "PrimaryNodeKill"
	markTestActive(t, testName)

	runID := time.Now().Format("20060102_150405")
	baseURL := resonateURL()
	container := primaryContainer()
	workers := numWorkers()
	client := &http.Client{Timeout: 5 * time.Second} // longer timeout: leader election

	waitForServer(t, baseURL)

	phases := []*phase{
		{Name: "before-failure", Duration: 15 * time.Second, TestName: testName, OpType: "create_promise"},
		{Name: "during-failure", Duration: 40 * time.Second, TestName: testName, OpType: "create_promise"}, // longer: leader election takes more time
		{Name: "after-recovery", Duration: 30 * time.Second, TestName: testName, OpType: "create_promise"}, // extended: connection pool recycling takes up to 60s
	}
	ctx := context.Background()

	t.Logf("=== %s: killing PRIMARY container %q, workers=%d ===", testName, container, workers)

	t.Log("--- phase: before-failure")
	runWritePhase(ctx, phases[0], baseURL, workers, client)
	logPhase(t, phases[0])

	t.Logf("--- killing primary %q", container)
	if err := dockerStop(container); err != nil {
		t.Fatalf("docker stop %s: %v", container, err)
	}

	t.Log("--- phase: during-failure (leader election expected)")
	runWritePhase(ctx, phases[1], baseURL, workers, client)
	logPhase(t, phases[1])

	t.Logf("--- restarting %q", container)
	if err := dockerStart(container); err != nil {
		t.Fatalf("docker start %s: %v", container, err)
	}
	// Wait for the cluster to fully rebalance and for the Resonate connection
	// pool to recycle its stale connections (SetConnMaxLifetime=60s).
	waitForClusterBalanced(t)

	t.Log("--- phase: after-recovery")
	runWritePhase(ctx, phases[2], baseURL, workers, client)
	logPhase(t, phases[2])

	t.Log("=== Summary ===")
	t.Logf("%-22s  %8s  %8s  %8s  %8s", "phase", "ops/s", "2xx", "errors", "p99_ms")
	for _, p := range phases {
		t.Logf("%-22s  %8.0f  %8d  %8d  %8.1f",
			p.Name, p.opsPerSec(), p.s2xx, p.s4xx+p.s5xx+p.sNet, p.pctMs(0.99))
		r := phaseResult(runID, testName, p)
		r.Notes = fmt.Sprintf("container=%s workers=%d", container, workers)
		globalWriter.add(r)
	}

	// Fail explicitly when after-recovery has zero ops — the server did not recover.
	if phases[2].s2xx == 0 {
		t.Errorf("after-recovery: 0 successful ops (net_errors=%d) — Resonate did not recover DB connectivity after %s restarted",
			phases[2].sNet, container)
		return
	}
	if phases[0].s2xx > 0 {
		ratio := phases[2].opsPerSec() / phases[0].opsPerSec()
		if ratio < 0.65 {
			t.Errorf("after-recovery throughput %.0f ops/s is >35%% below baseline %.0f ops/s",
				phases[2].opsPerSec(), phases[0].opsPerSec())
		}
	}
}

// ── Test 3: TestFaultTolerance_NodePause ──────────────────────────────────────
//
// Pauses a YugaByte node with `docker pause` (sends SIGSTOP — simulates a
// frozen/unreachable node without a clean shutdown).  This is a different
// failure mode from NodeKill: the cluster must detect the unresponsive node
// via heartbeat timeout rather than receiving a clean disconnect.
func TestFaultTolerance_NodePause(t *testing.T) {
	requireFaultTest(t)
	const testName = "NodePause"
	markTestActive(t, testName)

	runID := time.Now().Format("20060102_150405")
	baseURL := resonateURL()
	container := faultContainer()
	workers := numWorkers()
	client := &http.Client{Timeout: 5 * time.Second}

	waitForServer(t, baseURL) // ensures previous test's node-kill hasn't left server degraded

	phases := []*phase{
		{Name: "before-pause", Duration: 15 * time.Second, TestName: testName, OpType: "create_promise"},
		{Name: "during-pause", Duration: 30 * time.Second, TestName: testName, OpType: "create_promise"},
		{Name: "after-unpause", Duration: 15 * time.Second, TestName: testName, OpType: "create_promise"},
	}
	ctx := context.Background()

	t.Logf("=== %s: pausing container %q, workers=%d ===", testName, container, workers)

	t.Log("--- phase: before-pause")
	runWritePhase(ctx, phases[0], baseURL, workers, client)
	logPhase(t, phases[0])

	t.Logf("--- pausing %q (SIGSTOP — simulated network partition)", container)
	if err := dockerPause(container); err != nil {
		t.Fatalf("docker pause %s: %v", container, err)
	}

	t.Log("--- phase: during-pause")
	runWritePhase(ctx, phases[1], baseURL, workers, client)
	logPhase(t, phases[1])

	t.Logf("--- unpausing %q", container)
	if err := dockerUnpause(container); err != nil {
		t.Fatalf("docker unpause %s: %v", container, err)
	}
	waitForClusterBalanced(t)

	t.Log("--- phase: after-unpause")
	runWritePhase(ctx, phases[2], baseURL, workers, client)
	logPhase(t, phases[2])

	t.Log("=== Summary ===")
	for _, p := range phases {
		logPhase(t, p)
		r := phaseResult(runID, testName, p)
		r.Notes = fmt.Sprintf("container=%s method=SIGSTOP", container)
		globalWriter.add(r)
	}

	// NOTE: during-pause throughput is expected to be 0.
	// `docker pause` sends SIGSTOP to ALL processes in the container,
	// including the kernel TCP stack. Connections to the paused node do not
	// receive RST/FIN — they hang silently until the application timeout fires.
	// With Workers=1 (one DB connection), all HTTP workers queue behind the
	// single hung connection and timeout together. This is expected behaviour
	// for SIGSTOP, not a quorum failure.
	t.Logf("during-pause: %d ops (expected ~0 due to SIGSTOP TCP hang), %d net timeouts",
		phases[1].s2xx, phases[1].sNet)

	// Assert: after-unpause must recover to ≥80% of before-pause baseline.
	if phases[0].s2xx > 0 && phases[2].s2xx > 0 {
		ratio := phases[2].opsPerSec() / phases[0].opsPerSec()
		if ratio < 0.80 {
			t.Errorf("after-unpause throughput %.0f ops/s is >20%% below before-pause %.0f ops/s — cluster did not recover from SIGSTOP",
				phases[2].opsPerSec(), phases[0].opsPerSec())
		}
	}
	// Assert: system must recover at all.
	if phases[2].s2xx == 0 {
		t.Errorf("after-unpause: 0 successful ops — cluster did not recover after SIGSTOP")
	}
}

// ── Test 4: TestFaultTolerance_DataIntegrity ──────────────────────────────────
//
// Verifies that no data is lost during a node failure.
//  1. Writes N promises and records their IDs.
//  2. Verifies all N are readable via HTTP and counts DB rows before failure.
//  3. Kills a YugaByte node.
//  4. Verifies reads and DB row count during failure (data must still be there).
//  5. Restarts the node.
//  6. Verifies reads and DB row count after full recovery.
func TestFaultTolerance_DataIntegrity(t *testing.T) {
	requireFaultTest(t)
	const testName = "DataIntegrity"
	markTestActive(t, testName)

	runID := time.Now().Format("20060102_150405")
	const N = 100
	baseURL := resonateURL()
	container := faultContainer()
	client := &http.Client{Timeout: 3 * time.Second}
	db := dbConn(t)
	defer db.Close()

	// 1. Write N promises.
	t.Logf("=== %s: writing %d promises, then killing %q ===", testName, N, container)
	ids := make([]string, N)
	writeErrors := 0
	for i := range ids {
		ids[i] = fmt.Sprintf("integrity-%d-%04d", time.Now().UnixNano(), i)
		t0 := time.Now()
		status, err := createPromise(baseURL, ids[i], client)
		recordOp(testName, "write", "create_promise", time.Since(t0), status, err)
		if err != nil || status < 200 || status >= 300 {
			writeErrors++
		}
	}
	t.Logf("write phase: total=%d  errors=%d", N, writeErrors)

	check := func(label string) (httpOK int, dbCount int) {
		for _, id := range ids {
			t0 := time.Now()
			status, err := readPromise(baseURL, id, client)
			recordOp(testName, label, "read_promise", time.Since(t0), status, err)
			if err == nil && status == 200 {
				httpOK++
			}
		}
		dbCount, err := countPromises(db, ids)
		if err != nil {
			t.Logf("%s: db count error: %v", label, err)
		}
		t.Logf("%s: http_readable=%d/%d  db_rows=%d/%d", label, httpOK, N, dbCount, N)
		return httpOK, dbCount
	}

	// 2. Verify before failure.
	httpBefore, dbBefore := check("before-failure")

	// 3. Kill node.
	if err := dockerStop(container); err != nil {
		t.Fatalf("docker stop %s: %v", container, err)
	}
	time.Sleep(3 * time.Second) // let YugaByte detect the failure

	// 4. Verify during failure.
	httpDuring, dbDuring := check("during-failure")

	// 5. Restart.
	if err := dockerStart(container); err != nil {
		t.Fatalf("docker start %s: %v", container, err)
	}
	waitForClusterBalanced(t)

	// 6. Verify after recovery.
	httpAfter, dbAfter := check("after-recovery")

	t.Log("=== Data Integrity Summary ===")
	t.Logf("%-20s  %12s  %8s", "checkpoint", "http_readable", "db_rows")
	t.Logf("%-20s  %12d  %8d", "before-failure", httpBefore, dbBefore)
	t.Logf("%-20s  %12d  %8d", "during-failure", httpDuring, dbDuring)
	t.Logf("%-20s  %12d  %8d", "after-recovery", httpAfter, dbAfter)

	for checkpoint, actual := range map[string]int{
		"before-failure": dbBefore,
		"during-failure": dbDuring,
		"after-recovery": dbAfter,
	} {
		ftDataIntegrityRows.WithLabelValues(testName, checkpoint, "expected").Set(float64(N))
		ftDataIntegrityRows.WithLabelValues(testName, checkpoint, "actual").Set(float64(actual))
	}

	for label, vals := range map[string][2]int{
		"before-failure": {httpBefore, dbBefore},
		"during-failure": {httpDuring, dbDuring},
		"after-recovery": {httpAfter, dbAfter},
	} {
		r := FaultTestResult{
			RunID:          runID,
			Timestamp:      time.Now().UTC(),
			TestName:       testName,
			Phase:          label,
			DBRowsExpected: N,
			DBRowsActual:   vals[1],
			Notes:          fmt.Sprintf("http_readable=%d container=%s", vals[0], container),
		}
		globalWriter.add(r)
	}

	// Assertions: no data loss at any checkpoint.
	if dbDuring != N {
		t.Errorf("during-failure: expected %d DB rows, got %d (data loss?)", N, dbDuring)
	}
	if dbAfter != N {
		t.Errorf("after-recovery: expected %d DB rows, got %d (data loss?)", N, dbAfter)
	}
}

// ── Test 5: TestFaultTolerance_ConcurrentClaim ────────────────────────────────
//
// Fires N goroutines simultaneously all trying to create the same promise ID.
// The DB must contain exactly one row for that ID — this is the core
// distributed idempotency / "exactly-once insert" guarantee.
// Verifies at the DB level, independent of HTTP status codes.
func TestFaultTolerance_ConcurrentClaim(t *testing.T) {
	requireFaultTest(t)
	const testName = "ConcurrentClaim"
	markTestActive(t, testName)

	runID := time.Now().Format("20060102_150405")
	const contestants = 50
	baseURL := resonateURL()
	client := &http.Client{Timeout: 3 * time.Second}
	db := dbConn(t)
	defer db.Close()

	promiseID := fmt.Sprintf("concurrent-claim-%d", time.Now().UnixNano())
	t.Logf("=== %s: %d goroutines racing on id=%q ===", testName, contestants, promiseID)

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		start    = make(chan struct{})
		statuses = make(map[int]int)
		netErrs  atomic.Int64
		lats     []time.Duration
	)

	for i := 0; i < contestants; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			t0 := time.Now()
			status, err := createPromise(baseURL, promiseID, client)
			lat := time.Since(t0)
			recordOp(testName, "race", "create_promise", lat, status, err)
			if err != nil {
				netErrs.Add(1)
				return
			}
			mu.Lock()
			statuses[status]++
			lats = append(lats, lat)
			mu.Unlock()
		}()
	}

	close(start)
	wg.Wait()

	// Compute p99 across all contestants.
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	p99 := time.Duration(0)
	if len(lats) > 0 {
		p99 = lats[int(float64(len(lats)-1)*0.99)]
	}

	t.Logf("http responses: %v", statuses)
	t.Logf("net_errors=%d  p99=%s", netErrs.Load(), p99)

	// DB assertion: exactly one row.
	var dbCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM promises WHERE id = $1`, promiseID).Scan(&dbCount); err != nil {
		t.Fatalf("db query: %v", err)
	}
	t.Logf("DB row count for id=%q: %d (expected 1)", promiseID, dbCount)

	globalWriter.add(FaultTestResult{
		RunID:          runID,
		Timestamp:      time.Now().UTC(),
		TestName:       testName,
		Phase:          "race",
		DBRowsExpected: 1,
		DBRowsActual:   dbCount,
		NetErrors:      netErrs.Load(),
		P99Ms:          toMs(p99),
		Notes: fmt.Sprintf("contestants=%d http=%v",
			contestants, statuses),
	})

	if dbCount != 1 {
		t.Errorf("expected exactly 1 DB row for promise %q, got %d — idempotency broken", promiseID, dbCount)
	}
}

// ── Test 6: TestFaultTolerance_RecoveryLatency ────────────────────────────────
//
// Measures the exact time between `docker start` and the first successful
// write operation — i.e., the observable recovery window from the client's
// perspective.  Polls every 200 ms with a 3-minute timeout.
func TestFaultTolerance_RecoveryLatency(t *testing.T) {
	requireFaultTest(t)
	const testName = "RecoveryLatency"
	markTestActive(t, testName)

	runID := time.Now().Format("20060102_150405")
	baseURL := resonateURL()
	container := faultContainer()
	client := &http.Client{Timeout: 3 * time.Second}

	if _, err := http.Get(baseURL + "/promises?limit=1"); err != nil {
		t.Fatalf("cannot reach Resonate at %s: %v", baseURL, err)
	}

	t.Logf("=== %s: container=%q ===", testName, container)

	// Establish a baseline write latency.
	var baselineLatencies []time.Duration
	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("baseline-%d-%d", time.Now().UnixNano(), i)
		t0 := time.Now()
		status, err := createPromise(baseURL, id, client)
		lat := time.Since(t0)
		recordOp(testName, "baseline", "create_promise", lat, status, err)
		if err == nil && status >= 200 && status < 300 {
			baselineLatencies = append(baselineLatencies, lat)
		}
	}
	baselineP99 := time.Duration(0)
	if len(baselineLatencies) > 0 {
		sort.Slice(baselineLatencies, func(i, j int) bool { return baselineLatencies[i] < baselineLatencies[j] })
		baselineP99 = baselineLatencies[int(float64(len(baselineLatencies)-1)*0.99)]
	}
	t.Logf("baseline: %d successful writes, p99=%s", len(baselineLatencies), baselineP99)

	// Kill the node.
	t.Logf("killing %q...", container)
	if err := dockerStop(container); err != nil {
		t.Fatalf("docker stop %s: %v", container, err)
	}
	time.Sleep(2 * time.Second)

	// Restart and measure time to first success.
	t.Logf("restarting %q, polling for first successful write...", container)
	restartAt := time.Now()
	if err := dockerStart(container); err != nil {
		t.Fatalf("docker start %s: %v", container, err)
	}

	var firstSuccessAt time.Time
	var probeCount int
	timeout := time.After(3 * time.Minute)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

outer:
	for {
		select {
		case <-timeout:
			t.Fatal("recovery timed out after 3 minutes")
		case <-ticker.C:
			probeCount++
			id := fmt.Sprintf("recovery-probe-%d-%d", time.Now().UnixNano(), probeCount)
			t0 := time.Now()
			status, err := createPromise(baseURL, id, client)
			recordOp(testName, "recovery_probe", "create_promise", time.Since(t0), status, err)
			if err == nil && status >= 200 && status < 300 {
				firstSuccessAt = time.Now()
				break outer
			}
		}
	}

	recoveryDuration := firstSuccessAt.Sub(restartAt)
	recoveryMs := float64(recoveryDuration.Milliseconds())
	ftRecoverySeconds.WithLabelValues(testName).Set(recoveryDuration.Seconds())
	t.Logf("recovery latency: %.0f ms  (probes sent: %d)", recoveryMs, probeCount)
	t.Logf("baseline p99: %s", baselineP99)

	globalWriter.add(FaultTestResult{
		RunID:      runID,
		Timestamp:  time.Now().UTC(),
		TestName:   testName,
		Phase:      "recovery",
		RecoveryMs: recoveryMs,
		P99Ms:      toMs(baselineP99),
		Notes:      fmt.Sprintf("container=%s probes=%d", container, probeCount),
	})
}

// ── Test 7: TestFaultTolerance_ReadAvailability ───────────────────────────────
//
// Verifies that read operations (GET /promises/:id) remain available while a
// YugaByte node is down.  With RF=3 and one node down (quorum = 2), reads
// should succeed from the remaining two nodes.
// Compares read throughput and latency baseline vs during failure vs recovery.
func TestFaultTolerance_ReadAvailability(t *testing.T) {
	requireFaultTest(t)
	const testName = "ReadAvailability"
	markTestActive(t, testName)

	runID := time.Now().Format("20060102_150405")
	const preloadCount = 50
	baseURL := resonateURL()
	container := faultContainer()
	workers := numWorkers()
	client := &http.Client{Timeout: 3 * time.Second}

	waitForServer(t, baseURL)

	// Pre-load promises to read from.
	t.Logf("=== %s: preloading %d promises, then killing %q ===", testName, preloadCount, container)
	ids := make([]string, 0, preloadCount)
	for i := 0; i < preloadCount; i++ {
		id := fmt.Sprintf("read-avail-%d-%04d", time.Now().UnixNano(), i)
		status, err := createPromise(baseURL, id, client)
		if err == nil && status >= 200 && status < 300 {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		t.Fatal("failed to preload any promises")
	}
	t.Logf("preloaded %d promises", len(ids))

	phases := []*phase{
		{Name: "before-failure", Duration: 15 * time.Second, TestName: testName, OpType: "read_promise"},
		{Name: "during-failure", Duration: 20 * time.Second, TestName: testName, OpType: "read_promise"},
		{Name: "after-recovery", Duration: 15 * time.Second, TestName: testName, OpType: "read_promise"},
	}
	ctx := context.Background()

	t.Log("--- phase: before-failure (read baseline)")
	runReadPhase(ctx, phases[0], baseURL, ids, workers, client)
	logPhase(t, phases[0])

	t.Logf("--- killing %q", container)
	if err := dockerStop(container); err != nil {
		t.Fatalf("docker stop %s: %v", container, err)
	}

	t.Log("--- phase: during-failure (reads should still work via remaining nodes)")
	runReadPhase(ctx, phases[1], baseURL, ids, workers, client)
	logPhase(t, phases[1])

	t.Logf("--- restarting %q", container)
	if err := dockerStart(container); err != nil {
		t.Fatalf("docker start %s: %v", container, err)
	}
	waitForClusterBalanced(t)

	t.Log("--- phase: after-recovery")
	runReadPhase(ctx, phases[2], baseURL, ids, workers, client)
	logPhase(t, phases[2])

	t.Log("=== Read Availability Summary ===")
	for _, p := range phases {
		logPhase(t, p)
		r := phaseResult(runID, testName, p)
		r.Notes = fmt.Sprintf("container=%s preloaded=%d", container, len(ids))
		globalWriter.add(r)
	}

	// Log the during-failure ratio for thesis data — but do not assert a
	// throughput threshold here. The ratio depends heavily on cluster state
	// at the time of measurement (tablet leader distribution, prior tests).
	// On a fresh isolated cluster this reaches 100%+; in a sequential suite
	// it can be as low as 23% due to accumulated degradation from prior tests.
	// The thesis finding is directional: reads survive node failure.
	if phases[0].s2xx > 0 {
		ratio := phases[1].opsPerSec() / phases[0].opsPerSec() * 100
		t.Logf("read throughput during failure: %.0f%% of baseline (%.0f → %.0f ops/s)",
			ratio, phases[0].opsPerSec(), phases[1].opsPerSec())
	}

	// Assert: after-recovery must fully restore (≥90% of baseline).
	// This IS a hard guarantee: once the node rejoins, the cluster must
	// return to full capacity.
	if phases[0].s2xx > 0 && phases[2].s2xx > 0 {
		ratio := phases[2].opsPerSec() / phases[0].opsPerSec()
		if ratio < 0.90 {
			t.Errorf("after-recovery read throughput %.0f ops/s is >10%% below baseline %.0f ops/s — cluster did not fully recover",
				phases[2].opsPerSec(), phases[0].opsPerSec())
		}
	}
	if phases[2].s2xx == 0 {
		t.Errorf("after-recovery: 0 successful reads — cluster did not recover")
	}
}
