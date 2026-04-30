// Package yugabyte — direct-store fault-tolerance integration tests.
//
// Unlike the HTTP-level tests in benchmarks/fault_tolerance_test.go, these
// tests bypass the Resonate server entirely and call YugabyteStoreWorker.Execute
// directly.  This removes HTTP-layer noise and lets you measure raw store-layer
// resilience: transaction retries, latency spikes, and data-integrity guarantees.
//
// # What is tested
//
//   - TestStoreFT_FollowerNodeKill  — kill yb-node2 (follower) during concurrent writes
//   - TestStoreFT_PrimaryNodeKill  — kill yb-node1 (primary contact node) during writes
//   - TestStoreFT_NetworkPartition — SIGSTOP yb-node2 (hung node, no clean shutdown)
//   - TestStoreFT_ReadWriteMix     — interleaved reads + writes while a follower is killed
//
// # Structure of each test
//
//	Phase 1 "before-failure"  — establish a clean baseline (15 s)
//	[inject fault: docker stop / docker pause]
//	Phase 2 "during-failure"  — measure availability during the fault (30–40 s)
//	[resolve fault: docker start / docker unpause + wait for rebalance]
//	Phase 3 "after-recovery"  — measure recovery quality (30 s)
//	[assertions + data-integrity check]
//
// # Requirements
//
//  1. A 3-node YugabyteDB cluster:
//     docker compose -f docker/docker-compose.fault-tolerance.yml up -d
//  2. STORE_FAULT_TEST_ENABLED=true environment variable
//
// No Resonate server is required.
//
// # Environment variables
//
//	STORE_FAULT_TEST_ENABLED  — must be "true" to run (default: skip)
//	YB_FT_HOSTS               — comma-separated host:port list for the pgx pool
//	                            default: "localhost:5433,localhost:5434,localhost:5435"
//	                            (yb-node2 maps to 5434, yb-node3 maps to 5435 in
//	                             docker-compose.fault-tolerance.yml)
//	YB_FT_FAULT_CONTAINER     — follower container to kill (default: yb-node2)
//	YB_FT_PRIMARY_CONTAINER   — primary container to kill (default: yb-node1)
//	YB_FT_WORKERS             — concurrent goroutines per phase (default: 8)
//	YUGABYTE_USER             — DB user     (default: yugabyte)
//	YUGABYTE_PASSWORD         — DB password (default: yugabyte)
//	YUGABYTE_DB               — DB name     (default: resonate)
package yugabyte

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"errors"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/yugabyte/pgx/v5/pgconn"
	"github.com/yugabyte/pgx/v5/pgxpool"
)

// ── guard ─────────────────────────────────────────────────────────────────────

func requireStoreFaultTest(t *testing.T) {
	t.Helper()
	if os.Getenv("STORE_FAULT_TEST_ENABLED") != "true" {
		t.Skip("set STORE_FAULT_TEST_ENABLED=true to run direct-store fault-tolerance tests")
	}
}

// ── env helpers ───────────────────────────────────────────────────────────────

func envFT(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func ftHosts() string            { return envFT("YB_FT_HOSTS", "localhost:5433,localhost:5434,localhost:5435") }
func ftFaultContainer() string   { return envFT("YB_FT_FAULT_CONTAINER", "yb-node2") }
func ftPrimaryContainer() string { return envFT("YB_FT_PRIMARY_CONTAINER", "yb-node1") }
func ftWorkers() int {
	if s := os.Getenv("YB_FT_WORKERS"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			return n
		}
	}
	return 8
}

// ── store setup ───────────────────────────────────────────────────────────────

// newFaultTestWorker creates a YugabyteStoreWorker that connects to all nodes
// listed in YB_FT_HOSTS.  Each test gets a fresh metrics registry so counters
// start from zero and do not bleed across tests.
func newFaultTestWorker(t *testing.T) (*pgxpool.Pool, *YugabyteStoreWorker) {
	t.Helper()

	hosts := ftHosts()
	user := url.QueryEscape(envFT("YUGABYTE_USER", "yugabyte"))
	pass := url.QueryEscape(envFT("YUGABYTE_PASSWORD", "yugabyte"))
	db := envFT("YUGABYTE_DB", "resonate")

	// pgx supports multi-host DSNs:  host1:port1,host2:port2,...
	dsn := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable&connect_timeout=2", user, pass, hosts, db)

	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("pgxpool.ParseConfig: %v", err)
	}
	poolCfg.MaxConns = int32(ftWorkers() * 2)
	poolCfg.HealthCheckPeriod = 2 * time.Second
	// TCP keepalives so the kernel detects dead nodes within ~5 s.
	poolCfg.ConnConfig.Config.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		return (&net.Dialer{KeepAlive: 3 * time.Second, Timeout: 2 * time.Second}).DialContext(ctx, network, addr)
	}

	ctx := context.Background()
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		t.Fatalf("pgxpool.NewWithConfig: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("pool ping: %v (is the cluster running? YB_FT_HOSTS=%s)", err, hosts)
	}

	m := metrics.New(prometheus.NewRegistry())
	worker := &YugabyteStoreWorker{
		config: &Config{
			TxTimeout:  5 * time.Second,
			MaxRetries: 3,
			BatchSize:  1,
		},
		i:       0,
		db:      pool,
		sq:      make(chan *bus.SQE[t_aio.Submission, t_aio.Completion], 1), // unused — we call Execute directly
		flush:   make(chan int64, 1),
		aio:     nil,
		metrics: m,
	}

	return pool, worker
}

// resetFaultTestDB truncates the promises table so tests start from a clean state.
func resetFaultTestDB(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := pool.Exec(ctx, "TRUNCATE TABLE promises CASCADE"); err != nil {
		t.Logf("resetFaultTestDB: %v (ignored)", err)
	}
}

// ── store operations ──────────────────────────────────────────────────────────

// execCreatePromise calls Execute with a single CreatePromiseCommand and returns
// the latency and any error.  The promise ID is the caller's responsibility.
func execCreatePromise(worker *YugabyteStoreWorker, id string) (time.Duration, error) {
	txn := []*t_aio.Transaction{
		{
			Commands: []t_aio.Command{
				&t_aio.CreatePromiseCommand{
					Id:        id,
					State:     promise.Pending,
					Timeout:   time.Now().Add(time.Hour).UnixMilli(),
					Param:     promise.Value{Headers: map[string]string{}, Data: []byte("storeft")},
					Tags:      map[string]string{},
					CreatedOn: time.Now().UnixMilli(),
				},
			},
		},
	}
	t0 := time.Now()
	_, err := worker.Execute(txn)
	return time.Since(t0), err
}

// execReadPromise calls Execute with a single ReadPromiseCommand.
func execReadPromise(worker *YugabyteStoreWorker, id string) (time.Duration, error) {
	txn := []*t_aio.Transaction{
		{
			Commands: []t_aio.Command{
				&t_aio.ReadPromiseCommand{Id: id},
			},
		},
	}
	t0 := time.Now()
	_, err := worker.Execute(txn)
	return time.Since(t0), err
}

// ── storeFTPhase — per-phase metrics ─────────────────────────────────────────

// storeFTPhase tracks operation outcomes for a single test phase.
// Errors are split into "store errors" (transaction failures after all retries)
// and "network errors" (connection refused / EOF / timeout — node is unreachable).
// TestName and OpType are used for Prometheus labels; empty means skip recording.
type storeFTPhase struct {
	Name      string
	Duration  time.Duration
	TestName  string // Prometheus label — set at construction
	OpType    string // Prometheus label — e.g. "create_promise", "read_write_mix"
	mu        sync.Mutex
	lats      []time.Duration // latencies of successful ops only
	sOK       int64           // successful executions
	sStoreErr int64           // failed executions — non-network error (unexpected)
	sNetErr   int64           // failed executions — node unreachable (expected during fault)
}

func (p *storeFTPhase) record(lat time.Duration, err error) {
	// Record to Prometheus outside the lock — metric writes must not contend
	// with percentile computation.
	recordStoreFTOp(p.TestName, p.Name, p.OpType, lat, err)

	p.mu.Lock()
	defer p.mu.Unlock()
	if err == nil {
		p.sOK++
		p.lats = append(p.lats, lat)
		return
	}
	if isNetworkOrConnectionError(err) {
		p.sNetErr++
	} else {
		p.sStoreErr++
	}
}

func (p *storeFTPhase) totalOps() int64    { return p.sOK + p.sStoreErr + p.sNetErr }
func (p *storeFTPhase) totalErrors() int64 { return p.sStoreErr + p.sNetErr }

func (p *storeFTPhase) opsPerSec() float64 {
	if p.Duration.Seconds() == 0 {
		return 0
	}
	return float64(p.sOK) / p.Duration.Seconds()
}

func (p *storeFTPhase) pctMs(pct float64) float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.lats) == 0 {
		return 0
	}
	s := make([]time.Duration, len(p.lats))
	copy(s, p.lats)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return toMsFT(s[int(float64(len(s)-1)*pct)])
}

func (p *storeFTPhase) minMs() float64 {
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
	return toMsFT(m)
}

func (p *storeFTPhase) maxMs() float64 {
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
	return toMsFT(m)
}

func toMsFT(d time.Duration) float64 { return float64(d.Microseconds()) / 1000.0 }

// ── error classification ──────────────────────────────────────────────────────

// isNetworkOrConnectionError returns true for errors caused by a node being
// unreachable (connection refused, EOF, timeout, network op error).
// These are expected during fault injection and are not bugs.
func isNetworkOrConnectionError(err error) bool {
	if err == nil {
		return false
	}
	// io.EOF — server closed connection abruptly
	if errors.Is(err, io.EOF) {
		return true
	}
	// pgconn.ConnectError — initial connection failed
	var connErr *pgconn.ConnectError
	if errors.As(err, &connErr) {
		return true
	}
	// net.OpError — TCP-level failure (connection refused, reset, timeout)
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}
	// syscall.ECONNREFUSED / ECONNRESET / ETIMEDOUT
	var errno syscall.Errno
	if errors.As(err, &errno) {
		switch errno {
		case syscall.ECONNREFUSED, syscall.ECONNRESET, syscall.ETIMEDOUT:
			return true
		}
	}
	s := strings.ToLower(err.Error())
	for _, kw := range []string{"eof", "connection refused", "connection reset", "i/o timeout",
		"broken pipe", "no route to host", "connection timed out", "dial tcp"} {
		if strings.Contains(s, kw) {
			return true
		}
	}
	return false
}

// ── result output ─────────────────────────────────────────────────────────────

// StoreFTResult is one row in the output JSON/CSV.
type StoreFTResult struct {
	RunID          string    `json:"run_id"`
	Timestamp      time.Time `json:"timestamp"`
	TestName       string    `json:"test_name"`
	Phase          string    `json:"phase"`
	DurationSec    float64   `json:"duration_sec"`
	TotalOps       int64     `json:"total_ops"`
	OpsPerSec      float64   `json:"ops_per_sec"`
	StoreOK        int64     `json:"store_ok"`
	StoreErr       int64     `json:"store_err"`
	NetErr         int64     `json:"net_err"`
	MinMs          float64   `json:"min_ms"`
	P50Ms          float64   `json:"p50_ms"`
	P95Ms          float64   `json:"p95_ms"`
	P99Ms          float64   `json:"p99_ms"`
	MaxMs          float64   `json:"max_ms"`
	DBRowsExpected int       `json:"db_rows_expected,omitempty"`
	DBRowsActual   int       `json:"db_rows_actual,omitempty"`
	Notes          string    `json:"notes,omitempty"`
}

func makeStoreFTResult(runID, testName string, p *storeFTPhase) StoreFTResult {
	return StoreFTResult{
		RunID:       runID,
		Timestamp:   time.Now().UTC(),
		TestName:    testName,
		Phase:       p.Name,
		DurationSec: p.Duration.Seconds(),
		TotalOps:    p.totalOps(),
		OpsPerSec:   p.opsPerSec(),
		StoreOK:     p.sOK,
		StoreErr:    p.sStoreErr,
		NetErr:      p.sNetErr,
		MinMs:       p.minMs(),
		P50Ms:       p.pctMs(0.50),
		P95Ms:       p.pctMs(0.95),
		P99Ms:       p.pctMs(0.99),
		MaxMs:       p.maxMs(),
	}
}

// storeFTWriter accumulates results and flushes them to JSON + CSV on Close.
type storeFTWriter struct {
	mu      sync.Mutex
	records []StoreFTResult
}

var globalStoreFTWriter = &storeFTWriter{}

func (w *storeFTWriter) add(r StoreFTResult) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.records = append(w.records, r)
}

func (w *storeFTWriter) flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.records) == 0 {
		return
	}
	if err := os.MkdirAll("results", 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "storeFTWriter mkdir: %v\n", err)
		return
	}
	ts := time.Now().Format("20060102_150405")
	w.writeJSON("results/store_fault_" + ts + ".json")
	w.writeCSV("results/store_fault_" + ts + ".csv")
}

func (w *storeFTWriter) writeJSON(path string) {
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "storeFTWriter json create: %v\n", err)
		return
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(w.records); err != nil {
		fmt.Fprintf(os.Stderr, "storeFTWriter json encode: %v\n", err)
		return
	}
	fmt.Printf("store fault-tolerance results → %s\n", path)
}

func (w *storeFTWriter) writeCSV(path string) {
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "storeFTWriter csv create: %v\n", err)
		return
	}
	defer f.Close()
	cw := csv.NewWriter(f)
	_ = cw.Write([]string{
		"run_id", "timestamp", "test_name", "phase",
		"duration_sec", "total_ops", "ops_per_sec",
		"store_ok", "store_err", "net_err",
		"min_ms", "p50_ms", "p95_ms", "p99_ms", "max_ms",
		"db_rows_expected", "db_rows_actual", "notes",
	})
	ff := func(v float64) string { return strconv.FormatFloat(v, 'f', 3, 64) }
	for _, r := range w.records {
		_ = cw.Write([]string{
			r.RunID, r.Timestamp.Format(time.RFC3339),
			r.TestName, r.Phase,
			ff(r.DurationSec),
			strconv.FormatInt(r.TotalOps, 10),
			ff(r.OpsPerSec),
			strconv.FormatInt(r.StoreOK, 10),
			strconv.FormatInt(r.StoreErr, 10),
			strconv.FormatInt(r.NetErr, 10),
			ff(r.MinMs), ff(r.P50Ms), ff(r.P95Ms), ff(r.P99Ms), ff(r.MaxMs),
			strconv.Itoa(r.DBRowsExpected),
			strconv.Itoa(r.DBRowsActual),
			r.Notes,
		})
	}
	cw.Flush()
	fmt.Printf("store fault-tolerance results → %s\n", path)
}

// ── phase runners ─────────────────────────────────────────────────────────────

// runStoreFTWritePhase spawns workers goroutines that each call execCreatePromise
// in a tight loop for the duration of p.Duration.  Each goroutine generates
// unique IDs using an atomic counter.
func runStoreFTWritePhase(ctx context.Context, p *storeFTPhase, worker *YugabyteStoreWorker, prefix string, workerCount int) {
	var wg sync.WaitGroup
	var counter atomic.Int64
	deadline := time.Now().Add(p.Duration)
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) && ctx.Err() == nil {
				id := fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), counter.Add(1))
				lat, err := execCreatePromise(worker, id)
				p.record(lat, err)
			}
		}()
	}
	wg.Wait()
}

// runStoreFTMixPhase runs half the workers writing new promises and the other
// half reading seeded IDs.  Results are merged into a single phase so the
// combined availability is visible in one row.
func runStoreFTMixPhase(ctx context.Context, p *storeFTPhase, worker *YugabyteStoreWorker, seededIDs []string, prefix string, workerCount int) {
	readers := workerCount / 2
	if readers == 0 {
		readers = 1
	}
	writers := workerCount - readers

	var wg sync.WaitGroup
	var counter atomic.Int64
	var idx atomic.Int64
	n := int64(len(seededIDs))
	deadline := time.Now().Add(p.Duration)

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) && ctx.Err() == nil {
				id := fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), counter.Add(1))
				lat, err := execCreatePromise(worker, id)
				p.record(lat, err)
			}
		}()
	}

	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) && ctx.Err() == nil {
				id := seededIDs[(idx.Add(1)-1)%n]
				lat, err := execReadPromise(worker, id)
				p.record(lat, err)
			}
		}()
	}

	wg.Wait()
}

// ── phase logging ─────────────────────────────────────────────────────────────

func logStoreFTPhase(t *testing.T, p *storeFTPhase) {
	t.Helper()
	recordStoreFTPhase(p.TestName, p)
	t.Logf("  %-22s  ops/s=%6.0f  ok=%6d  storeErr=%5d  netErr=%5d  min=%6.1fms  p50=%6.1fms  p95=%7.1fms  p99=%7.1fms  max=%7.1fms",
		p.Name,
		p.opsPerSec(),
		p.sOK, p.sStoreErr, p.sNetErr,
		p.minMs(), p.pctMs(0.50), p.pctMs(0.95), p.pctMs(0.99), p.maxMs(),
	)
}

// ── Docker helpers ────────────────────────────────────────────────────────────

func ftDockerStop(container string) error {
	return exec.Command("docker", "stop", container).Run()
}

func ftDockerStart(container string) error {
	return exec.Command("docker", "start", container).Run()
}

func ftDockerPause(container string) error {
	return exec.Command("docker", "pause", container).Run()
}

func ftDockerUnpause(container string) error {
	return exec.Command("docker", "unpause", container).Run()
}

// waitForStoreRecovery polls the worker with a simple ReadPromise until it
// succeeds, confirming the DB write path is live again.
func waitForStoreRecovery(t *testing.T, worker *YugabyteStoreWorker, container string) {
	t.Helper()
	t.Logf("waiting for store to recover after %s restarts...", container)
	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		// ReadPromise on a non-existent ID is still a successful DB round-trip
		// (returns a zero-row result, not an error).
		if _, err := execReadPromise(worker, "recovery-probe"); err == nil {
			t.Log("store layer healthy again")
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Log("warning: store not healthy after 3 minutes — proceeding anyway")
}

// waitForClusterBalancedFT polls yb-admin until the load balancer reports idle.
// Identical logic to waitForClusterBalanced in fault_tolerance_test.go but
// self-contained in this package.
func waitForClusterBalancedFT(t *testing.T, worker *YugabyteStoreWorker, container string) {
	t.Helper()

	// Allow the rejoined node time to re-register with the master before polling.
	t.Log("waiting 15s for rejoined node to register with the cluster...")
	time.Sleep(15 * time.Second)

	masters := "yb-node1:7100,yb-node2:7100,yb-node3:7100"
	t.Log("polling YugaByte load balancer until idle...")
	start := time.Now()
	deadline := start.Add(5 * time.Minute)

	for time.Now().Before(deadline) {
		out, err := exec.Command("docker", "exec", "yb-node1",
			"bin/yb-admin", "-master_addresses", masters, "get_is_load_balancer_idle",
		).Output()
		if err == nil && strings.Contains(string(out), "Idle") {
			time.Sleep(5 * time.Second) // confirm — first "Idle" can be a false positive
			out2, err2 := exec.Command("docker", "exec", "yb-node1",
				"bin/yb-admin", "-master_addresses", masters, "get_is_load_balancer_idle",
			).Output()
			if err2 == nil && strings.Contains(string(out2), "Idle") {
				t.Logf("load balancer idle (elapsed %s)", time.Since(start).Round(time.Second))
				break
			}
		}
		time.Sleep(5 * time.Second)
	}

	waitForStoreRecovery(t, worker, container)
}

// countDBRows returns how many promises in the DB have IDs starting with prefix.
func countDBRows(t *testing.T, pool *pgxpool.Pool, prefix string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var n int
	if err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM promises WHERE id LIKE $1", prefix+"%").Scan(&n); err != nil {
		t.Logf("countDBRows(%q): %v", prefix, err)
		return -1
	}
	return n
}

// ── Test 1: TestStoreFT_FollowerNodeKill ──────────────────────────────────────
//
// Kills a follower YugaByte node (YB_FT_FAULT_CONTAINER, default yb-node2) while
// running a continuous write load directly on the store layer.
//
// Expected behaviour:
//   - During failure: <1% errors (RF=3, quorum = 2; one follower down is tolerated)
//   - After recovery: ≥1 successful op (cluster fully recovered)
//   - Data integrity: all successfully written promises are present in the DB
func TestStoreFT_FollowerNodeKill(t *testing.T) {
	requireStoreFaultTest(t)
	const testName = "StoreFT_FollowerNodeKill"
	markStoreFTTestActive(t, testName)

	runID := time.Now().Format("20060102_150405")
	container := ftFaultContainer()
	workers := ftWorkers()
	prefix := "storeft-fk-" + runID

	pool, worker := newFaultTestWorker(t)
	defer pool.Close()
	resetFaultTestDB(t, pool)

	phases := []*storeFTPhase{
		{Name: "before-failure", Duration: 15 * time.Second, TestName: testName, OpType: "create_promise"},
		{Name: "during-failure", Duration: 30 * time.Second, TestName: testName, OpType: "create_promise"},
		{Name: "after-recovery", Duration: 30 * time.Second, TestName: testName, OpType: "create_promise"},
	}
	ctx := context.Background()

	t.Logf("=== %s: killing follower %q, workers=%d, hosts=%s ===", testName, container, workers, ftHosts())

	// ── Phase 1: baseline ────────────────────────────────────────────────────
	t.Log("--- phase: before-failure")
	runStoreFTWritePhase(ctx, phases[0], worker, prefix+"-bf", workers)
	logStoreFTPhase(t, phases[0])

	// ── Inject fault ─────────────────────────────────────────────────────────
	t.Logf("--- killing follower %q", container)
	if err := ftDockerStop(container); err != nil {
		t.Fatalf("docker stop %s: %v", container, err)
	}

	// ── Phase 2: during failure ───────────────────────────────────────────────
	t.Log("--- phase: during-failure")
	runStoreFTWritePhase(ctx, phases[1], worker, prefix+"-df", workers)
	logStoreFTPhase(t, phases[1])

	// ── Resolve fault ─────────────────────────────────────────────────────────
	t.Logf("--- restarting follower %q", container)
	if err := ftDockerStart(container); err != nil {
		t.Fatalf("docker start %s: %v", container, err)
	}
	waitForClusterBalancedFT(t, worker, container)

	// ── Phase 3: after recovery ───────────────────────────────────────────────
	t.Log("--- phase: after-recovery")
	runStoreFTWritePhase(ctx, phases[2], worker, prefix+"-ar", workers)
	logStoreFTPhase(t, phases[2])

	// ── Summary ───────────────────────────────────────────────────────────────
	t.Log("=== Summary ===")
	t.Logf("%-22s  %8s  %8s  %8s  %8s", "phase", "ops/s", "ok", "errors", "p99_ms")
	for _, p := range phases {
		t.Logf("%-22s  %8.0f  %8d  %8d  %8.1f",
			p.Name, p.opsPerSec(), p.sOK, p.totalErrors(), p.pctMs(0.99))
		r := makeStoreFTResult(runID, testName, p)
		r.Notes = fmt.Sprintf("container=%s workers=%d hosts=%s", container, workers, ftHosts())
		globalStoreFTWriter.add(r)
	}

	// ── Data integrity: count persisted rows ──────────────────────────────────
	total := countDBRows(t, pool, prefix)
	expected := int(phases[0].sOK + phases[1].sOK + phases[2].sOK)
	recordStoreFTDataIntegrity(testName, expected, total)
	t.Logf("data integrity: expected=%d  actual=%d  (writes attempted=%d)",
		expected, total, phases[0].totalOps()+phases[1].totalOps()+phases[2].totalOps())
	if total != expected {
		t.Errorf("data integrity: expected %d rows, got %d — some successful writes were not persisted", expected, total)
	}

	// ── Availability assertions ───────────────────────────────────────────────
	totalDuring := phases[1].totalOps()
	errorsDuring := phases[1].totalErrors()
	budget := int64(50)
	if pct := totalDuring / 100; pct > budget {
		budget = pct
	}
	if errorsDuring > budget {
		t.Errorf("during-failure: %d errors out of %d ops — exceeds 1%% budget (%d); RF=3 cluster should remain writable with 1 follower down",
			errorsDuring, totalDuring, budget)
	}
	if phases[2].sOK == 0 {
		t.Errorf("after-recovery: 0 successful ops — store did not recover")
	}
}

// ── Test 2: TestStoreFT_PrimaryNodeKill ───────────────────────────────────────
//
// Kills the primary YugaByte node (the first host in YB_FT_HOSTS, default yb-node1).
// This is the node the pgx pool first dials; with multi-host DSN the pool must
// route connections to the remaining two nodes after the kill.
//
// Expected behaviour:
//   - During failure: elevated errors while the pool detects the dead node and
//     reconnects (usually < 10 s).  After that, near-zero errors.
//   - After recovery: ≥65% of baseline throughput once tablet leaders rebalance.
//
// NOTE: This test requires YB_FT_HOSTS to list all three nodes (default value
// "localhost:5433,localhost:5434,localhost:5435") so the pgx pool can fail over.
// Run docker-compose.fault-tolerance.yml which exposes yb-node2 on 5434 and
// yb-node3 on 5435.
func TestStoreFT_PrimaryNodeKill(t *testing.T) {
	requireStoreFaultTest(t)
	const testName = "StoreFT_PrimaryNodeKill"
	markStoreFTTestActive(t, testName)

	runID := time.Now().Format("20060102_150405")
	container := ftPrimaryContainer()
	workers := ftWorkers()
	prefix := "storeft-pk-" + runID

	pool, worker := newFaultTestWorker(t)
	defer pool.Close()
	resetFaultTestDB(t, pool)

	phases := []*storeFTPhase{
		{Name: "before-failure", Duration: 15 * time.Second, TestName: testName, OpType: "create_promise"},
		// Longer window: leader election + pgx pool reconnect takes 5–15 s.
		{Name: "during-failure", Duration: 40 * time.Second, TestName: testName, OpType: "create_promise"},
		{Name: "after-recovery", Duration: 30 * time.Second, TestName: testName, OpType: "create_promise"},
	}
	ctx := context.Background()

	t.Logf("=== %s: killing primary %q, workers=%d, hosts=%s ===", testName, container, workers, ftHosts())

	// ── Phase 1: baseline ────────────────────────────────────────────────────
	t.Log("--- phase: before-failure")
	runStoreFTWritePhase(ctx, phases[0], worker, prefix+"-bf", workers)
	logStoreFTPhase(t, phases[0])

	// ── Inject fault ─────────────────────────────────────────────────────────
	t.Logf("--- killing primary %q", container)
	if err := ftDockerStop(container); err != nil {
		t.Fatalf("docker stop %s: %v", container, err)
	}

	// ── Phase 2: during failure ───────────────────────────────────────────────
	t.Log("--- phase: during-failure (initial errors during leader election are expected)")
	runStoreFTWritePhase(ctx, phases[1], worker, prefix+"-df", workers)
	logStoreFTPhase(t, phases[1])

	// ── Resolve fault ─────────────────────────────────────────────────────────
	t.Logf("--- restarting primary %q", container)
	if err := ftDockerStart(container); err != nil {
		t.Fatalf("docker start %s: %v", container, err)
	}
	waitForClusterBalancedFT(t, worker, container)

	// ── Phase 3: after recovery ───────────────────────────────────────────────
	t.Log("--- phase: after-recovery")
	runStoreFTWritePhase(ctx, phases[2], worker, prefix+"-ar", workers)
	logStoreFTPhase(t, phases[2])

	// ── Summary ───────────────────────────────────────────────────────────────
	t.Log("=== Summary ===")
	t.Logf("%-22s  %8s  %8s  %8s  %8s", "phase", "ops/s", "ok", "errors", "p99_ms")
	for _, p := range phases {
		t.Logf("%-22s  %8.0f  %8d  %8d  %8.1f",
			p.Name, p.opsPerSec(), p.sOK, p.totalErrors(), p.pctMs(0.99))
		r := makeStoreFTResult(runID, testName, p)
		r.Notes = fmt.Sprintf("container=%s workers=%d hosts=%s", container, workers, ftHosts())
		globalStoreFTWriter.add(r)
	}

	// ── Data integrity ────────────────────────────────────────────────────────
	total := countDBRows(t, pool, prefix)
	expected := int(phases[0].sOK + phases[1].sOK + phases[2].sOK)
	recordStoreFTDataIntegrity(testName, expected, total)
	t.Logf("data integrity: expected=%d  actual=%d", expected, total)
	if total != expected {
		t.Errorf("data integrity: expected %d rows, got %d", expected, total)
	}

	// ── Assertions ────────────────────────────────────────────────────────────
	if phases[2].sOK == 0 {
		t.Errorf("after-recovery: 0 successful ops — store did not recover after primary kill")
	}
	if phases[0].sOK > 0 && phases[2].sOK > 0 {
		ratio := phases[2].opsPerSec() / phases[0].opsPerSec()
		t.Logf("throughput recovery ratio: %.0f%% (%.0f → %.0f ops/s); tablet rebalancing may still be in progress",
			ratio*100, phases[0].opsPerSec(), phases[2].opsPerSec())
		if ratio < 0.50 {
			t.Errorf("after-recovery throughput %.0f ops/s is >50%% below baseline %.0f ops/s",
				phases[2].opsPerSec(), phases[0].opsPerSec())
		}
	}
}

// ── Test 3: TestStoreFT_NetworkPartition ──────────────────────────────────────
//
// Pauses a YugaByte node with `docker pause` (sends SIGSTOP).  Unlike a clean
// shutdown, SIGSTOP does not send RST/FIN — the TCP connection hangs silently
// until the application-level timeout fires.  This simulates a network
// partition or a hung host.
//
// Expected behaviour:
//   - During pause: most ops succeed (only connections landing on the paused node
//     will time out; the pool retries on healthy nodes after the tx timeout).
//   - After unpause: ≥80% of baseline throughput restored within 30 s.
func TestStoreFT_NetworkPartition(t *testing.T) {
	requireStoreFaultTest(t)
	const testName = "StoreFT_NetworkPartition"
	markStoreFTTestActive(t, testName)

	runID := time.Now().Format("20060102_150405")
	container := ftFaultContainer()
	workers := ftWorkers()
	prefix := "storeft-np-" + runID

	pool, worker := newFaultTestWorker(t)
	defer pool.Close()
	resetFaultTestDB(t, pool)

	phases := []*storeFTPhase{
		{Name: "before-pause", Duration: 15 * time.Second, TestName: testName, OpType: "create_promise"},
		{Name: "during-pause", Duration: 30 * time.Second, TestName: testName, OpType: "create_promise"},
		{Name: "after-unpause", Duration: 15 * time.Second, TestName: testName, OpType: "create_promise"},
	}
	ctx := context.Background()

	t.Logf("=== %s: pausing container %q (SIGSTOP), workers=%d ===", testName, container, workers)

	// ── Phase 1: baseline ────────────────────────────────────────────────────
	t.Log("--- phase: before-pause")
	runStoreFTWritePhase(ctx, phases[0], worker, prefix+"-bp", workers)
	logStoreFTPhase(t, phases[0])

	// ── Inject fault: SIGSTOP ─────────────────────────────────────────────────
	t.Logf("--- pausing %q with SIGSTOP (silent hang, no clean shutdown)", container)
	if err := ftDockerPause(container); err != nil {
		t.Fatalf("docker pause %s: %v", container, err)
	}

	// ── Phase 2: during pause ─────────────────────────────────────────────────
	t.Log("--- phase: during-pause")
	runStoreFTWritePhase(ctx, phases[1], worker, prefix+"-dp", workers)
	logStoreFTPhase(t, phases[1])

	// ── Resolve fault ─────────────────────────────────────────────────────────
	t.Logf("--- unpausing %q", container)
	if err := ftDockerUnpause(container); err != nil {
		t.Fatalf("docker unpause %s: %v", container, err)
	}
	waitForClusterBalancedFT(t, worker, container)

	// ── Phase 3: after unpause ────────────────────────────────────────────────
	t.Log("--- phase: after-unpause")
	runStoreFTWritePhase(ctx, phases[2], worker, prefix+"-au", workers)
	logStoreFTPhase(t, phases[2])

	// ── Summary ───────────────────────────────────────────────────────────────
	t.Log("=== Summary ===")
	t.Logf("%-22s  %8s  %8s  %8s  %8s", "phase", "ops/s", "ok", "errors", "p99_ms")
	for _, p := range phases {
		t.Logf("%-22s  %8.0f  %8d  %8d  %8.1f",
			p.Name, p.opsPerSec(), p.sOK, p.totalErrors(), p.pctMs(0.99))
		r := makeStoreFTResult(runID, testName, p)
		r.Notes = fmt.Sprintf("container=%s method=SIGSTOP workers=%d", container, workers)
		globalStoreFTWriter.add(r)
	}

	// ── Assertions ────────────────────────────────────────────────────────────
	t.Logf("during-pause: %d ok, %d errors (elevated errors expected for connections landing on the paused node)",
		phases[1].sOK, phases[1].totalErrors())
	if phases[2].sOK == 0 {
		t.Errorf("after-unpause: 0 successful ops — store did not recover from SIGSTOP")
	}
	if phases[0].sOK > 0 && phases[2].sOK > 0 {
		ratio := phases[2].opsPerSec() / phases[0].opsPerSec()
		t.Logf("throughput recovery ratio: %.0f%%", ratio*100)
		if ratio < 0.80 {
			t.Errorf("after-unpause throughput %.0f ops/s is >20%% below before-pause %.0f ops/s",
				phases[2].opsPerSec(), phases[0].opsPerSec())
		}
	}
}

// ── Test 4: TestStoreFT_ReadWriteMix ──────────────────────────────────────────
//
// Kills a follower node while running a mixed read + write workload directly on
// the store layer.  Half the goroutines create new promises; the other half
// read previously seeded promises.
//
// This isolates the store-layer read-path resilience independently of the
// Resonate HTTP layer and is useful for comparing read vs. write error rates.
//
// Expected behaviour:
//   - During failure: <1% combined read+write errors (same RF=3 guarantee).
//   - All seeded promises remain readable before, during, and after the fault.
func TestStoreFT_ReadWriteMix(t *testing.T) {
	requireStoreFaultTest(t)
	const testName = "StoreFT_ReadWriteMix"
	const seedCount = 200
	markStoreFTTestActive(t, testName)

	runID := time.Now().Format("20060102_150405")
	container := ftFaultContainer()
	workers := ftWorkers()
	prefix := "storeft-rw-" + runID

	pool, worker := newFaultTestWorker(t)
	defer pool.Close()
	resetFaultTestDB(t, pool)

	// ── Seed promises for the read path ───────────────────────────────────────
	t.Logf("seeding %d promises...", seedCount)
	seededIDs := make([]string, 0, seedCount)
	seedErrors := 0
	for i := 0; i < seedCount; i++ {
		id := fmt.Sprintf("%s-seed-%04d", prefix, i)
		if _, err := execCreatePromise(worker, id); err != nil {
			seedErrors++
		} else {
			seededIDs = append(seededIDs, id)
		}
	}
	if len(seededIDs) == 0 {
		t.Fatalf("all %d seed writes failed — is the cluster running?", seedCount)
	}
	t.Logf("seeded %d/%d promises (errors=%d)", len(seededIDs), seedCount, seedErrors)

	phases := []*storeFTPhase{
		{Name: "before-failure", Duration: 15 * time.Second, TestName: testName, OpType: "read_write_mix"},
		{Name: "during-failure", Duration: 30 * time.Second, TestName: testName, OpType: "read_write_mix"},
		{Name: "after-recovery", Duration: 30 * time.Second, TestName: testName, OpType: "read_write_mix"},
	}
	ctx := context.Background()

	t.Logf("=== %s: killing follower %q, workers=%d (mix: %d readers + %d writers) ===",
		testName, container, workers, workers/2, workers-workers/2)

	// ── Phase 1: baseline (mixed) ─────────────────────────────────────────────
	t.Log("--- phase: before-failure (read+write mix)")
	runStoreFTMixPhase(ctx, phases[0], worker, seededIDs, prefix+"-bf", workers)
	logStoreFTPhase(t, phases[0])

	// ── Inject fault ─────────────────────────────────────────────────────────
	t.Logf("--- killing follower %q", container)
	if err := ftDockerStop(container); err != nil {
		t.Fatalf("docker stop %s: %v", container, err)
	}

	// ── Phase 2: during failure (mixed) ───────────────────────────────────────
	t.Log("--- phase: during-failure (read+write mix)")
	runStoreFTMixPhase(ctx, phases[1], worker, seededIDs, prefix+"-df", workers)
	logStoreFTPhase(t, phases[1])

	// ── Resolve fault ─────────────────────────────────────────────────────────
	t.Logf("--- restarting follower %q", container)
	if err := ftDockerStart(container); err != nil {
		t.Fatalf("docker start %s: %v", container, err)
	}
	waitForClusterBalancedFT(t, worker, container)

	// ── Phase 3: after recovery (mixed) ───────────────────────────────────────
	t.Log("--- phase: after-recovery (read+write mix)")
	runStoreFTMixPhase(ctx, phases[2], worker, seededIDs, prefix+"-ar", workers)
	logStoreFTPhase(t, phases[2])

	// ── Summary ───────────────────────────────────────────────────────────────
	t.Log("=== Summary ===")
	t.Logf("%-22s  %8s  %8s  %8s  %8s", "phase", "ops/s", "ok", "errors", "p99_ms")
	for _, p := range phases {
		t.Logf("%-22s  %8.0f  %8d  %8d  %8.1f",
			p.Name, p.opsPerSec(), p.sOK, p.totalErrors(), p.pctMs(0.99))
		r := makeStoreFTResult(runID, testName, p)
		r.Notes = fmt.Sprintf("container=%s workers=%d seeded=%d read_workers=%d write_workers=%d",
			container, workers, len(seededIDs), workers/2, workers-workers/2)
		globalStoreFTWriter.add(r)
	}

	// ── Data integrity: verify all seeded promises still readable ─────────────
	t.Log("verifying seeded promises are still readable after test...")
	unreadable := 0
	for _, id := range seededIDs {
		if _, err := execReadPromise(worker, id); err != nil {
			unreadable++
		}
	}
	readable := len(seededIDs) - unreadable
	recordStoreFTDataIntegrity(testName, len(seededIDs), readable)
	t.Logf("seeded promise readability: %d/%d readable, %d unreadable",
		readable, len(seededIDs), unreadable)
	if unreadable > 0 {
		t.Errorf("data integrity: %d seeded promises are unreadable after node recovery — data may be lost", unreadable)
	}

	// ── Availability assertions ────────────────────────────────────────────────
	totalDuring := phases[1].totalOps()
	errorsDuring := phases[1].totalErrors()
	budget := int64(50)
	if pct := totalDuring / 100; pct > budget {
		budget = pct
	}
	if errorsDuring > budget {
		t.Errorf("during-failure: %d errors out of %d ops — exceeds 1%% budget (%d)",
			errorsDuring, totalDuring, budget)
	}
	if phases[2].sOK == 0 {
		t.Errorf("after-recovery: 0 successful ops — store did not recover")
	}
}
