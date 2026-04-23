// dbprobe — continuous YugabyteDB fault-behaviour analyser.
//
// Runs 7 concurrent probes, each modelled after a real Resonate coroutine.
// Every error is printed immediately with its SQLSTATE code and whether it is
// retryable by the server's isRetryableError() logic.  A summary banner is
// reprinted every 5 s.
//
// Usage (from the resonate-fork root):
//
//	go run ./cmd/dbprobe \
//	    -hosts "localhost:5433,localhost:5434,localhost:5435" \
//	    -db resonate -user yugabyte -pass yugabyte \
//	    -statement-timeout 5000 -max-conns 16 -interval 200ms
//
// While it is running, kill the primary node and watch the output.
//
// NOTE: load_balance is disabled by default because the YugaByte smart driver
// calls yb_servers() and then tries to connect to the returned internal docker
// IPs (127.0.0.x from Mac Docker Desktop), which are unreachable from the host.
// Pass -load-balance=true only when running inside the docker network.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/yugabyte/pgx/v5"
	"github.com/yugabyte/pgx/v5/pgconn"
	"github.com/yugabyte/pgx/v5/pgxpool"
)

// ── colour helpers ────────────────────────────────────────────────────────────

const (
	cReset  = "\033[0m"
	cRed    = "\033[31m"
	cGreen  = "\033[32m"
	cYellow = "\033[33m"
	cCyan   = "\033[36m"
	cGray   = "\033[90m"
	cBold   = "\033[1m"
)

func colorize(color, s string) string { return color + s + cReset }

// ── error classification ──────────────────────────────────────────────────────

type errInfo struct {
	sqlstate  string
	msg       string
	retryable bool
	full      string
}

func classify(err error) errInfo {
	if err == nil {
		return errInfo{}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return errInfo{"ctx/deadline", "context deadline exceeded", false, err.Error()}
	}
	if errors.Is(err, context.Canceled) {
		return errInfo{"ctx/canceled", "context canceled", false, err.Error()}
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return errInfo{"conn/eof", "connection EOF", true, err.Error()}
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		retryable := pgErr.Code == "40001" || pgErr.Code == "40P01" ||
			pgErr.Code == "XX000" || pgErr.Code == "08006" || pgErr.Code == "57P01"
		return errInfo{
			sqlstate:  pgErr.Code + " " + sqlstateLabel(pgErr.Code),
			msg:       trimMsg(pgErr.Message),
			retryable: retryable,
			full:      err.Error(),
		}
	}

	var netErr *net.OpError
	if errors.As(err, &netErr) {
		label := "net_op_error"
		if errors.Is(netErr.Err, syscall.ECONNRESET) {
			label = "conn_reset"
		} else if errors.Is(netErr.Err, syscall.ECONNREFUSED) {
			label = "conn_refused"
		} else if errors.Is(netErr.Err, syscall.ETIMEDOUT) {
			label = "conn_timeout"
		}
		return errInfo{"net/" + label, netErr.Error(), true, err.Error()}
	}

	return errInfo{"unknown", err.Error(), false, err.Error()}
}

func sqlstateLabel(code string) string {
	switch code {
	case "40001":
		return "serialization_failure"
	case "40P01":
		return "deadlock_detected"
	case "XX000":
		return "raft_leader_election / internal"
	case "08006":
		return "connection_failure"
	case "57P01":
		return "admin_shutdown"
	case "57014":
		return "query_canceled (statement_timeout)"
	case "08001":
		return "connection_rejected"
	case "08004":
		return "connection_rejected_sqlclient"
	case "23505":
		return "unique_violation"
	case "23503":
		return "foreign_key_violation"
	case "42P01":
		return "undefined_table"
	default:
		return ""
	}
}

func trimMsg(s string) string {
	if len(s) > 120 {
		return s[:117] + "..."
	}
	return s
}

// ── metrics ───────────────────────────────────────────────────────────────────

type counters struct {
	ok   atomic.Int64
	errs sync.Map // sqlstate → *atomic.Int64
}

func (c *counters) recordOK() { c.ok.Add(1) }

func (c *counters) recordErr(sqlstate string) {
	v, _ := c.errs.LoadOrStore(sqlstate, new(atomic.Int64))
	v.(*atomic.Int64).Add(1)
}

func (c *counters) snapshot() (ok int64, errMap map[string]int64) {
	ok = c.ok.Swap(0)
	errMap = make(map[string]int64)
	c.errs.Range(func(k, v any) bool {
		n := v.(*atomic.Int64).Swap(0)
		if n > 0 {
			errMap[k.(string)] = n
		}
		return true
	})
	return
}

// ── probe definition ──────────────────────────────────────────────────────────

type probe struct {
	name string
	fn   func(ctx context.Context, pool *pgxpool.Pool) (int, error)
}

// withTx runs fn inside a single transaction at the given isolation level.
func withTx(ctx context.Context, pool *pgxpool.Pool, iso pgx.TxIsoLevel, fn func(pgx.Tx) (int, error)) (int, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: iso})
	if err != nil {
		return 0, err
	}
	n, err := fn(tx)
	if err != nil {
		_ = tx.Rollback(ctx)
		return 0, err
	}
	return n, tx.Commit(ctx)
}

func buildProbes() []probe {
	const fakeProcessID = "probe-pid-never-matches"
	const ttlNs = int64(60 * time.Second) // task TTL: 60 s in nanoseconds

	return []probe{

		// ── 1. timeout_promises ── mirrors timeoutPromises.go TX1 ────────────
		// Scans for pending promises whose deadline has passed.  Hot path on a
		// busy server; exercises the (state=1, timeout) index range scan.
		{
			name: "timeout_promises",
			fn: func(ctx context.Context, pool *pgxpool.Pool) (int, error) {
				return withTx(ctx, pool, pgx.ReadCommitted, func(tx pgx.Tx) (int, error) {
					nowNs := time.Now().UnixNano()
					rows, err := tx.Query(ctx, `
						SELECT id, state FROM promises
						WHERE state = 1 AND timeout <= $1
						ORDER BY sort_id ASC LIMIT 10`, nowNs)
					if err != nil {
						return 0, err
					}
					defer rows.Close()
					n := 0
					for rows.Next() {
						n++
					}
					return n, rows.Err()
				})
			},
		},

		// ── 2. enqueue_tasks ── mirrors enqueueTasks.go TX1 + TX2 ───────────
		// TX1: DISTINCT ON scan for tasks that can be enqueued (Init state,
		// no sibling already Enqueued/Claimed).  TX2: batch-UPDATE found tasks
		// to Enqueued.  Most expensive query; stresses the covering index.
		{
			name: "enqueue_tasks",
			fn: func(ctx context.Context, pool *pgxpool.Pool) (int, error) {
				nowNs := time.Now().UnixNano()

				type row struct{ id string }
				var found []row

				// TX1 — scan
				if _, err := withTx(ctx, pool, pgx.ReadCommitted, func(tx pgx.Tx) (int, error) {
					rows, err := tx.Query(ctx, `
						SELECT DISTINCT ON (root_promise_id)
							id, state, root_promise_id
						FROM tasks t1
						WHERE state = 1 AND expires_at <= $1
						AND NOT EXISTS (
							SELECT 1 FROM tasks t2
							WHERE t2.root_promise_id = t1.root_promise_id
							AND t2.state IN (2, 4)
						)
						ORDER BY root_promise_id, sort_id ASC
						LIMIT 10`, nowNs)
					if err != nil {
						return 0, err
					}
					defer rows.Close()
					for rows.Next() {
						var id string
						var state int
						var rootID string
						if err := rows.Scan(&id, &state, &rootID); err != nil {
							return 0, err
						}
						found = append(found, row{id})
					}
					return len(found), rows.Err()
				}); err != nil {
					return 0, err
				}

				if len(found) == 0 {
					return 0, nil
				}

				// TX2 — batch update to Enqueued (state=2)
				return withTx(ctx, pool, pgx.ReadCommitted, func(tx pgx.Tx) (int, error) {
					nowNs2 := time.Now().UnixNano()
					batch := &pgx.Batch{}
					for _, r := range found {
						batch.Queue(
							`UPDATE tasks SET state = 2, completed_on = $1 WHERE id = $2 AND state = 1`,
							nowNs2, r.id,
						)
					}
					br := tx.SendBatch(ctx, batch)
					defer br.Close()
					updated := 0
					for range found {
						tag, err := br.Exec()
						if err != nil {
							return 0, err
						}
						updated += int(tag.RowsAffected())
					}
					return updated, nil
				})
			},
		},

		// ── 3. heartbeat_tasks ── mirrors heartbeatTasks.go ─────────────────
		// Bulk-extends the expiry of all tasks owned by a process.  In prod
		// this is the liveness signal every worker sends to keep tasks alive.
		// Uses a fake process ID → always 0 rows affected, but the TX is real.
		{
			name: "heartbeat_tasks",
			fn: func(ctx context.Context, pool *pgxpool.Pool) (int, error) {
				return withTx(ctx, pool, pgx.ReadCommitted, func(tx pgx.Tx) (int, error) {
					nowNs := time.Now().UnixNano()
					tag, err := tx.Exec(ctx, `
						UPDATE tasks
						SET expires_at =
							CASE WHEN $1 > 9223372036854775807 - ttl
								THEN 9223372036854775807
								ELSE $1 + ttl
							END
						WHERE process_id = $2 AND state = 4`,
						nowNs, fakeProcessID)
					if err != nil {
						return 0, err
					}
					return int(tag.RowsAffected()), nil
				})
			},
		},

		// ── 4. timeout_tasks ── mirrors timeoutTasks.go TX1 ─────────────────
		// Finds Enqueued/Claimed tasks whose deadline or TTL has expired so
		// the server can reclaim them.  Uses the bitmask state & X predicate.
		{
			name: "timeout_tasks",
			fn: func(ctx context.Context, pool *pgxpool.Pool) (int, error) {
				return withTx(ctx, pool, pgx.ReadCommitted, func(tx pgx.Tx) (int, error) {
					nowNs := time.Now().UnixNano()
					rows, err := tx.Query(ctx, `
						SELECT id, state FROM tasks
						WHERE state & $1 != 0
						AND ((expires_at != 0 AND expires_at <= $2) OR timeout <= $2)
						ORDER BY root_promise_id, sort_id ASC
						LIMIT 10`,
						6, // 2 (Enqueued) | 4 (Claimed)
						nowNs)
					if err != nil {
						return 0, err
					}
					defer rows.Close()
					n := 0
					for rows.Next() {
						n++
					}
					return n, rows.Err()
				})
			},
		},

		// ── 5. create_promise ── mirrors createPromise.go TX1 + TX2 ─────────
		// TX1: read to check idempotency (promise may already exist).
		// TX2: batch INSERT promise + initial task.
		// Uses a unique timestamp-based ID so every iteration creates a new row.
		{
			name: "create_promise",
			fn: func(ctx context.Context, pool *pgxpool.Pool) (int, error) {
				nowNs := time.Now().UnixNano()
				id := fmt.Sprintf("__probe__%d__", nowNs)

				// TX1 — idempotency check
				var existed bool
				if _, err := withTx(ctx, pool, pgx.ReadCommitted, func(tx pgx.Tx) (int, error) {
					var state int
					err := tx.QueryRow(ctx,
						`SELECT state FROM promises WHERE id = $1`, id).Scan(&state)
					if err == nil {
						existed = true
						return 1, nil
					}
					if errors.Is(err, pgx.ErrNoRows) {
						return 0, nil
					}
					return 0, err
				}); err != nil {
					return 0, err
				}
				if existed {
					return 0, nil
				}

				// TX2 — batch INSERT promise + task
				return withTx(ctx, pool, pgx.ReadCommitted, func(tx pgx.Tx) (int, error) {
					batch := &pgx.Batch{}
					batch.Queue(`
						INSERT INTO promises
							(id, state, param_headers, param_data, timeout,
							 idempotency_key_for_create, tags, created_on)
						VALUES ($1, 1, '{}', '', $2, $1, '{}', $3)
						ON CONFLICT(id) DO NOTHING`,
						id,
						nowNs+int64(time.Hour),
						nowNs,
					)
					batch.Queue(`
						INSERT INTO tasks
							(id, recv, mesg, timeout, process_id, state,
							 root_promise_id, ttl, expires_at, created_on)
						VALUES ($1, '{}', '{}', $2, NULL, 1, $3, $4, $5, $6)
						ON CONFLICT(id) DO NOTHING`,
						id+"/task/0",
						nowNs+int64(time.Hour),
						id,
						ttlNs,
						nowNs+ttlNs,
						nowNs,
					)
					br := tx.SendBatch(ctx, batch)
					defer br.Close()
					tag, err := br.Exec()
					if err != nil {
						return 0, err
					}
					if _, err := br.Exec(); err != nil {
						return 0, err
					}
					return int(tag.RowsAffected()), nil
				})
			},
		},

		// ── 6. claim_task ── mirrors claimTask.go TX1 + TX2 + TX3 ───────────
		// TX1: read an Enqueued task.  TX2: update it to Claimed.
		// TX3: read the root promise.  Three separate round-trips per cycle.
		{
			name: "claim_task",
			fn: func(ctx context.Context, pool *pgxpool.Pool) (int, error) {
				// TX1 — find an Enqueued task
				var taskID string
				var rootPromiseID string
				if _, err := withTx(ctx, pool, pgx.ReadCommitted, func(tx pgx.Tx) (int, error) {
					err := tx.QueryRow(ctx, `
						SELECT id, root_promise_id FROM tasks
						WHERE state = 2
						LIMIT 1`).Scan(&taskID, &rootPromiseID)
					if err != nil {
						if errors.Is(err, pgx.ErrNoRows) {
							return 0, nil
						}
						return 0, err
					}
					return 1, nil
				}); err != nil {
					return 0, err
				}
				if rootPromiseID == "" {
					return 0, nil // nothing to claim
				}

				nowNs := time.Now().UnixNano()

				// TX2 — claim the task (mirrors TASK_UPDATE_STATEMENT: explicit counter,
				// no read-modify-write so no write-write contention with enqueue_tasks)
				if _, err := withTx(ctx, pool, pgx.ReadCommitted, func(tx pgx.Tx) (int, error) {
					tag, err := tx.Exec(ctx, `
						UPDATE tasks
						SET process_id = $1, state = 4, counter = 1,
						    attempt = 1, ttl = $2, expires_at = $3, completed_on = 0
						WHERE id = $4 AND state = 2`,
						fakeProcessID,
						ttlNs,
						nowNs+ttlNs,
						taskID,
					)
					if err != nil {
						return 0, err
					}
					return int(tag.RowsAffected()), nil
				}); err != nil {
					return 0, err
				}

				// TX3 — read the root promise
				return withTx(ctx, pool, pgx.ReadCommitted, func(tx pgx.Tx) (int, error) {
					var state int
					err := tx.QueryRow(ctx,
						`SELECT state FROM promises WHERE id = $1`, rootPromiseID).Scan(&state)
					if err != nil {
						if errors.Is(err, pgx.ErrNoRows) {
							return 0, nil
						}
						return 0, err
					}
					return 1, nil
				})
			},
		},

		// ── 7. complete_promise ── mirrors completePromise.go TX1 + TX2 ──────
		// TX1: read a probe-created pending promise.
		// TX2: batch of 4 commands — UpdatePromise + CompleteTasks +
		//      CreateTasksFromCallbacks + DeleteCallbacks.
		// In practice the probe won't have callbacks, so commands 3 & 4 are
		// no-ops, but the batch structure is identical to the real coroutine.
		{
			name: "complete_promise",
			fn: func(ctx context.Context, pool *pgxpool.Pool) (int, error) {
				// TX1 — find a probe promise to resolve
				var promiseID string
				if _, err := withTx(ctx, pool, pgx.ReadCommitted, func(tx pgx.Tx) (int, error) {
					err := tx.QueryRow(ctx, `
						SELECT id FROM promises
						WHERE id LIKE '__probe__%' AND state = 1
						LIMIT 1`).Scan(&promiseID)
					if err != nil {
						if errors.Is(err, pgx.ErrNoRows) {
							return 0, nil
						}
						return 0, err
					}
					return 1, nil
				}); err != nil {
					return 0, err
				}
				if promiseID == "" {
					return 0, nil // nothing to complete
				}

				nowNs := time.Now().UnixNano()

				// TX2 — 4-command complete batch (mirrors actual server batch)
				return withTx(ctx, pool, pgx.ReadCommitted, func(tx pgx.Tx) (int, error) {
					batch := &pgx.Batch{}
					// cmd 1: UpdatePromise → Resolved (state=2)
					batch.Queue(`
						UPDATE promises
						SET state = 2, value_headers = '{}', value_data = 'probe_complete',
						    idempotency_key_for_complete = $1, completed_on = $2
						WHERE id = $3 AND state = 1`,
						"probe_ikey", nowNs, promiseID,
					)
					// cmd 2: CompleteTasks (state=8) for this root promise
					batch.Queue(`
						UPDATE tasks SET state = 8, completed_on = $1
						WHERE root_promise_id = $2 AND state IN (1, 2, 4)`,
						nowNs, promiseID,
					)
					// cmd 3: CreateTasksFromCallbacks (INSERT … SELECT from callbacks)
					batch.Queue(`
						INSERT INTO tasks
							(id, recv, mesg, timeout, root_promise_id, created_on)
						SELECT id, recv, mesg, timeout, root_promise_id, $1
						FROM callbacks
						WHERE promise_id = $2
						ORDER BY id`,
						nowNs, promiseID,
					)
					// cmd 4: DeleteCallbacks
					batch.Queue(`DELETE FROM callbacks WHERE promise_id = $1`, promiseID)

					br := tx.SendBatch(ctx, batch)
					defer br.Close()
					tag, err := br.Exec()
					if err != nil {
						return 0, err
					}
					for range 3 { // drain remaining 3 results
						if _, err := br.Exec(); err != nil {
							return 0, err
						}
					}
					return int(tag.RowsAffected()), nil
				})
			},
		},
	}
}

// ── output ────────────────────────────────────────────────────────────────────

var mu sync.Mutex

func logOK(probeName string, dur time.Duration, rows int) {
	mu.Lock()
	defer mu.Unlock()
	ts := time.Now().Format("15:04:05.000")
	fmt.Printf("%s  %-22s  %s  %6s  rows=%-4d\n",
		colorize(cGray, ts),
		probeName,
		colorize(cGreen, "OK "),
		formatDur(dur),
		rows,
	)
}

func logErr(probeName string, dur time.Duration, info errInfo) {
	mu.Lock()
	defer mu.Unlock()
	ts := time.Now().Format("15:04:05.000")
	retryTag := colorize(cRed, "ERR[-]")
	if info.retryable {
		retryTag = colorize(cYellow, "ERR[R]")
	}
	fmt.Printf("%s  %-22s  %s  %6s  %s  %s\n",
		colorize(cGray, ts),
		probeName,
		retryTag,
		formatDur(dur),
		colorize(cBold, info.sqlstate),
		info.msg,
	)
}

func formatDur(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%dµs", d.Microseconds())
	}
	return fmt.Sprintf("%dms", d.Milliseconds())
}

func printBanner(pool *pgxpool.Pool, cnt *counters, interval time.Duration) {
	ok, errMap := cnt.snapshot()
	s := pool.Stat()
	secs := interval.Seconds()

	mu.Lock()
	defer mu.Unlock()

	fmt.Printf("\n%s\n", colorize(cCyan, strings.Repeat("─", 80)))
	fmt.Printf("  %s  total=%-3d acquired=%-3d idle=%-3d constructing=%-3d\n",
		colorize(cCyan, "POOL"),
		s.TotalConns(), s.AcquiredConns(), s.IdleConns(), s.ConstructingConns(),
	)
	fmt.Printf("  %s  ok=%.1f/s   err=%.1f/s\n",
		colorize(cCyan, "RATE"),
		float64(ok)/secs,
		totalErrs(errMap)/secs,
	)
	if len(errMap) == 0 {
		fmt.Printf("  %s  (none)\n", colorize(cCyan, "ERRS"))
	} else {
		for _, k := range sortedKeys(errMap) {
			color := cYellow
			if !isRetryableLabel(k) {
				color = cRed
			}
			fmt.Printf("  %s  %-50s  %d\n",
				colorize(cCyan, "ERRS"),
				colorize(color, k),
				errMap[k],
			)
		}
	}
	fmt.Printf("%s\n\n", colorize(cCyan, strings.Repeat("─", 80)))
}

func totalErrs(m map[string]int64) float64 {
	var t float64
	for _, v := range m {
		t += float64(v)
	}
	return t
}

func sortedKeys(m map[string]int64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func isRetryableLabel(label string) bool {
	for _, prefix := range []string{"40001", "40P01", "XX000", "08006", "57P01", "conn/eof", "net/"} {
		if strings.HasPrefix(label, prefix) {
			return true
		}
	}
	return false
}

// ── pool setup ────────────────────────────────────────────────────────────────

func newPool(hosts, db, user, pass string, maxConns int32, stmtTimeoutMs int, loadBalance bool) (*pgxpool.Pool, error) {
	parts := strings.Split(hosts, ",")
	for i, h := range parts {
		parts[i] = strings.TrimSpace(h)
	}

	params := fmt.Sprintf("sslmode=disable&connect_timeout=1&statement_timeout=%d", stmtTimeoutMs)
	if loadBalance {
		params += "&load_balance=true&yb_servers_refresh_interval=15"
	} else {
		params += "&load_balance=false"
	}
	dsn := fmt.Sprintf("postgres://%s:%s@%s/%s?%s",
		user, pass, strings.Join(parts, ","), db, params)

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}

	cfg.MaxConns = maxConns
	cfg.MinConns = maxConns / 2
	cfg.HealthCheckPeriod = 1 * time.Second
	cfg.MaxConnLifetime = 30 * time.Second
	cfg.MaxConnIdleTime = 10 * time.Second

	cfg.PrepareConn = func(ctx context.Context, conn *pgx.Conn) (bool, error) {
		return conn.Ping(ctx) == nil, nil
	}
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, fmt.Sprintf("SET statement_timeout = %d", stmtTimeoutMs))
		return err
	}
	cfg.ConnConfig.Config.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		d := &net.Dialer{KeepAlive: 3 * time.Second, Timeout: 1 * time.Second}
		return d.DialContext(ctx, network, addr)
	}

	return pgxpool.NewWithConfig(context.Background(), cfg)
}

// ── probe runner ──────────────────────────────────────────────────────────────

func runProbe(ctx context.Context, pool *pgxpool.Pool, p probe, interval time.Duration, cnt *counters) {
	const txTimeout = 10 * time.Second

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		start := time.Now()
		txCtx, cancel := context.WithTimeout(ctx, txTimeout)
		rows, err := p.fn(txCtx, pool)
		cancel()
		dur := time.Since(start)

		if err == nil {
			cnt.recordOK()
			logOK(p.name, dur, rows)
		} else {
			if ctx.Err() != nil {
				return
			}
			info := classify(err)
			cnt.recordErr(info.sqlstate)
			logErr(p.name, dur, info)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

// ── main ──────────────────────────────────────────────────────────────────────

func main() {
	hosts := flag.String("hosts", "localhost:5433,localhost:5434,localhost:5435",
		"comma-separated host:port list (docker-compose exposed ports)")
	db := flag.String("db", "resonate", "database name")
	user := flag.String("user", "yugabyte", "database user")
	pass := flag.String("pass", "yugabyte", "database password")
	maxConns := flag.Int("max-conns", 4, "pgxpool MaxConns")
	interval := flag.Duration("interval", 200*time.Millisecond, "sleep between probe iterations")
	bannerInterval := flag.Duration("banner", 5*time.Second, "how often to print the summary banner")
	stmtTimeout := flag.Int("statement-timeout", 20000, "per-statement timeout in milliseconds")
	loadBalance := flag.Bool("load-balance", false,
		"enable YugaByte smart driver load balancing (use true only inside docker; "+
			"from Mac the driver tries to connect to internal docker IPs → 127.0.0.x spam)")
	flag.Parse()

	fmt.Printf("\n%s\n", colorize(cBold+cCyan, "═══  YugabyteDB Fault Probe  ═══"))
	fmt.Printf("  hosts             : %s\n", *hosts)
	fmt.Printf("  database          : %s\n", *db)
	fmt.Printf("  max pool conns    : %d\n", *maxConns)
	fmt.Printf("  probe interval    : %s\n", *interval)
	fmt.Printf("  statement timeout : %dms\n", *stmtTimeout)
	fmt.Printf("  tx timeout        : 10s (fixed)\n")
	fmt.Printf("  load_balance      : %v\n", *loadBalance)
	fmt.Printf("\n  %s retryable error    %s non-retryable error\n",
		colorize(cYellow, "ERR[R]"), colorize(cRed, "ERR[-]"))
	fmt.Printf("\n  probes: timeout_promises · enqueue_tasks · heartbeat_tasks · timeout_tasks\n")
	fmt.Printf("          create_promise · claim_task · complete_promise\n")
	fmt.Printf("\n%s\n\n", strings.Repeat("─", 80))

	pool, err := newPool(*hosts, *db, *user, *pass, int32(*maxConns), *stmtTimeout, *loadBalance)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create pool: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()

	wctx, wcancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := pool.Ping(wctx); err != nil {
		fmt.Fprintf(os.Stderr, "initial ping failed: %v\n", err)
		wcancel()
		os.Exit(1)
	}
	wcancel()
	fmt.Printf("%s\n\n", colorize(cGreen, "  ✓  connected to "+*hosts))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cnt := &counters{}
	probes := buildProbes()

	var wg sync.WaitGroup
	for _, p := range probes {
		wg.Add(1)
		go func(p probe) {
			defer wg.Done()
			runProbe(ctx, pool, p, *interval, cnt)
		}(p)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(*bannerInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				printBanner(pool, cnt, *bannerInterval)
			}
		}
	}()

	wg.Wait()
	fmt.Printf("\n%s\n", colorize(cCyan, "  ── final snapshot ──"))
	printBanner(pool, cnt, *bannerInterval)
}
