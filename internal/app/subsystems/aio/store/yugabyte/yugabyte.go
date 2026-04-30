package yugabyte

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand" // nosemgrep
	"net"
	"net/url"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/migrations"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/yugabyte/pgx/v5"
	"github.com/yugabyte/pgx/v5/pgconn"
	"github.com/yugabyte/pgx/v5/pgxpool"
	"github.com/yugabyte/pgx/v5/stdlib"

	cmdUtil "github.com/resonatehq/resonate/cmd/util"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/schedule"
	"github.com/resonatehq/resonate/pkg/task"
)

const (
	// Schema is now managed by migrations (see internal/migrationfiles/migrations/yugabyte/)
	CREATE_TABLE_STATEMENT = `
	CREATE TABLE IF NOT EXISTS migrations (
		id INTEGER,
		PRIMARY KEY(id)
	);`

	DROP_TABLE_STATEMENT = `
	DROP TABLE promises;
	DROP TABLE callbacks;
	DROP TABLE schedules;
	DROP TABLE locks;
	DROP TABLE tasks;
	DROP TABLE migrations;`

	PROMISE_SELECT_STATEMENT = `
	SELECT
		id, state, param_headers, param_data, value_headers, value_data, timeout, tags, created_on, completed_on
	FROM
		promises
	WHERE
		id = $1`

	PROMISE_SELECT_ALL_STATEMENT = `
	SELECT
		id, state, param_headers, param_data, value_headers, value_data, timeout, tags, created_on, completed_on, sort_id
	FROM
		promises
	WHERE
		state = 1 AND timeout <= $1
	ORDER BY
		sort_id ASC
	LIMIT
		$2`

	PROMISE_SEARCH_STATEMENT = `
	SELECT
		id, state, param_headers, param_data, value_headers, value_data, timeout, tags, created_on, completed_on, sort_id
	FROM
		promises
	WHERE
		($1::int IS NULL OR sort_id < $1) AND
		id LIKE $2 AND
		state & $3 != 0 AND
		($4::jsonb IS NULL OR tags @> $4)
	ORDER BY
		sort_id DESC
	LIMIT
		$5`

	PROMISE_INSERT_STATEMENT = `
	INSERT INTO promises
		(id, state, param_headers, param_data, timeout, idempotency_key_for_create, tags, created_on)
	VALUES
		($1, $2, $3, $4, $5, $6, $7, $8)
	ON CONFLICT(id) DO NOTHING -- idempotency_key must be equal to id for this stmt`

	PROMISE_UPDATE_STATEMENT = `
	UPDATE
		promises
	SET
		state = $1, value_headers = $2, value_data = $3, idempotency_key_for_complete = $4, completed_on = $5
	WHERE
		id = $6 AND state = 1 -- idempotency_key must be equal to id for this stmt`

	CALLBACK_INSERT_STATEMENT = `
	INSERT INTO callbacks
		(id, promise_id, root_promise_id, recv, mesg, timeout, created_on)
	SELECT
		$1, $2, $3, $4, $5, $6, $7
	WHERE EXISTS
		(SELECT 1 FROM promises WHERE id = $2 AND state = 1)
	AND NOT EXISTS
		(SELECT 1 FROM callbacks WHERE id = $1)`

	CALLBACK_DELETE_STATEMENT = `
	DELETE FROM callbacks WHERE promise_id = $1`

	SCHEDULE_SELECT_STATEMENT = `
	SELECT
		id, description, cron, tags, promise_id, promise_timeout, promise_param_headers, promise_param_data, promise_tags, last_run_time, next_run_time, created_on
	FROM
		schedules
	WHERE
		id = $1`

	SCHEDULE_SELECT_ALL_STATEMENT = `
	SELECT
		id, cron, promise_id, promise_timeout, promise_param_headers, promise_param_data, promise_tags, last_run_time, next_run_time
	FROM
		schedules
	WHERE
		next_run_time <= $1
	ORDER BY
		next_run_time ASC, sort_id ASC
	LIMIT
		$2`

	SCHEDULE_SEARCH_STATEMENT = `
	SELECT
		id, cron, tags, last_run_time, next_run_time, created_on, sort_id
	FROM
		schedules
	WHERE
		($1::int IS NULL OR sort_id < $1) AND
		id LIKE $2 AND
		($3::jsonb IS NULL OR tags @> $3)
	ORDER BY
		sort_id DESC
	LIMIT
		$4`

	SCHEDULE_INSERT_STATEMENT = `
	INSERT INTO schedules
		(id, description, cron, tags, promise_id, promise_timeout, promise_param_headers, promise_param_data, promise_tags, next_run_time, idempotency_key, created_on)
	VALUES
		($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	ON CONFLICT(id) DO NOTHING -- idempotency_key must be equal to id for this stmt`

	SCHEDULE_UPDATE_STATEMENT = `
	UPDATE
		schedules
	SET
		last_run_time = next_run_time, next_run_time = $1
	WHERE
		id = $2 AND next_run_time = $3`

	SCHEDULE_DELETE_STATEMENT = `
	DELETE FROM schedules WHERE id = $1`

	TASK_SELECT_STATEMENT = `
	SELECT
		id, process_id, state, root_promise_id, recv, mesg, timeout, counter, attempt, ttl, expires_at, created_on, completed_on
	FROM
		tasks
	WHERE
		id = $1`

	TASK_VALIDATE_STATEMENT = `
	SELECT
		COUNT(id)
	FROM
		tasks
	WHERE
		id = $1 AND counter = $2 AND state = 4 -- state == Claimed`

	TASK_SELECT_ALL_STATEMENT = `
	SELECT
		id,
		process_id,
		state,
		root_promise_id,
		recv,
		mesg,
		timeout,
		counter,
		attempt,
		ttl,
		expires_at,
		created_on,
		completed_on
	FROM tasks
	WHERE
		state & $1 != 0 AND ((expires_at != 0 AND expires_at <= $2) OR timeout <= $2)
	ORDER BY root_promise_id, sort_id ASC
	LIMIT $3`

	TASK_SELECT_ENQUEUEABLE_STATEMENT = `
	SELECT DISTINCT ON (root_promise_id)
		id,
		process_id,
		state,
		root_promise_id,
		recv,
		mesg,
		timeout,
		counter,
		attempt,
		ttl,
		expires_at,
		created_on,
		completed_on
	FROM tasks t1
	WHERE
		state = 1 AND expires_at <= $1 -- State = 1 -> Init
	AND NOT EXISTS (
		SELECT 1
		FROM tasks t2
		WHERE t2.root_promise_id = t1.root_promise_id
		AND t2.state in (2, 4) -- 2 -> Enqueue, 4 -> Claimed
	)
	ORDER BY root_promise_id, sort_id ASC
	LIMIT $2`

	TASK_INSERT_STATEMENT = `
	INSERT INTO tasks
		(id, recv, mesg, timeout, process_id, state, root_promise_id, ttl, expires_at, created_on)
	VALUES
		($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	ON CONFLICT(id) DO NOTHING`

	TASK_INSERT_ALL_STATEMENT = `
	INSERT INTO tasks
		(id, recv, mesg, timeout, root_promise_id, created_on)
	SELECT
		id, recv, mesg, timeout, root_promise_id, $1
	FROM
		callbacks
	WHERE
		promise_id = $2
	ORDER BY
		id
	ON CONFLICT (id) DO NOTHING`

	TASK_UPDATE_STATEMENT = `
	UPDATE
		tasks
	SET
		process_id = $1, state = $2, counter = $3, attempt = $4, ttl = $5, expires_at = $6, completed_on = $7
	WHERE
		id = $8 AND state & $9 != 0 AND counter = $10`

	TASK_COMPLETE_BY_ROOT_ID_STATEMENT = `
	UPDATE
		tasks
	SET
		state = 8, completed_on = $1 -- State = 8 -> Completed
	WHERE
		root_promise_id = $2 AND state in (1, 2, 4) -- State in (Init, Enqueued, Claimed)`

	TASK_HEARTBEAT_STATEMENT = `
	UPDATE
		tasks
	SET
		expires_at =
			CASE
				WHEN $1 > 9223372036854775807 - ttl THEN 9223372036854775807 -- max int64
				ELSE $1 + ttl
			END
	WHERE
		process_id = $2 AND state = 4`

	// YugabyteDB does not support DELETE ... LIMIT; use a subquery instead.
	// Task states: Completed=8, Timedout=16 (terminal states with completed_on set).
	TASK_CLEANUP_STATEMENT = `
	DELETE FROM tasks
	WHERE id IN (
		SELECT id FROM tasks WHERE state IN (8, 16) AND completed_on < $1 LIMIT 1000
	)`

	// Promise states: Resolved=2, Rejected=4, Canceled=8, Timedout=16 (all terminal, all set completed_on).
	PROMISE_CLEANUP_STATEMENT = `
	DELETE FROM promises
	WHERE id IN (
		SELECT id FROM promises WHERE state IN (2, 4, 8, 16) AND completed_on < $1 LIMIT 1000
	)`
)

// Config

type Config struct {
	Size              int               `flag:"size" desc:"submission buffered channel size" default:"1000"`
	BatchSize         int               `flag:"batch-size" desc:"max submissions processed per iteration" default:"1000"`
	Workers           int               `flag:"workers" desc:"number of workers" default:"1" dst:"1"`
	MaxOpenConns      int               `flag:"max-open-conns" desc:"maximum number of open database connections (0 defaults to workers)" default:"0"`
	MaxIdleConns      int               `flag:"max-idle-conns" desc:"maximum number of idle database connections (0 defaults to workers)" default:"0"`
	ConnMaxIdleTime   time.Duration     `flag:"conn-max-idle-time" desc:"maximum amount of time a connection may be idle" default:"20s"`
	ConnMaxLifetime   time.Duration     `flag:"conn-max-lifetime" desc:"maximum amount of time a connection may be reused" default:"30s"`
	HealthCheckPeriod time.Duration     `flag:"health-check-period" desc:"how often pgxpool pings idle connections to detect failures" default:"2s"`
	Host              string            `flag:"host" desc:"yugabyte host" default:"localhost"`
	FallbackHosts     string            `flag:"fallback-hosts" desc:"comma-separated additional hosts for multi-node failover (e.g. yb-node2,yb-node3)" default:""`
	Port              string            `flag:"port" desc:"yugabyte YSQL port" default:"5433"`
	Username          string            `flag:"username" desc:"yugabyte username" default:"yugabyte"`
	Password          string            `flag:"password" desc:"yugabyte password" default:"yugabyte"`
	Database          string            `flag:"database" desc:"yugabyte database" default:"resonate" dst:"resonate_dst"`
	Query             map[string]string `flag:"query" desc:"yugabyte query options" dst:"{\"sslmode\":\"disable\"}" dev:"{\"sslmode\":\"disable\"}"`
	TxTimeout         time.Duration     `flag:"tx-timeout" desc:"yugabyte transaction timeout" default:"10s"`
	Reset             bool              `flag:"reset" desc:"reset yugabyte db on shutdown" default:"false" dst:"true"`
	LoadBalance       bool              `flag:"load-balance" desc:"enable cluster-aware load balancing" default:"true"`
	TopologyKeys      string            `flag:"topology-keys" desc:"topology-aware routing keys (e.g. cloud.region.zone1)" default:""`
	MaxRetries        int               `flag:"max-retries" desc:"max transaction retries on serialization/deadlock error" default:"3"`
	RefreshInterval   int               `flag:"refresh-interval" desc:"cluster node list refresh interval in seconds" default:"300"`
	CleanupRetention  time.Duration     `flag:"cleanup-retention" desc:"minimum age of completed promises/tasks before cleanup deletes them (0 disables cleanup)" default:"24h"`
	CleanupInterval   time.Duration     `flag:"cleanup-interval" desc:"how often the cleanup goroutine runs" default:"5m"`
}

func (c *Config) Bind(cmd *cobra.Command, flg *pflag.FlagSet, vip *viper.Viper, name string, prefix string, keyPrefix string) {
	cmdUtil.Bind(c, cmd, flg, vip, name, prefix, keyPrefix)
}

func (c *Config) Decode(value any, decodeHook mapstructure.DecodeHookFunc) error {
	decoderConfig := &mapstructure.DecoderConfig{
		Result:     c,
		DecodeHook: decodeHook,
	}

	decoder, err := mapstructure.NewDecoder(decoderConfig)
	if err != nil {
		return err
	}

	if err := decoder.Decode(value); err != nil {
		return err
	}

	return nil
}

func (c *Config) New(aio aio.AIO, metrics *metrics.Metrics) (aio.Subsystem, error) {
	return New(aio, metrics, c)
}

func (c *Config) NewDST(aio aio.AIO, metrics *metrics.Metrics, _ *rand.Rand, _ chan any) (aio.SubsystemDST, error) {
	return New(aio, metrics, c)
}

// Subsystem

type YugabyteStore struct {
	config  *Config
	sq      chan<- *bus.SQE[t_aio.Submission, t_aio.Completion]
	db      *pgxpool.Pool
	workers []*YugabyteStoreWorker
}

func (s *YugabyteStore) DB() *pgxpool.Pool {
	return s.db
}

type ConnConfig struct {
	Host              string
	FallbackHosts     string // comma-separated extra hosts, same port as Host
	Port              string
	Username          string
	Password          string
	Database          string
	Query             map[string]string
	MaxConns          int32
	MinConns          int32
	MaxConnLifetime   time.Duration
	MaxConnIdleTime   time.Duration
	HealthCheckPeriod time.Duration
}

func NewConn(ctx context.Context, config *ConnConfig) (*pgxpool.Pool, error) {
	rawQuery := make([]string, 0, len(config.Query))
	for _, q := range util.OrderedRangeKV(config.Query) {
		rawQuery = append(rawQuery, fmt.Sprintf("%s=%s", q.Key, q.Value))
	}

	allHosts := []string{fmt.Sprintf("%s:%s", config.Host, config.Port)}
	for _, h := range strings.Split(config.FallbackHosts, ",") {
		h = strings.TrimSpace(h)
		if h != "" {
			allHosts = append(allHosts, fmt.Sprintf("%s:%s", h, config.Port))
		}
	}

	dsn := fmt.Sprintf("postgres://%s:%s@%s/%s?%s",
		url.QueryEscape(config.Username),
		url.QueryEscape(config.Password),
		strings.Join(allHosts, ","),
		config.Database,
		strings.Join(rawQuery, "&"),
	)

	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}

	if config.MaxConns > 0 {
		poolConfig.MaxConns = config.MaxConns
	}
	if config.MinConns > 0 {
		poolConfig.MinConns = config.MinConns
	}
	if config.MaxConnLifetime > 0 {
		poolConfig.MaxConnLifetime = config.MaxConnLifetime
	}
	if config.MaxConnIdleTime > 0 {
		poolConfig.MaxConnIdleTime = config.MaxConnIdleTime
	}
	healthCheck := config.HealthCheckPeriod
	if healthCheck <= 0 {
		healthCheck = 5 * time.Second
	}
	poolConfig.HealthCheckPeriod = healthCheck

	poolConfig.PrepareConn = func(ctx context.Context, conn *pgx.Conn) (bool, error) {
		return conn.Ping(ctx) == nil, nil
	}

	return pgxpool.NewWithConfig(ctx, poolConfig)
}

func New(aio aio.AIO, metrics *metrics.Metrics, config *Config) (*YugabyteStore, error) {
	ctx := context.Background()
	sq := make(chan *bus.SQE[t_aio.Submission, t_aio.Completion], config.Size)
	workers := make([]*YugabyteStoreWorker, config.Workers)

	query := make(map[string]string, len(config.Query)+3)
	for k, v := range config.Query {
		query[k] = v
	}
	if config.LoadBalance {
		query["load_balance"] = "true"
	}
	if config.TopologyKeys != "" {
		query["topology_keys"] = config.TopologyKeys
	}
	if config.RefreshInterval > 0 {
		query["yb_servers_refresh_interval"] = strconv.Itoa(config.RefreshInterval)
	}
	// Limit how long a single connection attempt to a dead node can block.
	// connect_timeout: cap how long a TCP SYN to a dead node can block.
	// Lowered to 1s so the smart driver routes to a live node faster.
	if _, set := query["connect_timeout"]; !set {
		query["connect_timeout"] = "1"
	}

	maxOpenConns := config.MaxOpenConns
	if maxOpenConns <= 0 {
		maxOpenConns = config.Workers
	}
	maxIdleConns := config.MaxIdleConns
	if maxIdleConns <= 0 {
		maxIdleConns = config.Workers
	}

	connConfig := &ConnConfig{
		Host:              config.Host,
		FallbackHosts:     config.FallbackHosts,
		Port:              config.Port,
		Username:          config.Username,
		Password:          config.Password,
		Database:          config.Database,
		Query:             query,
		MaxConns:          int32(maxOpenConns),
		MinConns:          int32(maxIdleConns),
		MaxConnLifetime:   config.ConnMaxLifetime,
		MaxConnIdleTime:   config.ConnMaxIdleTime,
		HealthCheckPeriod: config.HealthCheckPeriod,
	}

	db, err := NewConn(ctx, connConfig)
	if err != nil {
		return nil, err
	}

	for i := 0; i < config.Workers; i++ {
		workers[i] = &YugabyteStoreWorker{
			config:  config,
			i:       i,
			db:      db,
			sq:      sq,
			flush:   make(chan int64, 1),
			aio:     aio,
			metrics: metrics,
		}
	}
	if config.CleanupRetention > 0 && config.CleanupInterval > 0 {
		retentionMs := config.CleanupRetention.Milliseconds()
		interval := config.CleanupInterval
		go func() {
			runCleanup(db, retentionMs)
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for range ticker.C {
				runCleanup(db, retentionMs)
			}
		}()
	}

	return &YugabyteStore{
		config:  config,
		sq:      sq,
		db:      db,
		workers: workers,
	}, nil
}

// runCleanup deletes terminal-state tasks and promises older than retentionMs,
// looping per-statement until a batch returns 0 rows so the per-tick LIMIT does
// not cap throughput when the backlog is large.
func runCleanup(db *pgxpool.Pool, retentionMs int64) {
	cutoff := time.Now().UnixMilli() - retentionMs
	drain := func(name, stmt string) {
		var total int64
		for {
			tag, err := db.Exec(context.Background(), stmt, cutoff)
			if err != nil {
				slog.Warn(name+" cleanup failed", "err", err, "deleted_before_err", total)
				return
			}
			n := tag.RowsAffected()
			total += n
			if n == 0 {
				break
			}
		}
		slog.Info(name+" cleanup", "deleted", total)
	}
	drain("task", TASK_CLEANUP_STATEMENT)
	drain("promise", PROMISE_CLEANUP_STATEMENT)
}

func (s *YugabyteStore) String() string {
	return "store:yugabyte"
}

func (s *YugabyteStore) Kind() t_aio.Kind {
	return t_aio.Store
}

func (s *YugabyteStore) Start(chan<- error) error {
	ctx := context.Background()
	if _, err := s.db.Exec(ctx, CREATE_TABLE_STATEMENT); err != nil {
		return err
	}

	// stdlib.OpenDBFromPool wraps the pgxpool in a database/sql facade so the
	// existing migration store (which expects *sql.DB) can use it without changes.
	sqlDB := stdlib.OpenDBFromPool(s.db)
	defer func() { _ = sqlDB.Close() }()

	ms := migrations.NewYugabyteMigrationStore(sqlDB)

	version, err := ms.GetCurrentVersion()
	if err != nil {
		return err
	}

	// Get pending migrations
	pending, err := migrations.GetPendingMigrations(version, ms)
	if err != nil {
		return err
	}

	// If version == 0, the db is fresh and we can apply all migrations automatically
	if version == 0 {
		if len(pending) > 0 {
			// Validate migration sequence
			if err := migrations.ValidateMigrationSequence(pending, version); err != nil {
				return err
			}

			// Apply all migrations
			if err := migrations.ApplyMigrations(pending, ms); err != nil {
				return err
			}
		}
	} else {
		// For existing databases, check for pending migrations and error if any exist
		if len(pending) > 0 {
			return errors.New("pending migrations, run `resonate migrate` for more information")
		}
	}

	for _, worker := range s.workers {
		go worker.Start()
	}

	return nil
}

func (s *YugabyteStore) Stop() error {
	close(s.sq)

	if s.config.Reset {
		if err := s.Reset(); err != nil {
			return err
		}
	}

	s.db.Close()
	return nil
}

func (s *YugabyteStore) Reset() error {
	_, err := s.db.Exec(context.Background(), DROP_TABLE_STATEMENT)
	return err
}

func (s *YugabyteStore) Enqueue(sqe *bus.SQE[t_aio.Submission, t_aio.Completion]) bool {
	select {
	case s.sq <- sqe:
		return true
	default:
		return false
	}
}

func (s *YugabyteStore) Flush(t int64) {
	for _, worker := range s.workers {
		worker.Flush(t)
	}
}

func (s *YugabyteStore) Process(sqes []*bus.SQE[t_aio.Submission, t_aio.Completion]) []*bus.CQE[t_aio.Submission, t_aio.Completion] {
	util.Assert(len(s.workers) > 0, "must be at least one worker")
	return s.workers[0].Process(sqes)
}

// Worker

type YugabyteStoreWorker struct {
	config  *Config
	i       int
	db      *pgxpool.Pool
	sq      <-chan *bus.SQE[t_aio.Submission, t_aio.Completion]
	flush   chan int64
	aio     aio.AIO
	metrics *metrics.Metrics
}

func (w *YugabyteStoreWorker) String() string {
	return "store:yugabyte"
}

func (w *YugabyteStoreWorker) Start() {
	counter := w.metrics.AioWorkerInFlight.WithLabelValues(w.String(), strconv.Itoa(w.i))
	w.metrics.AioWorker.WithLabelValues(w.String()).Inc()
	defer w.metrics.AioWorker.WithLabelValues(w.String()).Dec()

	for {
		sqes, ok := store.Collect(w.sq, w.flush, w.config.BatchSize)
		if len(sqes) > 0 {
			counter.Set(float64(len(sqes)))
			for _, cqe := range w.Process(sqes) {
				w.aio.EnqueueCQE(cqe)
				counter.Dec()
			}
		}
		if !ok {
			return
		}
	}
}

func (w *YugabyteStoreWorker) Flush(t int64) {
	// ignore case where flush channel is full,
	// this means the flush is waiting on the cq
	select {
	case w.flush <- t:
	default:
	}
}

func (w *YugabyteStoreWorker) Process(sqes []*bus.SQE[t_aio.Submission, t_aio.Completion]) []*bus.CQE[t_aio.Submission, t_aio.Completion] {
	return store.Process(w, sqes)
}

func (w *YugabyteStoreWorker) Execute(transactions []*t_aio.Transaction) ([]*t_aio.StoreCompletion, error) {
	start := time.Now()

	// Outer context caps the total retry budget across all attempts.
	outerCtx, outerCancel := context.WithTimeout(context.Background(), w.config.TxTimeout)
	defer outerCancel()

	// Per-attempt budget: a single tablet-leader lookup stalled on a rejoining
	// node must not consume the entire TxTimeout. Splitting the budget ensures
	// the next attempt (which the smart driver may route to a live node) still
	// has time to run.
	attemptBudget := w.config.TxTimeout / time.Duration(w.config.MaxRetries+1)

	var lastErr error
	for attempt := 0; attempt <= w.config.MaxRetries; attempt++ {
		if attempt > 0 {
			w.metrics.YugabyteTxRetries.Inc()
			backoff := time.Duration(1<<uint(attempt-1)) * 10 * time.Millisecond
			slog.Warn("retrying transaction", "attempt", attempt, "backoff", backoff, "err", lastErr)
			select {
			case <-time.After(backoff):
			case <-outerCtx.Done():
				w.metrics.YugabyteErrorsTotal.WithLabelValues(classifyError(outerCtx.Err())).Inc()
				w.metrics.YugabyteTxTotal.WithLabelValues("failed").Inc()
				w.metrics.YugabyteTxDuration.Observe(time.Since(start).Seconds())
				return nil, fmt.Errorf("tx retry cancelled: %w (last error: %v)", outerCtx.Err(), lastErr)
			}
		}

		attemptCtx, attemptCancel := context.WithTimeout(outerCtx, attemptBudget)
		results, err := w.executeOnce(attemptCtx, transactions)
		attemptCancel()
		if err == nil {
			w.metrics.YugabyteTxTotal.WithLabelValues("ok").Inc()
			w.metrics.YugabyteTxDuration.Observe(time.Since(start).Seconds())
			w.updatePoolStats()
			return results, nil
		}
		lastErr = err
		w.metrics.YugabyteErrorsTotal.WithLabelValues(classifyError(err)).Inc()

		if !isRetryableError(err) {
			w.metrics.YugabyteTxTotal.WithLabelValues("failed").Inc()
			w.metrics.YugabyteTxDuration.Observe(time.Since(start).Seconds())
			return nil, err
		}

		if outerCtx.Err() != nil {
			break
		}
	}

	w.metrics.YugabyteTxTotal.WithLabelValues("failed").Inc()
	w.metrics.YugabyteTxDuration.Observe(time.Since(start).Seconds())
	return nil, fmt.Errorf("tx failed after %d retries: %w", w.config.MaxRetries, lastErr)
}

func (w *YugabyteStoreWorker) updatePoolStats() {
	stat := w.db.Stat()
	w.metrics.YugabytePoolConns.WithLabelValues("acquired").Set(float64(stat.AcquiredConns()))
	w.metrics.YugabytePoolConns.WithLabelValues("idle").Set(float64(stat.IdleConns()))
	w.metrics.YugabytePoolConns.WithLabelValues("total").Set(float64(stat.TotalConns()))
	w.metrics.YugabytePoolConns.WithLabelValues("constructing").Set(float64(stat.ConstructingConns()))
}

func (w *YugabyteStoreWorker) executeOnce(ctx context.Context, transactions []*t_aio.Transaction) ([]*t_aio.StoreCompletion, error) {
	tx, err := w.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, err
	}

	results, err := w.performCommands(ctx, tx, transactions)
	if err != nil {
		rbCtx, rbCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		if rbErr := tx.Rollback(rbCtx); rbErr != nil {
			err = fmt.Errorf("tx failed: %w, unable to rollback: %v", err, rbErr)
		}
		rbCancel()
		return nil, err
	}

	return results, tx.Commit(ctx)
}

func isRetryableError(err error) bool {
	// Per-attempt context deadline: the attempt-scoped timeout fired (e.g. while
	// waiting for a tablet leader on a dead node).  The outer Execute() context
	// still has budget, so retrying with a fresh attempt context is safe — the
	// smart driver may route the next connection to a live node.
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	// Network-level errors: the connection to a YugaByte node died before or
	// during the transaction (hard kill, ECONNRESET, EOF).  The smart driver
	// will route the next attempt to a live node, so retrying is safe.
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var netErr *net.OpError
	if errors.As(err, &netErr) {
		return true
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "40001": // serialization_failure
			return true
		case "40P01": // deadlock_detected
			return true
		case "XX000": // internal_error — YugabyteDB during Raft leader election
			return true
		case "08006": // connection_failure — node went down mid-transaction
			return true
		case "57P01": // admin_shutdown — node restart/rolling upgrade
			return true
		case "57014": // query_canceled — statement_timeout fired because a tablet
			return true
		}
	}
	return false
}

// classifyError maps a YugaByte/pgx error to a human-readable label for Prometheus.
func classifyError(err error) string {
	if err == nil {
		return ""
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return "context_deadline_exceeded"
	}
	if errors.Is(err, context.Canceled) {
		return "context_canceled"
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "40001":
			return "serialization_failure"
		case "40P01":
			return "deadlock_detected"
		case "XX000":
			return "raft_leader_election"
		case "08006":
			return "connection_failure"
		case "57P01":
			return "admin_shutdown"
		case "57014":
			return "statement_timeout"
		case "08001":
			return "connection_rejected"
		case "08004":
			return "connection_rejected_sqlclient"
		case "23505":
			return "unique_violation"
		case "23503":
			return "foreign_key_violation"
		default:
			return "pg_" + pgErr.Code
		}
	}

	// Network-level errors — happen before a PgError is ever assigned.
	// These are the dominant error type during a YugaByte node kill.
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return "connection_eof"
	}
	var netErr *net.OpError
	if errors.As(err, &netErr) {
		if errors.Is(netErr.Err, syscall.ECONNRESET) {
			return "connection_reset"
		}
		if errors.Is(netErr.Err, syscall.ECONNREFUSED) {
			return "connection_refused"
		}
		if errors.Is(netErr.Err, syscall.ECONNABORTED) {
			return "connection_aborted"
		}
		return "net_op_error"
	}

	return "unknown"
}

func (w *YugabyteStoreWorker) performCommands(ctx context.Context, tx pgx.Tx, transactions []*t_aio.Transaction) ([]*t_aio.StoreCompletion, error) {
	completions := make([]*t_aio.StoreCompletion, len(transactions))
	var batch pgx.Batch
	cmdCounts := map[string]int{}
	trackCmd := func(name string) {
		w.metrics.YugabyteCommandTotal.WithLabelValues(name).Inc()
		cmdCounts[name]++
	}

	for i, transaction := range transactions {
		util.Assert(len(transaction.Commands) > 0, "expected a command")

		results := make([]t_aio.Result, len(transaction.Commands))
		completions[i] = &t_aio.StoreCompletion{Valid: true, Results: results}

		// fenceFailed is set to true by the fencing callback when the token is
		// invalid; all subsequent command callbacks for this transaction check it
		// and short-circuit so that their results[j] slots are left nil.
		fenceFailed := false

		if transaction.Fence != nil {
			capturedI := i
			flagPtr := &fenceFailed
			qq := batch.Queue(TASK_VALIDATE_STATEMENT, transaction.Fence.TaskId, transaction.Fence.TaskCounter)
			qq.QueryRow(func(row pgx.Row) error {
				var rowCount int64
				if err := row.Scan(&rowCount); err != nil {
					return store.StoreErr(err)
				}
				util.Assert(rowCount == 1 || rowCount == 0, "must be zero or one")
				if rowCount != 1 {
					*flagPtr = true
					completions[capturedI] = &t_aio.StoreCompletion{Valid: false}
				}
				return nil
			})
		}

		for j, command := range transaction.Commands {
			flagPtr := &fenceFailed

			switch v := command.(type) {
			// Promises
			case *t_aio.ReadPromiseCommand:
				trackCmd("read_promise")
				w.readPromise(&batch, results, j, flagPtr, v)

			case *t_aio.ReadPromisesCommand:
				trackCmd("read_promises")
				w.readPromises(&batch, results, j, flagPtr, v)

			case *t_aio.SearchPromisesCommand:
				trackCmd("search_promises")
				if err := w.searchPromises(&batch, results, j, flagPtr, v); err != nil {
					return nil, err
				}

			case *t_aio.CreatePromiseCommand:
				trackCmd("create_promise")
				if err := w.createPromise(&batch, results, j, flagPtr, v); err != nil {
					return nil, err
				}

			case *t_aio.UpdatePromiseCommand:
				trackCmd("update_promise")
				if err := w.updatePromise(&batch, results, j, flagPtr, v); err != nil {
					return nil, err
				}

			// Callbacks
			case *t_aio.CreateCallbackCommand:
				trackCmd("create_callback")
				if err := w.createCallback(&batch, results, j, flagPtr, v); err != nil {
					return nil, err
				}

			case *t_aio.DeleteCallbacksCommand:
				trackCmd("delete_callbacks")
				w.deleteCallbacks(&batch, results, j, flagPtr, v)

			// Schedules
			case *t_aio.ReadScheduleCommand:
				trackCmd("read_schedule")
				w.readSchedule(&batch, results, j, flagPtr, v)

			case *t_aio.ReadSchedulesCommand:
				trackCmd("read_schedules")
				w.readSchedules(&batch, results, j, flagPtr, v)

			case *t_aio.SearchSchedulesCommand:
				trackCmd("search_schedules")
				if err := w.searchSchedules(&batch, results, j, flagPtr, v); err != nil {
					return nil, err
				}

			case *t_aio.CreateScheduleCommand:
				trackCmd("create_schedule")
				if err := w.createSchedule(&batch, results, j, flagPtr, v); err != nil {
					return nil, err
				}

			case *t_aio.UpdateScheduleCommand:
				trackCmd("update_schedule")
				w.updateSchedule(&batch, results, j, flagPtr, v)

			case *t_aio.DeleteScheduleCommand:
				trackCmd("delete_schedule")
				w.deleteSchedule(&batch, results, j, flagPtr, v)

			// Tasks
			case *t_aio.ReadTaskCommand:
				trackCmd("read_task")
				w.readTask(&batch, results, j, flagPtr, v)

			case *t_aio.ReadTasksCommand:
				trackCmd("read_tasks")
				w.readTasks(&batch, results, j, flagPtr, v)

			case *t_aio.ReadEnqueueableTasksCommand:
				trackCmd("read_enqueueable_tasks")
				w.readEnqueueableTasks(&batch, results, j, flagPtr, v)

			case *t_aio.CreateTaskCommand:
				trackCmd("create_task")
				if err := w.createTask(&batch, results, j, flagPtr, v); err != nil {
					return nil, err
				}

			case *t_aio.CreateTasksCommand:
				trackCmd("create_tasks")
				w.createTasks(&batch, results, j, flagPtr, v)

			case *t_aio.UpdateTaskCommand:
				trackCmd("update_task")
				w.updateTask(&batch, results, j, flagPtr, v)

			case *t_aio.CompleteTasksCommand:
				trackCmd("complete_tasks")
				w.completeTasks(&batch, results, j, flagPtr, v)

			case *t_aio.HeartbeatTasksCommand:
				trackCmd("heartbeat_tasks")
				w.heartbeatTasks(&batch, results, j, flagPtr, v)

			case *t_aio.CreatePromiseAndTaskCommand:
				trackCmd("create_promise_and_task")
				if err := w.createPromiseAndTask(&batch, results, j, flagPtr, v); err != nil {
					return nil, err
				}

			default:
				panic(fmt.Sprintf("invalid command: %s", command.String()))
			}
		}
	}

	if batch.Len() == 0 {
		return completions, nil
	}

	dominant := dominantCommand(cmdCounts)
	batchStart := time.Now()
	br := tx.SendBatch(ctx, &batch)
	err := br.Close()
	w.metrics.YugabyteBatchDuration.WithLabelValues(dominant).Observe(time.Since(batchStart).Seconds())
	return completions, err
}

// dominantCommand returns the command type that appeared most often in the batch.
// When multiple types tie, the winner is arbitrary but consistent within a call.
func dominantCommand(counts map[string]int) string {
	best, maxN := "mixed", 0
	for cmd, n := range counts {
		if n > maxN {
			best, maxN = cmd, n
		}
	}
	return best
}

// Promises

func (w *YugabyteStoreWorker) readPromise(batch *pgx.Batch, results []t_aio.Result, j int, flagPtr *bool, cmd *t_aio.ReadPromiseCommand) {
	qq := batch.Queue(PROMISE_SELECT_STATEMENT, cmd.Id)
	qq.QueryRow(func(row pgx.Row) error {
		if *flagPtr {
			return nil
		}
		record := &promise.PromiseRecord{}
		rowsReturned := int64(1)
		if err := row.Scan(
			&record.Id,
			&record.State,
			&record.ParamHeaders,
			&record.ParamData,
			&record.ValueHeaders,
			&record.ValueData,
			&record.Timeout,
			&record.Tags,
			&record.CreatedOn,
			&record.CompletedOn,
		); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				rowsReturned = 0
			} else {
				return err
			}
		}
		var records []*promise.PromiseRecord
		if rowsReturned == 1 {
			records = append(records, record)
		}
		results[j] = &t_aio.QueryPromisesResult{
			RowsReturned: rowsReturned,
			Records:      records,
		}
		return nil
	})
}

func (w *YugabyteStoreWorker) readPromises(batch *pgx.Batch, results []t_aio.Result, j int, flagPtr *bool, cmd *t_aio.ReadPromisesCommand) {
	qq := batch.Queue(PROMISE_SELECT_ALL_STATEMENT, cmd.Time, cmd.Limit)
	qq.Query(func(rows pgx.Rows) error {
		if *flagPtr {
			return nil
		}
		rowsReturned := int64(0)
		var records []*promise.PromiseRecord
		var lastSortId int64
		for rows.Next() {
			record := &promise.PromiseRecord{}
			if err := rows.Scan(
				&record.Id,
				&record.State,
				&record.ParamHeaders,
				&record.ParamData,
				&record.ValueHeaders,
				&record.ValueData,
				&record.Timeout,
				&record.Tags,
				&record.CreatedOn,
				&record.CompletedOn,
				&record.SortId,
			); err != nil {
				return err
			}
			records = append(records, record)
			lastSortId = record.SortId
			rowsReturned++
		}
		results[j] = &t_aio.QueryPromisesResult{
			RowsReturned: rowsReturned,
			LastSortId:   lastSortId,
			Records:      records,
		}
		return nil
	})
}

func (w *YugabyteStoreWorker) searchPromises(batch *pgx.Batch, results []t_aio.Result, j int, flagPtr *bool, cmd *t_aio.SearchPromisesCommand) error {
	util.Assert(cmd.Id != "", "query cannot be empty")
	util.Assert(cmd.States != nil, "states cannot be empty")
	util.Assert(cmd.Tags != nil, "tags cannot be empty")

	id := strings.ReplaceAll(cmd.Id, "*", "%")
	mask := 0
	for _, state := range cmd.States {
		mask = mask | int(state)
	}
	var tags *string
	if len(cmd.Tags) > 0 {
		t, err := json.Marshal(cmd.Tags)
		if err != nil {
			return err
		}
		tags = util.ToPointer(string(t))
	}
	qq := batch.Queue(PROMISE_SEARCH_STATEMENT, cmd.SortId, id, mask, tags, cmd.Limit)
	qq.Query(func(rows pgx.Rows) error {
		if *flagPtr {
			return nil
		}
		rowsReturned := int64(0)
		var records []*promise.PromiseRecord
		var lastSortId int64
		for rows.Next() {
			record := &promise.PromiseRecord{}
			if err := rows.Scan(
				&record.Id,
				&record.State,
				&record.ParamHeaders,
				&record.ParamData,
				&record.ValueHeaders,
				&record.ValueData,
				&record.Timeout,
				&record.Tags,
				&record.CreatedOn,
				&record.CompletedOn,
				&record.SortId,
			); err != nil {
				return err
			}
			records = append(records, record)
			lastSortId = record.SortId
			rowsReturned++
		}
		results[j] = &t_aio.QueryPromisesResult{
			RowsReturned: rowsReturned,
			LastSortId:   lastSortId,
			Records:      records,
		}
		return nil
	})
	return nil
}

func (w *YugabyteStoreWorker) createPromise(batch *pgx.Batch, results []t_aio.Result, j int, flagPtr *bool, cmd *t_aio.CreatePromiseCommand) error {
	util.Assert(cmd.State.In(promise.Pending|promise.Resolved|promise.Timedout), "init state must be one of pending, resolved, timedout")
	util.Assert(cmd.Param.Headers != nil, "param headers must not be nil")
	util.Assert(cmd.Param.Data != nil, "param data must not be nil")
	util.Assert(cmd.Tags != nil, "tags must not be nil")

	headers, err := json.Marshal(cmd.Param.Headers)
	if err != nil {
		return err
	}
	tags, err := json.Marshal(cmd.Tags)
	if err != nil {
		return err
	}
	qq := batch.Queue(PROMISE_INSERT_STATEMENT, cmd.Id, cmd.State, headers, cmd.Param.Data, cmd.Timeout, cmd.Id, tags, cmd.CreatedOn)
	qq.Exec(func(ct pgconn.CommandTag) error {
		if *flagPtr {
			return nil
		}
		results[j] = &t_aio.AlterPromisesResult{RowsAffected: ct.RowsAffected()}
		return nil
	})
	return nil
}

func (w *YugabyteStoreWorker) updatePromise(batch *pgx.Batch, results []t_aio.Result, j int, flagPtr *bool, cmd *t_aio.UpdatePromiseCommand) error {
	util.Assert(cmd.State.In(promise.Resolved|promise.Rejected|promise.Canceled|promise.Timedout), "state must be canceled, resolved, rejected, or timedout")
	util.Assert(cmd.Value.Headers != nil, "value headers must not be nil")
	util.Assert(cmd.Value.Data != nil, "value data must not be nil")

	headers, err := json.Marshal(cmd.Value.Headers)
	if err != nil {
		return err
	}
	qq := batch.Queue(PROMISE_UPDATE_STATEMENT, cmd.State, headers, cmd.Value.Data, cmd.Id, cmd.CompletedOn, cmd.Id)
	qq.Exec(func(ct pgconn.CommandTag) error {
		if *flagPtr {
			return nil
		}
		results[j] = &t_aio.AlterPromisesResult{RowsAffected: ct.RowsAffected()}
		return nil
	})
	return nil
}

// Callbacks

func (w *YugabyteStoreWorker) createCallback(batch *pgx.Batch, results []t_aio.Result, j int, flagPtr *bool, cmd *t_aio.CreateCallbackCommand) error {
	util.Assert(cmd.Recv != nil, "recv must not be nil")
	util.Assert(cmd.Mesg != nil, "mesg must not be nil")

	mesg, err := json.Marshal(cmd.Mesg)
	if err != nil {
		return err
	}
	qq := batch.Queue(CALLBACK_INSERT_STATEMENT, cmd.Id, cmd.PromiseId, cmd.Mesg.Root, cmd.Recv, mesg, cmd.Timeout, cmd.CreatedOn)
	qq.Exec(func(ct pgconn.CommandTag) error {
		if *flagPtr {
			return nil
		}
		results[j] = &t_aio.AlterCallbacksResult{RowsAffected: ct.RowsAffected()}
		return nil
	})
	return nil
}

func (w *YugabyteStoreWorker) deleteCallbacks(batch *pgx.Batch, results []t_aio.Result, j int, flagPtr *bool, cmd *t_aio.DeleteCallbacksCommand) {
	qq := batch.Queue(CALLBACK_DELETE_STATEMENT, cmd.PromiseId)
	qq.Exec(func(ct pgconn.CommandTag) error {
		if *flagPtr {
			return nil
		}
		results[j] = &t_aio.AlterCallbacksResult{RowsAffected: ct.RowsAffected()}
		return nil
	})
}

// Schedules

func (w *YugabyteStoreWorker) readSchedule(batch *pgx.Batch, results []t_aio.Result, j int, flagPtr *bool, cmd *t_aio.ReadScheduleCommand) {
	qq := batch.Queue(SCHEDULE_SELECT_STATEMENT, cmd.Id)
	qq.QueryRow(func(row pgx.Row) error {
		if *flagPtr {
			return nil
		}
		record := &schedule.ScheduleRecord{}
		rowsReturned := int64(1)
		if err := row.Scan(
			&record.Id,
			&record.Description,
			&record.Cron,
			&record.Tags,
			&record.PromiseId,
			&record.PromiseTimeout,
			&record.PromiseParamHeaders,
			&record.PromiseParamData,
			&record.PromiseTags,
			&record.LastRunTime,
			&record.NextRunTime,
			&record.CreatedOn,
		); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				rowsReturned = 0
			} else {
				return err
			}
		}
		var records []*schedule.ScheduleRecord
		if rowsReturned == 1 {
			records = append(records, record)
		}
		results[j] = &t_aio.QuerySchedulesResult{
			RowsReturned: rowsReturned,
			Records:      records,
		}
		return nil
	})
}

func (w *YugabyteStoreWorker) readSchedules(batch *pgx.Batch, results []t_aio.Result, j int, flagPtr *bool, cmd *t_aio.ReadSchedulesCommand) {
	qq := batch.Queue(SCHEDULE_SELECT_ALL_STATEMENT, cmd.NextRunTime, cmd.Limit)
	qq.Query(func(rows pgx.Rows) error {
		if *flagPtr {
			return nil
		}
		rowsReturned := int64(0)
		var records []*schedule.ScheduleRecord
		for rows.Next() {
			record := &schedule.ScheduleRecord{}
			if err := rows.Scan(
				&record.Id,
				&record.Cron,
				&record.PromiseId,
				&record.PromiseTimeout,
				&record.PromiseParamHeaders,
				&record.PromiseParamData,
				&record.PromiseTags,
				&record.LastRunTime,
				&record.NextRunTime,
			); err != nil {
				return err
			}
			records = append(records, record)
			rowsReturned++
		}
		results[j] = &t_aio.QuerySchedulesResult{
			RowsReturned: rowsReturned,
			Records:      records,
		}
		return nil
	})
}

func (w *YugabyteStoreWorker) searchSchedules(batch *pgx.Batch, results []t_aio.Result, j int, flagPtr *bool, cmd *t_aio.SearchSchedulesCommand) error {
	util.Assert(cmd.Id != "", "query cannot be empty")
	util.Assert(cmd.Tags != nil, "tags cannot be empty")

	id := strings.ReplaceAll(cmd.Id, "*", "%")
	var tags *string
	if len(cmd.Tags) > 0 {
		t, err := json.Marshal(cmd.Tags)
		if err != nil {
			return err
		}
		tags = util.ToPointer(string(t))
	}
	qq := batch.Queue(SCHEDULE_SEARCH_STATEMENT, cmd.SortId, id, tags, cmd.Limit)
	qq.Query(func(rows pgx.Rows) error {
		if *flagPtr {
			return nil
		}
		rowsReturned := int64(0)
		var records []*schedule.ScheduleRecord
		var lastSortId int64
		for rows.Next() {
			record := &schedule.ScheduleRecord{}
			if err := rows.Scan(
				&record.Id,
				&record.Cron,
				&record.Tags,
				&record.LastRunTime,
				&record.NextRunTime,
				&record.CreatedOn,
				&record.SortId,
			); err != nil {
				return err
			}
			records = append(records, record)
			lastSortId = record.SortId
			rowsReturned++
		}
		results[j] = &t_aio.QuerySchedulesResult{
			RowsReturned: rowsReturned,
			LastSortId:   lastSortId,
			Records:      records,
		}
		return nil
	})
	return nil
}

func (w *YugabyteStoreWorker) createSchedule(batch *pgx.Batch, results []t_aio.Result, j int, flagPtr *bool, cmd *t_aio.CreateScheduleCommand) error {
	tags, err := json.Marshal(cmd.Tags)
	if err != nil {
		return err
	}
	promiseParamHeaders, err := json.Marshal(cmd.PromiseParam.Headers)
	if err != nil {
		return err
	}
	promiseTags, err := json.Marshal(cmd.PromiseTags)
	if err != nil {
		return err
	}
	qq := batch.Queue(SCHEDULE_INSERT_STATEMENT,
		cmd.Id, cmd.Description, cmd.Cron, tags,
		cmd.PromiseId, cmd.PromiseTimeout, promiseParamHeaders, cmd.PromiseParam.Data, promiseTags,
		cmd.NextRunTime, cmd.Id, cmd.CreatedOn,
	)
	qq.Exec(func(ct pgconn.CommandTag) error {
		if *flagPtr {
			return nil
		}
		results[j] = &t_aio.AlterSchedulesResult{RowsAffected: ct.RowsAffected()}
		return nil
	})
	return nil
}

func (w *YugabyteStoreWorker) updateSchedule(batch *pgx.Batch, results []t_aio.Result, j int, flagPtr *bool, cmd *t_aio.UpdateScheduleCommand) {
	qq := batch.Queue(SCHEDULE_UPDATE_STATEMENT, cmd.NextRunTime, cmd.Id, cmd.LastRunTime)
	qq.Exec(func(ct pgconn.CommandTag) error {
		if *flagPtr {
			return nil
		}
		results[j] = &t_aio.AlterSchedulesResult{RowsAffected: ct.RowsAffected()}
		return nil
	})
}

func (w *YugabyteStoreWorker) deleteSchedule(batch *pgx.Batch, results []t_aio.Result, j int, flagPtr *bool, cmd *t_aio.DeleteScheduleCommand) {
	qq := batch.Queue(SCHEDULE_DELETE_STATEMENT, cmd.Id)
	qq.Exec(func(ct pgconn.CommandTag) error {
		if *flagPtr {
			return nil
		}
		results[j] = &t_aio.AlterSchedulesResult{RowsAffected: ct.RowsAffected()}
		return nil
	})
}

// Tasks

func (w *YugabyteStoreWorker) readTask(batch *pgx.Batch, results []t_aio.Result, j int, flagPtr *bool, cmd *t_aio.ReadTaskCommand) {
	qq := batch.Queue(TASK_SELECT_STATEMENT, cmd.Id)
	qq.QueryRow(func(row pgx.Row) error {
		if *flagPtr {
			return nil
		}
		record := &task.TaskRecord{}
		rowsReturned := int64(1)
		if err := row.Scan(
			&record.Id,
			&record.ProcessId,
			&record.State,
			&record.RootPromiseId,
			&record.Recv,
			&record.Mesg,
			&record.Timeout,
			&record.Counter,
			&record.Attempt,
			&record.Ttl,
			&record.ExpiresAt,
			&record.CreatedOn,
			&record.CompletedOn,
		); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				rowsReturned = 0
			} else {
				return store.StoreErr(err)
			}
		}
		var records []*task.TaskRecord
		if rowsReturned == 1 {
			records = append(records, record)
		}
		results[j] = &t_aio.QueryTasksResult{
			RowsReturned: rowsReturned,
			Records:      records,
		}
		return nil
	})
}

func (w *YugabyteStoreWorker) readTasks(batch *pgx.Batch, results []t_aio.Result, j int, flagPtr *bool, cmd *t_aio.ReadTasksCommand) {
	util.Assert(len(cmd.States) > 0, "must provide at least one state")

	var states task.State
	for _, state := range cmd.States {
		states |= state
	}
	qq := batch.Queue(TASK_SELECT_ALL_STATEMENT, states, cmd.Time, cmd.Limit)
	qq.Query(func(rows pgx.Rows) error {
		if *flagPtr {
			return nil
		}
		rowsReturned := int64(0)
		var records []*task.TaskRecord
		for rows.Next() {
			record := &task.TaskRecord{}
			if err := rows.Scan(
				&record.Id,
				&record.ProcessId,
				&record.State,
				&record.RootPromiseId,
				&record.Recv,
				&record.Mesg,
				&record.Timeout,
				&record.Counter,
				&record.Attempt,
				&record.Ttl,
				&record.ExpiresAt,
				&record.CreatedOn,
				&record.CompletedOn,
			); err != nil {
				return store.StoreErr(err)
			}
			records = append(records, record)
			rowsReturned++
		}
		results[j] = &t_aio.QueryTasksResult{
			RowsReturned: rowsReturned,
			Records:      records,
		}
		return nil
	})
}

func (w *YugabyteStoreWorker) readEnqueueableTasks(batch *pgx.Batch, results []t_aio.Result, j int, flagPtr *bool, cmd *t_aio.ReadEnqueueableTasksCommand) {
	qq := batch.Queue(TASK_SELECT_ENQUEUEABLE_STATEMENT, cmd.Time, cmd.Limit)
	qq.Query(func(rows pgx.Rows) error {
		if *flagPtr {
			return nil
		}
		rowsReturned := int64(0)
		var records []*task.TaskRecord
		for rows.Next() {
			record := &task.TaskRecord{}
			if err := rows.Scan(
				&record.Id,
				&record.ProcessId,
				&record.State,
				&record.RootPromiseId,
				&record.Recv,
				&record.Mesg,
				&record.Timeout,
				&record.Counter,
				&record.Attempt,
				&record.Ttl,
				&record.ExpiresAt,
				&record.CreatedOn,
				&record.CompletedOn,
			); err != nil {
				return store.StoreErr(err)
			}
			records = append(records, record)
			rowsReturned++
		}
		results[j] = &t_aio.QueryTasksResult{
			RowsReturned: rowsReturned,
			Records:      records,
		}
		return nil
	})
}

func (w *YugabyteStoreWorker) createTask(batch *pgx.Batch, results []t_aio.Result, j int, flagPtr *bool, cmd *t_aio.CreateTaskCommand) error {
	util.Assert(cmd.Recv != nil, "recv must not be nil")
	util.Assert(cmd.Mesg != nil, "mesg must not be nil")
	util.Assert(cmd.State.In(task.Init|task.Claimed), "state must be init or claimed")
	util.Assert(cmd.State != task.Claimed || cmd.ProcessId != nil, "process id must be set if state is claimed")

	mesg, err := json.Marshal(cmd.Mesg)
	if err != nil {
		return store.StoreErr(err)
	}
	qq := batch.Queue(TASK_INSERT_STATEMENT, cmd.Id, cmd.Recv, mesg, cmd.Timeout, cmd.ProcessId, cmd.State, cmd.Mesg.Root, cmd.Ttl, cmd.ExpiresAt, cmd.CreatedOn)
	qq.Exec(func(ct pgconn.CommandTag) error {
		if *flagPtr {
			return nil
		}
		results[j] = &t_aio.AlterTasksResult{RowsAffected: ct.RowsAffected()}
		return nil
	})
	return nil
}

func (w *YugabyteStoreWorker) createTasks(batch *pgx.Batch, results []t_aio.Result, j int, flagPtr *bool, cmd *t_aio.CreateTasksCommand) {
	qq := batch.Queue(TASK_INSERT_ALL_STATEMENT, cmd.CreatedOn, cmd.PromiseId)
	qq.Exec(func(ct pgconn.CommandTag) error {
		if *flagPtr {
			return nil
		}
		results[j] = &t_aio.AlterTasksResult{RowsAffected: ct.RowsAffected()}
		return nil
	})
}

func (w *YugabyteStoreWorker) updateTask(batch *pgx.Batch, results []t_aio.Result, j int, flagPtr *bool, cmd *t_aio.UpdateTaskCommand) {
	util.Assert(len(cmd.CurrentStates) > 0, "must provide at least one current state")

	var currentStates task.State
	for _, state := range cmd.CurrentStates {
		currentStates |= state
	}
	qq := batch.Queue(TASK_UPDATE_STATEMENT,
		cmd.ProcessId, cmd.State, cmd.Counter, cmd.Attempt, cmd.Ttl, cmd.ExpiresAt, cmd.CompletedOn,
		cmd.Id, currentStates, cmd.CurrentCounter,
	)
	qq.Exec(func(ct pgconn.CommandTag) error {
		if *flagPtr {
			return nil
		}
		results[j] = &t_aio.AlterTasksResult{RowsAffected: ct.RowsAffected()}
		return nil
	})
}

func (w *YugabyteStoreWorker) completeTasks(batch *pgx.Batch, results []t_aio.Result, j int, flagPtr *bool, cmd *t_aio.CompleteTasksCommand) {
	qq := batch.Queue(TASK_COMPLETE_BY_ROOT_ID_STATEMENT, cmd.CompletedOn, cmd.RootPromiseId)
	qq.Exec(func(ct pgconn.CommandTag) error {
		if *flagPtr {
			return nil
		}
		results[j] = &t_aio.AlterTasksResult{RowsAffected: ct.RowsAffected()}
		return nil
	})
}

func (w *YugabyteStoreWorker) heartbeatTasks(batch *pgx.Batch, results []t_aio.Result, j int, flagPtr *bool, cmd *t_aio.HeartbeatTasksCommand) {
	qq := batch.Queue(TASK_HEARTBEAT_STATEMENT, cmd.Time, cmd.ProcessId)
	qq.Exec(func(ct pgconn.CommandTag) error {
		if *flagPtr {
			return nil
		}
		results[j] = &t_aio.AlterTasksResult{RowsAffected: ct.RowsAffected()}
		return nil
	})
}

func (w *YugabyteStoreWorker) createPromiseAndTask(batch *pgx.Batch, results []t_aio.Result, j int, flagPtr *bool, cmd *t_aio.CreatePromiseAndTaskCommand) error {
	pc := cmd.PromiseCommand
	tc := cmd.TaskCommand
	util.Assert(pc.State.In(promise.Pending|promise.Resolved|promise.Timedout), "init state must be one of pending, resolved, timedout")
	util.Assert(pc.Param.Headers != nil, "param headers must not be nil")
	util.Assert(pc.Param.Data != nil, "param data must not be nil")
	util.Assert(pc.Tags != nil, "tags must not be nil")
	util.Assert(tc.Recv != nil, "recv must not be nil")
	util.Assert(tc.Mesg != nil, "mesg must not be nil")
	util.Assert(tc.State.In(task.Init|task.Claimed), "state must be init or claimed")
	util.Assert(tc.State != task.Claimed || tc.ProcessId != nil, "process id must be set if state is claimed")

	headers, err := json.Marshal(pc.Param.Headers)
	if err != nil {
		return err
	}
	ptags, err := json.Marshal(pc.Tags)
	if err != nil {
		return err
	}
	mesg, err := json.Marshal(tc.Mesg)
	if err != nil {
		return err
	}

	// Both statements are always queued. TASK_INSERT_STATEMENT uses
	// ON CONFLICT DO NOTHING so it is safe to run even when the promise
	// insert is a no-op. The promise callback captures promiseRowsAffected
	// which the task callback uses to build the final result.
	var promiseRowsAffected int64
	qq1 := batch.Queue(PROMISE_INSERT_STATEMENT, pc.Id, pc.State, headers, pc.Param.Data, pc.Timeout, pc.Id, ptags, pc.CreatedOn)
	qq1.Exec(func(ct pgconn.CommandTag) error {
		if *flagPtr {
			return nil
		}
		promiseRowsAffected = ct.RowsAffected()
		return nil
	})
	qq2 := batch.Queue(TASK_INSERT_STATEMENT, tc.Id, tc.Recv, mesg, tc.Timeout, tc.ProcessId, tc.State, tc.Mesg.Root, tc.Ttl, tc.ExpiresAt, tc.CreatedOn)
	qq2.Exec(func(ct pgconn.CommandTag) error {
		if *flagPtr {
			return nil
		}
		results[j] = &t_aio.AlterPromisesResult{RowsAffected: promiseRowsAffected}
		return nil
	})
	return nil
}
