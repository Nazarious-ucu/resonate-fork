package yugabyte

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand" // nosemgrep
	"net/url"
	"strconv"
	"strings"
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
	SELECT GROUP BY (root_promise_id)
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
		id`

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
)

// Config

type Config struct {
	Size              int               `flag:"size" desc:"submission buffered channel size" default:"1000"`
	BatchSize         int               `flag:"batch-size" desc:"max submissions processed per iteration" default:"1000"`
	Workers           int               `flag:"workers" desc:"number of workers" default:"1" dst:"1"`
	MaxOpenConns      int               `flag:"max-open-conns" desc:"maximum number of open database connections (0 defaults to workers)" default:"0"`
	MaxIdleConns      int               `flag:"max-idle-conns" desc:"maximum number of idle database connections (0 defaults to workers)" default:"0"`
	ConnMaxIdleTime   time.Duration     `flag:"conn-max-idle-time" desc:"maximum amount of time a connection may be idle" default:"10s"`
	ConnMaxLifetime   time.Duration     `flag:"conn-max-lifetime" desc:"maximum amount of time a connection may be reused" default:"30s"`
	HealthCheckPeriod time.Duration     `flag:"health-check-period" desc:"how often pgxpool pings idle connections to detect failures" default:"5s"`
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

	// Build the host list: primary host + any fallback hosts, all on the same
	// port.  Multiple hosts in the DSN allow the pgx smart driver to fall over
	// to a live node when the primary contact node is unavailable, so the
	// connection pool survives a primary-node kill without returning errors.
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

	// Validate connections before handing them to workers so dead conns are
	// evicted immediately rather than surfacing as errors inside a transaction.
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
	// Without this, when a YugaByte node disappears mid-graceful-shutdown the
	// TCP SYN or in-flight query can hang until the OS TCP timeout fires (>60s),
	// causing Resonate's HTTP handlers to time out and return network errors to
	// callers.  2 seconds is enough for the smart driver to detect failure and
	// the pool to open a replacement connection to a live node.
	if _, set := query["connect_timeout"]; !set {
		query["connect_timeout"] = "2"
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

	return &YugabyteStore{
		config:  config,
		sq:      sq,
		db:      db,
		workers: workers,
	}, nil
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
	ctx, cancel := context.WithTimeout(context.Background(), w.config.TxTimeout)
	defer cancel()

	var lastErr error
	for attempt := 0; attempt <= w.config.MaxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(1<<uint(attempt-1)) * 10 * time.Millisecond
			slog.Warn("retrying transaction", "attempt", attempt, "backoff", backoff, "err", lastErr)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return nil, fmt.Errorf("tx retry cancelled: %w (last error: %v)", ctx.Err(), lastErr)
			}
		}

		results, err := w.executeOnce(ctx, transactions)
		if err == nil {
			return results, nil
		}
		lastErr = err

		if !isRetryableError(err) {
			return nil, err
		}
	}

	return nil, fmt.Errorf("tx failed after %d retries: %w", w.config.MaxRetries, lastErr)
}

func (w *YugabyteStoreWorker) executeOnce(ctx context.Context, transactions []*t_aio.Transaction) ([]*t_aio.StoreCompletion, error) {
	tx, err := w.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, err
	}

	results, err := w.performCommands(ctx, tx, transactions)
	if err != nil {
		if rbErr := tx.Rollback(ctx); rbErr != nil {
			err = fmt.Errorf("tx failed: %v, unable to rollback: %v", err, rbErr)
		}
		return nil, err
	}

	return results, tx.Commit(ctx)
}

func isRetryableError(err error) bool {
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
		}
	}
	return false
}

func (w *YugabyteStoreWorker) performCommands(ctx context.Context, tx pgx.Tx, transactions []*t_aio.Transaction) ([]*t_aio.StoreCompletion, error) {
	completions := make([]*t_aio.StoreCompletion, len(transactions))

	for i, transaction := range transactions {
		util.Assert(len(transaction.Commands) > 0, "expected a command")

		valid, err := w.validFencingToken(ctx, tx, transaction)
		if err != nil {
			return nil, err
		}

		if !valid {
			completions[i] = &t_aio.StoreCompletion{
				Valid: false,
			}
			continue
		}
		results := make([]t_aio.Result, len(transaction.Commands))

		completions[i] = &t_aio.StoreCompletion{
			Valid:   true,
			Results: results,
		}

		for j, command := range transaction.Commands {
			var err error

			switch v := command.(type) {
			// Promises
			case *t_aio.ReadPromiseCommand:
				results[j], err = w.readPromise(ctx, tx, v)
			case *t_aio.ReadPromisesCommand:
				results[j], err = w.readPromises(ctx, tx, v)
			case *t_aio.SearchPromisesCommand:
				results[j], err = w.searchPromises(ctx, tx, v)
			case *t_aio.CreatePromiseCommand:
				results[j], err = w.createPromise(ctx, tx, v)
			case *t_aio.UpdatePromiseCommand:
				results[j], err = w.updatePromise(ctx, tx, v)

			// Callbacks
			case *t_aio.CreateCallbackCommand:
				results[j], err = w.createCallback(ctx, tx, v)
			case *t_aio.DeleteCallbacksCommand:
				results[j], err = w.deleteCallbacks(ctx, tx, v)

			// Schedules
			case *t_aio.ReadScheduleCommand:
				results[j], err = w.readSchedule(ctx, tx, v)
			case *t_aio.ReadSchedulesCommand:
				results[j], err = w.readSchedules(ctx, tx, v)
			case *t_aio.SearchSchedulesCommand:
				results[j], err = w.searchSchedules(ctx, tx, v)
			case *t_aio.CreateScheduleCommand:
				results[j], err = w.createSchedule(ctx, tx, v)
			case *t_aio.UpdateScheduleCommand:
				results[j], err = w.updateSchedule(ctx, tx, v)
			case *t_aio.DeleteScheduleCommand:
				results[j], err = w.deleteSchedule(ctx, tx, v)

			// Tasks
			case *t_aio.ReadTaskCommand:
				results[j], err = w.readTask(ctx, tx, v)
			case *t_aio.ReadTasksCommand:
				results[j], err = w.readTasks(ctx, tx, v)
			case *t_aio.ReadEnqueueableTasksCommand:
				results[j], err = w.readEnqueueableTasks(ctx, tx, v)
			case *t_aio.CreateTaskCommand:
				results[j], err = w.createTask(ctx, tx, v)
			case *t_aio.CreateTasksCommand:
				results[j], err = w.createTasks(ctx, tx, v)
			case *t_aio.UpdateTaskCommand:
				results[j], err = w.updateTask(ctx, tx, v)
			case *t_aio.CompleteTasksCommand:
				results[j], err = w.completeTasks(ctx, tx, v)
			case *t_aio.HeartbeatTasksCommand:
				results[j], err = w.heartbeatTasks(ctx, tx, v)

			case *t_aio.CreatePromiseAndTaskCommand:
				results[j], err = w.createPromiseAndTask(ctx, tx, v)

			default:
				panic(fmt.Sprintf("invalid command: %s", command.String()))
			}

			if err != nil {
				return nil, err
			}
		}
	}

	return completions, nil
}

// Promises

func (w *YugabyteStoreWorker) readPromise(ctx context.Context, tx pgx.Tx, cmd *t_aio.ReadPromiseCommand) (*t_aio.QueryPromisesResult, error) {
	row := tx.QueryRow(ctx, PROMISE_SELECT_STATEMENT, cmd.Id)
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
			return nil, err
		}
	}

	var records []*promise.PromiseRecord
	if rowsReturned == 1 {
		records = append(records, record)
	}

	return &t_aio.QueryPromisesResult{
		RowsReturned: rowsReturned,
		Records:      records,
	}, nil
}

func (w *YugabyteStoreWorker) readPromises(ctx context.Context, tx pgx.Tx, cmd *t_aio.ReadPromisesCommand) (*t_aio.QueryPromisesResult, error) {
	rows, err := tx.Query(ctx, PROMISE_SELECT_ALL_STATEMENT, cmd.Time, cmd.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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
			return nil, err
		}

		records = append(records, record)
		lastSortId = record.SortId
		rowsReturned++
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &t_aio.QueryPromisesResult{
		RowsReturned: rowsReturned,
		LastSortId:   lastSortId,
		Records:      records,
	}, nil
}

func (w *YugabyteStoreWorker) searchPromises(ctx context.Context, tx pgx.Tx, cmd *t_aio.SearchPromisesCommand) (*t_aio.QueryPromisesResult, error) {
	util.Assert(cmd.Id != "", "query cannot be empty")
	util.Assert(cmd.States != nil, "states cannot be empty")
	util.Assert(cmd.Tags != nil, "tags cannot be empty")

	// convert query
	id := strings.ReplaceAll(cmd.Id, "*", "%")

	// convert list of state to bit mask
	mask := 0
	for _, state := range cmd.States {
		mask = mask | int(state)
	}

	// tags
	var tags *string

	if len(cmd.Tags) > 0 {
		t, err := json.Marshal(cmd.Tags)
		if err != nil {
			return nil, err
		}

		tags = util.ToPointer(string(t))
	}

	rows, err := tx.Query(ctx, PROMISE_SEARCH_STATEMENT, cmd.SortId, id, mask, tags, cmd.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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
			return nil, err
		}

		records = append(records, record)
		lastSortId = record.SortId
		rowsReturned++
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &t_aio.QueryPromisesResult{
		RowsReturned: rowsReturned,
		LastSortId:   lastSortId,
		Records:      records,
	}, nil
}

func (w *YugabyteStoreWorker) createPromise(ctx context.Context, tx pgx.Tx, cmd *t_aio.CreatePromiseCommand) (*t_aio.AlterPromisesResult, error) {
	util.Assert(cmd.State.In(promise.Pending|promise.Resolved|promise.Timedout), "init state must be one of pending, resolved, timedout")
	util.Assert(cmd.Param.Headers != nil, "param headers must not be nil")
	util.Assert(cmd.Param.Data != nil, "param data must not be nil")
	util.Assert(cmd.Tags != nil, "tags must not be nil")

	headers, err := json.Marshal(cmd.Param.Headers)
	if err != nil {
		return nil, err
	}

	tags, err := json.Marshal(cmd.Tags)
	if err != nil {
		return nil, err
	}

	tag, err := tx.Exec(ctx, PROMISE_INSERT_STATEMENT, cmd.Id, cmd.State, headers, cmd.Param.Data, cmd.Timeout, cmd.Id, tags, cmd.CreatedOn)
	if err != nil {
		return nil, err
	}

	return &t_aio.AlterPromisesResult{
		RowsAffected: tag.RowsAffected(),
	}, nil
}

func (w *YugabyteStoreWorker) createPromiseAndTask(ctx context.Context, tx pgx.Tx, cmd *t_aio.CreatePromiseAndTaskCommand) (*t_aio.AlterPromisesResult, error) {
	res, err := w.createPromise(ctx, tx, cmd.PromiseCommand)
	if err != nil {
		return nil, err
	}

	if res.RowsAffected == 1 {
		if _, err := w.createTask(ctx, tx, cmd.TaskCommand); err != nil {
			return nil, err
		}
	}

	return res, nil
}

func (w *YugabyteStoreWorker) updatePromise(ctx context.Context, tx pgx.Tx, cmd *t_aio.UpdatePromiseCommand) (*t_aio.AlterPromisesResult, error) {
	util.Assert(cmd.State.In(promise.Resolved|promise.Rejected|promise.Canceled|promise.Timedout), "state must be canceled, resolved, rejected, or timedout")
	util.Assert(cmd.Value.Headers != nil, "value headers must not be nil")
	util.Assert(cmd.Value.Data != nil, "value data must not be nil")

	headers, err := json.Marshal(cmd.Value.Headers)
	if err != nil {
		return nil, err
	}

	tag, err := tx.Exec(ctx, PROMISE_UPDATE_STATEMENT, cmd.State, headers, cmd.Value.Data, cmd.Id, cmd.CompletedOn, cmd.Id)
	if err != nil {
		return nil, err
	}

	return &t_aio.AlterPromisesResult{
		RowsAffected: tag.RowsAffected(),
	}, nil
}

// Callbacks

func (w *YugabyteStoreWorker) createCallback(ctx context.Context, tx pgx.Tx, cmd *t_aio.CreateCallbackCommand) (*t_aio.AlterCallbacksResult, error) {
	util.Assert(cmd.Recv != nil, "recv must not be nil")
	util.Assert(cmd.Mesg != nil, "mesg must not be nil")

	mesg, err := json.Marshal(cmd.Mesg)
	if err != nil {
		return nil, err
	}

	tag, err := tx.Exec(ctx, CALLBACK_INSERT_STATEMENT, cmd.Id, cmd.PromiseId, cmd.Mesg.Root, cmd.Recv, mesg, cmd.Timeout, cmd.CreatedOn)
	if err != nil {
		return nil, err
	}

	return &t_aio.AlterCallbacksResult{
		RowsAffected: tag.RowsAffected(),
	}, nil
}

func (w *YugabyteStoreWorker) deleteCallbacks(ctx context.Context, tx pgx.Tx, cmd *t_aio.DeleteCallbacksCommand) (*t_aio.AlterCallbacksResult, error) {
	tag, err := tx.Exec(ctx, CALLBACK_DELETE_STATEMENT, cmd.PromiseId)
	if err != nil {
		return nil, err
	}

	return &t_aio.AlterCallbacksResult{
		RowsAffected: tag.RowsAffected(),
	}, nil
}

// Schedules

func (w *YugabyteStoreWorker) readSchedule(ctx context.Context, tx pgx.Tx, cmd *t_aio.ReadScheduleCommand) (*t_aio.QuerySchedulesResult, error) {
	row := tx.QueryRow(ctx, SCHEDULE_SELECT_STATEMENT, cmd.Id)
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
			return nil, err
		}
	}

	var records []*schedule.ScheduleRecord
	if rowsReturned == 1 {
		records = append(records, record)
	}

	return &t_aio.QuerySchedulesResult{
		RowsReturned: rowsReturned,
		Records:      records,
	}, nil
}

func (w *YugabyteStoreWorker) readSchedules(ctx context.Context, tx pgx.Tx, cmd *t_aio.ReadSchedulesCommand) (*t_aio.QuerySchedulesResult, error) {
	rows, err := tx.Query(ctx, SCHEDULE_SELECT_ALL_STATEMENT, cmd.NextRunTime, cmd.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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
			return nil, err
		}

		records = append(records, record)
		rowsReturned++
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &t_aio.QuerySchedulesResult{
		RowsReturned: rowsReturned,
		Records:      records,
	}, nil
}

func (w *YugabyteStoreWorker) searchSchedules(ctx context.Context, tx pgx.Tx, cmd *t_aio.SearchSchedulesCommand) (*t_aio.QuerySchedulesResult, error) {
	util.Assert(cmd.Id != "", "query cannot be empty")
	util.Assert(cmd.Tags != nil, "tags cannot be empty")

	// convert query
	id := strings.ReplaceAll(cmd.Id, "*", "%")

	// tags
	var tags *string
	if len(cmd.Tags) > 0 {
		t, err := json.Marshal(cmd.Tags)
		if err != nil {
			return nil, err
		}

		tags = util.ToPointer(string(t))
	}

	rows, err := tx.Query(ctx, SCHEDULE_SEARCH_STATEMENT, cmd.SortId, id, tags, cmd.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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
			return nil, err
		}

		records = append(records, record)
		lastSortId = record.SortId
		rowsReturned++
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &t_aio.QuerySchedulesResult{
		RowsReturned: rowsReturned,
		LastSortId:   lastSortId,
		Records:      records,
	}, nil
}

func (w *YugabyteStoreWorker) createSchedule(ctx context.Context, tx pgx.Tx, cmd *t_aio.CreateScheduleCommand) (*t_aio.AlterSchedulesResult, error) {
	tags, err := json.Marshal(cmd.Tags)
	if err != nil {
		return nil, err
	}

	promiseParamHeaders, err := json.Marshal(cmd.PromiseParam.Headers)
	if err != nil {
		return nil, err
	}

	promiseTags, err := json.Marshal(cmd.PromiseTags)
	if err != nil {
		return nil, err
	}

	tag, err := tx.Exec(ctx, SCHEDULE_INSERT_STATEMENT,
		cmd.Id,
		cmd.Description,
		cmd.Cron,
		tags,
		cmd.PromiseId,
		cmd.PromiseTimeout,
		promiseParamHeaders,
		cmd.PromiseParam.Data,
		promiseTags,
		cmd.NextRunTime,
		cmd.Id,
		cmd.CreatedOn,
	)
	if err != nil {
		return nil, err
	}

	return &t_aio.AlterSchedulesResult{
		RowsAffected: tag.RowsAffected(),
	}, nil
}

func (w *YugabyteStoreWorker) updateSchedule(ctx context.Context, tx pgx.Tx, cmd *t_aio.UpdateScheduleCommand) (*t_aio.AlterSchedulesResult, error) {
	tag, err := tx.Exec(ctx, SCHEDULE_UPDATE_STATEMENT, cmd.NextRunTime, cmd.Id, cmd.LastRunTime)
	if err != nil {
		return nil, err
	}

	return &t_aio.AlterSchedulesResult{
		RowsAffected: tag.RowsAffected(),
	}, nil
}

func (w *YugabyteStoreWorker) deleteSchedule(ctx context.Context, tx pgx.Tx, cmd *t_aio.DeleteScheduleCommand) (*t_aio.AlterSchedulesResult, error) {
	tag, err := tx.Exec(ctx, SCHEDULE_DELETE_STATEMENT, cmd.Id)
	if err != nil {
		return nil, err
	}

	return &t_aio.AlterSchedulesResult{
		RowsAffected: tag.RowsAffected(),
	}, nil
}

// Tasks

func (w *YugabyteStoreWorker) readTask(ctx context.Context, tx pgx.Tx, cmd *t_aio.ReadTaskCommand) (*t_aio.QueryTasksResult, error) {
	row := tx.QueryRow(ctx, TASK_SELECT_STATEMENT, cmd.Id)
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
			return nil, store.StoreErr(err)
		}
	}

	var records []*task.TaskRecord
	if rowsReturned == 1 {
		records = append(records, record)
	}

	return &t_aio.QueryTasksResult{
		RowsReturned: rowsReturned,
		Records:      records,
	}, nil
}

func (w *YugabyteStoreWorker) readTasks(ctx context.Context, tx pgx.Tx, cmd *t_aio.ReadTasksCommand) (*t_aio.QueryTasksResult, error) {
	util.Assert(len(cmd.States) > 0, "must provide at least one state")

	var states task.State
	for _, state := range cmd.States {
		states |= state
	}

	rows, err := tx.Query(ctx, TASK_SELECT_ALL_STATEMENT, states, cmd.Time, cmd.Limit)
	if err != nil {
		return nil, store.StoreErr(err)
	}
	defer rows.Close()

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
			return nil, store.StoreErr(err)
		}

		records = append(records, record)
		rowsReturned++
	}

	if err := rows.Err(); err != nil {
		return nil, store.StoreErr(err)
	}

	return &t_aio.QueryTasksResult{
		RowsReturned: rowsReturned,
		Records:      records,
	}, nil
}

func (w *YugabyteStoreWorker) readEnqueueableTasks(ctx context.Context, tx pgx.Tx, cmd *t_aio.ReadEnqueueableTasksCommand) (*t_aio.QueryTasksResult, error) {
	rows, err := tx.Query(ctx, TASK_SELECT_ENQUEUEABLE_STATEMENT, cmd.Time, cmd.Limit)
	if err != nil {
		return nil, store.StoreErr(err)
	}
	defer rows.Close()

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
			return nil, store.StoreErr(err)
		}

		records = append(records, record)
		rowsReturned++
	}

	if err := rows.Err(); err != nil {
		return nil, store.StoreErr(err)
	}

	return &t_aio.QueryTasksResult{
		RowsReturned: rowsReturned,
		Records:      records,
	}, nil
}

func (w *YugabyteStoreWorker) createTask(ctx context.Context, tx pgx.Tx, cmd *t_aio.CreateTaskCommand) (*t_aio.AlterTasksResult, error) {
	util.Assert(cmd.Recv != nil, "recv must not be nil")
	util.Assert(cmd.Mesg != nil, "mesg must not be nil")
	util.Assert(cmd.State.In(task.Init|task.Claimed), "state must be init or claimed")
	util.Assert(cmd.State != task.Claimed || cmd.ProcessId != nil, "process id must be set if state is claimed")

	mesg, err := json.Marshal(cmd.Mesg)
	if err != nil {
		return nil, store.StoreErr(err)
	}

	tag, err := tx.Exec(ctx, TASK_INSERT_STATEMENT, cmd.Id, cmd.Recv, mesg, cmd.Timeout, cmd.ProcessId, cmd.State, cmd.Mesg.Root, cmd.Ttl, cmd.ExpiresAt, cmd.CreatedOn)
	if err != nil {
		return nil, err
	}

	return &t_aio.AlterTasksResult{
		RowsAffected: tag.RowsAffected(),
	}, nil
}

func (w *YugabyteStoreWorker) createTasks(ctx context.Context, tx pgx.Tx, cmd *t_aio.CreateTasksCommand) (*t_aio.AlterTasksResult, error) {
	tag, err := tx.Exec(ctx, TASK_INSERT_ALL_STATEMENT, cmd.CreatedOn, cmd.PromiseId)
	if err != nil {
		return nil, store.StoreErr(err)
	}

	return &t_aio.AlterTasksResult{
		RowsAffected: tag.RowsAffected(),
	}, nil
}

func (w *YugabyteStoreWorker) completeTasks(ctx context.Context, tx pgx.Tx, cmd *t_aio.CompleteTasksCommand) (*t_aio.AlterTasksResult, error) {
	tag, err := tx.Exec(ctx, TASK_COMPLETE_BY_ROOT_ID_STATEMENT, cmd.CompletedOn, cmd.RootPromiseId)
	if err != nil {
		return nil, store.StoreErr(err)
	}

	return &t_aio.AlterTasksResult{
		RowsAffected: tag.RowsAffected(),
	}, nil
}

func (w *YugabyteStoreWorker) updateTask(ctx context.Context, tx pgx.Tx, cmd *t_aio.UpdateTaskCommand) (*t_aio.AlterTasksResult, error) {
	util.Assert(len(cmd.CurrentStates) > 0, "must provide at least one current state")

	var currentStates task.State
	for _, state := range cmd.CurrentStates {
		currentStates |= state
	}

	tag, err := tx.Exec(ctx, TASK_UPDATE_STATEMENT,
		cmd.ProcessId,
		cmd.State,
		cmd.Counter,
		cmd.Attempt,
		cmd.Ttl,
		cmd.ExpiresAt,
		cmd.CompletedOn,
		cmd.Id,
		currentStates,
		cmd.CurrentCounter,
	)
	if err != nil {
		return nil, store.StoreErr(err)
	}

	return &t_aio.AlterTasksResult{
		RowsAffected: tag.RowsAffected(),
	}, nil
}

func (w *YugabyteStoreWorker) heartbeatTasks(ctx context.Context, tx pgx.Tx, cmd *t_aio.HeartbeatTasksCommand) (*t_aio.AlterTasksResult, error) {
	tag, err := tx.Exec(ctx, TASK_HEARTBEAT_STATEMENT, cmd.Time, cmd.ProcessId)
	if err != nil {
		return nil, store.StoreErr(err)
	}

	return &t_aio.AlterTasksResult{
		RowsAffected: tag.RowsAffected(),
	}, nil
}

func (w *YugabyteStoreWorker) validFencingToken(ctx context.Context, tx pgx.Tx, transaction *t_aio.Transaction) (bool, error) {
	// if the task is not provided continue with the operation
	if transaction.Fence == nil {
		return true, nil
	}

	var rowCount int64
	err := tx.QueryRow(ctx, TASK_VALIDATE_STATEMENT, transaction.Fence.TaskId, transaction.Fence.TaskCounter).Scan(&rowCount)

	if err != nil {
		return false, store.StoreErr(err)
	}
	util.Assert(rowCount == 1 || rowCount == 0, "must be zero or one")
	return rowCount == 1, nil
}
