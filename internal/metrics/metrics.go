package metrics

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	AioTotal             *prometheus.CounterVec
	AioInFlight          *prometheus.GaugeVec
	AioDuration          *prometheus.HistogramVec
	AioWorker            *prometheus.GaugeVec
	AioWorkerInFlight    *prometheus.GaugeVec
	AioPluginConnections *prometheus.GaugeVec
	ApiTotal             *prometheus.CounterVec
	ApiInFlight          *prometheus.GaugeVec
	ApiDuration          *prometheus.HistogramVec
	CoroutinesTotal      *prometheus.CounterVec
	CoroutinesInFlight   *prometheus.GaugeVec
	CoroutinesDuration   *prometheus.HistogramVec
	HttpRequestsTotal    *prometheus.CounterVec
	HttpRequestsDuration *prometheus.HistogramVec
	PromisesTotal        *prometheus.CounterVec
	SchedulesTotal       *prometheus.CounterVec
	TasksTotal           *prometheus.CounterVec

	// YugaByte-specific metrics for fault-tolerance monitoring
	YugabyteTxTotal       *prometheus.CounterVec
	YugabyteTxRetries     prometheus.Counter
	YugabyteTxDuration    prometheus.Histogram
	YugabytePoolConns     *prometheus.GaugeVec
	YugabyteErrorsTotal   *prometheus.CounterVec
	YugabyteCommandTotal  *prometheus.CounterVec
	YugabyteBatchDuration *prometheus.HistogramVec
}

func New(reg prometheus.Registerer) *Metrics {
	metrics := &Metrics{
		AioTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "aio_submissions_total",
			Help: "total number of aio submissions",
		}, []string{"type", "status"}),
		AioInFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "aio_submissions_in_flight",
			Help: "number of in flight aio submissions",
		}, []string{"type"}),
		AioDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "aio_duration_seconds",
			Help:    "duration of aio submissions in seconds",
			Buckets: prometheus.DefBuckets,
		}, []string{"type"}),
		AioWorker: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "aio_worker_count",
			Help: "number of aio subsystem workers",
		}, []string{"type"}),
		AioWorkerInFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "aio_worker_submissions_in_flight",
			Help: "number of in flight aio submissions",
		}, []string{"type", "worker"}),
		AioPluginConnections: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "aio_plugin_connections",
			Help: "number of aio plugin connections",
		}, []string{"type"}),
		ApiTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "api_requests_total",
			Help: "total number of api requests",
		}, []string{"type", "protocol", "status"}),
		ApiInFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "api_requests_in_flight",
			Help: "number of in flight api requests",
		}, []string{"type", "protocol"}),
		ApiDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "api_duration_seconds",
			Help:    "duration of api requests in seconds",
			Buckets: prometheus.DefBuckets,
		}, []string{"type", "protocol"}),
		CoroutinesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "coroutines_total",
			Help: "total number of coroutines",
		}, []string{"type"}),
		CoroutinesInFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "coroutines_in_flight",
			Help: "number of in flight coroutines",
		}, []string{"type"}),
		CoroutinesDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "coroutines_seconds",
			Help:    "duration of coroutines in seconds",
			Buckets: prometheus.DefBuckets,
		}, []string{"type"}),
		HttpRequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "count of http requests",
		}, []string{"method", "path", "status"}),
		HttpRequestsDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_requests_duration_seconds",
			Help:    "duration of http requests in seconds",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "path"}),
		PromisesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "promises_total",
			Help: "count of promises",
		}, []string{"state"}),
		SchedulesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "schedules_total",
			Help: "count of schedules",
		}, []string{"state"}),
		TasksTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tasks_total",
			Help: "count of tasks",
		}, []string{"state"}),

		// YugaByte-specific metrics for fault-tolerance monitoring
		YugabyteTxTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "yugabyte_tx_total",
			Help: "total number of YugaByte transactions by final status",
		}, []string{"status"}),
		YugabyteTxRetries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "yugabyte_tx_retries_total",
			Help: "total number of YugaByte transaction retry attempts (serialization/deadlock errors)",
		}),
		YugabyteTxDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "yugabyte_tx_duration_seconds",
			Help:    "duration of YugaByte transactions in seconds",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 15, 20, 30, 60},
		}),
		YugabytePoolConns: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "yugabyte_pool_connections",
			Help: "YugaByte connection pool connection counts by state",
		}, []string{"state"}),
		YugabyteErrorsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "yugabyte_errors_total",
			Help: "total number of errors received from YugaByte by error type",
		}, []string{"error_type"}),
		YugabyteCommandTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "yugabyte_command_total",
			Help: "total number of store commands queued to YugaByte by command type",
		}, []string{"command"}),
		YugabyteBatchDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "yugabyte_batch_duration_seconds",
			Help:    "round-trip latency of a YugaByte batch per dominant command type (SendBatch → all results received)",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5},
		}, []string{"command"}),
	}

	metrics.Enable(reg)
	return metrics
}

func (m *Metrics) Enable(reg prometheus.Registerer) {
	reg.MustRegister(m.AioTotal)
	reg.MustRegister(m.AioInFlight)
	reg.MustRegister(m.AioDuration)
	reg.MustRegister(m.AioWorker)
	reg.MustRegister(m.AioWorkerInFlight)
	reg.MustRegister(m.AioPluginConnections)
	reg.MustRegister(m.ApiTotal)
	reg.MustRegister(m.ApiInFlight)
	reg.MustRegister(m.ApiDuration)
	reg.MustRegister(m.CoroutinesTotal)
	reg.MustRegister(m.CoroutinesInFlight)
	reg.MustRegister(m.CoroutinesDuration)
	reg.MustRegister(m.HttpRequestsTotal)
	reg.MustRegister(m.HttpRequestsDuration)
	reg.MustRegister(m.PromisesTotal)
	reg.MustRegister(m.SchedulesTotal)
	reg.MustRegister(m.TasksTotal)
	reg.MustRegister(m.YugabyteTxTotal)
	reg.MustRegister(m.YugabyteTxRetries)
	reg.MustRegister(m.YugabyteTxDuration)
	reg.MustRegister(m.YugabytePoolConns)
	reg.MustRegister(m.YugabyteErrorsTotal)
	reg.MustRegister(m.YugabyteCommandTotal)
	reg.MustRegister(m.YugabyteBatchDuration)
}

func (m *Metrics) Disable(reg prometheus.Registerer) {
	reg.Unregister(m.AioTotal)
	reg.Unregister(m.AioInFlight)
	reg.Unregister(m.AioDuration)
	reg.Unregister(m.AioWorker)
	reg.Unregister(m.AioWorkerInFlight)
	reg.Unregister(m.AioPluginConnections)
	reg.Unregister(m.ApiTotal)
	reg.Unregister(m.ApiInFlight)
	reg.Unregister(m.ApiDuration)
	reg.Unregister(m.CoroutinesTotal)
	reg.Unregister(m.CoroutinesInFlight)
	reg.Unregister(m.CoroutinesDuration)
	reg.Unregister(m.HttpRequestsTotal)
	reg.Unregister(m.HttpRequestsDuration)
	reg.Unregister(m.PromisesTotal)
	reg.Unregister(m.SchedulesTotal)
	reg.Unregister(m.TasksTotal)
	reg.Unregister(m.YugabyteTxTotal)
	reg.Unregister(m.YugabyteTxRetries)
	reg.Unregister(m.YugabyteTxDuration)
	reg.Unregister(m.YugabytePoolConns)
	reg.Unregister(m.YugabyteErrorsTotal)
	reg.Unregister(m.YugabyteCommandTotal)
	reg.Unregister(m.YugabyteBatchDuration)
}
