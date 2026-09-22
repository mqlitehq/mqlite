package server

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/wire"
)

// MetricsHandler exposes only GET /metrics. Mount it on a private listener,
// never on the public API listener. It shares the API server's engine and
// counters, and refuses anonymous operation even in development mode.
func (s *Server) MetricsHandler() http.Handler {
	admins := make([]string, 0, len(s.tokens))
	for token := range s.tokens {
		admins = append(admins, token)
	}
	if len(admins) == 0 {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			writeErr(w, http.StatusForbidden, "permission_denied", "metrics require administrator authentication")
		})
	}
	if err := ValidateMonitorTokens(admins, s.MonitorTokens); err != nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			writeErr(w, http.StatusInternalServerError, "internal", "invalid monitoring credential configuration")
		})
	}
	metrics := s.authorize(monitorPermission, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			writeErr(w, http.StatusMethodNotAllowed, "unimplemented", "metrics require GET")
			return
		}
		s.handleMetrics(w, r)
	})
	handler := s.observeRequests(s.logging(s.auth(metrics)))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" {
			http.NotFound(w, r)
			return
		}
		handler.ServeHTTP(w, r)
	})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write([]byte(prometheusSnapshot(s.observation(r.Context()))))
}

// Prometheus quoted label values escape only these three characters. Go %q also
// escapes tabs/control characters, which is not valid Prometheus text escaping.
func promQuote(value string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, "\n", `\n`, `"`, `\"`).Replace(value) + `"`
}

func metricHeader(b *strings.Builder, name, kind, help string) {
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, kind)
}

func histogram(b *strings.Builder, name, labels string, h engine.DurationHistogram) {
	for i, bound := range h.BoundsSeconds {
		fmt.Fprintf(b, "%s_bucket{%s,le=%s} %d\n", name, labels,
			promQuote(strconv.FormatFloat(bound, 'g', -1, 64)), h.BucketCounts[i])
	}
	fmt.Fprintf(b, "%s_bucket{%s,le=\"+Inf\"} %d\n%s_sum{%s} %g\n%s_count{%s} %d\n",
		name, labels, h.Count, name, labels, h.SumSeconds, name, labels, h.Count)
}

// prometheusSnapshot is a serializer only: no database access, clocks or counter
// updates. Compatibility names below are exact projections, never another ledger.
func prometheusSnapshot(s wire.ObserveResponse) string {
	var b strings.Builder
	metricHeader(&b, "mqlite_info", "gauge", "Broker build and storage backend information.")
	fmt.Fprintf(&b, "mqlite_info{version=%s,backend=%s,schema_version=%s} 1\n", promQuote(s.Version), promQuote(s.Backend), promQuote(s.Runtime.SchemaVersion))
	metricHeader(&b, "mqlite_storage_read_available", "gauge", "Whether the latest SELECT 1 read probe succeeded; not a write health check.")
	read := 0
	if s.Runtime.ReadAvailable {
		read = 1
	}
	fmt.Fprintf(&b, "mqlite_storage_read_available %d\n", read)
	metricHeader(&b, "mqlite_storage_ping_seconds", "gauge", "Elapsed SELECT 1 read probe time, absent on failure.")
	if s.Runtime.ReadAvailable {
		fmt.Fprintf(&b, "mqlite_storage_ping_seconds %g\n", s.Runtime.PingSeconds)
	}
	metricHeader(&b, "mqlite_storage_size_available", "gauge", "Whether local database, WAL and SHM footprint measurement is available.")
	size := 0
	if s.Runtime.DBSizeAvailable {
		size = 1
	}
	fmt.Fprintf(&b, "mqlite_storage_size_available %d\n", size)
	metricHeader(&b, "mqlite_storage_size_bytes", "gauge", "Local database, WAL and SHM bytes, absent for memory, remote or unavailable files.")
	if s.Runtime.DBSizeAvailable {
		fmt.Fprintf(&b, "mqlite_storage_size_bytes %d\n", s.Runtime.DBSizeBytes)
	}
	for _, m := range []struct {
		name, help string
		value      float64
	}{
		{"mqlite_start_time_seconds", "Engine start time as Unix seconds.", float64(s.StartedAtMs) / 1000},
		{"mqlite_sample_timestamp_seconds", "Queue sample time as Unix seconds.", float64(s.SampledAtMs) / 1000},
		{"mqlite_collection_last_success_timestamp_seconds", "Most recent successful queue collection as Unix seconds, or zero before success.", float64(s.Collection.LastSuccessAtMs) / 1000},
		{"mqlite_collection_duration_seconds", "Elapsed time of the latest queue collection.", s.Collection.DurationSeconds},
	} {
		metricHeader(&b, m.name, "gauge", m.help)
		fmt.Fprintf(&b, "%s %g\n", m.name, m.value)
	}
	metricHeader(&b, "mqlite_collection_success", "gauge", "Whether the latest queue collection succeeded.")
	success := 0
	if s.Collection.State == "available" {
		success = 1
	}
	fmt.Fprintf(&b, "mqlite_collection_success %d\n", success)
	metricHeader(&b, "mqlite_collection_status", "gauge", "Latest collection availability and bounded failure classification.")
	fmt.Fprintf(&b, "mqlite_collection_status{state=%s,error_code=%s} 1\n", promQuote(s.Collection.State), promQuote(s.Collection.ErrorCode))
	metricHeader(&b, "mqlite_entities", "gauge", "Configured non-subscription queues and subscriptions.")
	if success == 1 {
		fmt.Fprintf(&b, "mqlite_entities{kind=\"queue\"} %d\nmqlite_entities{kind=\"subscription\"} %d\n", s.QueueCount, s.SubscriptionCount)
	}
	metricHeader(&b, "mqlite_queue_messages", "gauge", "Messages in a queue by state.")
	for _, q := range s.Queues {
		for _, state := range []struct {
			name string
			n    int64
		}{
			{"active", q.Active}, {"locked", q.Locked}, {"deferred", q.Deferred}, {"scheduled", q.Scheduled}, {"dead_lettered", q.DeadLettered},
		} {
			fmt.Fprintf(&b, "mqlite_queue_messages{queue=%s,state=%s} %d\n", promQuote(q.Queue), promQuote(state.name), state.n)
		}
	}
	for _, metric := range []struct {
		name, help string
		value      func(engine.QueueObservation) float64
	}{
		{"mqlite_queue_retained_messages", "All retained message rows, including unexpected states.", func(q engine.QueueObservation) float64 { return float64(q.Total) }},
		{"mqlite_queue_unexpected_messages", "Retained rows outside the five normal working states.", func(q engine.QueueObservation) float64 { return float64(q.Unexpected) }},
		{"mqlite_queue_oldest_message_age_seconds", "Age of the oldest active or locked message in seconds, zero when absent.", func(q engine.QueueObservation) float64 { return float64(q.OldestMessageAgeMs) / 1000 }},
		{"mqlite_queue_total", "Total messages in a queue.", func(q engine.QueueObservation) float64 { return float64(q.Total) }},
		{"mqlite_queue_oldest_message_age_ms", "Age of the oldest active or locked message in a queue, in milliseconds.", func(q engine.QueueObservation) float64 { return float64(q.OldestMessageAgeMs) }},
	} {
		metricHeader(&b, metric.name, "gauge", metric.help)
		for _, q := range s.Queues {
			fmt.Fprintf(&b, "%s{queue=%s} %g\n", metric.name, promQuote(q.Queue), metric.value(q))
		}
	}
	metricHeader(&b, "mqlite_message_events_total", "counter", "Confirmed message effects since engine start; overlapping event categories are not a ledger.")
	for _, m := range s.Messages {
		fmt.Fprintf(&b, "mqlite_message_events_total{queue=%s,event=%s} %d\n", promQuote(m.Queue), promQuote(m.Event), m.Count)
	}
	metricHeader(&b, "mqlite_messages_completed_total", "counter", "Messages successfully completed, cumulative since broker start.")
	for _, m := range s.Messages {
		if m.Event == "completed" {
			fmt.Fprintf(&b, "mqlite_messages_completed_total{queue=%s} %d\n", promQuote(m.Queue), m.Count)
		}
	}
	metricHeader(&b, "mqlite_storage_operations_total", "counter", "Completed storage wrapper operations by outcome, including callback rejections.")
	for _, o := range s.Storage.Operations {
		fmt.Fprintf(&b, "mqlite_storage_operations_total{%s} %d\n", storageLabels(o), o.Duration.Count)
	}
	metricHeader(&b, "mqlite_storage_operation_duration_seconds", "histogram", "Storage wrapper elapsed time including pool wait, retries and transaction callbacks.")
	for _, o := range s.Storage.Operations {
		histogram(&b, "mqlite_storage_operation_duration_seconds", storageLabels(o), o.Duration)
	}
	metricHeader(&b, "mqlite_storage_retries_total", "counter", "Safe storage retries within completed wrapper operations.")
	for _, o := range s.Storage.Operations {
		fmt.Fprintf(&b, "mqlite_storage_retries_total{%s} %d\n", storageLabels(o), o.Retries)
	}
	metricHeader(&b, "mqlite_storage_pool_connections", "gauge", "Database connection pool usage and configured capacity.")
	p := s.Storage.Pool
	for _, v := range []struct {
		state string
		n     int
	}{{"max_open", p.MaxOpen}, {"open", p.Open}, {"in_use", p.InUse}, {"idle", p.Idle}} {
		fmt.Fprintf(&b, "mqlite_storage_pool_connections{state=%s} %d\n", promQuote(v.state), v.n)
	}
	metricHeader(&b, "mqlite_storage_pool_waits_total", "counter", "Cumulative waits for a database connection.")
	fmt.Fprintf(&b, "mqlite_storage_pool_waits_total %d\n", p.WaitCount)
	metricHeader(&b, "mqlite_storage_pool_wait_duration_seconds_total", "counter", "Cumulative elapsed time waiting for a database connection.")
	fmt.Fprintf(&b, "mqlite_storage_pool_wait_duration_seconds_total %g\n", p.WaitDurationSeconds)
	metricHeader(&b, "mqlite_maintenance_runs_total", "counter", "Finished maintenance passes by task and outcome.")
	for _, m := range s.Maintenance {
		fmt.Fprintf(&b, "mqlite_maintenance_runs_total{task=%s,outcome=\"success\"} %d\nmqlite_maintenance_runs_total{task=%s,outcome=\"error\"} %d\nmqlite_maintenance_runs_total{task=%s,outcome=\"interrupted\"} %d\n", promQuote(m.Task), m.Runs-m.Failures-m.Interrupted, promQuote(m.Task), m.Failures, promQuote(m.Task), m.Interrupted)
	}
	metricHeader(&b, "mqlite_maintenance_enabled", "gauge", "Whether a task is scheduled automatically in this engine mode.")
	for _, m := range s.Maintenance {
		enabled := 0
		if m.Enabled {
			enabled = 1
		}
		fmt.Fprintf(&b, "mqlite_maintenance_enabled{task=%s} %d\n", promQuote(m.Task), enabled)
	}
	metricHeader(&b, "mqlite_maintenance_duration_seconds", "histogram", "Elapsed maintenance pass time by task.")
	for _, m := range s.Maintenance {
		histogram(&b, "mqlite_maintenance_duration_seconds", "task="+promQuote(m.Task), m.Duration)
	}
	metricHeader(&b, "mqlite_maintenance_last_success_timestamp_seconds", "gauge", "Most recent successful maintenance pass as Unix seconds, or zero before success.")
	for _, m := range s.Maintenance {
		fmt.Fprintf(&b, "mqlite_maintenance_last_success_timestamp_seconds{task=%s} %g\n", promQuote(m.Task), float64(m.LastSuccessAtMs)/1000)
	}
	metricHeader(&b, "mqlite_filter_failures_total", "counter", "Routing filter compilation or evaluation failures, excluding validation, dry runs and nonmatches.")
	for _, f := range s.Filters {
		fmt.Fprintf(&b, "mqlite_filter_failures_total{stage=%s} %d\n", promQuote(f.Stage), f.Count)
	}
	metricHeader(&b, "mqlite_rpc_requests_total", "counter", "Completed registered RPC requests including authentication and permission failures.")
	for _, r := range s.HTTP.Requests {
		fmt.Fprintf(&b, "mqlite_rpc_requests_total{rpc=%s,code=%s} %d\n", promQuote(r.RPC), promQuote(r.Code), r.Count)
	}
	metricHeader(&b, "mqlite_rpc_request_duration_seconds_total", "counter", "Cumulative whole RPC elapsed time including authentication and normal long polling.")
	for _, r := range s.HTTP.Requests {
		fmt.Fprintf(&b, "mqlite_rpc_request_duration_seconds_total{rpc=%s,code=%s} %g\n", promQuote(r.RPC), promQuote(r.Code), r.DurationSeconds)
	}
	metricHeader(&b, "mqlite_authentication_total", "counter", "One final authentication or authorization outcome per protected HTTP request.")
	for _, a := range s.HTTP.Authentication {
		fmt.Fprintf(&b, "mqlite_authentication_total{outcome=%s} %d\n", promQuote(a.Outcome), a.Count)
	}
	metricHeader(&b, "mqlite_rpc_duration_seconds", "histogram", "RPC handler latency by method.")
	for _, r := range s.HTTP.HandlerLatency {
		h := engine.DurationHistogram{Count: r.Count, SumSeconds: r.SumSeconds}
		for _, bucket := range r.Buckets {
			h.BoundsSeconds = append(h.BoundsSeconds, bucket.UpperBoundSeconds)
			h.BucketCounts = append(h.BucketCounts, bucket.Count)
		}
		histogram(&b, "mqlite_rpc_duration_seconds", "rpc="+promQuote(r.RPC), h)
	}
	return b.String()
}

func storageLabels(o engine.StorageOperation) string {
	return "operation=" + promQuote(o.Operation) + ",outcome=" + promQuote(o.Outcome) + ",error_code=" + promQuote(o.ErrorCode)
}
