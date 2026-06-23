package engine

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/ast"
	"github.com/expr-lang/expr/vm"
)

// Subscription filters are a single expr-lang boolean predicate over a message
// (MQLITE-17). The expression is type-checked + compiled once at Subscribe and run
// per message at publish (fan-out). This file owns the whole language surface — the
// env (the variables a filter sees), the Safe profile (resource caps + the curated
// time helpers), compilation, and the fail-closed evaluator.
//
// Safety posture: expr is memory-safe, side-effect-free and always-terminating by
// design (no IO, no unbounded loops). We add only pure time helpers, bound the
// source length and AST size (defense in depth against the parser-DoS class,
// CVE-2025-29786), and recover fail-closed so a bad filter can never crash the
// broker or silently match — it simply does not route to that subscription.

const (
	// maxFilterSourceLen bounds the raw expression length accepted at Subscribe — a
	// cheap first gate before the parser runs.
	maxFilterSourceLen = 4096
	// maxFilterNodes caps the compiled AST node count.
	maxFilterNodes = 1000
)

// filterEnv is the typed variable set a filter sees. expr type-checks every
// expression against this struct at Subscribe, so an unknown variable or function is
// a compile error (listing the available names) rather than a runtime surprise. The
// `expr` tags are the variable names in the filter language.
type filterEnv struct {
	// Core (raw message).
	Subject       string            `expr:"subject"`
	Properties    map[string]string `expr:"properties"`
	GroupID       string            `expr:"group_id"`
	CorrelationID string            `expr:"correlation_id"`
	ReplyTo       string            `expr:"reply_to"`
	MessageID     string            `expr:"message_id"`
	ContentType   string            `expr:"content_type"`

	// Time — both are the message's own timestamps (deterministic + replayable),
	// not a wall-clock read. Since fan-out evaluates at publish, enqueued_at *is*
	// "now"; visible_at equals enqueued_at for an immediate send and the scheduled
	// time for a delayed one (never nil). Compute a delay by subtraction:
	// `visible_at - enqueued_at > days(1)`.
	EnqueuedAt time.Time `expr:"enqueued_at"`
	VisibleAt  time.Time `expr:"visible_at"`

	// Derived — total-valued (cannot error on absence).
	SubjectParts []string `expr:"subject_parts"` // "orders.eu.new" -> [orders eu new]
	BodySize     int      `expr:"body_size"`     // byte length; route by size, not content
	PropertyKeys []string `expr:"property_keys"` // sorted custom-property names

	// Body content (MQLITE-47). Populated only when the filter references them
	// (see filterEntry.usesBody*), so filters that don't touch the body pay nothing.
	// Both are total-valued so an absent/unparseable body can't nil-panic the env:
	// body_text is the raw bytes as a string (""-default); body_json is the decoded
	// JSON object ({}-default, never nil — only attempted for a JSON content_type).
	// Reaching INTO an absent field (e.g. body_json.amount on {}) yields nil and is
	// handled fail-closed at eval (not routed, logged) — never a crash.
	BodyText string         `expr:"body_text"`
	BodyJSON map[string]any `expr:"body_json"`
}

// filterOptions is the Safe profile, the single definition of the language surface.
// Shared by every Compile so Subscribe-time type-checking and publish-time execution
// agree exactly.
func filterOptions() []expr.Option {
	durType := new(func(string) (time.Duration, error))
	intDur := new(func(int) time.Duration)
	return []expr.Option{
		expr.Env(filterEnv{}),
		expr.AsBool(), // a filter MUST be a boolean predicate
		expr.MaxNodes(maxFilterNodes),

		// Unambiguous, type-checked duration helpers — the recommended form (no
		// string parsing). days/weeks are fixed (24h / 7d); there is deliberately no
		// month/year helper (calendar-variable → ambiguous as a fixed duration).
		expr.Function("seconds", func(a ...any) (any, error) { return time.Duration(toInt(a[0])) * time.Second, nil }, intDur),
		expr.Function("minutes", func(a ...any) (any, error) { return time.Duration(toInt(a[0])) * time.Minute, nil }, intDur),
		expr.Function("hours", func(a ...any) (any, error) { return time.Duration(toInt(a[0])) * time.Hour, nil }, intDur),
		expr.Function("days", func(a ...any) (any, error) { return time.Duration(toInt(a[0])) * 24 * time.Hour, nil }, intDur),
		expr.Function("weeks", func(a ...any) (any, error) { return time.Duration(toInt(a[0])) * 7 * 24 * time.Hour, nil }, intDur),

		// Override the built-in duration() (= time.ParseDuration, which only goes up
		// to `h`) with one that also accepts d (=24h) and w (=7d), so duration("2w")
		// and duration("1d12h") parse. Same stop-at-day/week rule as the helpers.
		expr.DisableBuiltin("duration"),
		expr.Function("duration", func(a ...any) (any, error) {
			s, ok := a[0].(string)
			if !ok {
				return nil, fmt.Errorf("duration: want string, got %T", a[0])
			}
			return parseDuration(s)
		}, durType),
	}
}

// filterEntry is a compiled filter cached by subscription name. err is set only if a
// stored filter failed to compile — which Subscribe validation should prevent, but
// it is handled fail-closed regardless. usesBodyText/usesBodyJSON record whether the
// expression references the body fields, so the (potentially expensive) body
// projection is done only when a filter actually needs it.
type filterEntry struct {
	expr         string
	prog         *vm.Program
	err          error
	usesBodyText bool
	usesBodyJSON bool
}

// compiledFilter returns the cached compiled program for a subscription, recompiling
// when the stored expression changed (a re-subscribe). Concurrency-safe.
func (e *Engine) compiledFilter(sub, exprStr string) *filterEntry {
	e.filterMu.Lock()
	defer e.filterMu.Unlock()
	if ent, ok := e.filterCache[sub]; ok && ent.expr == exprStr {
		return ent
	}
	prog, err := compileFilter(exprStr)
	ent := &filterEntry{
		expr:         exprStr,
		prog:         prog,
		err:          err,
		usesBodyText: referencesVar(prog, "body_text"),
		usesBodyJSON: referencesVar(prog, "body_json"),
	}
	e.filterCache[sub] = ent
	return ent
}

// invalidateFilter drops a subscription's cached program, called on Subscribe so a
// changed filter is recompiled on the next publish.
func (e *Engine) invalidateFilter(sub string) {
	e.filterMu.Lock()
	delete(e.filterCache, sub)
	e.filterMu.Unlock()
}

// filterAccepts reports whether a fan-out target accepts a message. Plain queues and
// empty-filter subscriptions always accept; otherwise the compiled filter is run
// against the message env. Fail-closed: a stored-filter compile error or an eval
// failure means "do not route to this subscription" (logged) — never a crash, never
// a silent match. enqueuedAtMs is the publish time; visibleAtMs is the scheduled time
// (equal to enqueuedAtMs for an immediate send).
func (e *Engine) filterAccepts(t target, m OutMessage, enqueuedAtMs, visibleAtMs int64) bool {
	if t.entry == nil {
		return true // plain queue or no filter
	}
	if t.entry.err != nil {
		e.log.Error("subscription filter failed to compile; routing skipped (fail-closed)",
			"subscription", t.name, "error", t.entry.err)
		return false
	}
	if t.entry.prog == nil {
		return true // empty filter -> match all
	}
	env := buildFilterEnv(m, enqueuedAtMs, visibleAtMs)
	// Project the body only when this filter references it (otherwise body_text stays
	// "" and body_json stays {} — never read, so the value is irrelevant).
	if t.entry.usesBodyText {
		env.BodyText = string(m.Body)
	}
	if t.entry.usesBodyJSON {
		env.BodyJSON = decodeBodyJSON(m)
	}
	ok, err := evalFilter(t.entry.prog, env)
	if err != nil {
		e.log.Error("subscription filter evaluation failed; routing skipped (fail-closed)",
			"subscription", t.name, "expr", t.entry.expr, "error", err)
		return false
	}
	return ok
}

// compileFilter type-checks and compiles a filter expression. An empty expression
// means "match all" (a nil program). A bad expression returns ErrInvalidFilter
// wrapping the precise compiler error (position + available names/functions).
func compileFilter(source string) (*vm.Program, error) {
	if strings.TrimSpace(source) == "" {
		return nil, nil // match-all
	}
	if len(source) > maxFilterSourceLen {
		return nil, fmt.Errorf("%w: expression too long (%d > %d bytes)",
			ErrInvalidFilter, len(source), maxFilterSourceLen)
	}
	prog, err := expr.Compile(source, filterOptions()...)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrInvalidFilter, err.Error())
	}
	return prog, nil
}

// evalFilter runs a compiled filter against an env. It is FAIL-CLOSED: a runtime
// error, a non-bool result, or a panic all yield (false, err) — the caller treats
// that as "do not route to this subscription" and logs it. Never crashes the host,
// never silently matches.
func evalFilter(prog *vm.Program, env filterEnv) (matched bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			matched, err = false, fmt.Errorf("filter panic: %v", r)
		}
	}()
	out, runErr := expr.Run(prog, env)
	if runErr != nil {
		return false, runErr
	}
	b, ok := out.(bool)
	if !ok {
		return false, fmt.Errorf("filter returned %T, want bool", out)
	}
	return b, nil
}

// buildFilterEnv projects a message + its timestamps into the filter env. Properties
// is never nil (so `properties[...]` / `... in properties` are safe), and the derived
// fields are computed here. The body fields are left at their safe non-nil zero values
// (`""` / `{}`); the caller (filterAccepts) fills them only when the filter uses them.
func buildFilterEnv(m OutMessage, enqueuedAtMs, visibleAtMs int64) filterEnv {
	props := m.Properties
	if props == nil {
		props = map[string]string{}
	}
	return filterEnv{
		Subject:       m.Subject,
		Properties:    props,
		GroupID:       m.GroupID,
		CorrelationID: m.CorrelationID,
		ReplyTo:       m.ReplyTo,
		MessageID:     m.MessageID,
		ContentType:   m.ContentType,
		EnqueuedAt:    time.UnixMilli(enqueuedAtMs).UTC(),
		VisibleAt:     time.UnixMilli(visibleAtMs).UTC(),
		SubjectParts:  subjectParts(m.Subject),
		BodySize:      len(m.Body),
		PropertyKeys:  propertyKeys(props),
		BodyJSON:      map[string]any{}, // never nil; replaced with the decoded body if used
	}
}

// referencesVar reports whether a compiled program references the named variable
// (used to decide whether to project the body fields). A nil program references
// nothing.
func referencesVar(prog *vm.Program, name string) bool {
	if prog == nil {
		return false
	}
	found := false
	node := prog.Node()
	ast.Walk(&node, exprVisitor(func(n *ast.Node) {
		if id, ok := (*n).(*ast.IdentifierNode); ok && id.Value == name {
			found = true
		}
	}))
	return found
}

// exprVisitor adapts a func to expr's ast.Visitor.
type exprVisitor func(*ast.Node)

func (v exprVisitor) Visit(n *ast.Node) { v(n) }

// decodeBodyJSON decodes a message body into a JSON object for `body_json`. It is
// total-valued: a non-JSON content_type, an empty/invalid body, or a non-object JSON
// (array/scalar) all yield an empty map ({}), never nil — so `body_json` is always a
// map and only reaching into an absent *field* yields nil (handled fail-closed).
func decodeBodyJSON(m OutMessage) map[string]any {
	if len(m.Body) == 0 || !isJSONContentType(m.ContentType) {
		return map[string]any{}
	}
	var v map[string]any
	if err := json.Unmarshal(m.Body, &v); err != nil || v == nil {
		return map[string]any{}
	}
	return v
}

// isJSONContentType decides whether to attempt JSON decoding: a content_type that
// looks like JSON (e.g. application/json, application/vnd.api+json), or an unset
// content_type (best-effort). An explicit non-JSON type (text/plain, …) is respected
// — the body is not parsed and `body_json` stays {}.
func isJSONContentType(ct string) bool {
	return ct == "" || strings.Contains(strings.ToLower(ct), "json")
}

// subjectParts splits a dotted subject into its hierarchy, MQTT-style. An empty
// subject yields an empty slice (not [""]) so `len(subject_parts) == 0` is clean.
func subjectParts(subject string) []string {
	if subject == "" {
		return []string{}
	}
	return strings.Split(subject, ".")
}

// propertyKeys returns the custom-property names, sorted for determinism.
func propertyKeys(props map[string]string) []string {
	keys := make([]string, 0, len(props))
	for k := range props {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// toInt coerces an expr numeric argument to int. The type hints constrain the
// helpers to int, so this is just defensive.
func toInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	default:
		return 0
	}
}

// durSeg matches one leading <number><unit> segment, extending Go's units with
// d (day) and w (week). Longer-spelled units precede their suffixes so e.g. "ms"
// is not mis-read as "m"+"s".
var durSeg = regexp.MustCompile(`^([0-9]*\.?[0-9]+)(ns|us|µs|ms|s|m|h|d|w)`)

var unitDur = map[string]time.Duration{
	"ns": time.Nanosecond, "us": time.Microsecond, "µs": time.Microsecond,
	"ms": time.Millisecond, "s": time.Second, "m": time.Minute, "h": time.Hour,
	"d": 24 * time.Hour, "w": 7 * 24 * time.Hour,
}

// parseDuration is duration() extended with d (=24h) and w (=7d) on top of Go's
// time.ParseDuration units (ns/us/µs/ms/s/m/h). It accepts composites ("1d12h"),
// fractions ("1.5d"), and a leading sign. Stops at day/week — no calendar months
// or years (ambiguous as fixed durations).
func parseDuration(s string) (time.Duration, error) {
	orig := s
	neg := false
	switch {
	case strings.HasPrefix(s, "-"):
		neg, s = true, s[1:]
	case strings.HasPrefix(s, "+"):
		s = s[1:]
	}
	if s == "0" {
		return 0, nil
	}
	if s == "" {
		return 0, fmt.Errorf("invalid duration %q", orig)
	}
	var total time.Duration
	for s != "" {
		m := durSeg.FindStringSubmatch(s)
		if m == nil {
			return 0, fmt.Errorf("invalid duration %q", orig)
		}
		f, err := strconv.ParseFloat(m[1], 64)
		if err != nil {
			return 0, fmt.Errorf("invalid duration %q: %w", orig, err)
		}
		total += time.Duration(f * float64(unitDur[m[2]]))
		s = s[len(m[0]):]
	}
	if neg {
		total = -total
	}
	return total, nil
}

// FilterTestResult is a dry-run of a filter expression (TestFilter): whether it
// compiles and, if a sample message is given, whether it would match.
type FilterTestResult struct {
	Valid   bool   // the expression compiled
	Error   string // compile error, or a runtime/eval error when Ran
	Ran     bool   // a sample message was evaluated
	Matched bool   // the sample matched (meaningful only when Ran && Valid && Error == "")
}

// TestFilter compiles a filter expression and, when sample != nil, evaluates it against
// that message exactly as publish-time fan-out would (same env, body-field gating, and
// fail-closed handling). It is pure and read-only — nothing is enqueued — so a console
// or `mqlite expr` REPL can validate / test an expression before it is used (MQLITE-17).
func TestFilter(expr string, sample *OutMessage, enqueuedAtMs, visibleAtMs int64) FilterTestResult {
	prog, err := compileFilter(expr)
	if err != nil {
		return FilterTestResult{Error: err.Error()}
	}
	res := FilterTestResult{Valid: true}
	if sample == nil {
		return res
	}
	res.Ran = true
	if prog == nil {
		res.Matched = true // an empty expression matches every message
		return res
	}
	env := buildFilterEnv(*sample, enqueuedAtMs, visibleAtMs)
	if referencesVar(prog, "body_text") {
		env.BodyText = string(sample.Body)
	}
	if referencesVar(prog, "body_json") {
		env.BodyJSON = decodeBodyJSON(*sample)
	}
	matched, evalErr := evalFilter(prog, env)
	res.Matched = matched
	if evalErr != nil {
		res.Error = evalErr.Error()
	}
	return res
}
